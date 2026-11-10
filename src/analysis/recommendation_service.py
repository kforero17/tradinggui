"""Build a recommendation run from stored accounts, holdings and cached bars; diff and persist it.

``generate_run`` is what the weekly, daily and manual jobs call. The weekly
job scans the liquid universe for entries (``include_entries=True``); the
daily job and the dashboard's "Recheck holdings" re-evaluate positions and
re-scan only the ENTER/WATCH list of the latest entries run, so a WATCH that
triggers mid-week shows up as ENTER the same evening.
``latest_dashboard`` reads the stored runs back for the Today page: position
actions from the latest run of any job, entries from the latest run that
scanned for them, each superseded by its newest re-scan.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from time import perf_counter
from typing import Any

import pandas as pd
from loguru import logger

from ..config.settings import settings
from ..data.broker_import import apply_splits, match_round_trips
from ..data.database import db
from ..data.price_history import load_cached_histories, refresh_histories
from ..data.ticker_loader import TickerInfo, load_universe
from .recommendations import (
    ACTION_PRIORITY,
    AccountInfo,
    AccountPosition,
    RecommendationAction,
    TrendFeatures,
    TrendScore,
    build_recommendations,
    close_above_sma,
    entry_action,
    market_regime,
    trend_features,
    trend_scores,
)
from .signal_history import diff_runs, expired_signals, is_entry_row, to_rows
from .turtle import (
    BreakoutSignal,
    Direction,
    TurtleConfig,
    TurtleSystem,
    correlation_matrix,
    direction_of,
    evaluate_position,
    scan_breakouts,
)
from .universe import liquid_universe, with_required

__all__ = [
    "DEFAULT_TREND_EQUITY",
    "ENTRY_SCAN_BATCH_SIZE",
    "ENTRY_CORRELATION_CANDIDATES",
    "turtle_config",
    "generate_run",
    "latest_dashboard",
    "summary_counts",
    "heat",
]

# Copied from app.py so sizing matches the Trend page when no account equity is stored.
DEFAULT_TREND_EQUITY = 100_000.0
# About 140 KB of bars per ticker, so ~70 MB per batch.
ENTRY_SCAN_BATCH_SIZE = 500
# Correlations cover holdings plus this many top-scored entry candidates.
ENTRY_CORRELATION_CANDIDATES = 50

EQUITY_SOURCE_ACCOUNTS = "accounts"
EQUITY_SOURCE_DEFAULT = "default"
# Jobs that don't scan the universe but re-scan the latest entries run's ENTER/WATCH list.
RESCAN_JOBS = frozenset({"daily", "manual"})


def turtle_config() -> TurtleConfig:
    """The configured system (``TREND_SYSTEM``: S1 or S2) sized by ``TREND_RISK_PER_UNIT`` and
    ``TREND_MAX_UNIT_FRACTION``."""
    system = settings.TREND_SYSTEM.strip().upper()
    overrides = {
        "risk_per_unit": settings.TREND_RISK_PER_UNIT,
        "max_unit_fraction": settings.TREND_MAX_UNIT_FRACTION,
    }
    if system == TurtleSystem.SYSTEM_1.value:
        return TurtleConfig.system_1(**overrides)
    if system == TurtleSystem.SYSTEM_2.value:
        return TurtleConfig.system_2(**overrides)
    raise ValueError(f"TREND_SYSTEM must be S1 or S2, not {settings.TREND_SYSTEM!r}")


@dataclass(frozen=True)
class _Holding:
    account: str
    ticker: str
    shares: float
    entry_price: float
    added_at: datetime | None

    @property
    def direction(self) -> Direction:
        return direction_of(self.shares)


@dataclass
class _Scan:
    signals: list[BreakoutSignal] = field(default_factory=list)
    features: dict[tuple[str, Direction], TrendFeatures] = field(default_factory=dict)
    above_200d: list[bool] = field(default_factory=list)
    scanned: int = 0
    missing: int = 0


def _chunks(items: Sequence[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(items), size):
        yield list(items[start : start + size])


def _holdings(skipped: list[dict[str, str]]) -> list[_Holding]:
    holdings: list[_Holding] = []
    for row in db.get_portfolio_with_shares():
        ticker, shares, cost = row["ticker"].upper(), row["shares"], row["cost_basis"]
        if not shares or cost <= 0:
            reason = "zero shares" if not shares else "missing cost basis"
            logger.warning(f"Skipping {row['account']}/{ticker}: {reason}")
            skipped.append(
                {"account": row["account"], "ticker": ticker, "reason": reason}
            )
            continue
        holdings.append(
            _Holding(
                row["account"], ticker, shares, cost / abs(shares), row["added_at"]
            )
        )
    return holdings


def _load_histories(tickers: Iterable[str], refresh: bool) -> dict[str, pd.DataFrame]:
    """Refreshed bars when ``refresh``; otherwise the cache, downloading only what it lacks."""
    wanted = list(dict.fromkeys(t.upper() for t in tickers))
    if not wanted:
        return {}
    if refresh:
        return refresh_histories(wanted).histories
    histories = load_cached_histories(wanted)
    missing = [t for t in wanted if t not in histories]
    if missing:
        logger.info(f"{len(missing)} histories not cached; downloading them")
        histories.update(refresh_histories(missing).histories)
    return histories


def _splits(ohlc: pd.DataFrame) -> list[tuple[date, float]]:
    if "stock_splits" not in ohlc:
        return []
    splits = ohlc.loc[ohlc["stock_splits"] > 0, "stock_splits"]
    return [(when.date(), float(ratio)) for when, ratio in splits.items()]


def _broker_entry_dates(
    holdings: Sequence[_Holding], histories: Mapping[str, pd.DataFrame]
) -> dict[tuple[str, str, Direction], date]:
    """Earliest open-lot date per (account, symbol, direction) from FIFO-matched broker fills."""
    held = {(h.account, h.ticker) for h in holdings}
    fills = [f for f in db.get_broker_fills() if (f.account, f.symbol.upper()) in held]
    if not fills:
        return {}
    splits = {
        s: _splits(histories[s]) for s in {f.symbol for f in fills} if s in histories
    }
    _, open_positions = match_round_trips(apply_splits(fills, splits))
    return {
        (o.account, o.symbol.upper(), Direction[o.direction.name]): o.entry_date
        for o in open_positions
    }


def _evaluate_positions(
    holdings: Sequence[_Holding],
    histories: Mapping[str, pd.DataFrame],
    equity: float,
    config: TurtleConfig,
    now: datetime,
    skipped: list[dict[str, str]],
) -> list[AccountPosition]:
    entry_dates = _broker_entry_dates(holdings, histories)
    evaluated: list[AccountPosition] = []
    for h in holdings:
        ohlc = histories.get(h.ticker)
        if ohlc is None:
            logger.warning(f"No price history for held {h.account}/{h.ticker}")
            skipped.append(
                {"account": h.account, "ticker": h.ticker, "reason": "no price history"}
            )
            continue
        entry_date = entry_dates.get((h.account, h.ticker, h.direction)) or h.added_at
        if entry_date is None:
            logger.warning(f"No entry date for {h.account}/{h.ticker}; using today")
            entry_date = now
        try:
            status = evaluate_position(
                h.ticker, ohlc, h.shares, h.entry_price, entry_date, equity, config
            )
        except (IndexError, KeyError, ValueError) as e:
            logger.exception(f"Could not evaluate {h.account}/{h.ticker}")
            skipped.append({"account": h.account, "ticker": h.ticker, "reason": str(e)})
            continue
        evaluated.append(AccountPosition(h.account, status))
    return evaluated


def _scan_universe(
    universe: Sequence[str],
    preloaded: Mapping[str, pd.DataFrame],
    equity: float,
    config: TurtleConfig,
    directions: tuple[Direction, ...],
    refresh: bool,
) -> _Scan:
    """Breakouts, trend features and 200-day breadth over the universe, ``ENTRY_SCAN_BATCH_SIZE`` at a time.

    Universe bars come from the cache (or a refresh when ``refresh``); a ticker
    without cached bars is counted as missing, not downloaded.
    """
    scan = _Scan()
    min_bars = config.entry_period + config.n_period
    for batch in _chunks(universe, ENTRY_SCAN_BATCH_SIZE):
        histories = {t: preloaded[t] for t in batch if t in preloaded}
        rest = [t for t in batch if t not in histories]
        if rest:
            loaded = (
                refresh_histories(rest).histories
                if refresh
                else load_cached_histories(rest)
            )
            histories.update(loaded)
        scan.missing += len(batch) - len(histories)
        usable = {t: h for t, h in histories.items() if len(h) >= min_bars}
        scan.scanned += len(usable)
        scan.signals.extend(scan_breakouts(usable, equity, config, directions))
        for ticker, ohlc in usable.items():
            for direction in directions:
                scan.features[(ticker, direction)] = trend_features(
                    ohlc, direction, ticker
                )
            above = close_above_sma(ohlc)
            if above is not None:
                scan.above_200d.append(above)
    return scan


def _correlations(
    holdings: Sequence[_Holding],
    histories: Mapping[str, pd.DataFrame],
    signals: Sequence[BreakoutSignal],
    scores: Mapping[tuple[str, Direction], TrendScore],
) -> pd.DataFrame:
    """Return correlations over holdings plus the top-scored entry candidates."""
    held = {h.ticker for h in holdings}
    best: dict[str, float] = {}
    for s in signals:
        if s.ticker in held or s.skipped_after_winner or entry_action(s) is None:
            continue
        score = scores.get((s.ticker, s.direction))
        best[s.ticker] = max(best.get(s.ticker, -1.0), score.score if score else -1.0)
    top = sorted(best, key=lambda t: (-best[t], t))[:ENTRY_CORRELATION_CANDIDATES]
    selected = {t: histories[t] for t in held if t in histories}
    need = [t for t in top if t not in selected]
    selected.update({t: h for t, h in histories.items() if t in need})
    selected.update(load_cached_histories([t for t in need if t not in selected]))
    if not selected:
        return pd.DataFrame()
    return correlation_matrix(selected)


def _asset_types(
    metrics: pd.DataFrame, universe: Mapping[str, TickerInfo]
) -> dict[str, str]:
    types = {s: "etf" if info.is_etf else "stock" for s, info in universe.items()}
    if {"ticker", "asset_type"} <= set(metrics.columns):
        for ticker, asset_type in zip(metrics["ticker"], metrics["asset_type"]):
            if isinstance(asset_type, str) and asset_type:
                types[str(ticker).upper()] = asset_type
    return types


def _includes_entries(run: Mapping[str, Any]) -> bool:
    # Runs stored before ``include_entries`` was recorded: only weekly runs scanned.
    return bool(run["config"].get("include_entries", run["job"] == "weekly"))


def _latest_entries_run(before_id: int | None = None) -> dict | None:
    """The newest run that scanned for entries, optionally older than ``before_id``."""
    run = db.get_latest_run() if before_id is None else db.get_previous_run(before_id)
    while run is not None and not _includes_entries(run):
        run = db.get_previous_run(run["id"])
    return run


def _runs_since(run_id: int) -> list[dict]:
    """Runs newer than ``run_id``, oldest first."""
    runs: list[dict] = []
    run = db.get_latest_run()
    while run is not None and run["id"] > run_id:
        runs.append(run)
        run = db.get_previous_run(run["id"])
    return runs[::-1]


def _comparison_rows(include_entries: bool, rescan: bool = False) -> list[dict] | None:
    """What a new run is diffed against; None before the first run.

    Position actions compare with the latest run of any job. Entry signals
    compare with the latest run that scanned for entries, so a weekly run isn't
    reset by the daily runs in between. A re-scan (``rescan``) also sees the
    entry rows of the runs since then; the entries run comes first, so its
    ``first_seen_at`` wins.
    """
    latest = db.get_latest_run()
    if latest is None:
        return None
    rows = db.get_run_recommendations(latest["id"])
    if not include_entries and not rescan:
        return rows
    positions = [r for r in rows if not is_entry_row(r)]
    entries_run = _latest_entries_run()
    if entries_run is None:
        return positions
    runs = [entries_run, *(_runs_since(entries_run["id"]) if rescan else [])]
    entries = [
        r
        for run in runs
        for r in (
            rows if run["id"] == latest["id"] else db.get_run_recommendations(run["id"])
        )
        if is_entry_row(r)
    ]
    return positions + entries


def _rescan_targets() -> list[dict]:
    """The ENTER/WATCH rows of the latest entries run, which a daily/manual run re-scans."""
    entries_run = _latest_entries_run()
    if entries_run is None:
        return []
    return [r for r in db.get_run_recommendations(entries_run["id"]) if is_entry_row(r)]


def _target_key(row: Mapping[str, Any]) -> tuple[str, Direction]:
    return str(row["ticker"]), Direction[str(row["direction"])]


def _rescan(
    targets: Sequence[Mapping[str, Any]],
    preloaded: Mapping[str, pd.DataFrame],
    equity: float,
    config: TurtleConfig,
    directions: tuple[Direction, ...],
    refresh: bool,
) -> tuple[_Scan, dict[tuple[str, Direction], TrendScore]]:
    """Re-scan only the (ticker, direction) pairs in ``targets``.

    Scores are carried from the entries run: relative strength is a
    cross-sectional rank over the whole universe, which a handful of tickers
    can't reproduce. Breadth is left empty for the same reason.
    """
    pairs = {_target_key(r) for r in targets}
    tickers = list(dict.fromkeys(t for t, _ in pairs))
    scan = _scan_universe(tickers, preloaded, equity, config, directions, refresh)
    scan.signals = [s for s in scan.signals if (s.ticker, s.direction) in pairs]
    scan.features = {k: f for k, f in scan.features.items() if k in pairs}
    scan.above_200d = []
    scores = {
        _target_key(r): TrendScore(float(r["trend_score"]), r["rs_rank"])
        for r in targets
        if r["trend_score"] is not None
    }
    return scan, scores


def _config_json(config: TurtleConfig, **details: Any) -> dict[str, Any]:
    return {
        "system": config.system.value,
        "entry_period": config.entry_period,
        "exit_period": config.exit_period,
        "n_period": config.n_period,
        "stop_n": config.stop_n,
        "risk_per_unit": config.risk_per_unit,
        "max_unit_fraction": config.max_unit_fraction,
        "use_trend_filter": config.use_trend_filter,
        **details,
    }


def generate_run(
    job: str,
    *,
    include_entries: bool,
    tickers: list[str] | None = None,
    refresh: bool = False,
    now: datetime | None = None,
) -> int:
    """Evaluate holdings (and, with ``include_entries``, scan for entries), then store the run.

    ``job`` is 'weekly', 'daily' or 'manual'. ``tickers`` replaces the liquid
    universe for the entry scan (smoke runs); holdings and the watchlist are
    always added. ``refresh`` downloads fresh bars instead of reading the
    cache. ``now`` (naive UTC) stamps newly seen signals. Returns the run id.
    """
    started = perf_counter()
    now = now or datetime.utcnow()
    timings: dict[str, float] = {}

    def lap(stage: str, since: float) -> float:
        timings[stage] = round(perf_counter() - since, 3)
        return perf_counter()

    config = turtle_config()
    max_heat_pct = settings.MAX_PORTFOLIO_HEAT_PCT
    accounts = [AccountInfo.from_row(a) for a in db.get_accounts()]
    equity = db.total_equity()
    equity_source = EQUITY_SOURCE_ACCOUNTS
    if equity <= 0:
        logger.warning(
            f"No account equity stored; sizing from the default ${DEFAULT_TREND_EQUITY:,.0f}"
        )
        equity, equity_source = DEFAULT_TREND_EQUITY, EQUITY_SOURCE_DEFAULT

    skipped: list[dict[str, str]] = []
    holdings = _holdings(skipped)
    watchlist = [t.upper() for t in db.get_watchlist()]
    benchmark = settings.REGIME_BENCHMARK.upper()
    held_tickers = list(dict.fromkeys(h.ticker for h in holdings))
    stage = perf_counter()
    histories = _load_histories([*held_tickers, *watchlist, benchmark], refresh)
    stage = lap("load_holdings_bars", stage)

    positions = _evaluate_positions(holdings, histories, equity, config, now, skipped)
    stage = lap("evaluate_positions", stage)

    metrics = db.get_latest_metrics()
    directions: tuple[Direction, ...] = (Direction.LONG,)
    scan = _Scan()
    universe_size = 0
    asset_types: dict[str, str]
    scores: dict[tuple[str, Direction], TrendScore] | None = None
    rescan_targets = (
        _rescan_targets() if not include_entries and job in RESCAN_JOBS else []
    )
    if include_entries:
        info = load_universe()
        asset_types = _asset_types(metrics, info)
        if any(a.allow_short for a in accounts):
            directions = (Direction.LONG, Direction.SHORT)
        base = (
            [t.upper() for t in tickers]
            if tickers is not None
            else liquid_universe(
                metrics,
                info,
                min_price=settings.UNIVERSE_MIN_PRICE,
                min_dollar_volume=settings.UNIVERSE_MIN_DOLLAR_VOLUME,
                exclude_leveraged=settings.UNIVERSE_EXCLUDE_LEVERAGED_ETFS,
            )
        )
        universe = with_required(base, held_tickers, watchlist)
        universe_size = len(universe)
        scan = _scan_universe(universe, histories, equity, config, directions, refresh)
        stage = lap("scan_universe", stage)
    else:
        asset_types = _asset_types(metrics, {})
        for r in rescan_targets:
            if r["asset_type"]:
                asset_types.setdefault(str(r["ticker"]), str(r["asset_type"]))
        if rescan_targets:
            wanted = {d for _, d in map(_target_key, rescan_targets)}
            directions = tuple(d for d in Direction if d in wanted)
            scan, scores = _rescan(
                rescan_targets, histories, equity, config, directions, refresh
            )
            universe_size = len({r["ticker"] for r in rescan_targets})
            stage = lap("rescan_entries", stage)

    if scores is None:
        scores = trend_scores(scan.features)
    correlations = _correlations(holdings, histories, scan.signals, scores)
    stage = lap("scores_and_correlations", stage)

    held_directions: dict[str, set[Direction]] = {}
    for h in holdings:
        held_directions.setdefault(h.ticker, set()).add(h.direction)
    recommendations = build_recommendations(
        position_statuses=positions,
        breakout_signals=scan.signals,
        accounts=accounts,
        total_equity=equity,
        config=config,
        max_heat_pct=max_heat_pct,
        scores=scores,
        features=scan.features,
        open_positions_by_ticker=held_directions,
        correlations=correlations,
        asset_types=asset_types,
    )
    breadth = sum(scan.above_200d) / len(scan.above_200d) if scan.above_200d else None
    regime = market_regime(histories.get(benchmark), breadth, benchmark=benchmark)
    stage = lap("build", stage)

    diffed, expired = diff_runs(
        _comparison_rows(include_entries, rescan=bool(rescan_targets)),
        recommendations,
        now=now,
        include_entries=include_entries,
    )
    rows = to_rows(diffed)
    timings["total"] = round(perf_counter() - started, 3)
    run_id = db.create_recommendation_run(
        job,
        equity=equity,
        config=_config_json(
            config,
            max_heat_pct=max_heat_pct,
            equity_source=equity_source,
            include_entries=include_entries,
            directions=[d.name for d in directions],
            universe_size=universe_size,
            rescanned_entries=len(rescan_targets),
            scanned=scan.scanned,
            missing_histories=scan.missing,
            breadth_tickers=len(scan.above_200d),
            skipped_positions=skipped,
            expired_count=len(expired),
            timings_s=timings,
        ),
        regime=regime,
        rows=rows,
    )
    counts = summary_counts(rows, expired)
    logger.info(
        f"Recommendation run {run_id} ({job}): {counts['by_action']}, "
        f"{counts['new']} new, {len(expired)} expired, scanned {scan.scanned} "
        f"of {universe_size} in {perf_counter() - started:.1f}s; timings {timings}"
    )
    return run_id


def _rows_by_action(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts = {a.name: 0 for a in RecommendationAction}
    for row in rows:
        counts[str(row["action"])] = counts.get(str(row["action"]), 0) + 1
    return counts


def summary_counts(
    recommendations: Sequence[Mapping[str, Any]],
    expired: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Counts for the dashboard cards: per action, per direction, NEW, blocked, late, expired."""
    by_direction = {d.name: 0 for d in Direction}
    for row in recommendations:
        by_direction[str(row["direction"])] = (
            by_direction.get(str(row["direction"]), 0) + 1
        )
    return {
        "total": len(recommendations),
        "by_action": _rows_by_action(recommendations),
        "by_direction": by_direction,
        "new": sum(1 for r in recommendations if r["status"] == "NEW"),
        "blocked": sum(1 for r in recommendations if r["blocked_by"]),
        "late": sum(1 for r in recommendations if r["is_late"]),
        "expired": len(expired),
    }


def _number(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def heat(
    recommendations: Sequence[Mapping[str, Any]], equity: float, max_heat_pct: float
) -> dict[str, float | None]:
    """Open risk of the held positions and the risk of unblocked adds and entries, vs the budget."""
    open_risk = sum(
        _number(r["extra"].get("open_risk_dollars", r["risk_dollars"]))
        for r in recommendations
        if not is_entry_row(r)
    )
    planned = sum(
        _number(r["risk_dollars"])
        for r in recommendations
        if r["action"]
        in (RecommendationAction.ENTER.name, RecommendationAction.ADD_UNIT.name)
        and not r["blocked_by"]
    )
    budget = max_heat_pct * equity

    def pct(dollars: float) -> float | None:
        return dollars / equity if equity else None

    return {
        "equity": equity,
        "open_risk_dollars": open_risk,
        "open_risk_pct": pct(open_risk),
        "planned_risk_dollars": planned,
        "heat_after_planned_pct": pct(open_risk + planned),
        "budget_pct": max_heat_pct,
        "budget_dollars": budget,
        "remaining_dollars": budget - open_risk - planned,
    }


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _run_summary(run: Mapping[str, Any]) -> dict[str, Any]:
    return {k: run[k] for k in ("id", "run_at", "job", "equity", "config", "regime")}


def _row_sort_key(row: Mapping[str, Any]) -> tuple[int, bool, float, str, str]:
    score = row.get("trend_score")
    return (
        ACTION_PRIORITY[RecommendationAction[row["action"]]],
        score is None,
        -(score or 0.0),
        row["ticker"],
        row.get("account") or "",
    )


def _with_decisions(rows: list[dict]) -> list[dict]:
    decisions = {
        (d["signal_key"], d["first_seen_at"]): d
        for d in db.get_signal_decisions({r["signal_key"] for r in rows})
    }
    for row in rows:
        d = decisions.get((row["signal_key"], row["first_seen_at"]))
        row["decision"] = (
            {"status": d["status"], "note": d["note"], "decided_at": d["decided_at"]}
            if d
            else None
        )
    return rows


def _cap_watch(rows: list[dict], limit: int) -> tuple[list[dict], int]:
    """Keep the first ``limit`` WATCH rows of ``rows`` (already sorted by score); return the hidden count."""
    kept: list[dict] = []
    watch = hidden = 0
    for row in rows:
        if row["action"] == RecommendationAction.WATCH.name:
            watch += 1
            if watch > limit:
                hidden += 1
                continue
        kept.append(row)
    return kept, hidden


def latest_dashboard() -> dict[str, Any]:
    """The Today page's data, JSON-ready (ISO datetimes, no NaN).

    Position actions come from the latest run of any job; ENTER/WATCH rows
    from the latest run that scanned for entries, minus tickers now held. A
    row with the same ``signal_key`` from a newer (re-scan) run supersedes the
    entries-run row, the newest winning, so a WATCH that triggered mid-week
    shows as ENTER. Only the top ``MAX_WATCH_ROWS`` WATCH rows by score are
    kept; ``summary["watch_hidden"]`` counts the rest. Expired signals compare
    each part with the run before it of the same kind. Each row and expired
    row carries its journal ``decision`` (or None), matched on
    (``signal_key``, ``first_seen_at``).
    """
    latest = db.get_latest_run()
    if latest is None:
        return {
            "run": None,
            "entries_run": None,
            "recommendations": [],
            "expired": [],
            "summary": {**summary_counts([]), "watch_hidden": 0},
            "heat": None,
        }

    latest_rows = db.get_run_recommendations(latest["id"])
    positions = [r for r in latest_rows if not is_entry_row(r)]
    previous = db.get_previous_run(latest["id"])
    expired = expired_signals(
        db.get_run_recommendations(previous["id"]) if previous else None,
        (r["signal_key"] for r in positions),
        include_entries=False,
    )

    entries_run = _latest_entries_run()
    entries: list[dict] = []
    if entries_run is not None:
        entries_rows = (
            latest_rows
            if entries_run["id"] == latest["id"]
            else db.get_run_recommendations(entries_run["id"])
        )
        run_entries = [r for r in entries_rows if is_entry_row(r)]
        current = {r["signal_key"]: r for r in run_entries}
        for run in _runs_since(entries_run["id"]):
            rows = (
                latest_rows
                if run["id"] == latest["id"]
                else db.get_run_recommendations(run["id"])
            )
            for r in rows:
                if is_entry_row(r) and r["signal_key"] in current:
                    current[r["signal_key"]] = r
        held = {r["ticker"] for r in positions}
        entries = [r for r in current.values() if r["ticker"] not in held]
        previous_entries_run = _latest_entries_run(before_id=entries_run["id"])
        if previous_entries_run is not None:
            expired += [
                r
                for r in expired_signals(
                    db.get_run_recommendations(previous_entries_run["id"]),
                    (r["signal_key"] for r in run_entries),
                )
                if is_entry_row(r)
            ]

    recommendations, watch_hidden = _cap_watch(
        sorted(positions + entries, key=_row_sort_key), settings.MAX_WATCH_ROWS
    )
    max_heat_pct = latest["config"].get("max_heat_pct", settings.MAX_PORTFOLIO_HEAT_PCT)
    return _json_ready(
        {
            "run": _run_summary(latest),
            "entries_run": _run_summary(entries_run) if entries_run else None,
            "recommendations": _with_decisions(recommendations),
            "expired": _with_decisions(expired),
            "summary": {
                **summary_counts(recommendations, expired),
                "watch_hidden": watch_hidden,
            },
            "heat": heat(recommendations, latest["equity"] or 0.0, max_heat_pct),
        }
    )
