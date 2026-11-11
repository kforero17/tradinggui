"""Metrics refresh for the scheduled jobs.

Technicals for every ticker come from the batched bar cache (``refresh_histories``),
computed and stored one batch at a time so only a batch of histories is in memory.
Fundamentals (yfinance ``.info``) are then fetched only for holdings and for the
liquid stocks whose last fetch is older than FUNDAMENTALS_MAX_AGE_DAYS, at most
FUNDAMENTALS_MAX_PER_RUN of them (plus any holdings beyond that), so the slow,
rate-limited calls rotate through the universe instead of running every week.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from time import perf_counter

import pandas as pd
from loguru import logger

from ..analysis import metrics
from ..config.settings import settings
from ..data.database import db
from ..data.price_history import last_completed_session, refresh_histories
from ..data.ticker_loader import TickerInfo, load_universe

# Fundamentals are stored in chunks so a crash keeps what was already fetched
FUNDAMENTALS_STORE_CHUNK = 100
# Bars ending this many calendar days before the last completed session are stale:
# a delisted ticker keeps its old cache, which would otherwise look current
MAX_BAR_AGE_DAYS = 10


@dataclass(frozen=True)
class MetricsRefreshStats:
    """Counts and per-stage wall-clock seconds of one ``refresh_metrics`` run."""

    tickers: int  # processed, after the skip list
    skipped: int  # left out because they kept failing
    batches: int
    downloaded: int  # tickers whose bar cache was updated from a download
    computed: int  # technical rows stored
    failed: int  # no bars, stale bars or too little history
    stale: int  # of the failed, those whose last bar is older than MAX_BAR_AGE_DAYS
    liquid: int
    fundamentals_due: int  # selected for this run, after FUNDAMENTALS_MAX_PER_RUN
    fundamentals_fetched: int
    fundamentals_failed: int
    download_seconds: float
    compute_seconds: float
    fundamentals_seconds: float
    store_seconds: float
    total_seconds: float


def _asset_type(info: TickerInfo) -> str:
    return "etf" if info.is_etf else "stock"


def _is_liquid(row: dict) -> bool:
    dollar_volume = row.get("avg_dollar_volume_50d")
    return (
        row["last_price"] >= settings.UNIVERSE_MIN_PRICE
        and dollar_volume is not None
        and dollar_volume >= settings.UNIVERSE_MIN_DOLLAR_VOLUME
    )


def refresh_metrics(
    tickers: list[str] | None = None,
    *,
    batch_size: int = 500,
    fetch_fundamentals: bool = True,
) -> MetricsRefreshStats:
    """Refresh technicals for ``tickers`` (default: the universe plus holdings), then
    the fundamentals that are due.

    Tickers on the skip list are left out unless held. Technical rows carry no
    fundamentals keys, so the stored fundamentals are kept. Each batch's outcome is
    recorded in ``ticker_status``: a ticker succeeds when it has current bars and
    its metrics computed. With an explicit ``tickers`` list, only the holdings in it
    get their fundamentals forced.
    """
    started = perf_counter()
    universe = load_universe()
    holdings = {t.strip().upper() for t in db.get_portfolio_tickers()}
    if tickers is None:
        # Holdings missing from the listings still need fresh metrics
        requested = list(dict.fromkeys([*universe, *sorted(holdings)]))
    else:
        requested = list(dict.fromkeys(t.strip().upper() for t in tickers))
        holdings &= set(requested)
    skip = db.get_skipped_tickers() - holdings
    run = [t for t in requested if t not in skip]
    oldest_bar = pd.Timestamp(
        last_completed_session() - timedelta(days=MAX_BAR_AGE_DAYS)
    )
    total_batches = (len(run) + batch_size - 1) // batch_size
    logger.info(
        f"Refreshing metrics for {len(run)} tickers in {total_batches} batches "
        f"({len(requested) - len(run)} skipped, {len(holdings)} held)"
    )

    download_s = compute_s = fundamentals_s = store_s = 0.0
    downloaded = computed = failed_count = stale = 0
    # Liquid tickers from this run -> whether each is an ETF
    liquid: dict[str, bool] = {}

    for number, offset in enumerate(range(0, len(run), batch_size), 1):
        batch = run[offset : offset + batch_size]

        t0 = perf_counter()
        result = refresh_histories(batch)
        t1 = perf_counter()

        rows: list[dict] = []
        failed = list(result.failed)
        for ticker in batch:
            bars = result.histories.get(ticker)
            if bars is None:
                continue  # already in result.failed
            if bars.index[-1] < oldest_bar:
                logger.warning(
                    f"Bars for {ticker} end {bars.index[-1].date()}; treating as failed"
                )
                stale += 1
                failed.append(ticker)
                continue
            try:
                row = metrics.compute_technical_metrics(ticker, bars)
            except Exception:
                logger.exception(f"Could not compute metrics for {ticker}")
                row = None
            if row is None:
                failed.append(ticker)
                continue
            info = universe.get(ticker)
            if info is not None:
                row["asset_type"] = _asset_type(info)
            rows.append(row)
            if _is_liquid(row):
                liquid[ticker] = info is not None and info.is_etf
        t2 = perf_counter()

        if rows:
            db.store_metrics(rows)
        succeeded = [row["ticker"] for row in rows]
        if succeeded or failed:
            db.record_ticker_results(succeeded, failed)
        t3 = perf_counter()

        download_s += t1 - t0
        compute_s += t2 - t1
        store_s += t3 - t2
        downloaded += result.downloaded
        computed += len(rows)
        failed_count += len(failed)
        logger.info(
            f"Metrics batch {number}/{total_batches}: {len(rows)} stored, "
            f"{len(failed)} failed, {result.downloaded} downloaded | "
            f"download {t1 - t0:.1f}s, compute {t2 - t1:.1f}s, store {t3 - t2:.1f}s"
        )

    due: list[str] = []
    fetched = fundamentals_failed = 0
    if fetch_fundamentals:
        due = metrics.select_fundamentals_due(
            liquid,
            db.get_fundamentals_ages(),
            holdings,
            settings.FUNDAMENTALS_MAX_AGE_DAYS,
            datetime.utcnow(),
        )
        # Holdings come first in ``due``, so the slice keeps all of them; the
        # deferred, oldest-first tail is picked up by the next runs
        cap = max(settings.FUNDAMENTALS_MAX_PER_RUN, len(holdings))
        deferred = max(len(due) - cap, 0)
        due = due[:cap]
        logger.info(
            f"Fetching fundamentals for {len(due)} tickers "
            f"({len(liquid)} liquid, {len(holdings)} held); {deferred} deferred to "
            f"later runs by the cap of {settings.FUNDAMENTALS_MAX_PER_RUN} per run"
        )
        pending: list[dict] = []
        for i, ticker in enumerate(due, 1):
            t0 = perf_counter()
            fundamentals = metrics.fetch_fundamentals(ticker)
            fundamentals_s += perf_counter() - t0
            if fundamentals is None:
                fundamentals_failed += 1
            else:
                fetched += 1
                pending.append({"ticker": ticker, **fundamentals})
            if pending and (len(pending) >= FUNDAMENTALS_STORE_CHUNK or i == len(due)):
                t0 = perf_counter()
                db.store_metrics(pending)
                store_s += perf_counter() - t0
                pending = []
                logger.info(
                    f"Fundamentals {i}/{len(due)}: {fetched} fetched, "
                    f"{fundamentals_failed} failed, {fundamentals_s:.1f}s so far"
                )

    stats = MetricsRefreshStats(
        tickers=len(run),
        skipped=len(requested) - len(run),
        batches=total_batches,
        downloaded=downloaded,
        computed=computed,
        failed=failed_count,
        stale=stale,
        liquid=len(liquid),
        fundamentals_due=len(due),
        fundamentals_fetched=fetched,
        fundamentals_failed=fundamentals_failed,
        download_seconds=round(download_s, 2),
        compute_seconds=round(compute_s, 2),
        fundamentals_seconds=round(fundamentals_s, 2),
        store_seconds=round(store_s, 2),
        total_seconds=round(perf_counter() - started, 2),
    )
    logger.info(
        f"Metrics refresh done: {stats.computed}/{stats.tickers} computed, "
        f"{stats.failed} failed ({stats.stale} stale), {stats.skipped} skipped, "
        f"{stats.liquid} liquid, fundamentals {stats.fundamentals_fetched}/"
        f"{stats.fundamentals_due} | download {stats.download_seconds:.1f}s, "
        f"compute {stats.compute_seconds:.1f}s, "
        f"fundamentals {stats.fundamentals_seconds:.1f}s, "
        f"store {stats.store_seconds:.1f}s, total {stats.total_seconds:.1f}s"
    )
    return stats
