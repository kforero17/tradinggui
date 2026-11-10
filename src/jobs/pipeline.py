"""The scheduled weekly and daily jobs.

``run_weekly``: DB backup -> universe files -> metrics (bars, technicals,
rotating fundamentals) -> alerts -> recommendations with an entry scan -> digest.
``run_daily``: holdings and the latest ENTER/WATCH list on refreshed bars ->
digest, notifying only when something needs action or holdings' bars are
stale (the stop check ran on old prices). Both hold an exclusive file lock
(``JOB_LOCK_PATH``) so they never overlap.
"""

from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from loguru import logger

from ..analysis.metrics import evaluate_alerts
from ..analysis.recommendation_service import generate_run, latest_dashboard
from ..config.settings import settings
from ..data.database import db
from ..data.ticker_loader import ticker_loader
from ..data.universe_sources import UniverseRefreshResult, refresh_universe_files
from ..notify.digest import build_digest, send_digest, stale_line, write_report
from .metrics_refresh import MetricsRefreshStats, refresh_metrics

__all__ = [
    "JobAlreadyRunning",
    "JobReport",
    "job_lock",
    "backup_database",
    "evaluate_all_alerts",
    "actionable_rows",
    "run_weekly",
    "run_daily",
]

# Position actions that always notify on a daily run.
NOTIFY_ACTIONS = frozenset({"EXIT_STOP", "EXIT_CHANNEL", "ADD_UNIT"})
# Journal statuses that mean the user already acted on a signal; no more nagging.
DECIDED_STATUSES = frozenset({"taken", "skipped"})


class JobAlreadyRunning(RuntimeError):
    """Another job holds the job lock."""


@dataclass(frozen=True)
class JobReport:
    job: str
    run_id: int
    stage_seconds: dict[str, float]
    metrics: MetricsRefreshStats | None
    universe: UniverseRefreshResult | None
    alerts_triggered: int
    report_path: Path | None
    notified: bool
    # The dashboard summary after the run (counts by action, new, blocked, ...).
    summary: dict[str, Any] = field(default_factory=dict)
    # Holdings whose bars end before the last completed session: {ticker, last_bar}.
    stale_holdings: list[dict[str, str]] = field(default_factory=list)
    # The pre-run DB backup (weekly runs only).
    backup_path: Path | None = None

    def summary_line(self) -> str:
        actions = self.summary.get("by_action", {})
        counts = ", ".join(f"{a} {n}" for a, n in actions.items() if n) or "no rows"
        stages = " ".join(f"{s}={t:.1f}s" for s, t in self.stage_seconds.items())
        stale = (
            f"STALE bars for {len(self.stale_holdings)} holdings "
            f"({', '.join(s['ticker'] for s in self.stale_holdings)}); "
            if self.stale_holdings
            else ""
        )
        return (
            f"{self.job} run {self.run_id}: {stale}{counts}; "
            f"{self.summary.get('new', 0)} new; "
            f"alerts {self.alerts_triggered}; notified {self.notified}; "
            f"stages {stages}; report {self.report_path}"
        )


def _report_dict(
    job: str,
    run_id: int | None,
    stage_seconds: Mapping[str, float],
    metrics: MetricsRefreshStats | None,
    universe: UniverseRefreshResult | None,
    alerts_triggered: int | None,
) -> dict[str, Any]:
    return {
        "job": job,
        "run_id": run_id,
        "stage_seconds": dict(stage_seconds),
        "metrics": asdict(metrics) if metrics else None,
        "universe": asdict(universe) if universe else None,
        "alerts_triggered": alerts_triggered,
    }


@contextmanager
def job_lock(path: Path | None = None) -> Iterator[None]:
    """Hold an exclusive, non-blocking ``flock`` on ``path`` (default ``JOB_LOCK_PATH``).

    Raises JobAlreadyRunning if another process (or another open of the file)
    holds it. The lock goes away with the process, so a crash leaves no stale lock.
    """
    path = Path(path or settings.JOB_LOCK_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            handle.seek(0)
            holder = handle.read().strip() or "unknown"
            raise JobAlreadyRunning(f"Job lock {path} is held by pid {holder}") from e
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(f"{os.getpid()}\n")
            handle.flush()
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _stage(stages: dict[str, float], name: str) -> Iterator[None]:
    started = perf_counter()
    logger.info(f"Stage {name} started")
    try:
        yield
    finally:
        stages[name] = round(perf_counter() - started, 3)
        logger.info(f"Stage {name} took {stages[name]:.1f}s")


def _refresh_universe() -> UniverseRefreshResult | None:
    """Refresh the universe CSVs; a failure is logged and the old files are kept."""
    try:
        result = refresh_universe_files()
    except Exception:
        logger.exception("Universe refresh failed; keeping the old files")
        return None
    if not result.ok:
        logger.warning(
            f"Universe refresh partly failed (listings: {result.listed_error}, "
            f"S&P 500: {result.sp500_error}); kept the old files"
        )
    return result


def _select_tickers(tickers: list[str] | None, limit: int | None) -> list[str] | None:
    """Explicit tickers, or the first ``limit`` of the universe, plus holdings; None for all."""
    if tickers is None and limit is None:
        return None
    if tickers is not None:
        base = [t.strip().upper() for t in tickers if t.strip()]
    else:
        base = ticker_loader.load_unique_tickers()[:limit]
    held = sorted(t.strip().upper() for t in db.get_portfolio_tickers())
    return list(dict.fromkeys([*base, *held]))


def evaluate_all_alerts() -> int:
    """Evaluate active alerts over the latest metrics; marks and returns the triggered count.

    Same rules as ``evaluate_alerts`` per ticker, but only the metrics rows of
    tickers with a pending alert are converted, as plain dicts.
    """
    active = db.get_active_alerts()
    pending = {a["ticker"] for a in active if not a["is_triggered"]}
    if not pending:
        logger.info("No pending alerts")
        return 0
    frame = db.get_latest_metrics()
    if frame.empty:
        return 0
    rows = frame.loc[frame["ticker"].isin(pending)].to_dict("records")
    triggered = [alert_id for row in rows for alert_id in evaluate_alerts(row, active)]
    if triggered:
        db.mark_alerts_triggered(triggered)
    logger.info(f"{len(triggered)} of {len(pending)} alert tickers triggered")
    return len(triggered)


def _naive_utc(value: datetime) -> datetime:
    """``value`` as the naive UTC datetime the DB stores."""
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _decided_signals(rows: list[dict]) -> set[tuple[str, datetime]]:
    """(``signal_key``, ``first_seen_at``) of the rows' signals journaled taken or skipped."""
    return {
        (d["signal_key"], _naive_utc(d["first_seen_at"]))
        for d in db.get_signal_decisions({r["signal_key"] for r in rows})
        if d["status"] in DECIDED_STATUSES
    }


def actionable_rows(run_id: int) -> list[dict]:
    """Rows of ``run_id`` worth a notification.

    Every EXIT_STOP, EXIT_CHANNEL and ADD_UNIT, plus ENTER rows that are new:
    status NEW, or not already ENTER in the run before (a WATCH that just
    triggered keeps its signal key, so it is STILL_VALID but still news).
    Rows whose signal (``signal_key``, ``first_seen_at``) is journaled taken or
    skipped are left out; undecided EXIT/ADD rows keep notifying every day.
    """
    rows = db.get_run_recommendations(run_id)
    decided = _decided_signals(rows) if rows else set()
    previous = db.get_previous_run(run_id)
    entered_before = (
        {
            r["signal_key"]
            for r in db.get_run_recommendations(previous["id"])
            if r["action"] == "ENTER"
        }
        if previous
        else set()
    )
    return [
        r
        for r in rows
        if (r["signal_key"], _naive_utc(r["first_seen_at"])) not in decided
        and (
            r["action"] in NOTIFY_ACTIONS
            or (
                r["action"] == "ENTER"
                and (r["status"] == "NEW" or r["signal_key"] not in entered_before)
            )
        )
    ]


def _stale_holdings(dashboard: Mapping[str, Any]) -> list[dict[str, str]]:
    """The run's holdings on stale bars, logged as a warning when there are any."""
    stale = list(dashboard.get("stale_holdings") or [])
    if stale:
        logger.warning(
            f"{stale_line(stale)}: "
            + ", ".join(f"{s['ticker']} (last {s['last_bar']})" for s in stale)
        )
    return stale


def backup_database(now: datetime, keep: int = settings.DB_BACKUP_KEEP) -> Path:
    """Back up the live DB to ``<db dir>/backups`` and prune all but the newest ``keep``.

    The weekly job writes thousands of rows, so it takes a restorable copy first.
    A failed backup raises: the job must not write without one.
    """
    live = db.active_path
    backup_dir = live.parent / "backups"
    backup_path = db.backup_to(
        backup_dir / f"{live.stem}-{now:%Y%m%d-%H%M%S}{live.suffix}"
    )
    # Timestamped names sort chronologically.
    backups = sorted(backup_dir.glob(f"{live.stem}-*{live.suffix}"))
    for old in backups[: max(len(backups) - keep, 0)]:
        old.unlink()
        logger.info(f"Pruned old DB backup {old.name}")
    return backup_path


def run_weekly(
    *,
    tickers: list[str] | None = None,
    limit: int | None = None,
    notify: bool = True,
) -> JobReport:
    """The full weekly job; ``tickers`` or ``limit`` make it a smoke run on a subset.

    A smoke run skips the universe-file refresh and scans only the subset (plus
    holdings and the watchlist) for entries.
    """
    with job_lock():
        stages: dict[str, float] = {}
        started = perf_counter()
        with _stage(stages, "backup"):
            backup_path = backup_database(datetime.now())
        universe: UniverseRefreshResult | None = None
        if tickers is None and limit is None:
            with _stage(stages, "universe"):
                universe = _refresh_universe()
        selection = _select_tickers(tickers, limit)
        with _stage(stages, "metrics"):
            metrics = refresh_metrics(selection)
        with _stage(stages, "alerts"):
            alerts = evaluate_all_alerts()
        with _stage(stages, "recommendations"):
            run_id = generate_run("weekly", include_entries=True, tickers=selection)
        with _stage(stages, "digest"):
            dashboard = latest_dashboard()
            digest = build_digest(
                dashboard,
                _report_dict("weekly", run_id, stages, metrics, universe, alerts),
            )
            report_path = write_report(digest, "weekly", datetime.now())
            stale = _stale_holdings(dashboard)
            notified = send_digest(digest) if notify else False
        stages["total"] = round(perf_counter() - started, 3)
        return JobReport(
            job="weekly",
            run_id=run_id,
            stage_seconds=stages,
            metrics=metrics,
            universe=universe,
            alerts_triggered=alerts,
            report_path=report_path,
            notified=notified,
            summary=dashboard["summary"],
            stale_holdings=stale,
            backup_path=backup_path,
        )


def run_daily(*, notify: bool = True) -> JobReport:
    """Re-evaluate holdings and the ENTER/WATCH list on refreshed bars.

    The report is always written; the notification goes out only when
    ``actionable_rows`` finds something or some holdings' bars are stale (the
    download failed, so stops were checked on old prices).
    """
    with job_lock():
        stages: dict[str, float] = {}
        started = perf_counter()
        with _stage(stages, "recommendations"):
            run_id = generate_run("daily", include_entries=False, refresh=True)
        with _stage(stages, "digest"):
            dashboard = latest_dashboard()
            actionable = actionable_rows(run_id)
            digest = build_digest(
                dashboard, _report_dict("daily", run_id, stages, None, None, None)
            )
            report_path = write_report(digest, "daily", datetime.now())
            stale = _stale_holdings(dashboard)
            notified = False
            if not actionable and not stale:
                logger.info("Nothing actionable; no notification")
            elif notify:
                logger.info(
                    f"{len(actionable)} actionable rows, {len(stale)} holdings on "
                    "stale bars; notifying"
                )
                notified = send_digest(digest)
        stages["total"] = round(perf_counter() - started, 3)
        return JobReport(
            job="daily",
            run_id=run_id,
            stage_seconds=stages,
            metrics=None,
            universe=None,
            alerts_triggered=0,
            report_path=report_path,
            notified=notified,
            summary=dashboard["summary"],
            stale_holdings=stale,
        )
