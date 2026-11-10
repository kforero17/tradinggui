from datetime import date, datetime

import pandas as pd
import pytest

import src.main as legacy_main
from src.analysis import recommendation_service as service
from src.analysis.metrics import evaluate_alerts
from src.config.settings import settings
from src.data.database import Database
from src.data.universe_sources import UniverseRefreshResult
from src.jobs import __main__ as cli
from src.jobs import pipeline
from src.jobs.metrics_refresh import MetricsRefreshStats
from src.jobs.pipeline import JobAlreadyRunning, job_lock

WEEKLY_AT = datetime(2026, 11, 7, 11, 0)
STATS = MetricsRefreshStats(
    tickers=3,
    skipped=0,
    batches=1,
    downloaded=3,
    computed=3,
    failed=0,
    stale=0,
    liquid=2,
    fundamentals_due=1,
    fundamentals_fetched=1,
    fundamentals_failed=0,
    download_seconds=1.0,
    compute_seconds=0.5,
    fundamentals_seconds=0.2,
    store_seconds=0.1,
    total_seconds=1.8,
)
UNIVERSE_OK = UniverseRefreshResult(6000, 2000, 503, True, True)
METRICS = pd.DataFrame(
    {
        "ticker": ["AAPL", "MSFT", "XOM"],
        "last_price": [150.0, 300.0, float("nan")],
        "rsi_14": [55.0, 25.0, 40.0],
        "macd_histogram": [0.5, -0.2, 0.1],
        "pct_above_ma_100": [12.0, -3.0, 1.0],
    }
)


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "jobs.db")
    monkeypatch.setattr(settings, "REPORTS_DIR", tmp_path / "reports")
    monkeypatch.setattr(settings, "JOB_LOCK_PATH", tmp_path / "locks" / "job.lock")
    monkeypatch.setattr(settings, "SLACK_WEBHOOK_URL", None)
    database = Database()
    monkeypatch.setattr(pipeline, "db", database)
    monkeypatch.setattr(service, "db", database)
    monkeypatch.setattr(
        database, "get_latest_metrics", lambda ticker=None: METRICS.copy()
    )
    return database


def _row(ticker, action, status="NEW", direction="LONG", account=None, **extra):
    family = {
        "EXIT_STOP": "EXIT",
        "EXIT_CHANNEL": "EXIT",
        "ADD_UNIT": "ADD",
        "HOLD": "HOLD",
    }.get(action, "ENTRY")
    key = (
        f"{ticker}|{direction}|ENTRY"
        if family == "ENTRY"
        else f"{account or 'default'}|{ticker}|{direction}|{family}"
    )
    return {
        "signal_key": key,
        "ticker": ticker,
        "account": account if family == "ENTRY" else account or "default",
        "direction": direction,
        "action": action,
        "status": status,
        "first_seen_at": WEEKLY_AT,
        "trigger_price": 100.0,
        "stop_price": 95.0,
        "shares": 10,
        "risk_dollars": 50.0,
        "trend_score": 70.0,
        "extra": extra,
    }


def _store(database, job, rows):
    config = {"include_entries": job == "weekly", "max_heat_pct": 0.1}
    return database.create_recommendation_run(
        job, equity=100_000.0, config=config, regime={"label": "risk_on"}, rows=rows
    )


@pytest.fixture
def stubs(database, monkeypatch):
    """Stub every networked or heavy stage of the weekly job, recording the call order."""
    calls: list[tuple] = []
    sent: list = []

    def refresh_universe():
        calls.append(("universe",))
        return UNIVERSE_OK

    def refresh_metrics(tickers=None):
        calls.append(("metrics", tickers))
        return STATS

    def generate_run(job, *, include_entries, tickers=None, refresh=False):
        calls.append(("generate_run", job, include_entries, tickers, refresh))
        return _store(database, job, [_row("ENT", "ENTER")])

    def send(digest):
        calls.append(("send",))
        sent.append(digest)
        return True

    monkeypatch.setattr(pipeline, "refresh_universe_files", refresh_universe)
    monkeypatch.setattr(pipeline, "refresh_metrics", refresh_metrics)
    monkeypatch.setattr(pipeline, "generate_run", generate_run)
    monkeypatch.setattr(pipeline, "send_digest", send)
    return {"calls": calls, "sent": sent}


def test_run_weekly_runs_the_stages_in_order(stubs, database):
    report = pipeline.run_weekly()

    assert [c[0] for c in stubs["calls"]] == [
        "universe",
        "metrics",
        "generate_run",
        "send",
    ]
    assert stubs["calls"][1] == ("metrics", None)
    assert stubs["calls"][2] == ("generate_run", "weekly", True, None, False)
    assert list(report.stage_seconds) == [
        "universe",
        "metrics",
        "alerts",
        "recommendations",
        "digest",
        "total",
    ]
    assert report.job == "weekly" and report.run_id == database.get_latest_run()["id"]
    assert report.metrics == STATS and report.universe == UNIVERSE_OK
    assert report.notified is True
    assert report.summary["by_action"]["ENTER"] == 1
    assert report.report_path == settings.REPORTS_DIR / (
        f"digest_weekly_{date.today():%Y-%m-%d}.html"
    )
    html = report.report_path.read_text()
    assert "ENT" in html and "Run stats" in html and "Metrics refresh" in html
    assert "weekly run" in report.summary_line()


def test_weekly_smoke_run_skips_universe_and_keeps_holdings(
    stubs, database, monkeypatch
):
    database.add_portfolio_ticker("zzz", 1_000.0, 10)
    monkeypatch.setattr(
        pipeline.ticker_loader, "load_unique_tickers", lambda: ["A", "B", "C", "ZZZ"]
    )
    report = pipeline.run_weekly(limit=2, notify=False)

    expected = ["A", "B", "ZZZ"]
    assert ("universe",) not in stubs["calls"]
    assert ("metrics", expected) in stubs["calls"]
    assert ("generate_run", "weekly", True, expected, False) in stubs["calls"]
    assert ("send",) not in stubs["calls"]
    assert report.universe is None and report.notified is False
    assert "universe" not in report.stage_seconds

    stubs["calls"].clear()
    pipeline.run_weekly(tickers=[" msft", "aapl"], notify=False)
    assert ("metrics", ["MSFT", "AAPL", "ZZZ"]) in stubs["calls"]


def test_universe_refresh_failure_does_not_abort(stubs, monkeypatch):
    def broken():
        raise ConnectionError("nasdaqtrader.com unreachable")

    monkeypatch.setattr(pipeline, "refresh_universe_files", broken)
    report = pipeline.run_weekly(notify=False)
    assert report.universe is None
    assert [c[0] for c in stubs["calls"]] == ["metrics", "generate_run"]
    assert report.report_path.exists()


def test_partial_universe_failure_is_reported(stubs, monkeypatch):
    partial = UniverseRefreshResult(6000, 2000, 0, True, False, None, "HTTPError: 503")
    monkeypatch.setattr(pipeline, "refresh_universe_files", lambda: partial)
    report = pipeline.run_weekly(notify=False)
    assert report.universe == partial
    assert "HTTPError: 503" in report.report_path.read_text()


def test_alerts_are_evaluated_over_the_latest_metrics(stubs, database):
    aapl = database.create_alert("AAPL", "price_above", 100.0)
    database.create_alert("MSFT", "price_above", 400.0)
    msft_rsi = database.create_alert("MSFT", "rsi_oversold", 30.0)
    database.create_alert("XOM", "price_below", 10.0)  # NaN price never triggers
    database.create_alert("NOPE", "price_above", 1.0)  # no metrics row

    # The iterrows loop this replaces, for comparison.
    active = database.get_active_alerts()
    expected = sorted(
        a
        for _, row in METRICS.iterrows()
        for a in evaluate_alerts(row.to_dict(), active)
    )

    report = pipeline.run_weekly(notify=False)
    assert report.alerts_triggered == 2
    triggered = sorted(a["id"] for a in database.get_triggered_alerts())
    assert triggered == sorted([aapl, msft_rsi]) == expected
    # Already triggered alerts are not re-triggered.
    assert pipeline.evaluate_all_alerts() == 0


def test_no_alerts_skips_the_metrics_read(database, monkeypatch):
    def fail(ticker=None):
        raise AssertionError("metrics should not be read")

    monkeypatch.setattr(database, "get_latest_metrics", fail)
    assert pipeline.evaluate_all_alerts() == 0


@pytest.fixture
def daily(database, monkeypatch):
    """A weekly run stored, then a stubbed daily generate_run storing ``rows``."""
    weekly_rows = [
        _row("ENT", "ENTER"),
        _row("WCH", "WATCH"),
        _row("AAPL", "HOLD", account="Roth"),
    ]
    _store(database, "weekly", weekly_rows)
    sent: list = []
    state: dict = {}

    def generate_run(job, *, include_entries, tickers=None, refresh=False):
        assert (job, include_entries, refresh) == ("daily", False, True)
        return _store(database, "daily", state["rows"])

    def send(digest):
        sent.append(digest)
        return True

    monkeypatch.setattr(pipeline, "generate_run", generate_run)
    monkeypatch.setattr(pipeline, "send_digest", send)

    def run(rows, notify=True):
        state["rows"] = rows
        return pipeline.run_daily(notify=notify)

    return run, sent


HOLD = _row("AAPL", "HOLD", status="STILL_VALID", account="Roth")


@pytest.mark.parametrize(
    "rows, notifies",
    [
        ([HOLD], False),
        ([HOLD, _row("ENT", "ENTER", status="STILL_VALID")], False),
        ([_row("AAPL", "EXIT_STOP", account="Roth")], True),
        ([_row("AAPL", "EXIT_CHANNEL", status="STILL_VALID", account="Roth")], True),
        ([_row("AAPL", "ADD_UNIT", account="Roth")], True),
        ([HOLD, _row("NEW1", "ENTER")], True),
        # A WATCH that triggered keeps its key (STILL_VALID) but is news.
        ([HOLD, _row("WCH", "ENTER", status="STILL_VALID")], True),
        ([HOLD, _row("WCH", "WATCH", status="STILL_VALID")], False),
    ],
)
def test_run_daily_notifies_only_when_actionable(daily, rows, notifies):
    run, sent = daily
    report = run(rows)
    assert report.notified is notifies
    assert len(sent) == int(notifies)
    # The report is always written.
    assert report.report_path == settings.REPORTS_DIR / (
        f"digest_daily_{date.today():%Y-%m-%d}.html"
    )
    assert report.report_path.exists()
    assert list(report.stage_seconds) == ["recommendations", "digest", "total"]


@pytest.mark.parametrize(
    "status, first_seen_at, notifies",
    [
        ("taken", WEEKLY_AT, False),
        ("skipped", WEEKLY_AT, False),
        # Undecided stop hits keep nagging every day
        ("pending", WEEKLY_AT, True),
        # A decision on an earlier episode of the same key doesn't silence this one
        ("taken", datetime(2026, 10, 3, 11, 0), True),
    ],
)
def test_run_daily_skips_signals_already_decided(
    daily, database, status, first_seen_at, notifies
):
    run, sent = daily
    stop = _row("AAPL", "EXIT_STOP", status="STILL_VALID", account="Roth")
    database.upsert_signal_decision(stop["signal_key"], first_seen_at, status)
    report = run([HOLD, stop])
    assert report.notified is notifies
    assert len(sent) == int(notifies)


def test_actionable_rows_drops_only_decided_rows(database):
    _store(database, "weekly", [_row("WCH", "WATCH")])
    rows = [
        _row("AAPL", "EXIT_STOP", account="Roth"),
        _row("MSFT", "ADD_UNIT", account="Roth"),
        _row("NEW1", "ENTER"),
        _row("NEW2", "ENTER"),
        HOLD,
    ]
    run_id = _store(database, "daily", rows)
    database.upsert_signal_decision(rows[0]["signal_key"], WEEKLY_AT, "taken")
    database.upsert_signal_decision(rows[2]["signal_key"], WEEKLY_AT, "skipped")
    database.upsert_signal_decision(rows[1]["signal_key"], WEEKLY_AT, "pending")

    assert [r["ticker"] for r in pipeline.actionable_rows(run_id)] == ["MSFT", "NEW2"]


def test_run_daily_no_notify_still_writes_the_report(daily):
    run, sent = daily
    report = run([_row("AAPL", "EXIT_STOP", account="Roth")], notify=False)
    assert sent == [] and report.notified is False
    assert "AAPL" in report.report_path.read_text()


def test_job_lock_refuses_concurrent_use(database):
    with job_lock():
        assert settings.JOB_LOCK_PATH.read_text().strip().isdigit()
        with pytest.raises(JobAlreadyRunning, match="held by pid"):
            with job_lock():
                pass
        with pytest.raises(JobAlreadyRunning):
            pipeline.run_daily(notify=False)
    # Released on exit.
    with job_lock():
        pass


def test_lock_is_released_when_the_job_fails(stubs, monkeypatch):
    def boom(tickers=None):
        raise RuntimeError("download exploded")

    monkeypatch.setattr(pipeline, "refresh_metrics", boom)
    with pytest.raises(RuntimeError, match="exploded"):
        pipeline.run_weekly(notify=False)
    with job_lock():
        pass


@pytest.fixture
def cli_stubs(monkeypatch):
    calls: list[tuple] = []
    monkeypatch.setattr(cli, "setup_logging", lambda: None)

    def weekly(*, tickers=None, limit=None, notify=True):
        calls.append(("weekly", tickers, limit, notify))
        return _report("weekly")

    def daily(*, notify=True):
        calls.append(("daily", notify))
        return _report("daily")

    monkeypatch.setattr(cli, "run_weekly", weekly)
    monkeypatch.setattr(cli, "run_daily", daily)
    return calls


def _report(job):
    return pipeline.JobReport(
        job=job,
        run_id=7,
        stage_seconds={"recommendations": 1.25},
        metrics=None,
        universe=None,
        alerts_triggered=0,
        report_path=None,
        notified=False,
        summary={"by_action": {"ENTER": 2, "WATCH": 0}, "new": 1},
    )


def test_cli_parses_subcommands(cli_stubs, capsys):
    assert cli.main(["weekly", "--tickers", "aapl, msft,", "--no-notify"]) == 0
    assert cli.main(["weekly", "--limit", "150"]) == 0
    assert cli.main(["daily"]) == 0
    assert cli.main(["daily", "--no-notify"]) == 0
    assert cli_stubs == [
        ("weekly", ["AAPL", "MSFT"], None, False),
        ("weekly", None, 150, True),
        ("daily", True),
        ("daily", False),
    ]
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0].startswith("weekly run 7: ENTER 2; 1 new;")
    assert "recommendations=1.2s" in out[0] or "recommendations=1.3s" in out[0]


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["monthly"],
        ["weekly", "--limit", "0"],
        ["weekly", "--limit", "x"],
        ["weekly", "--tickers", ","],
        ["weekly", "--tickers", "A", "--limit", "5"],
        ["daily", "--limit", "5"],
    ],
)
def test_cli_rejects_bad_arguments(cli_stubs, argv):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2
    assert cli_stubs == []


def test_cli_exit_codes(cli_stubs, monkeypatch):
    def locked(*, notify=True):
        raise JobAlreadyRunning("Job lock is held by pid 1")

    def broken(*, notify=True):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "run_daily", locked)
    assert cli.main(["daily"]) == cli.EXIT_LOCKED == 2
    monkeypatch.setattr(cli, "run_daily", broken)
    assert cli.main(["daily"]) == cli.EXIT_ERROR == 1


def test_cli_exits_2_when_a_real_lock_is_held(database, monkeypatch):
    monkeypatch.setattr(cli, "setup_logging", lambda: None)
    with job_lock():
        assert cli.main(["daily", "--no-notify"]) == 2


def test_src_main_delegates_to_run_weekly(monkeypatch):
    calls = []
    monkeypatch.setattr(legacy_main, "setup_logging", lambda: None)
    monkeypatch.setattr(
        legacy_main.ticker_loader, "validate_ticker_files", lambda: True
    )
    monkeypatch.setattr(legacy_main, "display_database_summary", lambda: None)
    monkeypatch.setattr(
        legacy_main, "run_weekly", lambda: calls.append("weekly") or _report("weekly")
    )
    legacy_main.main()
    assert calls == ["weekly"]

    def locked():
        raise JobAlreadyRunning("held")

    monkeypatch.setattr(legacy_main, "run_weekly", locked)
    with pytest.raises(SystemExit) as exc:
        legacy_main.main()
    assert exc.value.code == 1
