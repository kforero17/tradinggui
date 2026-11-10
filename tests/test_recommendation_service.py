from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from loguru import logger

from src.analysis import recommendation_service as service
from src.config.settings import settings
from src.data.broker_import import Fill, Side
from src.data.database import Database
from src.data.price_history import RefreshResult
from src.data.ticker_loader import TickerInfo

FIRST_RUN = datetime(2026, 11, 7, 11, 0)
SECOND_RUN = datetime(2026, 11, 14, 11, 0)
DAILY_RUN = datetime(2026, 11, 16, 21, 30)
ENTRY_INDEX = 300


def _bars(closes, half_range: float = 1.0) -> pd.DataFrame:
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "open": closes,
            "high": closes + half_range,
            "low": closes - half_range,
            "close": closes,
            "volume": 1_000_000.0,
            "stock_splits": 0.0,
        },
        index=pd.bdate_range("2023-01-02", periods=len(closes)),
    )


FLAT = [100.0] * 399


def _inside_bar() -> pd.DataFrame:
    """Flat in a 99-101 channel, last bar inside it: 0.5N from both the high and the low."""
    bars = _bars(FLAT + [100.0])
    bars.iloc[-1, bars.columns.get_loc("high")] = 100.5
    bars.iloc[-1, bars.columns.get_loc("low")] = 99.5
    return bars


HISTORIES = {
    "SPY": _bars(np.linspace(300, 500, 400)),
    "AAPL": _bars(np.linspace(50, 150, 400)),  # held long in the Roth
    "XOM": _bars(np.linspace(150, 50, 400)),  # held short in taxable
    "ENT": _bars(FLAT + [103.0]),  # breaks the 55-day high of 101
    "WCH": _inside_bar(),  # WATCH both ways
    "DWN": _bars(FLAT + [97.0]),  # breaks the 55-day low of 99
    "ILLQ": _bars(FLAT + [103.0]),  # would break out, but is illiquid
}
# Every fixture history ends on this bar, the last completed session in these tests.
LAST_SESSION = HISTORIES["SPY"].index[-1].date()
UNIVERSE = {t: TickerInfo(t, f"{t} Corp", "NYSE", t == "SPY", False) for t in HISTORIES}
# SPY has no metrics row, so it is only the regime benchmark, not an entry candidate.
METRICS = pd.DataFrame(
    {
        "ticker": ["AAPL", "XOM", "ENT", "WCH", "DWN", "ILLQ"],
        "last_price": [150.0, 50.0, 103.0, 100.0, 97.0, 103.0],
        "avg_dollar_volume_50d": [1e10, 1e9, 5e7, 5e7, 5e7, 1e5],
        "asset_type": ["stock", "stock", "stock", "stock", "stock", "stock"],
    }
)


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "service.db")
    database = Database()
    monkeypatch.setattr(service, "db", database)
    # Metrics are written by the metrics job; stub the read so the test needs no metrics rows.
    monkeypatch.setattr(
        database, "get_latest_metrics", lambda ticker=None: METRICS.copy()
    )
    return database


@pytest.fixture
def prices(monkeypatch):
    """Everything but SPY is cached; SPY is only available through a refresh."""
    calls: dict[str, list[list[str]]] = {"cached": [], "refresh": []}

    def load_cached(tickers):
        tickers = [t.upper() for t in tickers]
        calls["cached"].append(tickers)
        return {t: HISTORIES[t] for t in tickers if t in HISTORIES and t != "SPY"}

    def refresh(tickers, chunk_size=None):
        tickers = [t.upper() for t in tickers]
        calls["refresh"].append(tickers)
        found = {t: HISTORIES[t] for t in tickers if t in HISTORIES}
        return RefreshResult(found, [t for t in tickers if t not in found], len(found))

    monkeypatch.setattr(service, "load_cached_histories", load_cached)
    monkeypatch.setattr(service, "refresh_histories", refresh)
    monkeypatch.setattr(service, "load_universe", lambda: UNIVERSE)
    monkeypatch.setattr(
        service, "last_completed_session", lambda now=None: LAST_SESSION
    )
    return calls


@pytest.fixture
def portfolio(database):
    database.delete_account(settings.DEFAULT_ACCOUNT)
    database.upsert_account(
        "Roth IRA", equity=60_000.0, cash=50_000.0, allow_short=False
    )
    database.upsert_account("taxable", equity=40_000.0, cash=30_000.0, allow_short=True)
    aapl_entry = float(HISTORIES["AAPL"]["close"].iloc[ENTRY_INDEX])
    xom_entry = float(HISTORIES["XOM"]["close"].iloc[ENTRY_INDEX])
    database.add_portfolio_ticker("AAPL", 100 * aapl_entry, 100, account="Roth IRA")
    database.add_portfolio_ticker("XOM", 50 * xom_entry, -50, account="taxable")
    database.add_portfolio_ticker("ZERO", 1_000.0, 0, account="taxable")
    entry_day = HISTORIES["AAPL"].index[ENTRY_INDEX].date()
    database.add_broker_fills(
        [Fill("Roth IRA", entry_day, "AAPL", Side.BUY, 100, aapl_entry)]
    )
    database.add_to_watchlist(["WCH"])
    return database


def _rows_by_ticker(database, run_id):
    return {r["ticker"]: r for r in database.get_run_recommendations(run_id)}


def test_generate_run_persists_ranked_routed_recommendations(portfolio, prices):
    run_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)

    run = portfolio.get_latest_run()
    assert run["id"] == run_id and run["job"] == "weekly"
    assert run["equity"] == pytest.approx(100_000.0)
    assert run["config"]["equity_source"] == "accounts"
    assert run["config"]["include_entries"] is True
    assert run["config"]["directions"] == ["LONG", "SHORT"]
    assert run["config"]["skipped_positions"] == [
        {"account": "taxable", "ticker": "ZERO", "reason": "zero shares"}
    ]
    assert run["regime"]["benchmark"] == "SPY"
    assert run["regime"]["benchmark_above_200d"] is True
    assert run["regime"]["breadth_above_200d"] is not None
    # SPY was missing from the cache, so it alone was downloaded.
    assert prices["refresh"] == [["SPY"]]

    rows = _rows_by_ticker(portfolio, run_id)
    assert set(rows) == {"AAPL", "XOM", "ENT", "WCH", "DWN"}
    assert all(
        r["status"] == "NEW" and r["first_seen_at"] == FIRST_RUN for r in rows.values()
    )

    assert rows["AAPL"]["account"] == "Roth IRA"
    assert rows["AAPL"]["signal_key"].startswith("Roth IRA|AAPL|LONG|")
    # The entry date comes from the broker fill, not the portfolio's added_at.
    assert rows["AAPL"]["extra"]["entry_date"] == HISTORIES["AAPL"].index[
        ENTRY_INDEX
    ].strftime("%Y-%m-%d")
    assert rows["XOM"]["direction"] == "SHORT" and rows["XOM"]["account"] == "taxable"

    assert rows["ENT"]["action"] == "ENTER" and rows["ENT"]["account"] == "Roth IRA"
    assert rows["ENT"]["signal_key"] == "ENT|LONG|ENTRY"
    assert rows["ENT"]["extra"]["order_type"] == "buy stop"
    watch = [
        r for r in portfolio.get_run_recommendations(run_id) if r["ticker"] == "WCH"
    ]
    assert {(r["direction"], r["action"]) for r in watch} == {
        ("LONG", "WATCH"),
        ("SHORT", "WATCH"),
    }
    assert rows["DWN"]["action"] == "ENTER" and rows["DWN"]["direction"] == "SHORT"
    assert rows["DWN"]["account"] == "taxable"
    assert rows["DWN"]["extra"]["check_borrow"] is True
    assert rows["ENT"]["asset_type"] == "stock"

    actions = [r["action"] for r in portfolio.get_run_recommendations(run_id)]
    order = ["EXIT_STOP", "EXIT_CHANNEL", "ADD_UNIT", "ENTER", "WATCH", "HOLD"]
    assert actions == sorted(actions, key=order.index)


def test_explicit_tickers_replace_the_liquid_universe(portfolio, prices):
    run_id = service.generate_run(
        "manual", include_entries=True, tickers=["ilLq"], now=FIRST_RUN
    )
    rows = _rows_by_ticker(portfolio, run_id)
    # ILLQ is scanned because it was asked for; the watchlist name is still added.
    assert {"ILLQ", "WCH"} <= set(rows) and "ENT" not in rows


def test_second_run_is_still_valid_and_dashboard_merges_decisions(portfolio, prices):
    first_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    second_id = service.generate_run("weekly", include_entries=True, now=SECOND_RUN)
    assert second_id > first_id
    rows = portfolio.get_run_recommendations(second_id)
    assert rows and all(r["status"] == "STILL_VALID" for r in rows)
    assert all(r["first_seen_at"] == FIRST_RUN for r in rows)

    portfolio.upsert_signal_decision(
        "ENT|LONG|ENTRY", FIRST_RUN, "taken", note="filled 101.2"
    )
    dashboard = service.latest_dashboard()
    assert dashboard["run"]["id"] == second_id
    assert dashboard["entries_run"]["id"] == second_id
    by_key = {r["signal_key"]: r for r in dashboard["recommendations"]}
    assert by_key["ENT|LONG|ENTRY"]["decision"]["status"] == "taken"
    assert by_key["ENT|LONG|ENTRY"]["decision"]["note"] == "filled 101.2"
    assert by_key["ENT|LONG|ENTRY"]["first_seen_at"] == FIRST_RUN.isoformat()
    assert by_key["WCH|LONG|ENTRY"]["decision"] is None
    assert dashboard["expired"] == []
    assert dashboard["summary"]["by_action"]["ENTER"] == 2
    assert dashboard["summary"]["new"] == 0
    heat = dashboard["heat"]
    assert heat["budget_pct"] == pytest.approx(settings.MAX_PORTFOLIO_HEAT_PCT)
    assert heat["budget_dollars"] == pytest.approx(
        100_000.0 * settings.MAX_PORTFOLIO_HEAT_PCT
    )
    assert heat["open_risk_dollars"] >= 0 and heat["planned_risk_dollars"] > 0


def test_daily_run_keeps_weekly_entries_on_the_dashboard(portfolio, prices):
    weekly_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    daily_id = service.generate_run("daily", include_entries=False, now=DAILY_RUN)

    daily_rows = portfolio.get_run_recommendations(daily_id)
    # Positions plus the re-scanned ENTER/WATCH list of the weekly run.
    assert {r["ticker"] for r in daily_rows} == {"AAPL", "XOM", "ENT", "WCH", "DWN"}
    assert all(r["status"] == "STILL_VALID" for r in daily_rows)
    assert all(
        r["first_seen_at"] == FIRST_RUN for r in daily_rows if r["action"] == "ENTER"
    )
    assert portfolio.get_latest_run()["config"]["expired_count"] == 0
    assert portfolio.get_latest_run()["config"]["rescanned_entries"] == 4

    dashboard = service.latest_dashboard()
    assert dashboard["run"]["id"] == daily_id
    assert dashboard["entries_run"]["id"] == weekly_id
    assert {r["ticker"] for r in dashboard["recommendations"]} == {
        "AAPL",
        "XOM",
        "ENT",
        "WCH",
        "DWN",
    }
    assert dashboard["expired"] == []

    # The next weekly run diffs entries against the weekly run, not the daily one.
    next_id = service.generate_run("weekly", include_entries=True, now=SECOND_RUN)
    entries = [
        r
        for r in portfolio.get_run_recommendations(next_id)
        if r["action"] in ("ENTER", "WATCH")
    ]
    assert entries and all(r["status"] == "STILL_VALID" for r in entries)


def test_expired_entries_show_on_the_dashboard(portfolio, prices, monkeypatch):
    service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    gone = {k: v for k, v in HISTORIES.items() if k != "ENT"}
    monkeypatch.setattr(
        service,
        "load_cached_histories",
        lambda ts: {t: gone[t] for t in ts if t in gone},
    )
    service.generate_run("weekly", include_entries=True, now=SECOND_RUN)
    dashboard = service.latest_dashboard()
    assert [r["signal_key"] for r in dashboard["expired"]] == ["ENT|LONG|ENTRY"]
    assert dashboard["expired"][0]["decision"] is None
    assert dashboard["summary"]["expired"] == 1


def test_default_equity_when_no_account_equity_is_stored(database, prices):
    run_id = service.generate_run("daily", include_entries=False, now=FIRST_RUN)
    run = database.get_latest_run()
    assert run["id"] == run_id
    assert run["equity"] == pytest.approx(service.DEFAULT_TREND_EQUITY)
    assert run["config"]["equity_source"] == "default"
    assert database.get_run_recommendations(run_id) == []


def test_dashboard_before_any_run(database):
    dashboard = service.latest_dashboard()
    assert dashboard["run"] is None and dashboard["recommendations"] == []
    assert dashboard["heat"] is None and dashboard["summary"]["total"] == 0


def test_invalid_trend_system_is_rejected(monkeypatch):
    monkeypatch.setattr(settings, "TREND_SYSTEM", "S3")
    with pytest.raises(ValueError, match="TREND_SYSTEM"):
        service.turtle_config()
    monkeypatch.setattr(settings, "TREND_SYSTEM", "s1")
    assert service.turtle_config().entry_period == 20


def test_daily_rescan_turns_a_triggered_watch_into_enter(
    portfolio, prices, monkeypatch
):
    weekly_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    # Mid-week WCH breaks its 55-day high; ILLQ was never on the weekly list.
    monkeypatch.setitem(HISTORIES, "WCH", _bars(FLAT + [103.0]))
    prices["cached"].clear()
    prices["refresh"].clear()
    daily_id = service.generate_run("daily", include_entries=False, now=DAILY_RUN)

    requested = {t for call in prices["cached"] + prices["refresh"] for t in call}
    assert "ILLQ" not in requested
    daily = {
        r["signal_key"]: r
        for r in portfolio.get_run_recommendations(daily_id)
        if r["action"] in ("ENTER", "WATCH")
    }
    assert set(daily) == {"ENT|LONG|ENTRY", "DWN|SHORT|ENTRY", "WCH|LONG|ENTRY"}
    wch = daily["WCH|LONG|ENTRY"]
    assert wch["action"] == "ENTER"
    # Same signal key as the weekly WATCH, so it keeps that first_seen_at.
    assert wch["status"] == "STILL_VALID" and wch["first_seen_at"] == FIRST_RUN
    weekly_wch = next(
        r
        for r in portfolio.get_run_recommendations(weekly_id)
        if r["signal_key"] == "WCH|LONG|ENTRY"
    )
    assert wch["trend_score"] == weekly_wch["trend_score"]
    # Daily runs never expire entry signals, though WCH's short WATCH is gone.
    assert portfolio.get_latest_run()["config"]["expired_count"] == 0

    dashboard = service.latest_dashboard()
    assert dashboard["entries_run"]["id"] == weekly_id
    by_key = {r["signal_key"]: r for r in dashboard["recommendations"]}
    assert by_key["WCH|LONG|ENTRY"]["action"] == "ENTER"
    assert by_key["WCH|LONG|ENTRY"]["run_id"] == daily_id
    assert by_key["WCH|LONG|ENTRY"]["first_seen_at"] == FIRST_RUN.isoformat()
    assert by_key["WCH|SHORT|ENTRY"]["action"] == "WATCH"
    assert by_key["WCH|SHORT|ENTRY"]["run_id"] == weekly_id
    assert dashboard["expired"] == []
    assert dashboard["summary"]["by_action"]["ENTER"] == 3

    # The next weekly run still carries the original first_seen_at forward.
    next_id = service.generate_run("weekly", include_entries=True, now=SECOND_RUN)
    rows = {r["signal_key"]: r for r in portfolio.get_run_recommendations(next_id)}
    assert rows["WCH|LONG|ENTRY"]["action"] == "ENTER"
    assert rows["WCH|LONG|ENTRY"]["first_seen_at"] == FIRST_RUN
    assert rows["WCH|LONG|ENTRY"]["status"] == "STILL_VALID"
    assert [r["signal_key"] for r in service.latest_dashboard()["expired"]] == [
        "WCH|SHORT|ENTRY"
    ]


def test_rescan_without_an_entries_run_scans_nothing(portfolio, prices):
    run_id = service.generate_run("manual", include_entries=False, now=FIRST_RUN)
    rows = portfolio.get_run_recommendations(run_id)
    assert {r["ticker"] for r in rows} == {"AAPL", "XOM"}
    assert portfolio.get_latest_run()["config"]["rescanned_entries"] == 0


@pytest.mark.parametrize("limit, shown", [(1, 1), (0, 0), (30, 2)])
def test_dashboard_caps_watch_rows_by_score(
    portfolio, prices, monkeypatch, limit, shown
):
    service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    monkeypatch.setattr(settings, "MAX_WATCH_ROWS", limit)
    dashboard = service.latest_dashboard()
    watch = [r for r in dashboard["recommendations"] if r["action"] == "WATCH"]
    assert len(watch) == shown
    assert dashboard["summary"]["watch_hidden"] == 2 - shown
    assert dashboard["summary"]["by_action"]["WATCH"] == shown
    # ENTER rows are never capped.
    assert dashboard["summary"]["by_action"]["ENTER"] == 2
    if shown == 1:
        all_watch = [
            r
            for r in portfolio.get_run_recommendations(dashboard["run"]["id"])
            if r["action"] == "WATCH"
        ]
        assert watch[0]["trend_score"] == max(r["trend_score"] for r in all_watch)


def test_turtle_config_sizes_from_settings(monkeypatch):
    monkeypatch.setattr(settings, "TREND_RISK_PER_UNIT", 0.01)
    monkeypatch.setattr(settings, "TREND_MAX_UNIT_FRACTION", 0.2)
    config = service.turtle_config()
    assert config.risk_per_unit == 0.01 and config.max_unit_fraction == 0.2


@pytest.fixture
def warnings_logged():
    messages: list[str] = []
    handler = logger.add(lambda m: messages.append(str(m)), level="WARNING")
    yield messages
    logger.remove(handler)


def test_skip_listed_tickers_leave_the_universe_unless_held_or_watched(
    portfolio, prices, monkeypatch
):
    monkeypatch.setattr(
        portfolio, "get_skipped_tickers", lambda today=None: {"ENT", "WCH", "AAPL"}
    )
    run_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    rows = _rows_by_ticker(portfolio, run_id)
    # ENT's frozen metrics row no longer qualifies it; WCH is on the watchlist and
    # AAPL is held, so both are still evaluated.
    assert "ENT" not in rows
    assert rows["WCH"]["action"] == "WATCH"
    assert rows["AAPL"]["action"] in ("HOLD", "ADD_UNIT", "EXIT_CHANNEL", "EXIT_STOP")
    assert rows["DWN"]["action"] == "ENTER"


def test_tickers_with_stale_bars_are_not_scanned(portfolio, prices, monkeypatch):
    # ENT's breakout bar is 15 sessions old, more than MAX_BAR_AGE_DAYS before the session.
    monkeypatch.setitem(HISTORIES, "ENT", _bars(FLAT[:-15] + [103.0]))
    run_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    rows = _rows_by_ticker(portfolio, run_id)
    assert "ENT" not in rows and rows["DWN"]["action"] == "ENTER"
    config = portfolio.get_latest_run()["config"]
    assert config["stale_histories"] == 1
    assert config["stale_holdings"] == []


def test_a_few_days_old_bars_are_still_scanned(portfolio, prices, monkeypatch):
    # Within MAX_BAR_AGE_DAYS: a missed download, not a dead ticker.
    monkeypatch.setitem(HISTORIES, "DWN", _bars(FLAT[:-2] + [97.0]))
    run_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    assert _rows_by_ticker(portfolio, run_id)["DWN"]["action"] == "ENTER"
    assert portfolio.get_latest_run()["config"]["stale_histories"] == 0


def test_held_ticker_without_cost_basis_gets_no_enter(portfolio, prices):
    # ENT breaks out, but it is already held: only the cost basis is missing.
    portfolio.add_portfolio_ticker("ENT", 0.0, 10, account="Roth IRA")
    run_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    assert "ENT" not in _rows_by_ticker(portfolio, run_id)

    dashboard = service.latest_dashboard()
    assert "ENT" not in {r["ticker"] for r in dashboard["recommendations"]}
    assert dashboard["skipped_positions"] == [
        {"account": "Roth IRA", "ticker": "ENT", "reason": "missing cost basis"},
        {"account": "taxable", "ticker": "ZERO", "reason": "zero shares"},
    ]


def test_dashboard_hides_entries_of_held_tickers_that_were_not_evaluated(
    portfolio, prices
):
    weekly_id = service.generate_run("weekly", include_entries=True, now=FIRST_RUN)
    assert "ENT" in _rows_by_ticker(portfolio, weekly_id)
    # Bought ENT after the weekly scan, without a cost basis yet.
    portfolio.add_portfolio_ticker("ENT", 0.0, 10, account="Roth IRA")
    service.generate_run("daily", include_entries=False, now=DAILY_RUN)
    dashboard = service.latest_dashboard()
    assert "ENT" not in {r["ticker"] for r in dashboard["recommendations"]}
    assert {"account": "Roth IRA", "ticker": "ENT", "reason": "missing cost basis"} in (
        dashboard["skipped_positions"]
    )


def test_holdings_with_old_bars_are_flagged_stale(portfolio, prices, monkeypatch):
    # The download failed, so the refresh hands back AAPL's cache, two sessions old.
    old_aapl = HISTORIES["AAPL"].iloc[:-2]
    monkeypatch.setitem(HISTORIES, "AAPL", old_aapl)
    run_id = service.generate_run(
        "daily", include_entries=False, refresh=True, now=DAILY_RUN
    )
    last_bar = old_aapl.index[-1].date().isoformat()
    config = portfolio.get_latest_run()["config"]
    assert config["stale_holdings"] == [{"ticker": "AAPL", "last_bar": last_bar}]

    rows = _rows_by_ticker(portfolio, run_id)
    assert rows["AAPL"]["extra"]["stale_bars"] is True
    assert f"bars stale (last {last_bar})" in rows["AAPL"]["reasons"]
    assert "stale_bars" not in rows["XOM"]["extra"]

    dashboard = service.latest_dashboard()
    assert dashboard["stale_holdings"] == [{"ticker": "AAPL", "last_bar": last_bar}]


def test_entry_date_source_and_account_mismatch_warning(
    portfolio, prices, warnings_logged
):
    xom_day = HISTORIES["XOM"].index[ENTRY_INDEX].date()
    # The broker's account string doesn't match the portfolio's "taxable".
    portfolio.add_broker_fills(
        [Fill("Individual -1234", xom_day, "XOM", Side.SELL, 50, 100.0)]
    )
    run_id = service.generate_run("daily", include_entries=False, now=FIRST_RUN)
    rows = _rows_by_ticker(portfolio, run_id)
    assert rows["AAPL"]["extra"]["entry_date_source"] == "broker"
    assert not any("entry date" in r for r in rows["AAPL"]["reasons"])
    assert rows["XOM"]["extra"]["entry_date_source"] == "added_at"
    assert (
        "entry date from added_at (no broker fills for account taxable)"
        in rows["XOM"]["reasons"]
    )
    mismatch = [m for m in warnings_logged if "other account names" in m]
    assert len(mismatch) == 1
    assert "taxable/XOM (fills under Individual -1234)" in mismatch[0]
    assert "AAPL" not in mismatch[0]
