"""Flask tests for the Today page, its JSON APIs, multi-account Portfolio routes and Trend shorts."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.analysis import recommendation_service as service
from src.config.settings import settings
from src.data.database import Database
from src.jobs.pipeline import job_lock
from src.web_app import app as app_module

FIRST_SEEN = datetime(2026, 11, 7, 11, 0, 0, 123456)


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "web.db")
    monkeypatch.setattr(settings, "JOB_LOCK_PATH", tmp_path / "job.lock")
    database = Database()
    monkeypatch.setattr(app_module, "db", database)
    monkeypatch.setattr(service, "db", database)
    return database


@pytest.fixture
def client(database):
    app_module.app.config["TESTING"] = True
    return app_module.app.test_client()


def _bars(closes) -> pd.DataFrame:
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {
            "open": closes,
            "high": closes + 1.0,
            "low": closes - 1.0,
            "close": closes,
            "volume": 1_000_000.0,
            "stock_splits": 0.0,
        },
        index=pd.bdate_range("2025-01-02", periods=len(closes)),
    )


@pytest.fixture
def offline_prices(monkeypatch):
    """No network: portfolio history, plots and Trend bars are synthetic."""
    history = _bars(np.linspace(90, 110, 300))
    monkeypatch.setattr(
        app_module.metrics_calculator, "_get_historical_data", lambda ticker: history
    )
    monkeypatch.setattr(app_module, "create_stock_plot", lambda *args: None)
    monkeypatch.setattr(app_module, "get_daily_ohlc", lambda ticker: history)
    return history


def _store_metrics(database: Database, *tickers: str) -> None:
    database.store_metrics([{"ticker": t, "last_price": 100.0} for t in tickers])


def _row(**overrides) -> dict:
    row = {
        "signal_key": "ENTER|LONG|AAPL",
        "ticker": "AAPL",
        "account": None,
        "direction": "LONG",
        "action": "ENTER",
        "status": "NEW",
        "first_seen_at": FIRST_SEEN,
        "trigger_price": 101.25,
        "stop_price": 95.10,
        "shares": 120,
        "risk_dollars": 738.0,
        "risk_pct_equity": 0.00738,
        "trend_score": 1.4,
        "rs_rank": 0.87,
        "asset_type": "stock",
        "reasons": ["55-day high breakout"],
        "blocked_by": [],
        "extra": {"order_type": "buy stop", "entry_level": 101.25},
    }
    return {**row, **overrides}


def _create_run(database: Database, job: str = "weekly", rows=None) -> int:
    return database.create_recommendation_run(
        job,
        equity=100_000.0,
        config={"system": "S2", "max_heat_pct": 0.1, "include_entries": True},
        regime={"benchmark": "SPY", "label": "risk_on"},
        rows=[_row()] if rows is None else rows,
    )


# ── Routing ───────────────────────────────────────────────────────


def test_root_renders_today_and_nav_lists_today_first(client):
    response = client.get("/")
    page = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "js/today.js" in page
    assert page.index(">Today</a>") < page.index(">Portfolio</a>")
    assert 'href="/portfolio"' in page


def test_portfolio_page_groups_positions_by_account(client, database, offline_prices):
    database.upsert_account("roth", 50_000.0, 5_000.0, allow_short=False)
    _store_metrics(database, "AAPL", "MSFT")
    database.add_portfolio_ticker("AAPL", cost_basis=900.0, shares=10.0)
    database.add_portfolio_ticker("AAPL", cost_basis=450.0, shares=5.0, account="roth")
    database.add_portfolio_ticker("MSFT", cost_basis=500.0, shares=5.0, account="roth")

    response = client.get("/portfolio")
    page = response.get_data(as_text=True)

    assert response.status_code == 200
    assert "Holdings by account" in page
    assert "<th>Account</th>" in page
    assert "roth subtotal" in page
    assert f"{settings.DEFAULT_ACCOUNT} subtotal" in page
    assert "All accounts" in page
    assert '<option value="roth">roth</option>' in page
    assert "/delete_stock/roth/AAPL" in page
    assert 'name="account" value="roth"' in page


def test_account_groups_subtotals_add_up_to_the_grand_total():
    gains = {"1D": {"gain_dollars": 1.0}, "YTD": {"gain_dollars": 2.0}}
    positions = [
        {"account": "a", "current_value": 100.0, "gains": gains},
        {"account": "b", "current_value": 50.0, "gains": gains},
    ]
    realized = {"gains": [{"account": "b", "realized_gain": 7.0}]}
    groups = app_module._account_groups(positions, [], realized, ["b", "a"])
    assert [g["account"] for g in groups] == ["b", "a"]
    assert groups[0]["summary"]["total_value"] == 50.0
    assert groups[0]["summary"]["ytd_total_gain_dollars"] == 9.0
    assert groups[1]["summary"]["ytd_total_gain_dollars"] == 2.0


# ── Recommendations API ───────────────────────────────────────────


def test_latest_without_runs_is_empty_and_stale(client):
    data = client.get("/api/recommendations/latest").get_json()
    assert data["success"] is True
    assert data["run"] is None
    assert data["recommendations"] == []
    assert data["weekly"]["stale"] is True
    assert data["weekly"]["latest_weekly_run_at"] is None


def test_latest_returns_the_stored_run(client, database):
    run_id = _create_run(database)
    data = client.get("/api/recommendations/latest").get_json()
    assert data["run"]["id"] == run_id
    assert data["weekly"]["stale"] is False
    [row] = data["recommendations"]
    assert row["ticker"] == "AAPL"
    assert row["first_seen_at"] == FIRST_SEEN.isoformat()
    assert row["decision"] is None
    assert data["summary"]["by_action"]["ENTER"] == 1


def test_weekly_run_older_than_eight_days_is_stale(database):
    _create_run(database)
    later = datetime.utcnow() + timedelta(days=app_module.WEEKLY_RUN_STALE_DAYS + 1)
    assert app_module._weekly_run_status(now=later)["stale"] is True
    assert app_module._weekly_run_status()["stale"] is False


def test_decision_round_trip(client, database):
    _create_run(database)
    row = client.get("/api/recommendations/latest").get_json()["recommendations"][0]

    response = client.post(
        "/api/recommendations/decision",
        json={
            "signal_key": row["signal_key"],
            "first_seen_at": row["first_seen_at"],
            "status": "taken",
            "note": "filled at 101.30",
        },
    )
    assert response.status_code == 200
    assert response.get_json()["decision"]["status"] == "taken"

    row = client.get("/api/recommendations/latest").get_json()["recommendations"][0]
    assert row["decision"]["status"] == "taken"
    assert row["decision"]["note"] == "filled at 101.30"

    # A note-only update keeps the status the page sends; a None note keeps the old one
    client.post(
        "/api/recommendations/decision",
        json={
            "signal_key": row["signal_key"],
            "first_seen_at": row["first_seen_at"],
            "status": "skipped",
        },
    )
    row = client.get("/api/recommendations/latest").get_json()["recommendations"][0]
    assert row["decision"] == {
        **row["decision"],
        "status": "skipped",
        "note": "filled at 101.30",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"signal_key": "k", "first_seen_at": "2026-11-07T11:00:00", "status": "maybe"},
        {"signal_key": "k", "first_seen_at": "yesterday", "status": "taken"},
        {"signal_key": "", "first_seen_at": "2026-11-07T11:00:00", "status": "taken"},
        {"signal_key": "k", "status": "taken"},
    ],
)
def test_bad_decision_is_400(client, payload):
    response = client.post("/api/recommendations/decision", json=payload)
    assert response.status_code == 400
    assert response.get_json()["success"] is False


def test_decision_accepts_utc_suffix(client, database):
    client.post(
        "/api/recommendations/decision",
        json={
            "signal_key": "k",
            "first_seen_at": "2026-11-07T11:00:00Z",
            "status": "taken",
        },
    )
    [decision] = database.get_signal_decisions(["k"])
    assert decision["first_seen_at"] == datetime(2026, 11, 7, 11, 0)


def test_recheck_runs_holdings_only_on_refreshed_bars(client, monkeypatch):
    calls = []

    def fake_generate_run(job, *, include_entries, refresh):
        calls.append((job, include_entries, refresh))
        return 42

    monkeypatch.setattr(service, "generate_run", fake_generate_run)
    response = client.post("/api/recommendations/recheck")
    assert response.get_json() == {"success": True, "run_id": 42}
    assert calls == [("manual", False, True)]


def test_recheck_failure_is_500_json(client, monkeypatch):
    def broken(job, *, include_entries, refresh):
        raise RuntimeError("no bars")

    monkeypatch.setattr(service, "generate_run", broken)
    response = client.post("/api/recommendations/recheck")
    assert response.status_code == 500
    assert "no bars" in response.get_json()["error"]


def test_recheck_is_409_while_a_job_holds_the_lock(client, monkeypatch):
    calls = []
    monkeypatch.setattr(
        service,
        "generate_run",
        lambda job, *, include_entries, refresh: calls.append(job),
    )
    with job_lock():
        response = client.post("/api/recommendations/recheck")
    assert response.status_code == 409
    assert response.get_json() == {
        "success": False,
        "error": "A scheduled job is running; try again shortly",
    }
    assert calls == []
    # Released: the next recheck runs
    assert client.post("/api/recommendations/recheck").status_code == 200
    assert calls == ["manual"]


# ── Accounts API ──────────────────────────────────────────────────


def test_accounts_crud(client):
    data = client.get("/api/accounts").get_json()
    assert [a["name"] for a in data["accounts"]] == [settings.DEFAULT_ACCOUNT]

    data = client.post(
        "/api/accounts",
        json={"name": "roth", "equity": "60000", "cash": 2500, "allow_short": False},
    ).get_json()
    roth = next(a for a in data["accounts"] if a["name"] == "roth")
    assert roth["equity"] == 60_000.0
    assert roth["allow_short"] is False
    assert data["total_equity"] == 60_000.0

    data = client.post(
        "/api/accounts", json={"name": "roth", "equity": 65000, "cash": 0}
    ).get_json()
    assert data["total_equity"] == 65_000.0

    data = client.delete("/api/accounts/roth").get_json()
    assert [a["name"] for a in data["accounts"]] == [settings.DEFAULT_ACCOUNT]
    assert client.delete("/api/accounts/roth").status_code == 404


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "", "equity": 1},
        {"name": "a/b", "equity": 1},
        {"name": "x", "equity": "lots"},
        {"name": "x", "equity": -5},
        {"name": "x", "equity": "nan"},
        {"name": "x", "cash": True},
    ],
)
def test_invalid_account_is_400(client, payload):
    assert client.post("/api/accounts", json=payload).status_code == 400


def test_delete_account_refused_while_referenced(client, database):
    database.upsert_account("roth", 1.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0, account="roth")
    response = client.delete("/api/accounts/roth")
    assert response.status_code == 400
    assert "still holds" in response.get_json()["error"]


def test_rename_account_via_api(client, database):
    database.upsert_account("ira", 10.0, 1.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0, account="ira")
    data = client.post("/api/accounts/ira/rename", json={"new_name": "roth"}).get_json()
    assert {a["name"] for a in data["accounts"]} == {settings.DEFAULT_ACCOUNT, "roth"}
    assert database.get_portfolio_with_shares(account="roth")[0]["ticker"] == "AAPL"

    response = client.post(
        "/api/accounts/roth/rename", json={"new_name": settings.DEFAULT_ACCOUNT}
    )
    assert response.status_code == 400
    assert "already exists" in response.get_json()["error"]
    assert (
        client.post("/api/accounts/nope/rename", json={"new_name": "x"}).status_code
        == 400
    )


# ── Portfolio form routes ─────────────────────────────────────────


def test_add_same_ticker_to_a_second_account(client, database, offline_prices):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    _store_metrics(database, "AAPL")
    for account in (settings.DEFAULT_ACCOUNT, "roth"):
        client.post(
            "/add_stock",
            data={
                "ticker": "aapl",
                "shares": "10",
                "cost_basis": "1000",
                "account": account,
            },
        )
    held = {(p["account"], p["ticker"]) for p in database.get_portfolio_with_shares()}
    assert held == {(settings.DEFAULT_ACCOUNT, "AAPL"), ("roth", "AAPL")}

    response = client.post(
        "/add_stock",
        data={"ticker": "AAPL", "shares": "1", "account": "roth"},
        follow_redirects=True,
    )
    assert "AAPL is already in the roth account." in response.get_data(as_text=True)


def test_short_in_long_only_account_flashes_the_reason(
    client, database, offline_prices
):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    _store_metrics(database, "XOM")
    response = client.post(
        "/add_stock",
        data={"ticker": "XOM", "shares": "10", "side": "short", "account": "roth"},
        follow_redirects=True,
    )
    page = response.get_data(as_text=True)
    assert "does not allow short positions" in page
    assert "Invalid share quantity" not in page
    assert database.get_portfolio_with_shares(account="roth") == []

    database.add_portfolio_ticker("XOM", 500.0, 10.0, account="roth")
    response = client.post(
        "/update_shares",
        data={"ticker": "XOM", "shares": "-10", "account": "roth"},
        follow_redirects=True,
    )
    assert "does not allow short positions" in response.get_data(as_text=True)
    assert database.get_portfolio_with_shares(account="roth")[0]["shares"] == 10.0


def test_update_and_delete_use_the_posted_account(client, database, offline_prices):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0, account="roth")
    client.post(
        "/update_shares", data={"ticker": "AAPL", "shares": "7", "account": "roth"}
    )
    client.post(
        "/update_cost_basis",
        data={"ticker": "AAPL", "cost_basis": "777", "account": "roth"},
    )
    roth = database.get_portfolio_with_shares(account="roth")[0]
    assert (roth["shares"], roth["cost_basis"]) == (7.0, 777.0)
    assert (
        database.get_portfolio_with_shares(account=settings.DEFAULT_ACCOUNT)[0][
            "shares"
        ]
        == 1.0
    )

    client.get("/delete_stock/roth/AAPL")
    assert database.get_portfolio_with_shares(account="roth") == []
    assert len(database.get_portfolio_with_shares()) == 1


def test_move_stock_route_moves_to_the_chosen_account(client, database, offline_prices):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    _store_metrics(database, "MRX")
    database.add_portfolio_ticker("MRX", 19150.0, 270.0)

    page = client.get("/portfolio").get_data(as_text=True)
    assert 'name="to_account"' in page

    response = client.post(
        "/move_stock",
        data={
            "ticker": "MRX",
            "account": settings.DEFAULT_ACCOUNT,
            "to_account": "roth",
        },
        follow_redirects=True,
    )
    assert f"Moved MRX from {settings.DEFAULT_ACCOUNT} to roth." in response.get_data(
        as_text=True
    )
    [moved] = database.get_portfolio_with_shares()
    assert (moved["account"], moved["shares"], moved["cost_basis"]) == (
        "roth",
        270.0,
        19150.0,
    )


def test_move_stock_into_long_only_account_flashes_the_reason(client, database):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("XOM", 500.0, -10.0)

    response = client.post(
        "/move_stock",
        data={
            "ticker": "XOM",
            "account": settings.DEFAULT_ACCOUNT,
            "to_account": "roth",
        },
        follow_redirects=True,
    )
    assert "does not allow short positions" in response.get_data(as_text=True)
    assert (
        database.get_portfolio_with_shares()[0]["account"] == settings.DEFAULT_ACCOUNT
    )


def test_legacy_delete_route_removes_from_default_account(client, database):
    database.add_portfolio_ticker("AAPL", 100.0, 1.0)
    response = client.get("/delete_stock/AAPL")
    assert response.status_code == 302
    assert response.headers["Location"].endswith("/portfolio")
    assert database.get_portfolio_with_shares() == []


def test_legacy_delete_finds_the_only_holding_account(client, database):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0)
    database.rename_account(settings.DEFAULT_ACCOUNT, "taxable")
    database.add_portfolio_ticker("MSFT", 100.0, 1.0, account="roth")

    response = client.get("/delete_stock/AAPL", follow_redirects=True)

    assert "AAPL has been removed from the taxable account." in response.get_data(
        as_text=True
    )
    assert [p["ticker"] for p in database.get_portfolio_with_shares()] == ["MSFT"]


def test_legacy_delete_refuses_a_ticker_held_in_several_accounts(client, database):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0, account="roth")

    response = client.get("/delete_stock/AAPL", follow_redirects=True)

    page = response.get_data(as_text=True)
    assert "AAPL is held in several accounts" in page
    assert "delete it from its account on the Portfolio page" in page
    assert len(database.get_portfolio_with_shares()) == 2


def test_deleting_a_missing_position_warns(client, database):
    response = client.get("/delete_stock/AAPL", follow_redirects=True)
    assert "No position AAPL in any account." in response.get_data(as_text=True)

    response = client.get(
        f"/delete_stock/{settings.DEFAULT_ACCOUNT}/AAPL", follow_redirects=True
    )
    page = response.get_data(as_text=True)
    assert f"No position AAPL in account {settings.DEFAULT_ACCOUNT}." in page
    assert "has been removed" not in page


@pytest.mark.parametrize(
    ("route", "field", "value"),
    [("/update_shares", "shares", "7"), ("/update_cost_basis", "cost_basis", "7")],
)
def test_updating_a_missing_position_warns(client, database, route, field, value):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0)

    response = client.post(
        route,
        data={"ticker": "AAPL", field: value, "account": "roth"},
        follow_redirects=True,
    )

    page = response.get_data(as_text=True)
    assert "No position AAPL in account roth." in page
    assert "Updated AAPL" not in page


def test_update_without_an_account_uses_the_only_holding_account(client, database):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    database.add_portfolio_ticker("AAPL", 100.0, 1.0, account="roth")

    client.post("/update_shares", data={"ticker": "AAPL", "shares": "7"})
    assert database.get_portfolio_with_shares(account="roth")[0]["shares"] == 7.0

    database.add_portfolio_ticker("AAPL", 100.0, 1.0)
    response = client.post(
        "/update_shares", data={"ticker": "AAPL", "shares": "9"}, follow_redirects=True
    )
    assert "AAPL is held in several accounts" in response.get_data(as_text=True)
    assert {p["shares"] for p in database.get_portfolio_with_shares()} == {1.0, 7.0}


def test_add_option_goes_to_the_chosen_account(client, database):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    client.post(
        "/add_option",
        data={
            "underlying": "SPY",
            "option_type": "put",
            "strike": "400",
            "expiry": "2027-01-15",
            "contracts": "1",
            "cost_basis": "250",
            "account": "roth",
        },
    )
    assert [o["account"] for o in database.get_option_positions()] == ["roth"]
    response = client.post(
        "/add_option",
        data={"underlying": "SPY", "option_type": "put", "account": "nope"},
        follow_redirects=False,
    )
    assert response.status_code == 302
    assert len(database.get_option_positions()) == 1


# ── Trend page ────────────────────────────────────────────────────


def test_trend_positions_include_shorts_per_account(client, database, offline_prices):
    database.upsert_account("taxable", 0.0, 0.0, allow_short=True)
    database.add_portfolio_ticker(
        "XOM", cost_basis=10_000.0, shares=-100.0, account="taxable"
    )
    database.add_portfolio_ticker("AAPL", cost_basis=9_000.0, shares=100.0)
    database.add_portfolio_ticker(
        "AAPL", cost_basis=4_500.0, shares=50.0, account="taxable"
    )
    database.add_portfolio_ticker("ZERO", cost_basis=0.0, shares=0.0)

    data = client.get("/api/trend/positions?equity=100000").get_json()

    assert data["success"] is True
    by_key = {(p["account"], p["ticker"]): p for p in data["positions"]}
    assert set(by_key) == {
        ("taxable", "XOM"),
        (settings.DEFAULT_ACCOUNT, "AAPL"),
        ("taxable", "AAPL"),
    }
    short = by_key[("taxable", "XOM")]
    assert short["direction"] == "SHORT"
    assert short["shares"] == -100.0
    assert short["entry_price"] == 100.0
    assert short["stop_price"] > short["entry_price"]
    assert [s["ticker"] for s in data["skipped"]] == ["ZERO"]


def test_trend_page_defaults_to_stored_equity(client, database):
    page = client.get("/trend").get_data(as_text=True)
    assert f'value="{int(app_module.DEFAULT_TREND_EQUITY)}"' in page
    database.upsert_account(settings.DEFAULT_ACCOUNT, 123_000.0, 0.0, allow_short=True)
    page = client.get("/trend").get_data(as_text=True)
    assert 'value="123000"' in page


def test_trend_chart_uses_requested_equity(
    client, database, offline_prices, monkeypatch
):
    database.add_portfolio_ticker("AAPL", cost_basis=9_000.0, shares=100.0)
    seen = []
    real = app_module.evaluate_position

    def spy(*args):
        seen.append(args[5])
        return real(*args)

    monkeypatch.setattr(app_module, "evaluate_position", spy)
    response = client.get("/api/trend/chart/AAPL?equity=250000")
    assert response.get_json()["success"] is True
    assert seen == [250_000.0]
