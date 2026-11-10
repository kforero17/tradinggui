import sqlite3
from datetime import datetime

import pytest

from src.config.settings import settings
from src.data.database import Database


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "accounts.db")
    return Database()


def _portfolio_columns(db_path) -> list[str]:
    with sqlite3.connect(db_path) as conn:
        return [row[1] for row in conn.execute("PRAGMA table_info(portfolio)")]


def _build_old_schema_db(db_path) -> None:
    """A pre-accounts DB: ticker-keyed portfolio (columns in the order older DBs have them)."""
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE portfolio (
            ticker VARCHAR NOT NULL, shares REAL DEFAULT 0.0, added_at TIMESTAMP,
            cost_basis REAL DEFAULT 0.0, PRIMARY KEY (ticker));
        INSERT INTO portfolio VALUES ('META', 42, '2026-10-08 14:17:46.637744', 30340.0);
        INSERT INTO portfolio VALUES ('SPCX', -100, '2026-03-01 10:00:00.000000', 14400.0);
        CREATE TABLE realized_gains (
            id VARCHAR NOT NULL, ticker VARCHAR NOT NULL, shares_sold FLOAT NOT NULL,
            cost_basis FLOAT NOT NULL, sale_price FLOAT NOT NULL,
            realized_gain FLOAT NOT NULL, sold_at DATETIME, year_start_price REAL,
            PRIMARY KEY (id));
        INSERT INTO realized_gains VALUES
            ('g1', 'IBIT', 10, 50.0, 55.0, 50.0, '2026-01-23 16:31:05', NULL);
        """
    )
    conn.commit()
    conn.close()


def test_migration_moves_old_portfolio_rows_to_default_account(tmp_path, monkeypatch):
    db_path = tmp_path / "old.db"
    _build_old_schema_db(db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)

    database = Database()

    positions = {p["ticker"]: p for p in database.get_portfolio_with_shares()}
    assert positions["META"] == {
        "account": settings.DEFAULT_ACCOUNT,
        "ticker": "META",
        "shares": 42.0,
        "cost_basis": 30340.0,
        "added_at": datetime(2026, 10, 8, 14, 17, 46, 637744),
    }
    assert positions["SPCX"]["shares"] == -100.0
    assert positions["SPCX"]["account"] == settings.DEFAULT_ACCOUNT
    assert _portfolio_columns(db_path)[:2] == ["account", "ticker"]

    backup = tmp_path / "old.db.pre-accounts.bak"
    assert backup.exists()
    assert "account" not in _portfolio_columns(backup)
    with sqlite3.connect(backup) as conn:
        assert conn.execute("SELECT COUNT(*) FROM portfolio").fetchone()[0] == 2

    [gain] = database.get_ytd_realized_gains(account=settings.DEFAULT_ACCOUNT)["gains"]
    assert gain["ticker"] == "IBIT"


def test_migration_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "old.db"
    _build_old_schema_db(db_path)
    monkeypatch.setattr(settings, "DB_PATH", db_path)
    first = Database().get_portfolio_with_shares()
    backup = tmp_path / "old.db.pre-accounts.bak"
    backup.unlink()

    second = Database()

    assert second.get_portfolio_with_shares() == first
    assert not backup.exists()
    assert [a["name"] for a in second.get_accounts()] == [settings.DEFAULT_ACCOUNT]


def test_fresh_db_gets_new_schema_without_backup(tmp_path, database):
    assert _portfolio_columns(tmp_path / "accounts.db")[:2] == ["account", "ticker"]
    assert not (tmp_path / "accounts.db.pre-accounts.bak").exists()
    [account] = database.get_accounts()
    assert account["name"] == settings.DEFAULT_ACCOUNT
    assert account["equity"] == 0.0


def test_same_ticker_in_two_accounts(database):
    database.upsert_account("roth", equity=50_000, cash=10_000, allow_short=False)
    database.add_portfolio_ticker("AAPL", cost_basis=1000.0, shares=10)
    database.add_portfolio_ticker("AAPL", cost_basis=2200.0, shares=20, account="roth")

    database.update_portfolio_shares("AAPL", 25, account="roth")
    database.update_portfolio_cost_basis("AAPL", 2700.0, account="roth")

    positions = {
        (p["account"], p["ticker"]): p for p in database.get_portfolio_with_shares()
    }
    assert positions[(settings.DEFAULT_ACCOUNT, "AAPL")]["shares"] == 10
    assert positions[("roth", "AAPL")]["shares"] == 25
    assert positions[("roth", "AAPL")]["cost_basis"] == 2700.0
    assert database.get_portfolio_tickers() == ["AAPL"]
    assert [p["account"] for p in database.get_portfolio_with_shares("roth")] == [
        "roth"
    ]

    realized = database.sell_position("AAPL", sale_price=120.0, account="roth")

    assert realized == pytest.approx((120.0 - 108.0) * 25)
    [remaining] = database.get_portfolio_with_shares()
    assert remaining["account"] == settings.DEFAULT_ACCOUNT
    [gain] = database.get_ytd_realized_gains()["gains"]
    assert gain["account"] == "roth"

    database.delete_portfolio_ticker("AAPL")
    assert database.get_portfolio_with_shares() == []


def test_short_refused_in_long_only_account(database):
    database.upsert_account("roth", equity=50_000, cash=10_000, allow_short=False)

    with pytest.raises(ValueError, match="does not allow short"):
        database.add_portfolio_ticker(
            "TSLA", cost_basis=5000.0, shares=-10, account="roth"
        )
    assert database.get_portfolio_with_shares("roth") == []

    database.add_portfolio_ticker("TSLA", cost_basis=5000.0, shares=10, account="roth")
    with pytest.raises(ValueError, match="does not allow short"):
        database.update_portfolio_shares("TSLA", -5, account="roth")
    assert database.get_portfolio_with_shares("roth")[0]["shares"] == 10


def test_short_can_shrink_but_not_grow_once_shorting_is_turned_off(database):
    database.upsert_account("taxable", equity=80_000, cash=20_000, allow_short=True)
    database.add_portfolio_ticker(
        "SPCX", cost_basis=14400.0, shares=-100, account="taxable"
    )
    database.upsert_account("taxable", equity=80_000, cash=20_000, allow_short=False)

    database.update_portfolio_shares("SPCX", -50, account="taxable")
    with pytest.raises(ValueError):
        database.update_portfolio_shares("SPCX", -150, account="taxable")

    assert database.get_portfolio_with_shares("taxable")[0]["shares"] == -50


def test_unknown_account_is_refused(database):
    with pytest.raises(ValueError, match="Unknown account"):
        database.add_portfolio_ticker("AAPL", shares=1, account="nope")


def test_account_crud_and_total_equity(database):
    database.upsert_account("roth", equity=50_000, cash=10_000, allow_short=False)
    database.upsert_account("taxable", equity=80_000, cash=20_000, allow_short=True)
    database.upsert_account("roth", equity=55_000, cash=12_000, allow_short=False)

    accounts = {a["name"]: a for a in database.get_accounts()}
    assert set(accounts) == {settings.DEFAULT_ACCOUNT, "roth", "taxable"}
    assert accounts["roth"]["equity"] == 55_000
    assert accounts["roth"]["cash"] == 12_000
    assert accounts["taxable"]["allow_short"] is True
    assert database.total_equity() == pytest.approx(135_000)

    database.add_portfolio_ticker("AAPL", shares=1, account="roth")
    with pytest.raises(ValueError, match="still holds"):
        database.delete_account("roth")

    database.delete_portfolio_ticker("AAPL", account="roth")
    database.delete_account("roth")
    assert "roth" not in {a["name"] for a in database.get_accounts()}
    assert database.total_equity() == pytest.approx(80_000)


def test_option_positions_carry_account(database):
    database.upsert_account("taxable", equity=80_000, cash=20_000, allow_short=True)
    position_id = database.add_option_position(
        "SPCX",
        "put",
        140.0,
        datetime(2027, 1, 15).date(),
        -2,
        1000.0,
        account="taxable",
    )

    assert database.get_option_positions("taxable")[0]["account"] == "taxable"
    assert database.get_option_positions(settings.DEFAULT_ACCOUNT) == []
    with pytest.raises(ValueError, match="still holds"):
        database.delete_account("taxable")

    database.close_option_position(position_id, close_price=0.0)
    [gain] = database.get_ytd_realized_gains(account="taxable")["gains"]
    assert gain["realized_gain"] == pytest.approx(1000.0)
