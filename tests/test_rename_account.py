from datetime import date

import pytest

from src.config.settings import settings
from src.data.database import Database


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "rename.db")
    return Database()


def test_rename_moves_positions_options_and_realized_gains(database):
    database.upsert_account("ira", 50_000.0, 1_000.0, allow_short=False)
    database.add_portfolio_ticker(
        "AAPL", cost_basis=1_000.0, shares=10.0, account="ira"
    )
    database.add_portfolio_ticker("MSFT", cost_basis=500.0, shares=2.0, account="ira")
    database.add_option_position(
        "SPY", "put", 400.0, date(2027, 1, 15), 1, 250.0, account="ira"
    )
    database.sell_position("MSFT", 300.0, account="ira")
    database.add_portfolio_ticker("AAPL", cost_basis=100.0, shares=1.0)

    database.rename_account("ira", "roth")

    accounts = {a["name"]: a for a in database.get_accounts()}
    assert "ira" not in accounts
    assert (accounts["roth"]["equity"], accounts["roth"]["cash"]) == (50_000.0, 1_000.0)
    assert accounts["roth"]["allow_short"] is False
    assert [
        p["ticker"] for p in database.get_portfolio_with_shares(account="roth")
    ] == ["AAPL"]
    assert database.get_portfolio_with_shares(account="ira") == []
    assert [o["account"] for o in database.get_option_positions()] == ["roth"]
    assert database.get_ytd_realized_gains(account="roth")["transactions_count"] == 1
    assert database.get_ytd_realized_gains(account="ira")["transactions_count"] == 0
    # Other accounts are untouched
    assert (
        len(database.get_portfolio_with_shares(account=settings.DEFAULT_ACCOUNT)) == 1
    )
    # The renamed account can be deleted only once its positions are gone
    with pytest.raises(ValueError):
        database.delete_account("roth")


def test_rename_refuses_missing_old_or_existing_new(database):
    database.upsert_account("roth", 0.0, 0.0, allow_short=False)
    with pytest.raises(ValueError, match="Unknown account"):
        database.rename_account("nope", "other")
    with pytest.raises(ValueError, match="already exists"):
        database.rename_account("roth", settings.DEFAULT_ACCOUNT)
    with pytest.raises(ValueError, match="must not be empty"):
        database.rename_account("roth", "  ")
    assert {a["name"] for a in database.get_accounts()} == {
        settings.DEFAULT_ACCOUNT,
        "roth",
    }
