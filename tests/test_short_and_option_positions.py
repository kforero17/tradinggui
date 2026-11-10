from datetime import date, timedelta

import pandas as pd
import pytest

from src.analysis.metrics import (
    calculate_period_gains,
    calculate_portfolio_summary,
    option_quote_from_chain_row,
    value_option_position,
)
from src.config.settings import settings
from src.data.database import Database, option_contract_symbol


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "portfolio.db")
    return Database()


def _closes(*prices: float) -> pd.DataFrame:
    index = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=len(prices))
    return pd.DataFrame({"close": prices}, index=index)


def test_short_period_gain_is_positive_when_price_falls():
    gains = calculate_period_gains(_closes(100.0, 90.0), shares=-10)
    assert gains["1D"] == {"gain_dollars": 100.0, "gain_percent": 10.0}


def test_long_period_gain_is_unchanged():
    gains = calculate_period_gains(_closes(100.0, 90.0), shares=10)
    assert gains["1D"] == {"gain_dollars": -100.0, "gain_percent": -10.0}


def test_summary_percentages_use_gross_exposure():
    long_position = {
        "current_value": 1100.0,
        "gains": {"1D": {"gain_dollars": 100.0}, "YTD": {"gain_dollars": 100.0}},
    }
    short_position = {
        "current_value": -900.0,
        "gains": {"1D": {"gain_dollars": 100.0}, "YTD": {"gain_dollars": 100.0}},
    }

    summary = calculate_portfolio_summary([long_position, short_position])

    assert summary["total_value"] == 200.0
    assert summary["day_gain_dollars"] == 200.0
    assert summary["day_gain_percent"] == 10.0  # 200 / (1000 long + 1000 short)


@pytest.mark.parametrize(
    "bid,ask,last,mark", [(4.0, 5.0, 3.0, 4.5), (0.0, 5.0, 3.0, 3.0)]
)
def test_option_mark_prefers_midpoint(bid, ask, last, mark):
    row = pd.Series(
        {
            "bid": bid,
            "ask": ask,
            "lastPrice": last,
            "change": 0.5,
            "impliedVolatility": 0.6,
        }
    )
    assert option_quote_from_chain_row(row)["mark"] == mark


@pytest.mark.parametrize(
    "contracts,cost_basis,expected_value,expected_unrealized,expected_day",
    [
        (2, 1000.0, 1600.0, 600.0, 100.0),  # bought 2 at $5, now $8
        (-2, 1000.0, -1600.0, -600.0, -100.0),  # wrote 2 at $5, now $8
    ],
)
def test_option_valuation(
    contracts, cost_basis, expected_value, expected_unrealized, expected_day
):
    position = {
        "contracts": contracts,
        "cost_basis": cost_basis,
        "expiry": date.today() + timedelta(days=30),
    }
    quote = {"mark": 8.0, "change": 0.5}

    valued = value_option_position(position, quote)

    assert valued["premium_per_share"] == 5.0
    assert valued["current_value"] == expected_value
    assert valued["unrealized"] == expected_unrealized
    assert valued["gains"]["1D"]["gain_dollars"] == expected_day
    assert not valued["expired"]


def test_option_without_quote_is_left_out_of_totals():
    position = {
        "contracts": 1,
        "cost_basis": 500.0,
        "expiry": date.today() - timedelta(days=1),
    }
    valued = value_option_position(position, None)

    assert valued["expired"]
    assert calculate_portfolio_summary([valued])["total_value"] == 0.0


def test_occ_contract_symbol():
    assert (
        option_contract_symbol("SPCX", date(2027, 1, 15), "put", 140.0)
        == "SPCX270115P00140000"
    )
    assert (
        option_contract_symbol("SPCX", date(2027, 1, 15), "call", 142.5)
        == "SPCX270115C00142500"
    )


def test_covering_short_records_gain_against_short_proceeds(database):
    database.add_portfolio_ticker("SPCX", cost_basis=14400.0)
    database.update_portfolio_shares("SPCX", -100)

    realized = database.sell_position("SPCX", sale_price=120.0, year_start_price=None)

    assert realized == pytest.approx(2400.0)
    assert database.get_portfolio_with_shares() == []
    [record] = database.get_ytd_realized_gains()["gains"]
    assert record["shares_sold"] == -100


def test_closing_written_option_records_premium_kept(database):
    expiry = date(2027, 1, 15)
    position_id = database.add_option_position(
        "SPCX", "put", 140.0, expiry, contracts=-2, cost_basis=1000.0
    )

    realized = database.close_option_position(position_id, close_price=1.0)

    assert realized == pytest.approx(
        800.0
    )  # received $5/share, paid $1/share to close, on 200 shares
    assert database.get_option_positions() == []
    [record] = database.get_ytd_realized_gains()["gains"]
    assert record["ticker"] == "SPCX270115P00140000"


def test_closing_long_option_records_loss(database):
    position_id = database.add_option_position(
        "SPCX", "put", 140.0, date(2027, 1, 15), contracts=3, cost_basis=1500.0
    )
    assert database.close_option_position(
        position_id, close_price=0.0
    ) == pytest.approx(-1500.0)


def test_closing_unknown_option_returns_none(database):
    assert database.close_option_position("missing", close_price=1.0) is None
