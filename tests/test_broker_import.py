from datetime import date

import pytest

from src.config.settings import settings
from src.data.broker_import import (
    BrokerFormat,
    Direction,
    Fill,
    MissingAccountError,
    Side,
    UnsupportedFormatError,
    apply_splits,
    detect_format,
    match_round_trips,
    parse_export,
)
from src.data.database import Database

ETRADE_EXPORT = """Buys & Sells Activity Types

Account Activity for Roth IRA -6524 from Prior Year

Total:,-13692.44

Activity/Trade Date,Transaction Date,Settlement Date,Activity Type,Description,Symbol,Cusip,Quantity #,Price $,Amount $,Commission,Category,Note
04/23/2025,04/23/2025,04/24/2025,Bought To Cover,APPLOVIN CORP UNSOLICITED TRADE,APP,--,20.0,261.294,-5225.88,0.0,--,--
04/07/2025,04/07/2025,04/08/2025,Sold,APPLE INC UNSOLICITED TRADE,AAPL,--,-54.0,175.98,9502.64,0.27,--,--
03/04/2025,03/04/2025,03/05/2025,Sold Short,APPLOVIN CORP UNSOLICITED TRADE,APP,--,-20.0,313.16,6263.02,0.18,--,--



"For all accounts, the Activity Date and the Processing Date are displayed."
"""

FIDELITY_EXPORT = """

Run Date,Account,Account Number,Action,Symbol,Description,Type,Price ($),Quantity,Commission ($),Fees ($),Accrued Interest ($),Amount ($),Settlement Date
10/20/2026,BrokerageLink,652100787,YOU SOLD VANGUARD FTSE EMR MKT ETF (VWO) (Cash),VWO,VANGUARD FTSE EMR MKT ETF,Cash,58.28,"-0.381","","","",22.2,10/21/2026
10/20/2026,BrokerageLink,652100787,YOU SOLD VANGUARD FTSE EMR MKT ETF (VWO) (Cash),VWO,VANGUARD FTSE EMR MKT ETF,Cash,58.28,"-204","",0.25,"",11887.85,10/21/2026
07/10/2026,BrokerageLink,652100787,YOU BOUGHT VANGUARD FTSE EMR MKT ETF (VWO) (Cash),VWO,VANGUARD FTSE EMR MKT ETF,Cash,59.67,204.381,"","","","-12171.66",07/13/2026
05/18/2023,BrokerageLink,652100787,YOU SOLD STEM INC COM 1 FOR 20 R/S (85859N102) (Cash),"",STEM INC COM,Cash,4.14,"-175","",0.0,"",724.5,05/22/2023

The data and information in this spreadsheet is provided to you solely for your use.
"Date downloaded 11/06/2026 08:56 am"
"""


def _fill(
    day: int, side: Side, quantity: float, price: float, symbol: str = "XYZ"
) -> Fill:
    return Fill("Acct", date(2025, 1, day), symbol, side, quantity, price)


def test_detects_both_formats_and_rejects_others():
    assert detect_format(ETRADE_EXPORT) is BrokerFormat.ETRADE
    assert detect_format(FIDELITY_EXPORT) is BrokerFormat.FIDELITY
    with pytest.raises(UnsupportedFormatError):
        detect_format("a,b\n1,2")


def test_parses_etrade_account_sides_and_fees():
    fills = parse_export(ETRADE_EXPORT)
    assert {f.account for f in fills} == {"Roth IRA -6524"}
    assert [(f.symbol, f.side, f.quantity) for f in fills] == [
        ("APP", Side.COVER, 20.0),
        ("AAPL", Side.SELL, 54.0),
        ("APP", Side.SHORT, 20.0),
    ]
    assert fills[1].fees == 0.27


def test_parses_fidelity_quoted_fractional_quantities_and_skips_symbol_less_rows():
    fills = parse_export(FIDELITY_EXPORT)
    assert [f.quantity for f in fills] == [0.381, 204.0, 204.381]
    assert fills[0].account == "BrokerageLink -0787"
    assert fills[1].fees == 0.25


def test_whole_and_fractional_sale_on_one_day_is_one_round_trip():
    trips, open_positions = match_round_trips(parse_export(FIDELITY_EXPORT))
    assert len(trips) == 1 and not open_positions
    assert trips[0].quantity == pytest.approx(204.381)
    assert trips[0].basis_known


def test_fifo_matches_partial_sells_across_lots():
    fills = [
        _fill(1, Side.BUY, 10, 100.0),
        _fill(2, Side.BUY, 10, 110.0),
        _fill(3, Side.SELL, 15, 120.0),
        _fill(4, Side.SELL, 5, 90.0),
    ]
    trips, open_positions = match_round_trips(fills)
    assert len(trips) == 1 and not open_positions
    trip = trips[0]
    assert trip.entry_date == date(2025, 1, 1)
    assert trip.exit_date == date(2025, 1, 4)
    # Cost 10*100 + 10*110 = 2100; proceeds 15*120 + 5*90 = 2250
    assert trip.pnl == pytest.approx(150.0)


def test_short_and_cover_form_a_short_round_trip():
    trips, _ = match_round_trips(parse_export(ETRADE_EXPORT))
    app = next(t for t in trips if t.symbol == "APP")
    assert app.direction is Direction.SHORT
    assert app.pnl == pytest.approx((313.16 - 261.294) * 20 - 0.18)


def test_sell_without_opening_fill_is_marked_basis_unknown():
    trips, _ = match_round_trips(
        [_fill(1, Side.BUY, 5, 10.0), _fill(2, Side.SELL, 8, 12.0)]
    )
    assert trips[0].quantity == 5
    assert trips[0].unmatched_quantity == 3
    assert not trips[0].basis_known


def test_open_lots_are_reported_as_open_positions():
    trips, open_positions = match_round_trips(
        [_fill(1, Side.BUY, 10, 10.0), _fill(2, Side.SELL, 4, 12.0)]
    )
    assert not trips
    assert open_positions[0].quantity == 6


def test_splits_restate_earlier_fills_on_todays_share_basis():
    fills = [
        _fill(1, Side.BUY, 10, 600.0, "NVDA"),
        _fill(20, Side.SELL, 100, 70.0, "NVDA"),
    ]
    adjusted = apply_splits(fills, {"NVDA": [(date(2025, 1, 10), 10.0)]})
    trips, open_positions = match_round_trips(adjusted)
    assert not open_positions and trips[0].basis_known
    assert trips[0].entry_price == pytest.approx(60.0)
    assert trips[0].pnl == pytest.approx(1000.0)


def test_database_skips_already_imported_fills(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "fills.db")
    database = Database()
    fills = parse_export(ETRADE_EXPORT)
    assert database.add_broker_fills(fills) == 3
    assert database.add_broker_fills(fills) == 0
    assert sorted(f.dedupe_key for f in database.get_broker_fills()) == sorted(
        f.dedupe_key for f in fills
    )


ETRADE_TRADES_EXPORT = """Trade Date,Order Type,Security,Cusip,Transaction Description,Quantity,Executed Price,Commission,Net Amount
1/24/2024,Sell,FREY,35834F104,"FREYR BATTERY INC NEW UNSOLICITED TRADE",75,1.405,0.01,105.36
2/15/2024,Buy,AAPL,037833100,"APPLE INC DIVIDEND REINVESTMENT",0.072,183.77661,0.0000,13.08
9/25/2024,Buy,AHR,398182303,"AMERICAN HEALTHCARE REIT INC UNSOLICITED TRADE",150,26.5451,0.0000,3981.77
11/13/2024,Sell,AHR,398182303,"AMERICAN HEALTHCARE REIT INC UNSOLICITED TRADE",150,26.685,0.12,4002.61


"""


def test_parses_etrade_trade_download_into_the_supplied_account():
    assert detect_format(ETRADE_TRADES_EXPORT) is BrokerFormat.ETRADE_TRADES
    fills = parse_export(
        ETRADE_TRADES_EXPORT, "tradesdownload.csv", account="Individual Brokerage -7066"
    )
    assert {f.account for f in fills} == {"Individual Brokerage -7066"}
    assert [(f.trade_date, f.symbol, f.side, f.quantity) for f in fills] == [
        (date(2024, 1, 24), "FREY", Side.SELL, 75.0),
        (date(2024, 2, 15), "AAPL", Side.BUY, 0.072),
        (date(2024, 9, 25), "AHR", Side.BUY, 150.0),
        (date(2024, 11, 13), "AHR", Side.SELL, 150.0),
    ]
    ahr = next(t for t in match_round_trips(fills)[0] if t.symbol == "AHR")
    assert ahr.pnl == pytest.approx((26.685 - 26.5451) * 150 - 0.12)


def test_etrade_trade_download_without_an_account_is_rejected():
    with pytest.raises(MissingAccountError):
        parse_export(ETRADE_TRADES_EXPORT)
