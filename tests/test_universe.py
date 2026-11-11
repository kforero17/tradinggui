import math

import pandas as pd
import pytest

from src.analysis.universe import (
    is_leveraged_or_inverse,
    liquid_universe,
    with_required,
)
from src.data.ticker_loader import TickerInfo


def _info(symbol: str, name: str = "", is_etf: bool = False) -> TickerInfo:
    return TickerInfo(symbol, name or f"{symbol} Inc.", "NASDAQ", is_etf, False)


def _metrics(rows: list[tuple[str, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["ticker", "last_price", "avg_dollar_volume_50d"])


@pytest.mark.parametrize(
    "name",
    [
        "ProShares UltraPro QQQ",
        "ProShares UltraShort S&P500",
        "ProShares Ultra Bloomberg Crude Oil",
        "ProShares Short S&P500",
        "Direxion Daily Semiconductor Bull 3X Shares",
        "Direxion Daily S&P 500 Bear 1X Shares",
        "MicroSectors FANG+ Index -3X Inverse Leveraged ETNs",
        "GraniteShares 2x Long NVDA Daily ETF",
        "AXS 1.25X NVDA Bear Daily ETF",
        "AdvisorShares MSOS Daily Leveraged ETF",
        "Leverage Shares MANGOS ETF",
        "UltraPro Short Dow30",
    ],
)
def test_leveraged_and_inverse_names_match(name):
    assert is_leveraged_or_inverse(name)


@pytest.mark.parametrize(
    "name",
    [
        "SPDR S&P 500 ETF Trust",
        "Invesco QQQ Trust, Series 1",
        "iShares Short Treasury Bond ETF",
        "JPMorgan Ultra-Short Income ETF",
        "iShares Ultra Short Duration Bond Active ETF",
        "Angel Oak UltraShort Income ETF",
        "EA Bridgeway Ultra-Small Company Market ETF",
        "Innovator U.S. Equity Ultra Buffer ETF - April",
        "Invesco BulletShares 2026 Corporate Bond ETF",
        "State Street SPDR S&P Leveraged Loan ETF",
        "TrueShares Quarterly Bull Hedge ETF",
        "First Trust Long/Short Equity",
        "",
    ],
)
def test_unlevered_names_do_not_match(name):
    assert not is_leveraged_or_inverse(name)


def test_liquidity_thresholds_are_inclusive_and_sorted_by_dollar_volume():
    universe = {t: _info(t) for t in ("AAA", "BBB", "CCC", "DDD", "EEE")}
    metrics = _metrics(
        [
            ("AAA", 5.0, 10_000_000),  # exactly at both floors
            ("BBB", 4.99, 50_000_000),  # too cheap
            ("CCC", 50.0, 9_999_999),  # too illiquid
            ("DDD", 20.0, 80_000_000),
            ("EEE", 30.0, 20_000_000),
        ]
    )
    assert liquid_universe(
        metrics, universe, min_price=5, min_dollar_volume=10_000_000
    ) == ["DDD", "EEE", "AAA"]


def test_missing_or_nan_liquidity_is_excluded():
    universe = {t: _info(t) for t in ("AAA", "BBB", "CCC")}
    metrics = _metrics(
        [("AAA", 10.0, math.nan), ("BBB", None, 20_000_000), ("CCC", 10.0, 20_000_000)]
    )
    assert liquid_universe(metrics, universe, min_price=5, min_dollar_volume=1) == [
        "CCC"
    ]
    no_column = metrics.drop(columns=["avg_dollar_volume_50d"])
    assert liquid_universe(no_column, universe, min_price=5, min_dollar_volume=1) == []


def test_tickers_outside_the_universe_are_dropped():
    metrics = _metrics([("GONE", 10.0, 20_000_000), ("AAA", 10.0, 20_000_000)])
    assert liquid_universe(
        metrics, {"AAA": _info("AAA")}, min_price=5, min_dollar_volume=1
    ) == ["AAA"]


def test_leveraged_filter_applies_to_etfs_only_and_can_be_disabled():
    universe = {
        "TQQQ": _info("TQQQ", "ProShares UltraPro QQQ", is_etf=True),
        "SPY": _info("SPY", "SPDR S&P 500 ETF Trust", is_etf=True),
        # An operating company whose name looks like a leverage multiple.
        "TXG": _info("TXG", "10x Genomics, Inc. Class A Common Stock"),
    }
    metrics = _metrics([("TQQQ", 80.0, 3e9), ("SPY", 600.0, 3e10), ("TXG", 12.0, 5e7)])
    kept = liquid_universe(metrics, universe, min_price=5, min_dollar_volume=1)
    assert kept == ["SPY", "TXG"]
    everything = liquid_universe(
        metrics, universe, min_price=5, min_dollar_volume=1, exclude_leveraged=False
    )
    assert everything == ["SPY", "TQQQ", "TXG"]


def test_with_required_is_an_order_preserving_union():
    assert with_required(
        ["AAPL", "MSFT"], ["msft", " TSLA "], ["NVDA", "AAPL", ""]
    ) == [
        "AAPL",
        "MSFT",
        "TSLA",
        "NVDA",
    ]
