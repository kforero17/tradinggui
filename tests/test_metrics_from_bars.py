from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from tenacity import wait_none

from src.analysis import metrics
from src.analysis.metrics import (
    FUNDAMENTALS_KEYS,
    StockMetricsCalculator,
    calculate_bollinger_bands,
    calculate_macd,
    calculate_rsi,
    compute_technical_metrics,
    fetch_fundamentals,
    select_fundamentals_due,
)
from src.config.settings import settings

TECHNICAL_KEYS = {
    "last_price",
    "ma_100",
    "ema_100",
    "pct_above_ma_100",
    "pct_above_ema_100",
    "rsi_14",
    "macd",
    "macd_signal",
    "macd_histogram",
    "bb_upper",
    "bb_middle",
    "bb_lower",
}


def _bars(n: int = 300, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    close = 100 * np.cumprod(1 + rng.normal(0.0005, 0.02, n))
    volume = rng.integers(100_000, 2_000_000, n).astype(float)
    index = pd.bdate_range("2025-01-02", periods=n)
    return pd.DataFrame(
        {
            "open": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": volume,
            "stock_splits": 0.0,
        },
        index=index,
    )


def _old_momentum_metrics(hist_data: pd.DataFrame) -> dict:
    """StockMetricsCalculator.calculate_momentum_metrics as it was before compute_technical_metrics."""
    close_prices = hist_data["close"]
    ma_100 = close_prices.rolling(window=100).mean().iloc[-1]
    ema_100 = close_prices.ewm(span=100, adjust=False).mean().iloc[-1]
    last_price = close_prices.iloc[-1]
    return {
        "last_price": last_price,
        "ma_100": ma_100,
        "ema_100": ema_100,
        "pct_above_ma_100": (last_price - ma_100) / ma_100 * 100 if ma_100 else 0,
        "pct_above_ema_100": (last_price - ema_100) / ema_100 * 100 if ema_100 else 0,
        "rsi_14": calculate_rsi(close_prices, period=14),
        **calculate_macd(close_prices),
        **calculate_bollinger_bands(close_prices),
    }


# ── compute_technical_metrics ─────────────────────────────────────────


def test_matches_the_old_per_ticker_metrics():
    bars = _bars()
    row = compute_technical_metrics("AAPL", bars)

    expected = _old_momentum_metrics(bars)
    assert row["ticker"] == "AAPL"
    assert TECHNICAL_KEYS <= row.keys()
    for key, value in expected.items():
        assert row[key] == pytest.approx(value, rel=1e-12), key
    assert isinstance(row["updated_at"], datetime)


def test_keys_are_stock_metrics_columns():
    from src.data.database import StockMetrics

    row = compute_technical_metrics("AAPL", _bars())
    assert row.keys() <= set(StockMetrics.__table__.columns.keys())
    assert not row.keys() & set(FUNDAMENTALS_KEYS)


def test_avg_dollar_volume_uses_the_liquidity_lookback():
    bars = _bars()
    lookback = settings.UNIVERSE_LIQUIDITY_LOOKBACK_DAYS
    tail = bars.tail(lookback)
    expected = float((tail["close"] * tail["volume"]).mean())

    row = compute_technical_metrics("AAPL", bars)
    assert row["avg_dollar_volume_50d"] == pytest.approx(expected)
    # Older bars don't count
    assert row["avg_dollar_volume_50d"] != pytest.approx(
        float((bars["close"] * bars["volume"]).mean())
    )


def test_none_for_short_history():
    assert compute_technical_metrics("NEW", _bars(n=99)) is None
    assert compute_technical_metrics("NEW", _bars(n=100)) is not None
    assert compute_technical_metrics("NEW", None) is None


def test_nan_closes_do_not_count_as_history():
    bars = _bars(n=105)
    bars.iloc[:10, bars.columns.get_loc("close")] = np.nan
    assert compute_technical_metrics("GAPPY", bars) is None


def test_get_metrics_shape_is_unchanged(monkeypatch):
    calc = StockMetricsCalculator()
    monkeypatch.setattr(calc, "_get_historical_data", lambda ticker: _bars())
    monkeypatch.setattr(
        metrics,
        "fetch_fundamentals",
        lambda ticker: {
            **dict.fromkeys(FUNDAMENTALS_KEYS),
            "pe_ratio": 20.0,
            "fundamentals_updated_at": datetime(2026, 11, 1),
        },
    )
    result = calc.get_metrics("AAPL")
    assert result["ticker"] == "AAPL"
    assert (TECHNICAL_KEYS | set(FUNDAMENTALS_KEYS)) <= result.keys()
    assert result["pe_ratio"] == 20.0


# ── fetch_fundamentals ────────────────────────────────────────────────


class _NoWait:
    def wait(self) -> None:
        pass


class _FakeTicker:
    def __init__(self, info):
        self._info = info

    @property
    def info(self):
        if isinstance(self._info, Exception):
            raise self._info
        return self._info


@pytest.fixture
def fake_info(monkeypatch):
    infos: dict[str, object] = {}
    calls: list[str] = []

    def ticker(symbol):
        calls.append(symbol)
        return _FakeTicker(infos[symbol])

    monkeypatch.setattr(metrics, "_fundamentals_rate_limiter", _NoWait())
    monkeypatch.setattr(metrics._fetch_info.retry, "wait", wait_none())
    monkeypatch.setattr(metrics.yf, "Ticker", ticker)
    return infos, calls


def test_fetch_fundamentals_maps_info(fake_info):
    infos, _ = fake_info
    infos["AAPL"] = {
        "quoteType": "EQUITY",
        "marketCap": 3e12,
        "trailingPE": 30.0,
        "priceToBook": 40.0,
        "priceToSalesTrailing12Months": 8.0,
        "enterpriseValue": 3.1e12,
        "ebitda": 1.3e11,
        "sector": "Technology",
        "industry": "Consumer Electronics",
    }
    result = fetch_fundamentals("AAPL")
    assert result["pe_ratio"] == 30.0
    assert result["ebitda_ev"] == pytest.approx(3.1e12 / 1.3e11)
    assert result["sector"] == "Technology"
    assert isinstance(result["fundamentals_updated_at"], datetime)
    assert result.keys() == {*FUNDAMENTALS_KEYS, "fundamentals_updated_at"}


def test_fetch_fundamentals_for_an_etf_keeps_only_market_cap(fake_info):
    infos, _ = fake_info
    infos["SPY"] = {"quoteType": "ETF", "marketCap": 5e11, "trailingPE": 25.0}
    result = fetch_fundamentals("SPY")
    assert result["market_cap"] == 5e11
    assert result["pe_ratio"] is None


def test_fetch_fundamentals_returns_none_when_retries_run_out(fake_info):
    infos, calls = fake_info
    infos["AAPL"] = Exception("Too Many Requests")
    assert fetch_fundamentals("AAPL") is None
    assert calls == ["AAPL"] * 3


def test_fetch_fundamentals_returns_none_on_other_errors_and_empty_info(fake_info):
    infos, calls = fake_info
    infos["BAD"] = KeyError("boom")
    infos["EMPTY"] = {}
    assert fetch_fundamentals("BAD") is None
    assert fetch_fundamentals("EMPTY") is None
    assert calls == ["BAD", "EMPTY"]  # not retried


@pytest.mark.parametrize(
    "info",
    [
        {"trailingPegRatio": None},  # Yahoo's known degraded reply
        {"marketCap": 3e12, "trailingPE": 30.0},  # no quoteType
        {"quoteType": "EQUITY", "trailingPegRatio": None, "marketCap": None},
        {"quoteType": "ETF", "trailingPE": 25.0},  # nothing an ETF maps
    ],
)
def test_fetch_fundamentals_returns_none_for_degraded_info(fake_info, info):
    infos, calls = fake_info
    infos["AAPL"] = info
    assert fetch_fundamentals("AAPL") is None
    assert calls == ["AAPL"]  # not retried


def test_get_metrics_keeps_technicals_when_fundamentals_fail(monkeypatch, fake_info):
    """Regression: a RetryError from .info used to drop the whole ticker."""
    infos, _ = fake_info
    infos["AAPL"] = Exception("Rate limited")
    calc = StockMetricsCalculator()
    monkeypatch.setattr(calc, "_get_historical_data", lambda ticker: _bars())

    result = calc.get_metrics("AAPL")
    assert result is not None
    assert result["last_price"] == pytest.approx(_bars()["close"].iloc[-1])
    assert all(result[key] is None for key in FUNDAMENTALS_KEYS)
    assert "fundamentals_updated_at" not in result


# ── select_fundamentals_due ───────────────────────────────────────────

NOW = datetime(2026, 11, 7, 6, 0)


def test_etfs_are_never_due_unless_held():
    due = select_fundamentals_due(
        {"SPY": True, "AAPL": False}, {}, [], max_age_days=28, now=NOW
    )
    assert due == ["AAPL"]


def test_holdings_are_always_due():
    ages = {"MSFT": NOW - timedelta(days=1), "AAPL": NOW - timedelta(days=1)}
    due = select_fundamentals_due(
        {"AAPL": False}, ages, ["MSFT", "QQQ"], max_age_days=28, now=NOW
    )
    # Held tickers come first, even fresh, illiquid or not in the candidates
    assert due == ["QQQ", "MSFT"]


def test_only_stale_candidates_are_due():
    ages = {
        "FRESH": NOW - timedelta(days=27),
        # Less than 28 x 24h ago, but 28 calendar days back
        "EDGE": NOW - timedelta(days=27, hours=7),
        "OLD": NOW - timedelta(days=60),
        "NEVER": None,
    }
    candidates = dict.fromkeys(["FRESH", "EDGE", "OLD", "NEVER", "NEW"], False)
    due = select_fundamentals_due(candidates, ages, [], max_age_days=28, now=NOW)
    assert set(due) == {"EDGE", "OLD", "NEVER", "NEW"}


def test_oldest_first():
    ages = {
        "A": NOW - timedelta(days=30),
        "B": NOW - timedelta(days=90),
        "C": NOW - timedelta(days=45),
        "D": None,
    }
    candidates = dict.fromkeys(["A", "B", "C", "D", "E"], False)
    due = select_fundamentals_due(candidates, ages, [], max_age_days=28, now=NOW)
    assert due == ["D", "E", "B", "C", "A"]


def test_non_candidates_are_not_due():
    ages = {"ILLIQUID": None}
    assert select_fundamentals_due({}, ages, [], max_age_days=28, now=NOW) == []
