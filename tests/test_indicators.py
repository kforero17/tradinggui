import numpy as np
import pandas as pd
import pytest

from src.analysis.indicators import (
    adx,
    donchian_high,
    donchian_low,
    pct_from_high,
    pct_from_low,
    rate_of_change,
    true_range,
    turtle_n,
)


def _ohlc(rows: list[tuple[float, float, float, float]]) -> pd.DataFrame:
    index = pd.bdate_range("2024-01-01", periods=len(rows))
    return pd.DataFrame(rows, index=index, columns=["open", "high", "low", "close"])


def test_true_range_uses_previous_close_on_gaps():
    bars = _ohlc([(10, 11, 9, 10), (14, 15, 13, 14), (8, 9, 7, 8)])
    assert true_range(bars).tolist() == [2.0, 5.0, 7.0]


def test_turtle_n_seeds_with_simple_average_then_smooths():
    rows = [(10, 11, 9, 10)] * 3 + [(10, 14, 10, 12)]
    n = turtle_n(_ohlc(rows), period=3)
    assert np.isnan(n.iloc[1])
    assert n.iloc[2] == pytest.approx(2.0)
    # (2 * 2 + 4) / 3: the book's N = ((period - 1) * PDN + TR) / period
    assert n.iloc[3] == pytest.approx(8 / 3)


def test_donchian_channels_exclude_the_current_bar():
    bars = _ohlc([(10, 11, 9, 10), (10, 12, 8, 10), (10, 20, 1, 10)])
    assert donchian_high(bars, 2).iloc[2] == 12
    assert donchian_low(bars, 2).iloc[2] == 8


def test_adx_is_high_in_a_steady_trend_and_low_in_a_choppy_range():
    trend = _ohlc([(100 + i, 101 + i, 99 + i, 100 + i) for i in range(80)])
    chop = _ohlc(
        [(100, 101, 99, 100) if i % 2 else (102, 103, 101, 102) for i in range(80)]
    )
    trend_adx, chop_adx = adx(trend), adx(chop)
    # Wilder needs about two periods before the first value.
    assert trend_adx.iloc[:20].isna().all()
    assert trend_adx.index.equals(trend.index)
    assert trend_adx.iloc[-1] > 50
    assert chop_adx.iloc[-1] < 20
    # A range that turns into a steady trend: ADX climbs from low to high.
    breakout = _ohlc(
        [tuple(float(v) for v in row) for row in chop.to_numpy()[:60]]
        + [(100 + i, 101 + i, 99 + i, 100 + i) for i in range(3, 63)]
    )
    rising = adx(breakout)
    assert rising.iloc[59] < 20
    assert rising.iloc[-1] > 50
    assert rising.iloc[-1] > rising.iloc[80] > rising.iloc[59]


def test_adx_is_zero_on_perfectly_flat_bars():
    flat = _ohlc([(100, 101, 99, 100)] * 40)
    assert adx(flat).iloc[-1] == pytest.approx(0.0)


def test_rate_of_change_is_fractional():
    close = pd.Series([100.0, 110.0, 121.0])
    assert np.isnan(rate_of_change(close, 1).iloc[0])
    assert rate_of_change(close, 1).iloc[2] == pytest.approx(0.10)
    assert rate_of_change(close, 2).iloc[2] == pytest.approx(0.21)


def test_pct_from_high_and_low_measure_against_the_window_including_today():
    bars = _ohlc(
        [(100, 110, 95, 100), (100, 105, 90, 99), (99, 100, 98, 99), (99, 120, 98, 120)]
    )
    from_high = pct_from_high(bars, period=3)
    from_low = pct_from_low(bars, period=3)
    assert np.isnan(from_high.iloc[1])
    assert from_high.iloc[2] == pytest.approx(99 / 110 - 1)
    assert from_low.iloc[2] == pytest.approx(99 / 90 - 1)
    # A close at a new high is 0% from it; the window has rolled past the 90 low.
    assert from_high.iloc[3] == pytest.approx(0.0)
    assert from_low.iloc[3] == pytest.approx(120 / 90 - 1)
    assert (from_high.dropna() <= 0).all()
    assert (from_low.dropna() >= 0).all()
