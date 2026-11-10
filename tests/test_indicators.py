import numpy as np
import pandas as pd
import pytest

from src.analysis.indicators import donchian_high, donchian_low, true_range, turtle_n


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
