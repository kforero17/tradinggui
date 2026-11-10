"""Series-returning technical indicators for trend-following rules.

Every function takes a daily OHLC frame with lowercase ``open/high/low/close``
columns (the shape returned by ``src.data.price_history.get_daily_ohlc``) and
returns a Series aligned to its index.
"""

import pandas as pd

__all__ = ["true_range", "turtle_n", "ema", "donchian_high", "donchian_low"]

TURTLE_N_PERIOD = 20


def true_range(ohlc: pd.DataFrame) -> pd.Series:
    """max(H - L, H - PDC, PDC - L); the first bar has no prior close so uses H - L."""
    previous_close = ohlc["close"].shift(1)
    ranges = pd.concat(
        [
            ohlc["high"] - ohlc["low"],
            (ohlc["high"] - previous_close).abs(),
            (previous_close - ohlc["low"]).abs(),
        ],
        axis=1,
    )
    return ranges.max(axis=1)


def turtle_n(ohlc: pd.DataFrame, period: int = TURTLE_N_PERIOD) -> pd.Series:
    """The Turtles' N: N = ((period - 1) * PDN + TR) / period.

    Seeded with a simple average of the first ``period`` true ranges, as the
    book prescribes; earlier bars are NaN.
    """
    ranges = true_range(ohlc)
    values = [float("nan")] * len(ranges)
    if len(ranges) < period:
        return pd.Series(values, index=ohlc.index, dtype=float)

    previous_n = float(ranges.iloc[:period].mean())
    values[period - 1] = previous_n
    for i in range(period, len(ranges)):
        previous_n = ((period - 1) * previous_n + float(ranges.iloc[i])) / period
        values[i] = previous_n
    return pd.Series(values, index=ohlc.index, dtype=float)


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def donchian_high(ohlc: pd.DataFrame, period: int) -> pd.Series:
    """Highest high of the ``period`` bars *before* each bar, so today's high can break it."""
    return ohlc["high"].rolling(period, min_periods=period).max().shift(1)


def donchian_low(ohlc: pd.DataFrame, period: int) -> pd.Series:
    """Lowest low of the ``period`` bars before each bar."""
    return ohlc["low"].rolling(period, min_periods=period).min().shift(1)
