"""Series-returning technical indicators for trend-following rules.

Every function takes a daily OHLC frame with lowercase ``open/high/low/close``
columns (the shape returned by ``src.data.price_history.get_daily_ohlc``) and
returns a Series aligned to its index.
"""

import pandas as pd

__all__ = [
    "true_range",
    "turtle_n",
    "ema",
    "donchian_high",
    "donchian_low",
    "adx",
    "rate_of_change",
    "pct_from_high",
    "pct_from_low",
]

TURTLE_N_PERIOD = 20
ADX_PERIOD = 14
TRADING_DAYS_PER_YEAR = 252


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


def _wilder(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing, an EMA with alpha = 1 / period; NaN until ``period`` values exist."""
    return series.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def adx(ohlc: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
    """Wilder's Average Directional Index (0-100): trend strength, whichever the direction.

    +DM is today's high minus yesterday's when that beats the drop in the low
    (and -DM the reverse); both and the true range are Wilder-smoothed into
    +DI and -DI, and ADX is the smoothed DX = 100 * |+DI - -DI| / (+DI + -DI).
    The first bar has no prior bar, so it is left out; ADX is NaN for roughly
    the first ``2 * period`` bars.
    """
    up_move = ohlc["high"].diff()
    down_move = -ohlc["low"].diff()
    has_prior = ohlc["close"].shift(1).notna()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0).where(has_prior)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0).where(
        has_prior
    )
    atr = _wilder(true_range(ohlc).where(has_prior), period)
    plus_di = 100 * _wilder(plus_dm, period) / atr
    minus_di = 100 * _wilder(minus_dm, period) / atr
    di_sum = plus_di + minus_di
    # No directional movement at all (a perfectly flat stretch) is a DX of 0, not 0 / 0.
    dx = (100 * (plus_di - minus_di).abs() / di_sum).mask(di_sum == 0, 0.0)
    return _wilder(dx, period)


def rate_of_change(close: pd.Series, days: int) -> pd.Series:
    """Fractional change over ``days`` bars: 0.10 is a 10% gain."""
    return close / close.shift(days) - 1


def pct_from_high(ohlc: pd.DataFrame, period: int = TRADING_DAYS_PER_YEAR) -> pd.Series:
    """Close relative to the highest high of the last ``period`` bars (today included), so <= 0."""
    return ohlc["close"] / ohlc["high"].rolling(period, min_periods=period).max() - 1


def pct_from_low(ohlc: pd.DataFrame, period: int = TRADING_DAYS_PER_YEAR) -> pd.Series:
    """Close relative to the lowest low of the last ``period`` bars (today included), so >= 0.

    The short-side mirror of ``pct_from_high``.
    """
    return ohlc["close"] / ohlc["low"].rolling(period, min_periods=period).min() - 1
