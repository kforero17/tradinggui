"""Daily OHLC history for trend-following rules and trade review, cached on disk per day.

Prices are split-adjusted but not dividend-adjusted (``auto_adjust=False``),
so they line up with the fill prices in broker exports once those fills are
restated with ``broker_import.apply_splits``.
"""

from __future__ import annotations

import threading
from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger

from ..analysis.metrics import RateLimiter
from ..config.settings import settings

# metrics configures SSL before importing yfinance, so import it only afterwards.
import yfinance as yf  # noqa: E402

__all__ = ["DEFAULT_HISTORY_START", "get_daily_ohlc", "get_splits", "get_histories"]

DEFAULT_HISTORY_START = date(2015, 1, 1)
OHLC_COLUMNS = ["open", "high", "low", "close", "volume", "stock_splits"]

_rate_limiter = RateLimiter(calls_per_second=settings.REQUESTS_PER_SECOND * 4)
_memory_cache: dict[str, tuple[date, pd.DataFrame]] = {}
_lock = threading.Lock()


def _cache_path(ticker: str) -> Path:
    return settings.PRICE_CACHE_DIR / f"{ticker.upper()}.csv"


def _read_cache(ticker: str) -> pd.DataFrame | None:
    path = _cache_path(ticker)
    if not path.exists() or date.fromtimestamp(path.stat().st_mtime) < date.today():
        return None
    frame = pd.read_csv(path, index_col=0, parse_dates=True)
    return None if frame.empty else frame


def _download(ticker: str) -> pd.DataFrame:
    _rate_limiter.wait()
    raw = yf.Ticker(ticker).history(
        start=DEFAULT_HISTORY_START.isoformat(), auto_adjust=False, actions=True
    )
    if raw.empty:
        raise ValueError(f"No price history returned for {ticker}")
    frame = raw.rename(columns=lambda c: c.lower().replace(" ", "_"))
    frame.index = frame.index.tz_localize(None).normalize()
    return frame.reindex(columns=OHLC_COLUMNS).fillna({"stock_splits": 0.0})


def get_daily_ohlc(ticker: str, start: date = DEFAULT_HISTORY_START) -> pd.DataFrame:
    """Daily bars from ``start`` (no earlier than 2015) with lowercase columns.

    Each ticker's full history is fetched at most once per day and cached on disk.
    """
    key = ticker.upper()
    today = date.today()
    with _lock:
        fetched_on, cached = _memory_cache.get(key, (None, None))
    if cached is None or fetched_on != today:
        cached = _read_cache(key)
        if cached is None:
            cached = _download(key)
            settings.PRICE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cached.to_csv(_cache_path(key))
            logger.info(f"Cached {len(cached)} daily bars for {key}")
        with _lock:
            _memory_cache[key] = (today, cached)
    return cached.loc[pd.Timestamp(start) :]


def get_splits(
    ticker: str, start: date = DEFAULT_HISTORY_START
) -> list[tuple[date, float]]:
    ohlc = get_daily_ohlc(ticker, start)
    splits = ohlc.loc[ohlc["stock_splits"] > 0, "stock_splits"]
    return [(when.date(), float(ratio)) for when, ratio in splits.items()]


def get_histories(
    tickers: list[str], start: date = DEFAULT_HISTORY_START
) -> dict[str, pd.DataFrame]:
    """Histories for every ticker that has data; failures are logged and left out."""
    histories: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        try:
            histories[ticker] = get_daily_ohlc(ticker, start)
        except Exception:
            logger.exception(f"Could not load price history for {ticker}")
    return histories
