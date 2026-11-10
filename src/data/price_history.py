"""Daily OHLC history for trend-following rules and trade review, cached on disk per ticker.

Prices are split-adjusted but not dividend-adjusted (``auto_adjust=False``),
so they line up with the fill prices in broker exports once those fills are
restated with ``broker_import.apply_splits``.

The cache is incremental: ``refresh_histories`` downloads only the bars after
each ticker's last cached bar, many tickers per ``yf.download`` call. A new
split re-downloads that ticker's full history, because split adjustment
rewrites every earlier bar.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from time import perf_counter
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from loguru import logger

from ..analysis.metrics import RateLimiter
from ..config.settings import settings

# metrics configures SSL before importing yfinance, so import it only afterwards.
import yfinance as yf  # noqa: E402

__all__ = [
    "DEFAULT_HISTORY_START",
    "RefreshResult",
    "get_daily_ohlc",
    "get_histories",
    "get_splits",
    "last_completed_session",
    "load_cached_histories",
    "refresh_histories",
]

DEFAULT_HISTORY_START = date(2015, 1, 1)
OHLC_COLUMNS = ["open", "high", "low", "close", "volume", "stock_splits"]
PRICE_COLUMNS = ["open", "high", "low", "close"]

MARKET_TZ = ZoneInfo("America/New_York")
# Daily bars are treated as final a little after the 16:00 close.
SESSION_COMPLETE_AT = time(17, 0)
# Re-fetch a few bars already in the cache so late corrections overwrite them.
REFRESH_OVERLAP = timedelta(days=5)
CHECKED_FILE = "_checked.json"

_batch_rate_limiter = RateLimiter(calls_per_second=settings.BATCH_REQUESTS_PER_SECOND)
# ticker -> (session the bars are complete through, bars)
_memory_cache: dict[str, tuple[date, pd.DataFrame]] = {}
_lock = threading.Lock()
_checked_lock = threading.Lock()


@dataclass(frozen=True)
class RefreshResult:
    """``histories`` holds every requested ticker with data, keyed by upper-case symbol.

    ``failed`` lists requested tickers with no bars at all (nothing cached and
    nothing downloaded); ``downloaded`` counts tickers whose cache was updated
    from a download in this call.
    """

    histories: dict[str, pd.DataFrame]
    failed: list[str]
    downloaded: int


def _now() -> datetime:
    return datetime.now(MARKET_TZ)


def _session_close(session: date) -> datetime:
    return datetime.combine(session, SESSION_COMPLETE_AT, tzinfo=MARKET_TZ)


def last_completed_session(now: datetime | None = None) -> date:
    """The latest US weekday whose daily bar is final (holidays are not modelled).

    A naive ``now`` is taken to be New York time.
    """
    now = _now() if now is None else now
    now = (
        now.replace(tzinfo=MARKET_TZ)
        if now.tzinfo is None
        else now.astimezone(MARKET_TZ)
    )
    session = (
        now.date()
        if now.time() >= SESSION_COMPLETE_AT
        else now.date() - timedelta(days=1)
    )
    while session.weekday() >= 5:
        session -= timedelta(days=1)
    return session


def _chunks(items: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _cache_path(ticker: str) -> Path:
    return settings.PRICE_CACHE_DIR / f"{ticker.upper()}.csv"


def _read_cache(ticker: str) -> pd.DataFrame | None:
    """Cached bars whatever their age, or None when there are none."""
    path = _cache_path(ticker)
    if not path.exists():
        return None
    try:
        frame = pd.read_csv(path, index_col=0, parse_dates=True)
    except Exception:
        logger.exception(f"Ignoring unreadable price cache {path}")
        return None
    return None if frame.empty else frame


def _write_atomic(path: Path, write: Callable[[Path], object]) -> None:
    """Write via a uniquely named temp file and rename, so readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        write(tmp)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _write_cache(ticker: str, frame: pd.DataFrame) -> None:
    _write_atomic(_cache_path(ticker), frame.to_csv)


def _checked_path() -> Path:
    return settings.PRICE_CACHE_DIR / CHECKED_FILE


def _read_checked() -> dict[str, str]:
    path = _checked_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        logger.warning(f"Ignoring unreadable {path}")
        return {}


def _record_checked(tickers: Iterable[str], when: datetime) -> None:
    """Remember that these tickers were downloaded at ``when``, so a day with no new
    bar (a holiday) doesn't trigger another download on every call."""
    stamp = when.isoformat()
    with _checked_lock:
        checked = _read_checked()
        checked.update({ticker: stamp for ticker in tickers})
        _write_atomic(
            _checked_path(), lambda p: p.write_text(json.dumps(checked, sort_keys=True))
        )


def _checked_since(stamp: str | None, moment: datetime) -> bool:
    if not stamp:
        return False
    try:
        return datetime.fromisoformat(stamp) >= moment
    except (TypeError, ValueError):
        return False


def _last_bar(frame: pd.DataFrame) -> date:
    return frame.index[-1].date()


def _normalize(raw: pd.DataFrame, through: date) -> pd.DataFrame:
    """yfinance columns to ``OHLC_COLUMNS`` on a naive daily index, without empty
    rows or bars after the last completed session."""
    frame = raw.rename(columns=lambda c: str(c).lower().replace(" ", "_"))
    index = pd.DatetimeIndex(frame.index)
    if index.tz is not None:
        index = index.tz_localize(None)
    frame.index = index.normalize()
    frame.index.name = "Date"
    frame = frame.reindex(columns=OHLC_COLUMNS)
    # Tickers without data come back as all-NaN rows in a multi-ticker download.
    frame = frame.dropna(how="all", subset=PRICE_COLUMNS)
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    return frame.loc[: pd.Timestamp(through)].fillna({"stock_splits": 0.0})


def _split_download(
    raw: pd.DataFrame | None, chunk: list[str]
) -> dict[str, pd.DataFrame]:
    """Per-ticker frames from a ``yf.download`` result (not yet normalized)."""
    if raw is None or raw.empty:
        return {}
    if not isinstance(raw.columns, pd.MultiIndex):
        # A flat layout only happens for a single ticker.
        return {chunk[0]: raw} if len(chunk) == 1 else {}
    wanted = set(chunk)
    for level in range(raw.columns.nlevels):
        present = wanted & set(raw.columns.get_level_values(level))
        if present:
            return {t: raw.xs(t, axis=1, level=level) for t in present}
    return {}


def _download_chunk(
    chunk: list[str], start: date, through: date
) -> dict[str, pd.DataFrame] | None:
    """New bars per ticker from one rate-limited call; None when the download failed."""
    _batch_rate_limiter.wait()
    try:
        raw = yf.download(
            list(chunk),
            start=start.isoformat(),
            group_by="ticker",
            auto_adjust=False,
            actions=True,
            threads=True,
            progress=False,
        )
    except Exception:
        logger.exception(f"Batch download failed for {len(chunk)} tickers from {start}")
        return None
    frames = {t: _normalize(f, through) for t, f in _split_download(raw, chunk).items()}
    frames = {t: f for t, f in frames.items() if not f.empty}
    # Nothing at all for the whole chunk (even the overlap bars) means the call failed.
    return frames or None


def _merge(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([old.reindex(columns=OHLC_COLUMNS), new])
    return merged[~merged.index.duplicated(keep="last")].sort_index()


def _has_new_split(old: pd.DataFrame, new: pd.DataFrame) -> bool:
    known = set(old.index[old["stock_splits"].fillna(0) > 0])
    return any(when not in known for when in new.index[new["stock_splits"] > 0])


def refresh_histories(
    tickers: Iterable[str], chunk_size: int | None = None
) -> RefreshResult:
    """Bring each ticker's cached bars up to the last completed session.

    Only tickers whose last bar is older than that session, and that weren't
    already checked after it, are downloaded: one rate-limited ``yf.download``
    per chunk, uncached tickers (full history) and cached ones (recent bars
    only) in separate chunks.
    """
    started = perf_counter()
    chunk_size = chunk_size or settings.DOWNLOAD_CHUNK_SIZE
    now = _now()
    session = last_completed_session(now)
    session_close = _session_close(session)
    requested = list(dict.fromkeys(t.strip().upper() for t in tickers))

    histories: dict[str, pd.DataFrame] = {}
    for ticker in requested:
        cached = _read_cache(ticker)
        if cached is not None:
            histories[ticker] = cached
    checked = _read_checked()
    stale = [
        t
        for t in requested
        if (t not in histories or _last_bar(histories[t]) < session)
        and not _checked_since(checked.get(t), session_close)
    ]
    uncached = [t for t in stale if t not in histories]
    # Oldest first, so each chunk's start date is close to all of its tickers' last bars.
    behind = sorted(
        (t for t in stale if t in histories), key=lambda t: _last_bar(histories[t])
    )

    jobs: list[tuple[list[str], date]] = [
        (chunk, DEFAULT_HISTORY_START) for chunk in _chunks(uncached, chunk_size)
    ]
    jobs += [
        (chunk, _last_bar(histories[chunk[0]]) - REFRESH_OVERLAP)
        for chunk in _chunks(behind, chunk_size)
    ]

    calls = downloaded = 0
    resplit: list[str] = []
    done: list[str] = []
    for chunk, start in jobs:
        calls += 1
        frames = _download_chunk(chunk, start, session)
        if frames is None:
            continue
        for ticker in chunk:
            new = frames.get(ticker)
            old = histories.get(ticker)
            if new is not None and old is not None and _has_new_split(old, new):
                resplit.append(ticker)
                continue
            done.append(ticker)
            if new is None:
                continue
            histories[ticker] = new if old is None else _merge(old, new)
            _write_cache(ticker, histories[ticker])
            downloaded += 1

    for chunk in _chunks(resplit, chunk_size):
        calls += 1
        logger.info(f"New split for {', '.join(chunk)}; re-downloading full history")
        frames = _download_chunk(chunk, DEFAULT_HISTORY_START, session)
        if frames is None:
            continue
        for ticker in chunk:
            done.append(ticker)
            if ticker in frames:
                histories[ticker] = frames[ticker]
                _write_cache(ticker, frames[ticker])
                downloaded += 1

    if done:
        _record_checked(done, now)
    failed = [t for t in requested if t not in histories]
    logger.info(
        f"Refreshed price histories: tickers={len(requested)} stale={len(stale)} "
        f"chunks={calls} downloaded={downloaded} failed={len(failed)} "
        f"in {perf_counter() - started:.1f}s"
    )
    return RefreshResult(histories=histories, failed=failed, downloaded=downloaded)


def load_cached_histories(tickers: Iterable[str]) -> dict[str, pd.DataFrame]:
    """Cached bars for each ticker that has any, keyed by upper-case symbol; no network."""
    histories: dict[str, pd.DataFrame] = {}
    for ticker in dict.fromkeys(t.strip().upper() for t in tickers):
        cached = _read_cache(ticker)
        if cached is not None:
            histories[ticker] = cached
    return histories


def _memory_cached(key: str, session: date) -> pd.DataFrame | None:
    with _lock:
        cached_session, cached = _memory_cache.get(key, (None, None))
    return cached if cached_session == session else None


def _remember(histories: dict[str, pd.DataFrame], session: date) -> None:
    with _lock:
        for key, frame in histories.items():
            _memory_cache[key] = (session, frame)


def get_daily_ohlc(ticker: str, start: date = DEFAULT_HISTORY_START) -> pd.DataFrame:
    """Daily bars from ``start`` (no earlier than 2015) with lowercase columns.

    Stale bars are brought up to date incrementally and kept in memory for the session.
    """
    key = ticker.upper()
    session = last_completed_session()
    cached = _memory_cached(key, session)
    if cached is None:
        result = refresh_histories([key])
        cached = result.histories.get(key)
        if cached is None:
            raise ValueError(f"No price history returned for {key}")
        _remember({key: cached}, session)
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
    session = last_completed_session()
    frames = {t: _memory_cached(t.upper(), session) for t in tickers}
    missing = [t.upper() for t, frame in frames.items() if frame is None]
    if missing:
        try:
            result = refresh_histories(missing)
        except Exception:
            logger.exception(f"Could not refresh price histories for {missing}")
        else:
            _remember(result.histories, session)
            for ticker in result.failed:
                logger.warning(f"No price history for {ticker}")
            for t in frames:
                if frames[t] is None:
                    frames[t] = result.histories.get(t.upper())
    return {
        t: frame.loc[pd.Timestamp(start) :]
        for t, frame in frames.items()
        if frame is not None
    }
