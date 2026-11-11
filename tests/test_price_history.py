import json
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
import pytest

from src.config.settings import settings
from src.data import price_history
from src.data.price_history import (
    DEFAULT_HISTORY_START,
    MARKET_TZ,
    get_daily_ohlc,
    get_histories,
    get_splits,
    last_completed_session,
    load_cached_histories,
    refresh_histories,
)

YF_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Adj Close",
    "Volume",
    "Dividends",
    "Stock Splits",
]
# Tuesday after the close, so the last completed session is that Tuesday.
NOW = datetime(2026, 11, 10, 18, 0, tzinfo=MARKET_TZ)
SESSION = date(2026, 11, 10)


def _bars(start: str, end: str, base: float = 100.0) -> pd.DataFrame:
    """Synthetic yfinance-shaped bars, one per weekday."""
    index = pd.bdate_range(start, end, name="Date")
    close = base + np.arange(len(index), dtype=float)
    return pd.DataFrame(
        {
            "Open": close - 0.5,
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
            "Adj Close": close,
            "Volume": 1_000_000.0,
            "Dividends": 0.0,
            "Stock Splits": 0.0,
        },
        index=index,
    )


def _as_cache(bars: pd.DataFrame) -> pd.DataFrame:
    """The cached (lowercase, split-only) form of yfinance-shaped bars."""
    frame = bars.rename(columns=lambda c: c.lower().replace(" ", "_"))
    return frame[price_history.OHLC_COLUMNS]


class FakeYahoo:
    """Stands in for ``yf.download`` over a fixed set of per-ticker bars."""

    def __init__(self, market: dict[str, pd.DataFrame]) -> None:
        self.market = market
        self.calls: list[tuple[list[str], date]] = []

    def download(self, tickers, start, **kwargs):
        assert kwargs["group_by"] == "ticker" and kwargs["auto_adjust"] is False
        assert kwargs["actions"] is True
        tickers = list(tickers)
        self.calls.append((tickers, date.fromisoformat(start)))
        frames = {}
        for ticker in tickers:
            bars = self.market.get(ticker)
            if bars is None:
                # yfinance returns all-NaN price columns for a symbol it can't find.
                frames[ticker] = pd.DataFrame(
                    np.nan,
                    index=pd.DatetimeIndex([], name="Date"),
                    columns=YF_COLUMNS[:6],
                )
            else:
                frames[ticker] = bars.loc[pd.Timestamp(start) :]
        # Columns are (Ticker, Price), and the union index leaves NaN rows for
        # tickers without a bar on some dates.
        return pd.concat(frames, axis=1, names=["Ticker", "Price"])


class _NoWait:
    def wait(self) -> None:
        pass


@pytest.fixture(autouse=True)
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "PRICE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(price_history, "_batch_rate_limiter", _NoWait())
    monkeypatch.setattr(price_history, "_now", lambda: NOW)
    price_history._memory_cache.clear()
    yield tmp_path
    price_history._memory_cache.clear()


def _install(monkeypatch, market: dict[str, pd.DataFrame]) -> FakeYahoo:
    fake = FakeYahoo(market)
    monkeypatch.setattr(price_history.yf, "download", fake.download)
    monkeypatch.setattr(
        price_history.yf,
        "Ticker",
        lambda *a, **k: pytest.fail("yf.Ticker must not be used"),
    )
    return fake


def _seed_cache(ticker: str, bars: pd.DataFrame) -> None:
    _as_cache(bars).to_csv(settings.PRICE_CACHE_DIR / f"{ticker}.csv")


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        (datetime(2026, 11, 14, 12, 0), date(2026, 11, 13)),  # Saturday
        (datetime(2026, 11, 15, 20, 0), date(2026, 11, 13)),  # Sunday
        (datetime(2026, 11, 16, 9, 30), date(2026, 11, 13)),  # Monday before the close
        (datetime(2026, 11, 16, 17, 30), date(2026, 11, 16)),  # Monday after the close
        (datetime(2026, 11, 10, 16, 59), date(2026, 11, 9)),  # Tuesday, just before
        (
            datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc),
            date(2026, 11, 10),
        ),  # 17:30 NY
        (
            datetime(2026, 11, 10, 21, 30, tzinfo=timezone.utc),
            date(2026, 11, 9),
        ),  # 16:30 NY
    ],
)
def test_last_completed_session(now, expected):
    assert last_completed_session(now) == expected


def test_uncached_ticker_downloads_full_history(monkeypatch, cache_dir):
    fake = _install(monkeypatch, {"AAPL": _bars("2026-10-01", "2026-11-10")})

    result = refresh_histories(["aapl"])

    assert fake.calls == [(["AAPL"], DEFAULT_HISTORY_START)]
    assert result.downloaded == 1 and result.failed == []
    assert result.histories["AAPL"].index[-1] == pd.Timestamp(SESSION)
    assert (cache_dir / "AAPL.csv").exists()


def test_incremental_merge_appends_without_duplicates(monkeypatch):
    market = _bars("2026-10-01", "2026-11-10")
    _seed_cache("AAPL", market.loc[:"2026-11-03"])
    # A late correction inside the overlap window must win over the cached bar.
    market.loc["2026-11-03", "Close"] = 999.0
    fake = _install(monkeypatch, {"AAPL": market})

    result = refresh_histories(["AAPL"])

    assert fake.calls == [(["AAPL"], date(2026, 10, 29))]
    bars = result.histories["AAPL"]
    assert bars.index.is_unique and bars.index.is_monotonic_increasing
    assert len(bars) == len(market)
    assert bars.loc["2026-11-03", "close"] == 999.0
    cached = load_cached_histories(["AAPL"])["AAPL"]
    assert len(cached) == len(market) and cached.index[-1] == pd.Timestamp(SESSION)


def test_new_split_forces_full_redownload(monkeypatch):
    before = _bars("2026-10-01", "2026-11-10")
    _seed_cache("AAPL", before.loc[:"2026-11-03"])
    # A 2:1 split on 2026-11-06: the restated history halves every earlier bar.
    after = before.copy()
    pre_split = after.index < "2026-11-06"
    after.loc[pre_split, ["Open", "High", "Low", "Close", "Adj Close"]] /= 2
    after.loc["2026-11-06", "Stock Splits"] = 2.0
    fake = _install(monkeypatch, {"AAPL": after})

    result = refresh_histories(["AAPL"])

    assert fake.calls == [
        (["AAPL"], date(2026, 10, 29)),
        (["AAPL"], DEFAULT_HISTORY_START),
    ]
    bars = result.histories["AAPL"]
    pd.testing.assert_frame_equal(
        bars, _as_cache(after), check_freq=False, check_names=False
    )
    assert get_splits("AAPL") == [(date(2026, 11, 6), 2.0)]

    # The next session's overlap still contains the split, which is now known.
    later = datetime(2026, 11, 11, 18, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr(price_history, "_now", lambda: later)
    fake.market["AAPL"] = pd.concat(
        [after, _bars("2026-11-11", "2026-11-11", base=200.0)]
    )
    fake.calls.clear()
    refresh_histories(["AAPL"])
    assert fake.calls == [(["AAPL"], date(2026, 11, 5))]


def test_multi_ticker_download_is_split_per_ticker(monkeypatch):
    aapl = _bars("2026-10-01", "2026-11-10", base=100.0)
    msft = _bars(
        "2026-10-15", "2026-11-10", base=400.0
    )  # listed later: NaN rows before
    fake = _install(monkeypatch, {"AAPL": aapl, "MSFT": msft})

    result = refresh_histories(["AAPL", "MSFT", "DEAD"])

    assert len(fake.calls) == 1 and fake.calls[0][0] == ["AAPL", "MSFT", "DEAD"]
    assert set(result.histories) == {"AAPL", "MSFT"}
    assert result.failed == ["DEAD"]
    assert result.downloaded == 2
    pd.testing.assert_frame_equal(
        result.histories["MSFT"], _as_cache(msft), check_freq=False, check_names=False
    )
    assert result.histories["AAPL"]["close"].iloc[0] == 100.0


def test_fresh_cache_makes_no_download(monkeypatch):
    _seed_cache("AAPL", _bars("2026-10-01", "2026-11-10"))
    fake = _install(monkeypatch, {})

    result = refresh_histories(["AAPL"])

    assert fake.calls == []
    assert result.downloaded == 0 and set(result.histories) == {"AAPL"}


def test_checked_sidecar_prevents_second_download_on_a_holiday(monkeypatch, cache_dir):
    # No bar for the session (a market holiday): the cache can't catch up today.
    market = _bars("2026-10-01", "2026-11-09")
    _seed_cache("AAPL", market.loc[:"2026-11-06"])
    fake = _install(monkeypatch, {"AAPL": market})

    refresh_histories(["AAPL"])
    refresh_histories(["AAPL"])

    assert len(fake.calls) == 1
    checked = json.loads((cache_dir / "_checked.json").read_text())
    assert datetime.fromisoformat(checked["AAPL"]) == NOW

    # Once the next session completes it is checked again.
    later = datetime(2026, 11, 11, 18, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr(price_history, "_now", lambda: later)
    refresh_histories(["AAPL"])
    assert len(fake.calls) == 2


def test_uncached_and_cached_tickers_go_to_separate_chunks(monkeypatch):
    market = {
        t: _bars("2026-10-01", "2026-11-10") for t in ["AAPL", "MSFT", "NVDA", "JPM"]
    }
    _seed_cache("AAPL", market["AAPL"].loc[:"2026-11-05"])
    _seed_cache("JPM", market["JPM"].loc[:"2026-10-20"])
    fake = _install(monkeypatch, market)

    result = refresh_histories(["AAPL", "MSFT", "JPM", "NVDA"], chunk_size=10)

    assert fake.calls == [
        (["MSFT", "NVDA"], DEFAULT_HISTORY_START),
        # Oldest last bar first; the chunk starts from its oldest bar.
        (["JPM", "AAPL"], date(2026, 10, 15)),
    ]
    assert result.downloaded == 4


def test_chunk_size_limits_tickers_per_call(monkeypatch):
    market = {t: _bars("2026-11-02", "2026-11-10") for t in ["A", "B", "C"]}
    fake = _install(monkeypatch, market)

    refresh_histories(["A", "B", "C"], chunk_size=2)

    assert [tickers for tickers, _ in fake.calls] == [["A", "B"], ["C"]]


def test_unfinished_session_bar_is_not_cached(monkeypatch):
    # Yahoo returns the in-progress bar for today; it must not be stored as final.
    fake = _install(monkeypatch, {"AAPL": _bars("2026-11-02", "2026-11-11")})

    result = refresh_histories(["AAPL"])

    assert fake.calls and result.histories["AAPL"].index[-1] == pd.Timestamp(SESSION)


def test_failed_download_is_not_marked_checked(monkeypatch, cache_dir):
    def boom(*args, **kwargs):
        raise ConnectionError("offline")

    monkeypatch.setattr(price_history.yf, "download", boom)

    result = refresh_histories(["AAPL"])

    assert result.failed == ["AAPL"] and result.histories == {}
    assert not (cache_dir / "_checked.json").exists()


def test_uncached_ticker_dropped_from_chunk_is_retried(monkeypatch, cache_dir):
    # 2026-11-10: a run out of file descriptors dropped thousands of never-cached
    # tickers; marking them checked hid them from the next run until the next close.
    market = {t: _bars("2026-10-01", "2026-11-10") for t in ["AAPL", "MSFT"]}
    fake = _install(monkeypatch, market)
    real_download = fake.download

    def drops_msft(tickers, start, **kwargs):
        return real_download(tickers, start, **kwargs).drop(columns="MSFT", level=0)

    monkeypatch.setattr(price_history.yf, "download", drops_msft)
    result = refresh_histories(["AAPL", "MSFT"])

    checked = json.loads((cache_dir / "_checked.json").read_text())
    assert "AAPL" in checked and "MSFT" not in checked
    assert result.failed == ["MSFT"]

    monkeypatch.setattr(price_history.yf, "download", real_download)
    result = refresh_histories(["AAPL", "MSFT"])

    assert fake.calls[-1] == (["MSFT"], DEFAULT_HISTORY_START)
    assert result.failed == [] and "MSFT" in result.histories


def test_cached_ticker_dropped_from_chunk_is_not_marked_checked(monkeypatch, cache_dir):
    market = {t: _bars("2026-10-01", "2026-11-10") for t in ["AAPL", "MSFT"]}
    _seed_cache("AAPL", market["AAPL"].loc[:"2026-11-03"])
    _seed_cache("MSFT", market["MSFT"].loc[:"2026-11-03"])
    fake = _install(monkeypatch, market)
    real_download = fake.download

    def drops_msft(tickers, start, **kwargs):
        # The threaded download silently leaves MSFT out of an otherwise good chunk.
        return real_download(tickers, start, **kwargs).drop(columns="MSFT", level=0)

    monkeypatch.setattr(price_history.yf, "download", drops_msft)
    result = refresh_histories(["AAPL", "MSFT"])

    checked = json.loads((cache_dir / "_checked.json").read_text())
    assert "AAPL" in checked and "MSFT" not in checked
    assert result.failed == [] and result.downloaded == 1
    assert result.histories["MSFT"].index[-1] == pd.Timestamp("2026-11-03")

    # The next call asks for MSFT again, and only MSFT.
    monkeypatch.setattr(price_history.yf, "download", real_download)
    result = refresh_histories(["AAPL", "MSFT"])

    assert fake.calls[-1][0] == ["MSFT"]
    assert result.histories["MSFT"].index[-1] == pd.Timestamp(SESSION)


def test_layouts_other_than_ticker_first_are_handled():
    bars = _bars("2026-11-02", "2026-11-06")
    bars.index = bars.index.tz_localize("America/New_York")
    price_first = pd.concat({"AAPL": bars}, axis=1).swaplevel(axis=1)

    split = price_history._split_download(price_first, ["AAPL"])
    flat = price_history._split_download(bars, ["AAPL"])

    for frames in (split, flat):
        normalized = price_history._normalize(frames["AAPL"], SESSION)
        assert normalized.index.tz is None
        assert normalized.index[0] == pd.Timestamp("2026-11-02")
        assert list(normalized.columns) == price_history.OHLC_COLUMNS


def test_get_daily_ohlc_refreshes_once_and_keeps_memory_cache(monkeypatch):
    fake = _install(monkeypatch, {"AAPL": _bars("2026-10-01", "2026-11-10")})

    first = get_daily_ohlc("AAPL", start=date(2026, 11, 2))
    (cache_file := settings.PRICE_CACHE_DIR / "AAPL.csv").unlink()
    second = get_daily_ohlc("aapl", start=date(2026, 11, 2))

    assert len(fake.calls) == 1 and not cache_file.exists()
    assert first.index[0] == pd.Timestamp("2026-11-02")
    pd.testing.assert_frame_equal(first, second)


def test_get_daily_ohlc_raises_without_data(monkeypatch):
    _install(monkeypatch, {})

    with pytest.raises(ValueError, match="No price history"):
        get_daily_ohlc("DEAD")


def test_get_histories_batches_and_omits_missing(monkeypatch):
    market = {t: _bars("2026-10-01", "2026-11-10") for t in ["SPY", "QQQ"]}
    fake = _install(monkeypatch, market)

    histories = get_histories(["SPY", "QQQ", "DEAD"], start=date(2026, 11, 1))

    assert len(fake.calls) == 1
    assert set(histories) == {"SPY", "QQQ"}
    assert histories["SPY"].index[0] == pd.Timestamp("2026-11-02")
