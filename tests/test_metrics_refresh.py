from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.analysis import metrics
from src.config.settings import settings
from src.data.database import Database, TickerStatus
from src.data.price_history import RefreshResult
from src.data.ticker_loader import TickerInfo
from src.jobs import metrics_refresh
from src.jobs.metrics_refresh import MetricsRefreshStats, refresh_metrics

SESSION = date(2026, 11, 6)


def _bars(price: float, n: int = 150, end: date = SESSION) -> pd.DataFrame:
    close = price * (1 + 0.001 * np.sin(np.arange(n)))
    return pd.DataFrame(
        {
            "open": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": 1_000_000.0,
            "stock_splits": 0.0,
        },
        index=pd.bdate_range(end=end, periods=n),
    )


def _info(symbol: str, is_etf: bool = False) -> TickerInfo:
    return TickerInfo(symbol, symbol, "NYSE", is_etf, False)


# AAA/BBB/HELD are liquid stocks, ETF1 a liquid ETF, CCC trades under the minimum
# price, SHORT has too little history, STALE stopped trading, NOBARS has no data
# and DEAD is on the skip list. HELD is on the skip list too, but held.
HISTORIES = {
    "AAA": _bars(100.0),
    "BBB": _bars(50.0),
    "ETF1": _bars(400.0),
    "CCC": _bars(2.0),
    "HELD": _bars(30.0),
    "SHORT": _bars(80.0, n=40),
    "STALE": _bars(60.0, end=SESSION - timedelta(days=30)),
    "DEAD": _bars(10.0),
}
UNIVERSE = {
    symbol: _info(symbol, is_etf=symbol == "ETF1")
    for symbol in ["AAA", "BBB", "ETF1", "CCC", "SHORT", "STALE", "NOBARS", "DEAD"]
}


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "metrics.db")
    database = Database()
    monkeypatch.setattr(metrics_refresh, "db", database)
    monkeypatch.setattr(metrics_refresh, "load_universe", lambda: dict(UNIVERSE))
    monkeypatch.setattr(metrics_refresh, "last_completed_session", lambda: SESSION)

    requested: list[list[str]] = []

    def fake_refresh(tickers):
        batch = list(tickers)
        requested.append(batch)
        found = {t: HISTORIES[t] for t in batch if t in HISTORIES}
        return RefreshResult(
            histories=found,
            failed=[t for t in batch if t not in found],
            downloaded=len(found),
        )

    monkeypatch.setattr(metrics_refresh, "refresh_histories", fake_refresh)

    fetched: list[str] = []

    def fake_fundamentals(ticker):
        fetched.append(ticker)
        return {
            **dict.fromkeys(metrics.FUNDAMENTALS_KEYS),
            "pe_ratio": 15.0,
            "sector": "Industrials",
            "fundamentals_updated_at": datetime.utcnow(),
        }

    monkeypatch.setattr(metrics, "fetch_fundamentals", fake_fundamentals)

    # BBB's fundamentals are fresh, HELD's too but it's held
    fresh = datetime.utcnow() - timedelta(days=3)
    database.store_metrics(
        [
            {
                "ticker": "BBB",
                "pe_ratio": 12.0,
                "sector": "Energy",
                "fundamentals_updated_at": fresh,
            },
            {"ticker": "HELD", "pe_ratio": 9.0, "fundamentals_updated_at": fresh},
        ]
    )
    database.add_portfolio_ticker("HELD", shares=10)
    for _ in range(settings.TICKER_MAX_CONSECUTIVE_FAILURES):
        database.record_ticker_results([], ["DEAD", "HELD"])
    assert database.get_skipped_tickers() == {"DEAD", "HELD"}

    return database, requested, fetched, fresh


def _row(database: Database, ticker: str) -> dict:
    return database.get_latest_metrics(ticker).iloc[0].to_dict()


def _status(database: Database) -> dict[str, TickerStatus]:
    session = database.Session()
    try:
        return {s.ticker: s for s in session.query(TickerStatus)}
    finally:
        session.close()


def test_full_universe_run(env):
    database, requested, fetched, fresh = env

    stats = refresh_metrics(batch_size=3)

    # Universe plus holdings, minus the skipped (and not held) DEAD, in batches of 3
    processed = [t for batch in requested for t in batch]
    assert "DEAD" not in processed
    assert set(processed) == set(UNIVERSE) - {"DEAD"} | {"HELD"}
    assert all(len(batch) <= 3 for batch in requested)

    # Technical rows with asset type and liquidity
    for ticker in ["AAA", "BBB", "ETF1", "CCC", "HELD"]:
        row = _row(database, ticker)
        assert row["last_price"] == pytest.approx(HISTORIES[ticker]["close"].iloc[-1])
        assert row["ma_100"] > 0
        assert row["avg_dollar_volume_50d"] == pytest.approx(
            float(HISTORIES[ticker]["close"].tail(50).mean() * 1_000_000)
        )
    assert _row(database, "ETF1")["asset_type"] == "etf"
    assert _row(database, "AAA")["asset_type"] == "stock"
    # HELD isn't in the listings, so its asset type is left unset
    assert _row(database, "HELD")["asset_type"] is None
    for ticker in ["SHORT", "STALE", "NOBARS", "DEAD"]:
        assert database.get_latest_metrics(ticker).empty

    # Fundamentals: the holding first, then the never-fetched liquid stock; not the
    # fresh BBB, the ETF or the illiquid CCC
    assert fetched == ["HELD", "AAA"]
    assert _row(database, "AAA")["pe_ratio"] == 15.0
    assert _row(database, "HELD")["pe_ratio"] == 15.0
    # A technicals-only row keeps the stored fundamentals
    bbb = _row(database, "BBB")
    assert bbb["pe_ratio"] == 12.0
    assert bbb["sector"] == "Energy"
    assert database.get_fundamentals_ages()["BBB"] == fresh
    assert database.get_fundamentals_ages()["CCC"] is None

    # Ticker status
    status = _status(database)
    for ticker in ["AAA", "BBB", "ETF1", "CCC", "HELD"]:
        assert status[ticker].consecutive_failures == 0
        assert status[ticker].last_ok == date.today()
        assert status[ticker].skip_until is None
    for ticker in ["SHORT", "STALE", "NOBARS"]:
        assert status[ticker].consecutive_failures == 1
        assert status[ticker].last_ok is None
    assert (
        status["DEAD"].consecutive_failures == settings.TICKER_MAX_CONSECUTIVE_FAILURES
    )
    assert database.get_skipped_tickers() == {"DEAD"}

    assert isinstance(stats, MetricsRefreshStats)
    assert stats.tickers == 8
    assert stats.skipped == 1
    assert stats.batches == 3
    assert stats.downloaded == 7
    assert stats.computed == 5
    assert stats.failed == 3
    assert stats.stale == 1
    assert stats.liquid == 4  # AAA, BBB, ETF1, HELD
    assert stats.fundamentals_due == 2
    assert stats.fundamentals_fetched == 2
    assert stats.fundamentals_failed == 0
    for seconds in [
        stats.download_seconds,
        stats.compute_seconds,
        stats.fundamentals_seconds,
        stats.store_seconds,
    ]:
        assert 0 <= seconds <= stats.total_seconds


def test_explicit_tickers_force_only_the_holdings_among_them(env):
    database, requested, fetched, _ = env

    stats = refresh_metrics(["aaa", "BBB", "DEAD"])

    assert requested == [["AAA", "BBB"]]
    assert fetched == ["AAA"]
    assert stats.tickers == 2
    assert stats.skipped == 1
    assert stats.batches == 1


def test_a_failed_fundamentals_fetch_keeps_the_stored_values(env, monkeypatch):
    database, _, _, fresh = env
    monkeypatch.setattr(metrics, "fetch_fundamentals", lambda ticker: None)

    stats = refresh_metrics(["HELD", "AAA"])

    assert stats.fundamentals_due == 2
    assert stats.fundamentals_failed == 2
    held = _row(database, "HELD")
    assert held["pe_ratio"] == 9.0
    assert held["last_price"] == pytest.approx(HISTORIES["HELD"]["close"].iloc[-1])
    assert database.get_fundamentals_ages()["HELD"] == fresh


@pytest.mark.parametrize(
    "cap, expected",
    [
        (3, ["HELD", "AAA", "BBB"]),
        # Holdings first, then never-fetched before the oldest; the rest is deferred
        (2, ["HELD", "AAA"]),
        # Holdings survive even a cap below their count
        (0, ["HELD"]),
    ],
)
def test_fundamentals_are_capped_per_run(env, monkeypatch, cap, expected):
    database, _, fetched, _ = env
    database.store_metrics(
        [
            {
                "ticker": "BBB",
                "fundamentals_updated_at": datetime.utcnow() - timedelta(days=40),
            }
        ]
    )
    monkeypatch.setattr(settings, "FUNDAMENTALS_MAX_PER_RUN", cap)

    stats = refresh_metrics()

    assert fetched == expected
    assert stats.fundamentals_due == len(expected)
    assert stats.fundamentals_fetched == len(expected)


def test_fundamentals_can_be_skipped(env):
    _, _, fetched, _ = env
    stats = refresh_metrics(fetch_fundamentals=False)
    assert fetched == []
    assert stats.fundamentals_due == 0
    assert stats.computed == 5


def test_fundamentals_are_stored_in_chunks(env, monkeypatch):
    database, _, _, _ = env
    monkeypatch.setattr(metrics_refresh, "FUNDAMENTALS_STORE_CHUNK", 1)
    stores: list[list[str]] = []
    real_store = database.store_metrics

    def store(rows):
        stores.append([r["ticker"] for r in rows])
        real_store(rows)

    monkeypatch.setattr(database, "store_metrics", store)
    refresh_metrics(["AAA", "HELD"])
    assert stores == [["AAA", "HELD"], ["HELD"], ["AAA"]]


def test_get_metrics_batch_delegates(monkeypatch):
    calls = []

    def fake_refresh(tickers):
        calls.append(tickers)
        # 11 counts, then 5 timings
        return MetricsRefreshStats(*([len(tickers)] * 11), *([0.0] * 5))

    monkeypatch.setattr(metrics_refresh, "refresh_metrics", fake_refresh)
    assert metrics.metrics_calculator.get_metrics_batch(["AAPL", "MSFT"]) == []
    assert calls == [["AAPL", "MSFT"]]
