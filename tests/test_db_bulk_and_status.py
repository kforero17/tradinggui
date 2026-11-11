from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from src.config.settings import settings
from src.data.database import Database


@pytest.fixture
def database(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "DB_PATH", tmp_path / "metrics.db")
    return Database()


def _metrics(database: Database, ticker: str) -> dict:
    return database.get_latest_metrics(ticker).iloc[0].to_dict()


def test_technicals_only_upsert_keeps_fundamentals(database):
    fetched_at = datetime(2026, 11, 1, 6, 0)
    database.store_metrics(
        [
            {
                "ticker": "AAPL",
                "last_price": 200.0,
                "rsi_14": 55.0,
                "pe_ratio": 30.0,
                "market_cap": 3e12,
                "sector": "Technology",
                "asset_type": "stock",
                "fundamentals_updated_at": fetched_at,
            }
        ]
    )

    database.store_metrics([{"ticker": "AAPL", "last_price": 210.0, "rsi_14": 61.0}])

    row = _metrics(database, "AAPL")
    assert row["last_price"] == 210.0
    assert row["rsi_14"] == 61.0
    assert row["pe_ratio"] == 30.0
    assert row["market_cap"] == 3e12
    assert row["sector"] == "Technology"
    assert row["asset_type"] == "stock"
    assert database.get_fundamentals_ages() == {"AAPL": fetched_at}


def test_explicit_none_clears_a_column(database):
    database.store_metrics([{"ticker": "SPY", "pe_ratio": 25.0}])
    database.store_metrics([{"ticker": "SPY", "pe_ratio": None}])
    assert pd.isna(database.get_all_metrics_df().iloc[0]["pe_ratio"])


def test_bulk_upsert_handles_mixed_key_sets_and_many_rows(database):
    full = [
        {"ticker": f"T{i:04d}", "last_price": float(i), "pe_ratio": 10.0}
        for i in range(1200)
    ]
    database.store_metrics(full)
    database.store_metrics(
        [{"ticker": f"T{i:04d}", "last_price": i + 0.5} for i in range(600)]
        + [{"ticker": "NEW", "last_price": 1.0, "pe_ratio": 5.0}]
    )

    df = database.get_all_metrics_df().set_index("ticker")
    assert len(df) == 1201
    assert df.loc["T0001", "last_price"] == 1.5
    assert df.loc["T0001", "pe_ratio"] == 10.0
    assert df.loc["T0900", "last_price"] == 900.0
    assert df.loc["NEW", "pe_ratio"] == 5.0
    assert database.get_fundamentals_ages()["NEW"] is None


def test_unknown_metrics_column_raises(database):
    with pytest.raises(ValueError, match="Unknown"):
        database.store_metrics([{"ticker": "AAPL", "not_a_column": 1}])


def test_recent_tickers(database):
    database.store_metrics([{"ticker": "AAPL"}, {"ticker": "MSFT"}])
    now = datetime.utcnow()
    assert database.get_recent_tickers(now - timedelta(hours=1)) == {"AAPL", "MSFT"}
    assert database.get_recent_tickers(now + timedelta(hours=1)) == set()


def test_crypto_metrics_bulk_upsert(database):
    database.store_crypto_metrics(
        [{"instrument": "BTCUSD", "last_price": 90_000.0, "ma_50": 85_000.0}]
    )
    database.store_crypto_metrics([{"instrument": "BTCUSD", "last_price": 91_000.0}])

    row = database.get_latest_crypto_metrics("BTCUSD").iloc[0]
    assert row["last_price"] == 91_000.0
    assert row["ma_50"] == 85_000.0


def test_ticker_skipped_after_max_failures_and_reset_by_success(database):
    today = date(2026, 11, 10)
    for _ in range(settings.TICKER_MAX_CONSECUTIVE_FAILURES - 1):
        database.record_ticker_results([], ["DEAD"], today=today)
    assert database.get_skipped_tickers(today) == set()

    database.record_ticker_results(["AAPL"], ["DEAD"], today=today)

    skip_until = today + timedelta(days=settings.TICKER_SKIP_DAYS)
    assert database.get_skipped_tickers(today) == {"DEAD"}
    assert database.get_skipped_tickers(skip_until - timedelta(days=1)) == {"DEAD"}
    assert database.get_skipped_tickers(skip_until) == set()

    database.record_ticker_results(["DEAD"], [], today=skip_until)
    assert database.get_skipped_tickers(skip_until) == set()
    database.record_ticker_results([], ["DEAD"], today=skip_until)
    assert database.get_skipped_tickers(skip_until) == set()


def test_ticker_failing_again_after_skip_is_skipped_again(database):
    today = date(2026, 11, 10)
    for _ in range(settings.TICKER_MAX_CONSECUTIVE_FAILURES):
        database.record_ticker_results([], ["DEAD"], today=today)
    retry_day = today + timedelta(days=settings.TICKER_SKIP_DAYS)

    database.record_ticker_results([], ["DEAD"], today=retry_day)

    assert database.get_skipped_tickers(retry_day) == {"DEAD"}


def test_ticker_in_both_sets_counts_as_success(database):
    today = date(2026, 11, 10)
    for _ in range(settings.TICKER_MAX_CONSECUTIVE_FAILURES):
        database.record_ticker_results(["FLAKY"], ["FLAKY"], today=today)
    assert database.get_skipped_tickers(today) == set()
