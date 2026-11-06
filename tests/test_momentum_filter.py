from unittest.mock import Mock

import pandas as pd
import pytest

from src.web_app import app as app_module
from src.web_app.app import MomentumDirection, filter_momentum_stocks

LARGE_CAP = 10e9
SMALL_CAP = 1e9


@pytest.fixture
def latest_metrics(monkeypatch):
    metrics = pd.DataFrame(
        [
            {"ticker": "UP_BIG", "pct_above_ma_100": 25.0, "market_cap": LARGE_CAP},
            {"ticker": "UP_EDGE", "pct_above_ma_100": 5.0, "market_cap": LARGE_CAP},
            {"ticker": "UP_SMALL", "pct_above_ma_100": 2.0, "market_cap": LARGE_CAP},
            {"ticker": "FLAT", "pct_above_ma_100": 0.0, "market_cap": LARGE_CAP},
            {"ticker": "DOWN_SMALL", "pct_above_ma_100": -2.0, "market_cap": LARGE_CAP},
            {"ticker": "DOWN_EDGE", "pct_above_ma_100": -5.0, "market_cap": LARGE_CAP},
            {"ticker": "DOWN_BIG", "pct_above_ma_100": -30.0, "market_cap": LARGE_CAP},
            {
                "ticker": "DOWN_TINY_CAP",
                "pct_above_ma_100": -40.0,
                "market_cap": SMALL_CAP,
            },
            {
                "ticker": "UP_TINY_CAP",
                "pct_above_ma_100": 40.0,
                "market_cap": SMALL_CAP,
            },
            {"ticker": "NO_MA", "pct_above_ma_100": None, "market_cap": LARGE_CAP},
            {"ticker": "NO_CAP", "pct_above_ma_100": -50.0, "market_cap": None},
        ]
    ).assign(
        last_price=100.0,
        pct_above_ema_100=0.0,
        rsi_14=50.0,
        pe_ratio=20.0,
        pb_ratio=3.0,
        ps_ratio=4.0,
        ebitda_ev=12.0,
    )
    stub_db = Mock()
    stub_db.get_latest_metrics.return_value = metrics
    stub_db.get_triggered_alerts.return_value = []
    monkeypatch.setattr(app_module, "db", stub_db)
    return metrics


def _tickers(df: pd.DataFrame) -> list[str]:
    return df["ticker"].tolist()


def test_bullish_keeps_stocks_at_or_above_threshold_strongest_first(latest_metrics):
    result = filter_momentum_stocks(min_market_cap=2e9, min_momentum_pct=5.0)
    assert _tickers(result) == ["UP_BIG", "UP_EDGE"]


def test_bearish_keeps_stocks_at_or_below_negative_threshold_weakest_first(
    latest_metrics,
):
    result = filter_momentum_stocks(
        min_market_cap=2e9, min_momentum_pct=5.0, direction=MomentumDirection.BEARISH
    )
    assert _tickers(result) == ["DOWN_BIG", "DOWN_EDGE"]


def test_zero_threshold_includes_flat_stocks_in_both_directions(latest_metrics):
    bullish = filter_momentum_stocks(min_market_cap=2e9, min_momentum_pct=0.0)
    bearish = filter_momentum_stocks(
        min_market_cap=2e9, min_momentum_pct=0.0, direction=MomentumDirection.BEARISH
    )
    assert _tickers(bullish) == ["UP_BIG", "UP_EDGE", "UP_SMALL", "FLAT"]
    assert _tickers(bearish) == ["DOWN_BIG", "DOWN_EDGE", "DOWN_SMALL", "FLAT"]


def test_negative_bearish_threshold_allows_stocks_slightly_above_ma(latest_metrics):
    result = filter_momentum_stocks(
        min_market_cap=2e9, min_momentum_pct=-2.0, direction=MomentumDirection.BEARISH
    )
    assert _tickers(result) == [
        "DOWN_BIG",
        "DOWN_EDGE",
        "DOWN_SMALL",
        "FLAT",
        "UP_SMALL",
    ]


@pytest.mark.parametrize("direction", list(MomentumDirection))
def test_missing_data_and_small_caps_are_excluded(latest_metrics, direction):
    result = filter_momentum_stocks(
        min_market_cap=2e9, min_momentum_pct=-100.0, direction=direction
    )
    assert {"NO_MA", "NO_CAP", "DOWN_TINY_CAP", "UP_TINY_CAP"}.isdisjoint(
        _tickers(result)
    )


@pytest.fixture
def client():
    app_module.app.config["TESTING"] = True
    return app_module.app.test_client()


def test_momentum_page_renders_bearish_view(latest_metrics, client):
    response = client.get("/momentum?direction=bearish&min_momentum_pct=5")
    page = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Bearish Momentum Stocks" in page
    assert "Minimum % Below 100-Day MA" in page
    assert page.index("DOWN_BIG") < page.index("DOWN_EDGE")
    assert "UP_BIG" not in page


@pytest.mark.parametrize("query", ["", "?direction=sideways"])
def test_momentum_page_defaults_to_bullish_view(latest_metrics, client, query):
    response = client.get(f"/momentum{query}")
    page = response.get_data(as_text=True)
    assert response.status_code == 200
    assert "Bullish Momentum Stocks" in page
    assert "Minimum % Above 100-Day MA" in page
    assert "UP_BIG" in page
    assert "DOWN_BIG" not in page
