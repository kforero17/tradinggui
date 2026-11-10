from unittest.mock import Mock

import pandas as pd
import pytest

from src.data.crypto_feed import CryptoFeed


@pytest.mark.parametrize(
    "setting,verify", [(None, True), ("false", True), ("true", False)]
)
@pytest.mark.parametrize("method", ["get-tickers", "get-candlestick"])
def test_requests_honor_ssl_configuration(monkeypatch, setting, verify, method):
    if setting is None:
        monkeypatch.delenv("DISABLE_SSL_VERIFY", raising=False)
    else:
        monkeypatch.setenv("DISABLE_SSL_VERIFY", setting)
    response = Mock()
    response.json.return_value = {"code": 0, "result": {"data": []}}
    get = Mock(return_value=response)
    monkeypatch.setattr("src.data.crypto_feed.requests.get", get)
    assert CryptoFeed()._get(method, instrument_name="DOGE_USD") == []
    assert get.call_args.kwargs["verify"] is verify
    assert get.call_args.kwargs["timeout"] == 15
    response.raise_for_status.assert_called_once()


def test_quote_units_and_market_selection():
    row = dict(
        i="BTC_USD", a="100", c="0.025", vv="2000000", v="20000", h="110", l="90"
    )
    quote = CryptoFeed.normalize(row)
    assert quote["price_change_24h"] == 2.5
    assert quote["volume_24h"] == 2000000
    assert quote["instrument"] == "BTCUSD"
    assert CryptoFeed.normalize(dict(row, i="BTCUSD-PERP")) is None
    assert CryptoFeed.normalize(dict(row, i="BTC_USDT")) is None
    with pytest.raises(ValueError):
        CryptoFeed.normalize(dict(row, a="NaN"))


def test_candle_cache_and_outage_fallback(monkeypatch):
    feed = CryptoFeed()
    get = Mock(
        return_value=[dict(t=1700000000000, o="10", h="12", l="9", c="11", v="20")]
    )
    monkeypatch.setattr(feed, "_get", get)
    frame, stale = feed.candles("BTCUSD")
    assert not stale
    assert frame.index[0].year == 2023
    assert frame.iloc[0]["close"] == 11
    feed.candles("BTCUSD")
    assert get.call_count == 1
    feed.candle_seconds = -1
    get.side_effect = RuntimeError("offline")
    cached, stale = feed.candles("BTCUSD")
    assert stale
    pd.testing.assert_frame_equal(frame, cached)
    with pytest.raises(RuntimeError):
        feed.candles("ETHUSD")
    with pytest.raises(ValueError):
        feed.candles("../BTCUSD")


def test_refresh_keeps_success_on_failure(monkeypatch):
    import src.data.crypto_feed as module

    feed = CryptoFeed()
    monkeypatch.setattr(
        feed,
        "_get",
        Mock(
            return_value=[
                dict(i="BTC_USD", a="100", c="0.01", vv="1000", h="101", l="99")
            ]
        ),
    )
    store = Mock()
    monkeypatch.setattr(module.db, "store_crypto_metrics", store)
    feed.refresh()
    before = feed.last_success
    assert before
    assert store.call_args.args[0][0]["last_price"] == 100
    feed._get.return_value = []
    with pytest.raises(ValueError):
        feed.refresh()
    assert feed.last_success == before


def test_crypto_routes(monkeypatch):
    import src.web_app.app as module

    monkeypatch.setattr(module.crypto_feed, "start", Mock())
    monkeypatch.setattr(
        module.db,
        "get_latest_crypto_metrics",
        lambda **kwargs: pd.DataFrame(
            [
                dict(
                    instrument="BTCUSD",
                    last_price=100,
                    volume_24h=1000,
                    updated_at=pd.Timestamp("2026-09-22"),
                    ma_50=float("nan"),
                )
            ]
        ),
    )
    history = pd.DataFrame(
        {"close": range(100, 160)},
        index=pd.date_range("2026-07-01", periods=60, tz="UTC"),
    )
    monkeypatch.setattr(module.crypto_feed, "candles", lambda *args: (history, False))
    client = module.app.test_client()
    assert client.get("/crypto").status_code == 200
    quote = client.get("/api/crypto/quotes").get_json()["instruments"][0]
    assert quote["ma_50"] is None
    chart = client.get("/get_crypto_plot/BTCUSD").get_json()
    import json

    assert len(json.loads(chart["plot_data"])["data"]) == 3
    assert client.get("/get_crypto_plot/BTCUSD?timeframe=invalid").status_code == 400
