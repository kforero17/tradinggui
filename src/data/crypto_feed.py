"""Public Crypto.com quotes with a process-local refresh worker and candle cache."""

from datetime import datetime, timezone
from threading import Lock, Thread, Event
import math
import re
import time

import pandas as pd
import requests
from loguru import logger

from .database import db
from ..config.security import security_config


class CryptoFeed:
    refresh_seconds = 60
    candle_seconds = 300
    base_url = "https://api.crypto.com/exchange/v1/public/"

    def __init__(self):
        self._lock = Lock()
        self._candle_lock = Lock()
        self._started = False
        self._candles = {}
        self.last_success = None
        self.error = None

    def _get(self, method, **params):
        # requests does not honor the global ssl context override used by
        # configure_ssl_verification; pass the existing app setting explicitly.
        response = requests.get(
            self.base_url + method,
            params=params,
            timeout=15,
            verify=not security_config.should_disable_ssl(),
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise ValueError("Market data provider rejected the request")
        return payload["result"]["data"]

    @staticmethod
    def normalize(row):
        # Only spot USD markets; exclude derivatives and other quote currencies.
        if not re.fullmatch(r"[A-Z0-9]+_USD", row.get("i", "")):
            return None
        values = {
            key: float(row[field])
            for key, field in {
                "last_price": "a",
                "price_change_24h": "c",
                "volume_24h": "vv",
                "high_24h": "h",
                "low_24h": "l",
            }.items()
        }
        if not all(math.isfinite(value) for value in values.values()):
            raise ValueError("Non-finite market quote")
        values["price_change_24h"] *= 100
        return dict(instrument=row["i"].replace("_", ""), timeframe="1D", **values)

    def refresh(self):
        records = []
        for row in self._get("get-tickers"):
            try:
                record = self.normalize(row)
                if record:
                    records.append(record)
            except (KeyError, ValueError, TypeError):
                continue
        if not records:
            raise ValueError("No valid USD quotes returned")
        db.store_crypto_metrics(records)
        with self._lock:
            self.last_success = datetime.now(timezone.utc).isoformat()
            self.error = None

    def start(self):
        with self._lock:
            if self._started:
                return
            self._started = True
        Thread(target=self._run, name="crypto-quotes", daemon=True).start()

    def _run(self):
        while True:
            try:
                self.refresh()
            except Exception as exc:
                logger.warning(f"Crypto refresh failed: {exc}")
                with self._lock:
                    self.error = "Live feed unavailable. Showing saved quotes; retrying automatically."
            Event().wait(self.refresh_seconds)

    def status(self):
        with self._lock:
            return {
                "last_success": self.last_success,
                "error": self.error,
                "refresh_seconds": self.refresh_seconds,
            }

    def candles(self, instrument, timeframe="1D"):
        if not re.fullmatch(r"[A-Z0-9]+USD", instrument) or timeframe not in (
            "1h",
            "1D",
        ):
            raise ValueError("Unsupported instrument or timeframe")
        key = (instrument, timeframe)
        # Serialize cold loads to avoid duplicate requests and provider bursts.
        with self._candle_lock:
            cached = self._candles.get(key)
            if cached and time.monotonic() - cached[0] < self.candle_seconds:
                return cached[1].copy(), False
            try:
                rows = self._get(
                    "get-candlestick",
                    instrument_name=instrument[:-3] + "_USD",
                    timeframe=timeframe,
                    count=180,
                )
                frame = pd.DataFrame(rows).rename(
                    columns={
                        "o": "open",
                        "h": "high",
                        "l": "low",
                        "c": "close",
                        "v": "volume",
                        "t": "timestamp",
                    }
                )
                frame["timestamp"] = pd.to_datetime(
                    frame["timestamp"], unit="ms", utc=True
                )
                for col in ("open", "high", "low", "close", "volume"):
                    frame[col] = pd.to_numeric(frame[col], errors="coerce")
                frame = (
                    frame.dropna()
                    .drop_duplicates("timestamp")
                    .set_index("timestamp")
                    .sort_index()
                )
                if frame.empty:
                    raise ValueError("No candles available")
                self._candles[key] = (time.monotonic(), frame)
                return frame.copy(), False
            except Exception:
                if cached:
                    return cached[1].copy(), True
                raise


crypto_feed = CryptoFeed()
