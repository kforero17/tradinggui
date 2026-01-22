
from typing import Any, Optional
import pandas as pd
from loguru import logger

from ..config.settings import settings


class CryptoMetricsCalculator:
    """Calculator for crypto trend metrics using MCP data."""

    def __init__(self):
        self.ma_period = settings.CRYPTO_MA_PERIOD
        self.default_timeframe = settings.CRYPTO_DEFAULT_TIMEFRAME

    def calculate_trend_metrics(self, candlestick_data: list[dict]) -> dict[str, float]:
        """Calculate trend metrics from candlestick data.

        Args:
            candlestick_data: List of OHLCV candles from MCP get_candlestick tool.
                Each candle has: open, high, low, close, volume, timestamp

        Returns:
            Dict with trend metrics (last_price, ma_20, ema_20, pct_above_ma_20, pct_above_ema_20)
        """
        if not candlestick_data or len(candlestick_data) < self.ma_period:
            logger.warning(f"Insufficient data for trend calculation. Need {self.ma_period}, got {len(candlestick_data) if candlestick_data else 0}")
            return {}

        df = pd.DataFrame(candlestick_data)
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df.sort_values('timestamp')

        close_prices = df['close'].dropna()
        if len(close_prices) < self.ma_period:
            return {}

        last_price = float(close_prices.iloc[-1])
        ma = float(close_prices.rolling(window=self.ma_period).mean().iloc[-1])
        ema = float(close_prices.ewm(span=self.ma_period, adjust=False).mean().iloc[-1])

        pct_above_ma = ((last_price - ma) / ma * 100) if ma else 0
        pct_above_ema = ((last_price - ema) / ema * 100) if ema else 0

        return {
            'last_price': last_price,
            f'ma_{self.ma_period}': ma,
            f'ema_{self.ma_period}': ema,
            f'pct_above_ma_{self.ma_period}': round(pct_above_ma, 2),
            f'pct_above_ema_{self.ma_period}': round(pct_above_ema, 2),
        }

    def process_ticker_data(self, ticker_data: dict) -> dict[str, Any]:
        """Extract relevant fields from MCP ticker data.

        Args:
            ticker_data: Ticker data from MCP get_ticker tool.
                Contains: last, change, high, low, volume, volume_value, etc.

        Returns:
            Dict with extracted ticker metrics
        """
        if not ticker_data:
            return {}

        return {
            'last_price': float(ticker_data.get('last', 0)),
            'price_change_24h': float(ticker_data.get('change', 0)) * 100,
            'high_24h': float(ticker_data.get('high', 0)),
            'low_24h': float(ticker_data.get('low', 0)),
            'volume_24h': float(ticker_data.get('volume', 0)),
        }

    def get_metrics(
        self,
        instrument: str,
        candlestick_data: list[dict],
        ticker_data: dict,
        timeframe: str = None
    ) -> Optional[dict[str, Any]]:
        """Combine candlestick and ticker data into full metrics.

        Args:
            instrument: Crypto instrument name (e.g., "BTCUSD")
            candlestick_data: OHLCV data from MCP get_candlestick
            ticker_data: Current price data from MCP get_ticker
            timeframe: Candlestick timeframe used (e.g., "1h")

        Returns:
            Dict with all crypto metrics ready for database storage
        """
        timeframe = timeframe or self.default_timeframe

        ticker_metrics = self.process_ticker_data(ticker_data)
        trend_metrics = self.calculate_trend_metrics(candlestick_data)

        if not ticker_metrics and not trend_metrics:
            logger.warning(f"No metrics calculated for {instrument}")
            return None

        metrics = {
            'instrument': instrument,
            'timeframe': timeframe,
            **ticker_metrics,
        }

        if trend_metrics:
            metrics['ma_20'] = trend_metrics.get(f'ma_{self.ma_period}')
            metrics['ema_20'] = trend_metrics.get(f'ema_{self.ma_period}')
            metrics['pct_above_ma_20'] = trend_metrics.get(f'pct_above_ma_{self.ma_period}')
            metrics['pct_above_ema_20'] = trend_metrics.get(f'pct_above_ema_{self.ma_period}')

        logger.info(f"Calculated metrics for {instrument}: price={metrics.get('last_price')}, ma_20={metrics.get('ma_20')}")
        return metrics

    def candlestick_to_dataframe(self, candlestick_data: list[dict]) -> pd.DataFrame:
        """Convert MCP candlestick data to a DataFrame for plotting.

        Args:
            candlestick_data: List of candles from MCP get_candlestick

        Returns:
            DataFrame with datetime index and OHLCV columns
        """
        if not candlestick_data:
            return pd.DataFrame()

        df = pd.DataFrame(candlestick_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()

        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df


crypto_metrics_calculator = CryptoMetricsCalculator()
