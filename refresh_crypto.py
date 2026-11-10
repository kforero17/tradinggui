#!/usr/bin/env python3
"""Utility script to refresh crypto data in the database.

This script is designed to be called by Claude with MCP crypto data.
It processes ticker and candlestick data and stores metrics in the database.

Usage (from Claude):
    1. Fetch ticker and candlestick data from MCP for each symbol
    2. Call this script with the data to store it

Example standalone usage:
    python refresh_crypto.py
    (Will show configured symbols and current database state)
"""

from src.config.ssl_config import configure_ssl_verification
configure_ssl_verification()

from src.config.settings import settings
from src.data.database import db
from src.analysis.crypto_metrics import crypto_metrics_calculator
from loguru import logger
import sys


def store_crypto_metrics(instrument: str, ticker_data: dict, candlestick_data: list, timeframe: str = None):
    """Store crypto metrics for a single instrument.

    Args:
        instrument: Crypto symbol (e.g., "BTCUSD")
        ticker_data: Ticker data from MCP get_ticker
        candlestick_data: List of candles from MCP get_candlestick
        timeframe: Candlestick timeframe (default from settings)

    Returns:
        dict with stored metrics or None on failure
    """
    timeframe = timeframe or settings.CRYPTO_DEFAULT_TIMEFRAME

    metrics = crypto_metrics_calculator.get_metrics(
        instrument=instrument,
        candlestick_data=candlestick_data,
        ticker_data=ticker_data,
        timeframe=timeframe
    )

    if metrics:
        db.store_crypto_metrics([metrics])
        logger.success(f"Stored metrics for {instrument}")
        return metrics
    else:
        logger.warning(f"Failed to calculate metrics for {instrument}")
        return None


def show_status():
    """Display current crypto data status."""
    print("\n=== Crypto Data Status ===\n")

    print(f"Configured MA Period: {settings.CRYPTO_MA_PERIOD}")
    print(f"Default Timeframe: {settings.CRYPTO_DEFAULT_TIMEFRAME}")
    print(f"Configured Symbols: {len(settings.CRYPTO_DEFAULT_SYMBOLS)}")

    print("\nDefault symbols:")
    for i, sym in enumerate(settings.CRYPTO_DEFAULT_SYMBOLS, 1):
        print(f"  {i:2}. {sym}")

    print("\n--- Database Status ---")
    df = db.get_latest_crypto_metrics()
    if df.empty:
        print("No crypto data in database yet.")
        print("\nTo populate data, use Claude to fetch MCP data and call store_crypto_metrics()")
    else:
        print(f"Total instruments in database: {len(df)}")
        print("\nInstruments with data:")
        for _, row in df.iterrows():
            ma_status = "MA50 calculated" if row.get('ma_50') else "No MA data"
            print(f"  {row['instrument']}: ${row['last_price']:.2f} ({ma_status})")


if __name__ == "__main__":
    show_status()
