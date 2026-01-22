#!/usr/bin/env python3
"""Test rate limiting and StockDex fallback."""
import os
import time

os.environ['DISABLE_SSL_VERIFY'] = 'true'

from src.config.ssl_config import configure_ssl_verification
configure_ssl_verification()

from src.analysis.metrics import metrics_calculator
from src.config.settings import settings

print("=" * 60)
print("Rate Limiting Test")
print("=" * 60)
print(f"Max workers: {settings.MAX_CONCURRENT_WORKERS}")
print(f"Requests per second: {settings.REQUESTS_PER_SECOND}")
print()

# Test with a small batch of tickers
test_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

print(f"Testing with {len(test_tickers)} tickers: {test_tickers}")
print()

start_time = time.time()

for ticker in test_tickers:
    print(f"Fetching {ticker}...")
    result = metrics_calculator.get_metrics(ticker)
    if result:
        print(f"  ✓ {ticker}: price=${result.get('last_price', 'N/A'):.2f}")
    else:
        print(f"  ✗ {ticker}: Failed")

elapsed = time.time() - start_time
print()
print(f"Total time: {elapsed:.1f}s for {len(test_tickers)} tickers")
print(f"Average: {elapsed/len(test_tickers):.1f}s per ticker")
print("=" * 60)
