#!/usr/bin/env python3
"""Test SSL bypass for yfinance with corporate VPN."""
import os
import sys

# Ensure DISABLE_SSL_VERIFY is set before importing anything
os.environ['DISABLE_SSL_VERIFY'] = 'true'

# MUST import ssl_config FIRST and call configure before other imports
from src.config.ssl_config import configure_ssl_verification
configure_ssl_verification()

# Now import libraries that use SSL
import yfinance as yf
from curl_cffi import requests


def test_curl_cffi_patch():
    """Test that curl_cffi session has verify=False after patch."""
    session = requests.Session(impersonate="chrome")
    if session.verify:
        print("FAIL: curl_cffi session still has verify=True")
        print("      The monkey-patch did not work")
        return False
    print("PASS: curl_cffi Session patched (verify=False)")
    return True


def test_yfinance_fetch():
    """Test that yfinance can fetch data through corporate VPN."""
    try:
        ticker = yf.Ticker("AAPL")
        info = ticker.info
        price = info.get('currentPrice') or info.get('regularMarketPrice')
        print(f"PASS: yfinance fetch successful")
        print(f"      AAPL price: ${price}")
        return True
    except Exception as e:
        print(f"FAIL: yfinance fetch failed")
        print(f"      Error: {e}")
        return False


def test_historical_data():
    """Test fetching historical data (used by the web app)."""
    try:
        ticker = yf.Ticker("NVDA")
        hist = ticker.history(period="5d")
        if hist.empty:
            print("FAIL: Historical data is empty")
            return False
        print(f"PASS: Historical data fetch successful")
        print(f"      NVDA last 5 days: {len(hist)} rows")
        return True
    except Exception as e:
        print(f"FAIL: Historical data fetch failed")
        print(f"      Error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("SSL Bypass Test for Corporate VPN")
    print("=" * 60)
    print()

    results = []

    print("Test 1: curl_cffi patch verification")
    results.append(test_curl_cffi_patch())
    print()

    print("Test 2: yfinance ticker info fetch")
    results.append(test_yfinance_fetch())
    print()

    print("Test 3: yfinance historical data fetch")
    results.append(test_historical_data())
    print()

    print("=" * 60)
    if all(results):
        print("ALL TESTS PASSED - SSL bypass is working!")
        sys.exit(0)
    else:
        print("SOME TESTS FAILED - Check the errors above")
        sys.exit(1)
