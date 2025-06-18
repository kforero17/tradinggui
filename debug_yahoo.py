#!/usr/bin/env python3
"""Debug script to test Yahoo Finance API with a single ticker."""

import sys
import traceback
from datetime import datetime, timedelta
from yahoo_fin import stock_info as si
import requests
import json

def test_direct_request():
    """Test direct HTTP request to Yahoo Finance."""
    print("=== Testing Direct HTTP Request ===")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    # Test URL that yahoo_fin might be using
    ticker = "AAPL"
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
    
    print(f"Testing URL: {url}")
    print(f"Headers: {headers}")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Text (first 500 chars): {response.text[:500]}")
        
        if response.status_code != 200:
            print(f"❌ Request failed with status {response.status_code}")
            return False
        
        if response.text.startswith('<!DOCTYPE html>') or response.text.startswith('<html'):
            print("❌ Received HTML response instead of data")
            return False
            
        print("✅ Direct request successful")
        return True
        
    except Exception as e:
        print(f"❌ Direct request failed: {e}")
        traceback.print_exc()
        return False

def test_yahoo_fin():
    """Test yahoo_fin library."""
    print("\n=== Testing yahoo_fin Library ===")
    
    ticker = "AAPL"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    try:
        print(f"Testing ticker: {ticker}")
        start_date = datetime.today() - timedelta(days=30)
        print(f"Start date: {start_date}")
        
        # Test with our headers
        print("Attempting to fetch data with custom headers...")
        df = si.get_data(ticker, start_date=start_date, headers=headers)
        
        if df is None:
            print("❌ get_data returned None")
            return False
        elif df.empty:
            print("❌ get_data returned empty DataFrame")
            return False
        else:
            print(f"✅ Successfully fetched {len(df)} rows")
            print(f"Columns: {df.columns.tolist()}")
            print(f"Date range: {df.index.min()} to {df.index.max()}")
            print(f"Sample data:\n{df.head(3)}")
            return True
            
    except Exception as e:
        print(f"❌ yahoo_fin failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        
        # Try without headers
        try:
            print("\nTrying without custom headers...")
            df = si.get_data(ticker, start_date=start_date)
            if df is not None and not df.empty:
                print(f"✅ Success without custom headers: {len(df)} rows")
                return True
        except Exception as e2:
            print(f"❌ Also failed without headers: {e2}")
        
        return False

def test_simple_price():
    """Test simple price fetching."""
    print("\n=== Testing Simple Price Fetch ===")
    
    ticker = "AAPL"
    
    try:
        print(f"Getting live price for {ticker}...")
        price = si.get_live_price(ticker)
        print(f"✅ Live price: ${price:.2f}")
        return True
    except Exception as e:
        print(f"❌ Live price failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🔍 Yahoo Finance API Debug Tool")
    print("=" * 50)
    
    # Run tests
    test1 = test_direct_request()
    test2 = test_yahoo_fin()
    test3 = test_simple_price()
    
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    print(f"Direct HTTP Request: {'✅ PASS' if test1 else '❌ FAIL'}")
    print(f"yahoo_fin Library: {'✅ PASS' if test2 else '❌ FAIL'}")
    print(f"Simple Price Fetch: {'✅ PASS' if test3 else '❌ FAIL'}")
    
    if not any([test1, test2, test3]):
        print("\n❌ All tests failed - Yahoo Finance API may be blocked or changed")
        print("💡 Recommendations:")
        print("   1. Check network connectivity")
        print("   2. Try running from a different network")
        print("   3. Consider using alternative data sources")
        print("   4. Implement proxy/VPN if corporate firewall is blocking")
    elif test3 and not test2:
        print("\n⚠️  Simple API works but historical data doesn't")
        print("💡 This suggests rate limiting or API endpoint changes")

if __name__ == "__main__":
    main() 