import os
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
from loguru import logger
import sys
from pathlib import Path

# Add src to Python path and import directly
sys.path.insert(0, str(Path(__file__).parent))
from src.data.database import db

def setup_logging():
    """Configure logging for testing."""
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    logger.remove()  # Remove default handler
    logger.add(
        sys.stderr,
        format=log_format,
        level="DEBUG",
        colorize=True
    )

def test_single_ticker(ticker: str) -> None:
    """Test fetching data for a single ticker using yfinance."""
    logger.info(f"Testing ticker: {ticker}")
    
    try:
        # Create a Ticker object
        stock = yf.Ticker(ticker)
        
        # Get historical data
        logger.info(f"Fetching historical data for {ticker}")
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)  # 1 year of data
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            logger.warning(f"No historical data available for {ticker}")
            return
            
        logger.info(f"Successfully fetched {len(hist)} days of historical data")
        
        # Get stock info
        logger.info(f"Fetching stock info for {ticker}")
        info = stock.info
        
        # Extract key metrics
        metrics = {
            'ticker': ticker,
            'last_price': hist['Close'].iloc[-1] if not hist.empty else None,
            'pe_ratio': info.get('trailingPE', None),
            'market_cap': info.get('marketCap', None),
            'dividend_yield': info.get('dividendYield', None),
            'beta': info.get('beta', None),
            'volume': hist['Volume'].iloc[-1] if not hist.empty else None,
            'timestamp': datetime.now().isoformat()
        }
        
        # Log the metrics
        logger.info(f"Metrics for {ticker}:")
        for key, value in metrics.items():
            if key != 'timestamp':
                logger.info(f"  {key}: {value}")
        
        # Store in database
        db.store_metrics([metrics])
        logger.success(f"Successfully stored metrics for {ticker}")
        
    except Exception as e:
        logger.error(f"Error processing {ticker}: {str(e)}")
        logger.exception("Full error details:")

def test_batch_processing(tickers: list, batch_size: int = 2) -> None:
    """Test batch processing of multiple tickers."""
    total = len(tickers)
    logger.info(f"Testing batch processing with {total} tickers, batch size {batch_size}")
    
    successful = 0
    failed = 0
    
    for i in range(0, total, batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size
        
        logger.info(f"Processing batch {batch_num}/{total_batches}: {batch}")
        
        for ticker in batch:
            try:
                test_single_ticker(ticker)
                successful += 1
            except Exception as e:
                logger.error(f"Failed to process {ticker}: {str(e)}")
                failed += 1
            
            # Add a small delay between tickers in the same batch
            time.sleep(1)
        
        # Add a longer delay between batches
        if i + batch_size < total:
            logger.info("Waiting 5 seconds before next batch...")
            time.sleep(5)
    
    logger.info(f"Batch processing complete. Successful: {successful}, Failed: {failed}")

def main():
    """Main test function."""
    setup_logging()
    logger.info("🔍 Testing yfinance library for stock data fetching")
    
    # Test single ticker
    logger.info("\n=== Testing Single Ticker ===")
    test_single_ticker("AAPL")
    
    # Test batch processing
    logger.info("\n=== Testing Batch Processing ===")
    test_tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]
    test_batch_processing(test_tickers, batch_size=2)
    
    logger.info("\n=== Test Complete ===")

if __name__ == "__main__":
    main() 