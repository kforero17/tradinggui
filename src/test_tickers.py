from typing import List
from loguru import logger
import sys
from pathlib import Path

from .config.settings import settings
from .data.database import db
from .analysis.metrics import metrics_calculator

def setup_logging():
    """Configure logging for testing."""
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
    )
    
    logger.remove()
    logger.add(
        sys.stderr,
        format=log_format,
        level="DEBUG",  # More verbose for testing
        colorize=True
    )

def test_tickers(tickers: List[str]) -> None:
    """Test processing of specific tickers."""
    total = len(tickers)
    successful = 0
    failed = 0
    
    logger.info(f"Starting test with {total} tickers: {', '.join(tickers)}")
    
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"\n[{i}/{total}] Processing {ticker}")
        
        try:
            # Get metrics
            metrics = metrics_calculator.get_metrics(ticker)
            
            if metrics:
                # Print metrics for inspection
                logger.info(f"Metrics for {ticker}:")
                for key, value in metrics.items():
                    logger.info(f"  {key}: {value}")
                
                # Store in database
                db.store_metrics([metrics])
                successful += 1
                logger.success(f"Successfully processed {ticker}")
            else:
                failed += 1
                logger.warning(f"No metrics available for {ticker}")
                
        except Exception as e:
            failed += 1
            logger.error(f"Error processing {ticker}: {e}")
    
    # Print summary
    logger.info("\nTest Summary:")
    logger.info(f"Total tickers: {total}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    
    # Print database contents
    logger.info("\nDatabase Contents:")
    df = db.get_latest_metrics()
    if not df.empty:
        logger.info("\n" + df.to_string())
    else:
        logger.warning("No data in database")

def main():
    """Test main function."""
    try:
        # Setup logging
        setup_logging()
        
        # Test tickers
        tickers_to_test = ["AAPL", "NVDA", "TSLA", "MSFT", "GLD"]
        test_tickers(tickers_to_test)
        
    except KeyboardInterrupt:
        logger.warning("Test interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 