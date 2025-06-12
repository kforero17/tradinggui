import time
from typing import List, Optional
from loguru import logger
import sys
from pathlib import Path

from .config.settings import settings
from .data.ticker_loader import ticker_loader
from .data.database import db
from .analysis.metrics import metrics_calculator

def setup_logging():
    """Configure logging."""
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
        level=settings.LOG_LEVEL,
        colorize=True
    )
    
    if settings.LOG_FILE:
        logger.add(
            settings.LOG_FILE,
            format=log_format,
            level=settings.LOG_LEVEL,
            rotation="1 day",
            retention="7 days"
        )

def process_tickers_batch(tickers: List[str], batch_size: int = 20) -> None:
    """Process tickers using batch processing for better efficiency and rate limiting."""
    total = len(tickers)
    logger.info(f"Starting batch processing of {total} tickers with batch size {batch_size}")
    
    try:
        # Use the new batch processing method
        all_metrics = metrics_calculator.get_metrics_batch(tickers, batch_size=batch_size)
        
        # Store all successful metrics in the database
        if all_metrics:
            logger.info(f"Storing {len(all_metrics)} successful metrics in database")
            db.store_metrics(all_metrics)
            
        # Calculate and log final statistics
        successful = len(all_metrics)
        failed = total - successful
        success_rate = (successful / total) * 100 if total > 0 else 0
        
        logger.info(f"Batch processing complete:")
        logger.info(f"  Total tickers: {total}")
        logger.info(f"  Successful: {successful}")
        logger.info(f"  Failed: {failed}")
        logger.info(f"  Success rate: {success_rate:.1f}%")
        
        # Log some sample results
        if all_metrics:
            logger.info("Sample processed tickers:")
            for i, metrics in enumerate(all_metrics[:5]):  # Show first 5
                ticker = metrics['ticker']
                pe_ratio = metrics.get('pe_ratio', 'N/A')
                last_price = metrics.get('last_price', 'N/A')
                logger.info(f"  {ticker}: Price=${last_price:.2f}, P/E={pe_ratio}")
                
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        raise

def process_tickers_individual(tickers: List[str]) -> None:
    """Fallback method: Process tickers individually with rate limiting."""
    total = len(tickers)
    successful = 0
    failed = 0
    
    logger.info(f"Processing {total} tickers individually")
    
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"[{i}/{total}] Processing {ticker}")
        
        try:
            metrics = metrics_calculator.get_metrics(ticker)
            if metrics:
                db.store_metrics([metrics])
                successful += 1
                logger.success(f"✓ {ticker} processed successfully")
            else:
                failed += 1
                logger.warning(f"✗ {ticker} failed - no metrics available")
                
        except Exception as e:
            logger.error(f"✗ {ticker} failed - error: {e}")
            failed += 1
            
        # Rate limiting between individual requests
        if i < total:
            time.sleep(settings.YAHOO_FIN_RATE_LIMIT)
    
    success_rate = (successful / total) * 100 if total > 0 else 0
    logger.info(f"Individual processing complete:")
    logger.info(f"  Successful: {successful}, Failed: {failed}, Total: {total}")
    logger.info(f"  Success rate: {success_rate:.1f}%")

def display_database_summary():
    """Display a summary of the data in the database."""
    try:
        df = db.get_latest_metrics()
        if not df.empty:
            total_records = len(df)
            logger.info(f"Database contains {total_records} stock records")
            
            # Show some statistics
            if 'pe_ratio' in df.columns:
                valid_pe = df['pe_ratio'].dropna()
                if not valid_pe.empty:
                    logger.info(f"P/E ratios: min={valid_pe.min():.2f}, max={valid_pe.max():.2f}, avg={valid_pe.mean():.2f}")
            
            if 'last_price' in df.columns:
                valid_prices = df['last_price'].dropna()
                if not valid_prices.empty:
                    logger.info(f"Stock prices: min=${valid_prices.min():.2f}, max=${valid_prices.max():.2f}")
            
            # Show sample tickers
            logger.info(f"Sample tickers: {', '.join(df['ticker'].head(10).tolist())}")
        else:
            logger.warning("Database is empty")
    except Exception as e:
        logger.error(f"Error displaying database summary: {e}")

def main():
    """Main execution function."""
    try:
        # Setup logging
        setup_logging()
        logger.info("🚀 Starting stock metrics collection")
        
        # Display current mode
        if metrics_calculator.use_mock_data:
            logger.info("📊 Running in MOCK DATA mode (for testing)")
        else:
            logger.info("🌐 Running in LIVE DATA mode")
        
        # Validate input files
        if not ticker_loader.validate_ticker_files():
            logger.error("❌ Missing required input files")
            sys.exit(1)
        
        # Load tickers
        logger.info("📁 Loading ticker symbols...")
        tickers = ticker_loader.load_unique_tickers()
        if not tickers:
            logger.error("❌ No valid tickers found")
            sys.exit(1)
        
        logger.info(f"📈 Found {len(tickers)} unique tickers to process")
        
        # Process all tickers using batch method for optimal performance
        logger.info("⚡ Processing all tickers using batch method")
        process_tickers_batch(tickers, batch_size=20)
        
        # Display final summary
        logger.info("📊 Displaying database summary:")
        display_database_summary()
        
        logger.success("✅ Stock metrics collection completed successfully!")
        
    except KeyboardInterrupt:
        logger.warning("⚠️  Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        logger.exception("Full error details:")
        sys.exit(1)

if __name__ == "__main__":
    main() 