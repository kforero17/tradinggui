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
        level="DEBUG",  # Temporarily set to DEBUG for troubleshooting
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

def process_tickers_batch(tickers: List[str]) -> None:
    """Process tickers using batch processing for better efficiency and rate limiting."""
    total = len(tickers)
    logger.info(f"Starting to process a batch of {total} tickers.")
    
    try:
        all_metrics = metrics_calculator.get_metrics_batch(tickers)
        
        if all_metrics:
            logger.info(f"Storing {len(all_metrics)} new metrics in database.")
            db.store_metrics(all_metrics)
        else:
            logger.info("No new metrics were generated in this batch.")
            
    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        raise

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
        
        # Process all tickers in batches of 50
        batch_size = 50
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        for batch_num, i in enumerate(range(0, len(tickers), batch_size), 1):
            batch = tickers[i:i+batch_size]
            logger.info(f"⚡ Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")
            process_tickers_batch(batch)
        
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