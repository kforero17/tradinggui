"""``python -m src.main``: the full weekly job (same as ``python -m src.jobs weekly``)."""

import sys

from loguru import logger

from .config.settings import settings
from .data.database import db
from .data.ticker_loader import ticker_loader
from .jobs.pipeline import JobAlreadyRunning, run_weekly

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)


def setup_logging() -> None:
    """Log to stderr and, if set, ``LOG_FILE``, both at ``LOG_LEVEL``.

    Colors only when stderr is a terminal, so launchd's log files stay plain.
    """
    logger.remove()
    logger.add(sys.stderr, format=LOG_FORMAT, level=settings.LOG_LEVEL)
    if settings.LOG_FILE:
        settings.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            settings.LOG_FILE,
            format=LOG_FORMAT,
            level=settings.LOG_LEVEL,
            rotation="1 day",
            retention="7 days",
        )


def display_database_summary() -> None:
    """Log a summary of the data in the database."""
    try:
        df = db.get_latest_metrics()
        if not df.empty:
            logger.info(f"Database contains {len(df)} stock records")
            if "pe_ratio" in df.columns:
                valid_pe = df["pe_ratio"].dropna()
                if not valid_pe.empty:
                    logger.info(
                        f"P/E ratios: min={valid_pe.min():.2f}, "
                        f"max={valid_pe.max():.2f}, avg={valid_pe.mean():.2f}"
                    )
            if "last_price" in df.columns:
                valid_prices = df["last_price"].dropna()
                if not valid_prices.empty:
                    logger.info(
                        f"Stock prices: min=${valid_prices.min():.2f}, "
                        f"max=${valid_prices.max():.2f}"
                    )
            logger.info(f"Sample tickers: {', '.join(df['ticker'].head(10).tolist())}")
        else:
            logger.warning("Database is empty")
    except Exception as e:
        logger.error(f"Error displaying database summary: {e}")


def main() -> None:
    """Run the weekly job; exit 1 on missing input files, a held job lock or an error."""
    try:
        setup_logging()
        logger.info("🚀 Starting stock metrics collection (weekly job)")

        if not ticker_loader.validate_ticker_files():
            logger.error("❌ Missing required input files")
            sys.exit(1)

        report = run_weekly()
        logger.info(report.summary_line())
        display_database_summary()
        logger.success("✅ Stock metrics collection completed successfully!")

    except JobAlreadyRunning as e:
        logger.error(f"❌ {e}; another job is running")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.warning("⚠️  Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        logger.exception("Full error details:")
        sys.exit(1)


if __name__ == "__main__":
    main()
