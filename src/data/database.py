from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, Column, Float, String, DateTime, func, text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from ..config.settings import settings
from loguru import logger

Base = declarative_base()

class StockMetrics(Base):
    __tablename__ = settings.TABLE_NAME
    
    ticker = Column(String, primary_key=True)
    last_price = Column(Float)
    ma_100 = Column(Float)
    ema_100 = Column(Float)
    pct_above_ma_100 = Column(Float)
    pct_above_ema_100 = Column(Float)
    pe_ratio = Column(Float)
    pb_ratio = Column(Float)
    ps_ratio = Column(Float)
    market_cap = Column(Float)
    enterprise_value = Column(Float)
    ebitda = Column(Float)
    ebitda_ev = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow)

class Portfolio(Base):
    __tablename__ = 'portfolio'
    ticker = Column(String, primary_key=True)
    shares = Column(Float, default=0.0)
    added_at = Column(DateTime, default=datetime.utcnow)


class CryptoMetrics(Base):
    __tablename__ = settings.CRYPTO_TABLE_NAME

    instrument = Column(String, primary_key=True)
    last_price = Column(Float)
    price_change_24h = Column(Float)
    high_24h = Column(Float)
    low_24h = Column(Float)
    volume_24h = Column(Float)
    ma_20 = Column(Float)
    ema_20 = Column(Float)
    pct_above_ma_20 = Column(Float)
    pct_above_ema_20 = Column(Float)
    timeframe = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow)

class Database:
    def __init__(self):
        self.engine = create_engine(f"sqlite:///{settings.DB_PATH}")
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
        self._migrate_portfolio_table()
        logger.info(f"Database initialized at {settings.DB_PATH}")

    def _migrate_portfolio_table(self) -> None:
        """Add shares and added_at columns to portfolio table if they don't exist."""
        inspector = inspect(self.engine)
        if 'portfolio' not in inspector.get_table_names():
            return

        columns = [c['name'] for c in inspector.get_columns('portfolio')]
        with self.engine.connect() as conn:
            if 'shares' not in columns:
                conn.execute(text("ALTER TABLE portfolio ADD COLUMN shares REAL DEFAULT 0.0"))
                logger.info("Migrated portfolio table: added 'shares' column")
            if 'added_at' not in columns:
                conn.execute(text("ALTER TABLE portfolio ADD COLUMN added_at TIMESTAMP"))
                logger.info("Migrated portfolio table: added 'added_at' column")
            conn.commit()

    def store_metrics(self, metrics_list: List[dict]) -> None:
        """Store metrics in the database."""
        if not metrics_list:
            logger.warning("No metrics to store")
            return

        session = self.Session()
        try:
            for metrics in metrics_list:
                metrics['updated_at'] = datetime.utcnow()
                stock_metrics = StockMetrics(**metrics)
                session.merge(stock_metrics)  # Use merge for upsert behavior
            
            session.commit()
            logger.info(f"Stored {len(metrics_list)} records in database")
        except Exception as e:
            session.rollback()
            logger.error(f"Error storing metrics: {e}")
            raise
        finally:
            session.close()

    def has_recent_metrics(self, ticker: str, age_limit_days: int) -> bool:
        """Check if a ticker has recent metrics."""
        session = self.Session()
        try:
            latest_update = session.query(func.max(StockMetrics.updated_at)).filter_by(ticker=ticker).scalar()
            if latest_update:
                if datetime.utcnow() - latest_update < timedelta(days=age_limit_days):
                    logger.debug(f"Ticker {ticker} has recent data. Last updated: {latest_update}. Skipping.")
                    return True
            return False
        finally:
            session.close()

    def get_latest_metrics(self, ticker: Optional[str] = None) -> pd.DataFrame:
        """Retrieve latest metrics from database for each ticker."""
        session = self.Session()
        try:
            # Subquery to find the latest update time for each ticker
            latest_updates = session.query(
                StockMetrics.ticker, 
                func.max(StockMetrics.updated_at).label('latest_update')
            ).group_by(StockMetrics.ticker).subquery()

            # Main query to get the full record for the latest update
            query = session.query(StockMetrics).join(
                latest_updates,
                (StockMetrics.ticker == latest_updates.c.ticker) &
                (StockMetrics.updated_at == latest_updates.c.latest_update)
            )

            if ticker:
                query = query.filter(StockMetrics.ticker == ticker)
            
            df = pd.read_sql(query.statement, session.bind)
            return df
        finally:
            session.close()

    def get_tickers(self) -> List[str]:
        """Get list of all tickers in database."""
        session = self.Session()
        try:
            return [ticker[0] for ticker in session.query(StockMetrics.ticker).all()]
        finally:
            session.close()

    def get_portfolio_tickers(self) -> List[str]:
        """Get all tickers from the portfolio."""
        session = self.Session()
        try:
            return [p.ticker for p in session.query(Portfolio.ticker).all()]
        finally:
            session.close()

    def add_portfolio_ticker(self, ticker: str) -> None:
        """Add a ticker to the portfolio."""
        session = self.Session()
        try:
            existing = session.query(Portfolio).filter_by(ticker=ticker).first()
            if not existing:
                session.add(Portfolio(ticker=ticker, added_at=datetime.utcnow()))
                session.commit()
                logger.info(f"Added {ticker} to portfolio.")
            else:
                logger.warning(f"Ticker {ticker} already in portfolio.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding {ticker} to portfolio: {e}")
            raise
        finally:
            session.close()

    def delete_portfolio_ticker(self, ticker: str) -> None:
        """Delete a ticker from the portfolio."""
        session = self.Session()
        try:
            record = session.query(Portfolio).filter_by(ticker=ticker).first()
            if record:
                session.delete(record)
                session.commit()
                logger.info(f"Deleted {ticker} from portfolio.")
            else:
                logger.warning(f"Ticker {ticker} not found in portfolio for deletion.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting {ticker} from portfolio: {e}")
            raise
        finally:
            session.close()

    def update_portfolio_shares(self, ticker: str, shares: float) -> None:
        """Update the number of shares for a portfolio position."""
        session = self.Session()
        try:
            record = session.query(Portfolio).filter_by(ticker=ticker).first()
            if record:
                record.shares = shares
                session.commit()
                logger.info(f"Updated {ticker} shares to {shares}")
            else:
                logger.warning(f"Ticker {ticker} not found in portfolio")
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating shares for {ticker}: {e}")
            raise
        finally:
            session.close()

    def get_portfolio_with_shares(self) -> List[dict]:
        """Get all portfolio positions with share quantities and add dates."""
        session = self.Session()
        try:
            positions = session.query(Portfolio).all()
            return [
                {'ticker': p.ticker, 'shares': p.shares or 0.0, 'added_at': p.added_at}
                for p in positions
            ]
        finally:
            session.close()

    def store_crypto_metrics(self, metrics_list: List[dict]) -> None:
        """Store crypto metrics in the database."""
        if not metrics_list:
            logger.warning("No crypto metrics to store")
            return

        session = self.Session()
        try:
            for metrics in metrics_list:
                metrics['updated_at'] = datetime.utcnow()
                crypto_metrics = CryptoMetrics(**metrics)
                session.merge(crypto_metrics)

            session.commit()
            logger.info(f"Stored {len(metrics_list)} crypto records in database")
        except Exception as e:
            session.rollback()
            logger.error(f"Error storing crypto metrics: {e}")
            raise
        finally:
            session.close()

    def get_latest_crypto_metrics(self, instrument: Optional[str] = None) -> pd.DataFrame:
        """Retrieve latest crypto metrics from database."""
        session = self.Session()
        try:
            query = session.query(CryptoMetrics)
            if instrument:
                query = query.filter(CryptoMetrics.instrument == instrument)

            df = pd.read_sql(query.statement, session.bind)
            return df
        finally:
            session.close()

    def has_recent_crypto_metrics(self, instrument: str, age_limit_minutes: int) -> bool:
        """Check if a crypto instrument has recent metrics."""
        session = self.Session()
        try:
            latest_update = session.query(func.max(CryptoMetrics.updated_at)).filter_by(instrument=instrument).scalar()
            if latest_update:
                if datetime.utcnow() - latest_update < timedelta(minutes=age_limit_minutes):
                    return True
            return False
        finally:
            session.close()


# Create global database instance
db = Database() 
