from typing import List, Optional
import pandas as pd
from sqlalchemy import create_engine, Column, Float, String, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
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

class Database:
    def __init__(self):
        self.engine = create_engine(f"sqlite:///{settings.DB_PATH}")
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
        logger.info(f"Database initialized at {settings.DB_PATH}")

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

# Create global database instance
db = Database() 