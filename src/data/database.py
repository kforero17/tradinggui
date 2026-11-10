from typing import List, Optional
from pathlib import Path
import uuid
import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Float,
    String,
    Date,
    DateTime,
    Boolean,
    func,
    text,
    inspect,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import date, datetime, timedelta
from ..config.settings import settings
from .broker_import import Fill, Side
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
    # Technical indicators
    rsi_14 = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    macd_histogram = Column(Float)
    bb_upper = Column(Float)
    bb_middle = Column(Float)
    bb_lower = Column(Float)
    # Sector/industry classification
    sector = Column(String)
    industry = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow)


class Portfolio(Base):
    __tablename__ = "portfolio"
    ticker = Column(String, primary_key=True)
    # Negative shares are a short position; cost_basis is then the total short-sale proceeds.
    shares = Column(Float, default=0.0)
    cost_basis = Column(Float, default=0.0)
    added_at = Column(DateTime, default=datetime.utcnow)


OPTION_CONTRACT_MULTIPLIER = 100


def option_contract_symbol(
    underlying: str, expiry: date, option_type: str, strike: float
) -> str:
    """Build the OCC symbol, e.g. SPCX270115P00140000 for a $140 put expiring 2027-01-15."""
    return (
        f"{underlying}{expiry:%y%m%d}{option_type[0].upper()}{round(strike * 1000):08d}"
    )


class OptionPosition(Base):
    __tablename__ = "option_positions"
    id = Column(String, primary_key=True)
    underlying = Column(String, nullable=False)
    option_type = Column(String, nullable=False)  # 'call' or 'put'
    strike = Column(Float, nullable=False)
    expiry = Column(Date, nullable=False)
    # Negative contracts are written (short) options; cost_basis is then the total premium received.
    contracts = Column(Float, nullable=False)
    cost_basis = Column(Float, default=0.0)
    added_at = Column(DateTime, default=datetime.utcnow)


class RealizedGains(Base):
    __tablename__ = "realized_gains"
    id = Column(String, primary_key=True)
    ticker = Column(String, nullable=False)
    shares_sold = Column(Float, nullable=False)
    cost_basis = Column(Float, nullable=False)
    sale_price = Column(Float, nullable=False)
    year_start_price = Column(Float, nullable=True)
    realized_gain = Column(Float, nullable=False)
    sold_at = Column(DateTime, default=datetime.utcnow)


class CryptoMetrics(Base):
    __tablename__ = settings.CRYPTO_TABLE_NAME

    instrument = Column(String, primary_key=True)
    last_price = Column(Float)
    price_change_24h = Column(Float)
    high_24h = Column(Float)
    low_24h = Column(Float)
    volume_24h = Column(Float)
    ma_50 = Column(Float)
    ema_50 = Column(Float)
    pct_above_ma_50 = Column(Float)
    pct_above_ema_50 = Column(Float)
    timeframe = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow)


class Alert(Base):
    __tablename__ = "alerts"
    id = Column(String, primary_key=True)
    ticker = Column(String, nullable=False)
    alert_type = Column(
        String, nullable=False
    )  # price_above, price_below, rsi_overbought, rsi_oversold, macd_crossover, pct_change
    threshold = Column(Float, nullable=False)
    is_active = Column(Boolean, default=True)
    is_triggered = Column(Boolean, default=False)
    triggered_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class BrokerFill(Base):
    """A buy/sell fill imported from a broker CSV export; ``id`` is ``Fill.dedupe_key``."""

    __tablename__ = "broker_fills"
    id = Column(String, primary_key=True)
    account = Column(String, nullable=False)
    trade_date = Column(Date, nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fees = Column(Float, default=0.0)
    source = Column(String)
    imported_at = Column(DateTime, default=datetime.utcnow)


class WatchlistEntry(Base):
    __tablename__ = "trend_watchlist"
    ticker = Column(String, primary_key=True)
    added_at = Column(DateTime, default=datetime.utcnow)


class Database:
    def __init__(self):
        import shutil
        import tempfile

        db_path = settings.DB_PATH
        self._original_db_path = db_path

        # Test if the configured path is writable by SQLite
        # VirtioFS/FUSE mounts can cause disk I/O errors with SQLite
        try:
            import sqlite3

            test_conn = sqlite3.connect(str(db_path))
            test_conn.execute("PRAGMA journal_mode=WAL")
            test_conn.execute("SELECT 1")
            test_conn.close()
        except sqlite3.OperationalError:
            # Fall back to /tmp with a copy of the original DB
            tmp_db = Path(tempfile.gettempdir()) / "stock_metrics.db"
            if db_path.exists() and not tmp_db.exists():
                shutil.copy2(str(db_path), str(tmp_db))
                logger.warning(f"SQLite I/O error on {db_path}, copied to {tmp_db}")
            elif not tmp_db.exists():
                logger.warning(
                    f"SQLite I/O error on {db_path}, creating fresh DB at {tmp_db}"
                )
            db_path = tmp_db

        self._active_db_path = db_path
        self.engine = create_engine(
            f"sqlite:///{db_path}",
            connect_args={"timeout": 30},
        )
        # Use WAL journal mode for better concurrency
        from sqlalchemy import event

        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.close()

        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
        self._migrate_portfolio_table()
        self._migrate_realized_gains_table()
        self._migrate_crypto_ma_columns()
        self._migrate_technical_indicators_columns()
        logger.info(f"Database initialized at {db_path}")

    def _migrate_portfolio_table(self) -> None:
        """Add shares, added_at, and cost_basis columns to portfolio table if they don't exist."""
        inspector = inspect(self.engine)
        if "portfolio" not in inspector.get_table_names():
            return

        columns = [c["name"] for c in inspector.get_columns("portfolio")]
        with self.engine.connect() as conn:
            if "shares" not in columns:
                conn.execute(
                    text("ALTER TABLE portfolio ADD COLUMN shares REAL DEFAULT 0.0")
                )
                logger.info("Migrated portfolio table: added 'shares' column")
            if "added_at" not in columns:
                conn.execute(
                    text("ALTER TABLE portfolio ADD COLUMN added_at TIMESTAMP")
                )
                logger.info("Migrated portfolio table: added 'added_at' column")
            if "cost_basis" not in columns:
                conn.execute(
                    text("ALTER TABLE portfolio ADD COLUMN cost_basis REAL DEFAULT 0.0")
                )
                logger.info("Migrated portfolio table: added 'cost_basis' column")
            conn.commit()

    def _migrate_realized_gains_table(self) -> None:
        """Add year_start_price column to realized_gains table if it doesn't exist."""
        inspector = inspect(self.engine)
        if "realized_gains" not in inspector.get_table_names():
            return

        columns = [c["name"] for c in inspector.get_columns("realized_gains")]
        with self.engine.connect() as conn:
            if "year_start_price" not in columns:
                conn.execute(
                    text("ALTER TABLE realized_gains ADD COLUMN year_start_price REAL")
                )
                logger.info(
                    "Migrated realized_gains table: added 'year_start_price' column"
                )
            conn.commit()

    def _migrate_crypto_ma_columns(self) -> None:
        """Migrate crypto_metrics table to MA50 columns."""
        inspector = inspect(self.engine)
        if settings.CRYPTO_TABLE_NAME not in inspector.get_table_names():
            return

        columns = [c["name"] for c in inspector.get_columns(settings.CRYPTO_TABLE_NAME)]
        with self.engine.connect() as conn:
            if "ma_50" not in columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.CRYPTO_TABLE_NAME} ADD COLUMN ma_50 REAL"
                    )
                )
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.CRYPTO_TABLE_NAME} ADD COLUMN ema_50 REAL"
                    )
                )
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.CRYPTO_TABLE_NAME} ADD COLUMN pct_above_ma_50 REAL"
                    )
                )
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.CRYPTO_TABLE_NAME} ADD COLUMN pct_above_ema_50 REAL"
                    )
                )
                logger.info("Migrated crypto_metrics table: added MA50 columns")
            conn.commit()

    def _migrate_technical_indicators_columns(self) -> None:
        """Add RSI, MACD, Bollinger Bands, sector/industry columns to stock metrics table."""
        inspector = inspect(self.engine)
        if settings.TABLE_NAME not in inspector.get_table_names():
            return

        columns = [c["name"] for c in inspector.get_columns(settings.TABLE_NAME)]
        new_columns = [
            ("rsi_14", "REAL"),
            ("macd", "REAL"),
            ("macd_signal", "REAL"),
            ("macd_histogram", "REAL"),
            ("bb_upper", "REAL"),
            ("bb_middle", "REAL"),
            ("bb_lower", "REAL"),
            ("sector", "TEXT"),
            ("industry", "TEXT"),
        ]

        with self.engine.connect() as conn:
            for col_name, col_type in new_columns:
                if col_name not in columns:
                    conn.execute(
                        text(
                            f"ALTER TABLE {settings.TABLE_NAME} ADD COLUMN {col_name} {col_type}"
                        )
                    )
                    logger.info(
                        f"Migrated {settings.TABLE_NAME} table: added '{col_name}' column"
                    )
            conn.commit()

    def store_metrics(self, metrics_list: List[dict]) -> None:
        """Store metrics in the database."""
        if not metrics_list:
            logger.warning("No metrics to store")
            return

        session = self.Session()
        try:
            for metrics in metrics_list:
                metrics["updated_at"] = datetime.utcnow()
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
            latest_update = (
                session.query(func.max(StockMetrics.updated_at))
                .filter_by(ticker=ticker)
                .scalar()
            )
            if latest_update:
                if datetime.utcnow() - latest_update < timedelta(days=age_limit_days):
                    logger.debug(
                        f"Ticker {ticker} has recent data. Last updated: {latest_update}. Skipping."
                    )
                    return True
            return False
        finally:
            session.close()

    def get_latest_metrics(self, ticker: Optional[str] = None) -> pd.DataFrame:
        """Retrieve latest metrics from database for each ticker."""
        session = self.Session()
        try:
            # Subquery to find the latest update time for each ticker
            latest_updates = (
                session.query(
                    StockMetrics.ticker,
                    func.max(StockMetrics.updated_at).label("latest_update"),
                )
                .group_by(StockMetrics.ticker)
                .subquery()
            )

            # Main query to get the full record for the latest update
            query = session.query(StockMetrics).join(
                latest_updates,
                (StockMetrics.ticker == latest_updates.c.ticker)
                & (StockMetrics.updated_at == latest_updates.c.latest_update),
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

    def add_portfolio_ticker(self, ticker: str, cost_basis: float = 0.0) -> None:
        """Add a ticker to the portfolio with optional cost basis."""
        session = self.Session()
        try:
            existing = session.query(Portfolio).filter_by(ticker=ticker).first()
            if not existing:
                session.add(
                    Portfolio(
                        ticker=ticker, cost_basis=cost_basis, added_at=datetime.utcnow()
                    )
                )
                session.commit()
                logger.info(
                    f"Added {ticker} to portfolio with cost basis ${cost_basis:.2f}"
                )
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
        """Get all portfolio positions with share quantities, cost basis, and add dates."""
        session = self.Session()
        try:
            positions = session.query(Portfolio).all()
            return [
                {
                    "ticker": p.ticker,
                    "shares": p.shares or 0.0,
                    "cost_basis": p.cost_basis or 0.0,
                    "added_at": p.added_at,
                }
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
                metrics["updated_at"] = datetime.utcnow()
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

    def get_latest_crypto_metrics(
        self, instrument: Optional[str] = None
    ) -> pd.DataFrame:
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

    def has_recent_crypto_metrics(
        self, instrument: str, age_limit_minutes: int
    ) -> bool:
        """Check if a crypto instrument has recent metrics."""
        session = self.Session()
        try:
            latest_update = (
                session.query(func.max(CryptoMetrics.updated_at))
                .filter_by(instrument=instrument)
                .scalar()
            )
            if latest_update:
                if datetime.utcnow() - latest_update < timedelta(
                    minutes=age_limit_minutes
                ):
                    return True
            return False
        finally:
            session.close()

    def update_portfolio_cost_basis(self, ticker: str, cost_basis: float) -> None:
        """Update the cost basis for a portfolio position."""
        session = self.Session()
        try:
            record = session.query(Portfolio).filter_by(ticker=ticker).first()
            if record:
                record.cost_basis = cost_basis
                session.commit()
                logger.info(f"Updated {ticker} cost basis to ${cost_basis:.2f}")
            else:
                logger.warning(f"Ticker {ticker} not found in portfolio")
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating cost basis for {ticker}: {e}")
            raise
        finally:
            session.close()

    def record_realized_gain(
        self, ticker: str, shares_sold: float, cost_basis: float, sale_price: float
    ) -> float:
        """Record a realized gain/loss from selling a position."""
        session = self.Session()
        try:
            realized_gain = (sale_price - cost_basis) * shares_sold
            record = RealizedGains(
                id=str(uuid.uuid4()),
                ticker=ticker,
                shares_sold=shares_sold,
                cost_basis=cost_basis,
                sale_price=sale_price,
                realized_gain=realized_gain,
                sold_at=datetime.utcnow(),
            )
            session.add(record)
            session.commit()
            logger.info(f"Recorded realized gain for {ticker}: ${realized_gain:.2f}")
            return realized_gain
        except Exception as e:
            session.rollback()
            logger.error(f"Error recording realized gain for {ticker}: {e}")
            raise
        finally:
            session.close()

    def get_ytd_realized_gains(self) -> dict:
        """Get total realized gains for the current year."""
        session = self.Session()
        try:
            start_of_year = datetime(datetime.utcnow().year, 1, 1)
            gains = (
                session.query(RealizedGains)
                .filter(RealizedGains.sold_at >= start_of_year)
                .all()
            )

            total_gain = sum(g.realized_gain for g in gains)
            gain_count = len(gains)

            return {
                "ytd_realized_gain_dollars": round(total_gain, 2),
                "transactions_count": gain_count,
                "gains": [
                    {
                        "ticker": g.ticker,
                        "shares_sold": g.shares_sold,
                        "cost_basis": g.cost_basis,
                        "sale_price": g.sale_price,
                        "realized_gain": g.realized_gain,
                        "sold_at": g.sold_at,
                    }
                    for g in gains
                ],
            }
        finally:
            session.close()

    def sell_position(
        self, ticker: str, sale_price: float, year_start_price: Optional[float] = None
    ) -> Optional[float]:
        """Sell a long position (or cover a short) and record the realized gain for YTD tracking.

        Args:
            ticker: Stock ticker symbol
            sale_price: Price per share at time of sale, or buy-to-cover price for a short
            year_start_price: Price per share at start of year (Jan 1), used only for
                positions held since before Jan 1 or without a cost basis

        Returns:
            The YTD realized gain (sale_price - reference_price) * shares, where
            reference_price is the purchase (or short-sale) price for positions opened
            this year. Shares are negative for a short, so a cover below the short-sale
            price is a gain.
        """
        session = self.Session()
        try:
            record = session.query(Portfolio).filter_by(ticker=ticker).first()
            if not record:
                logger.warning(f"Ticker {ticker} not found in portfolio for sale")
                return None

            shares = record.shares or 0.0
            cost_basis = record.cost_basis or 0.0

            if shares == 0:
                logger.warning(f"No shares to sell for {ticker}")
                session.delete(record)
                session.commit()
                return 0.0

            cost_per_share = cost_basis / abs(shares)

            start_of_year = datetime(datetime.utcnow().year, 1, 1)
            bought_this_year = (
                record.added_at is not None and record.added_at >= start_of_year
            )
            if cost_per_share > 0 and (bought_this_year or not year_start_price):
                reference_price = cost_per_share
            else:
                reference_price = year_start_price or cost_per_share
            realized_gain = (sale_price - reference_price) * shares

            gain_record = RealizedGains(
                id=str(uuid.uuid4()),
                ticker=ticker,
                shares_sold=shares,
                cost_basis=cost_per_share,
                sale_price=sale_price,
                year_start_price=year_start_price,
                realized_gain=realized_gain,
                sold_at=datetime.utcnow(),
            )
            session.add(gain_record)
            session.delete(record)
            session.commit()

            logger.info(
                f"Sold {shares} shares of {ticker} at ${sale_price:.2f}, "
                f"YTD realized gain: ${realized_gain:.2f} (reference price: ${reference_price:.2f})"
            )
            return realized_gain
        except Exception as e:
            session.rollback()
            logger.error(f"Error selling {ticker}: {e}")
            raise
        finally:
            session.close()

    # ── Option positions ────────────────────────────────────────────────

    def add_option_position(
        self,
        underlying: str,
        option_type: str,
        strike: float,
        expiry: date,
        contracts: float,
        cost_basis: float = 0.0,
    ) -> str:
        """Open an option position. Returns the position id."""
        session = self.Session()
        try:
            position_id = str(uuid.uuid4())
            session.add(
                OptionPosition(
                    id=position_id,
                    underlying=underlying,
                    option_type=option_type,
                    strike=strike,
                    expiry=expiry,
                    contracts=contracts,
                    cost_basis=cost_basis,
                    added_at=datetime.utcnow(),
                )
            )
            session.commit()
            symbol = option_contract_symbol(underlying, expiry, option_type, strike)
            logger.info(
                f"Added {contracts} contracts of {symbol} with cost basis ${cost_basis:.2f}"
            )
            return position_id
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding option position on {underlying}: {e}")
            raise
        finally:
            session.close()

    def get_option_positions(self) -> List[dict]:
        """Get all open option positions, nearest expiry first."""
        session = self.Session()
        try:
            positions = (
                session.query(OptionPosition)
                .order_by(
                    OptionPosition.expiry,
                    OptionPosition.underlying,
                    OptionPosition.strike,
                )
                .all()
            )
            return [
                {
                    "id": p.id,
                    "underlying": p.underlying,
                    "option_type": p.option_type,
                    "strike": p.strike,
                    "expiry": p.expiry,
                    "contracts": p.contracts,
                    "cost_basis": p.cost_basis or 0.0,
                    "added_at": p.added_at,
                    "contract_symbol": option_contract_symbol(
                        p.underlying, p.expiry, p.option_type, p.strike
                    ),
                }
                for p in positions
            ]
        finally:
            session.close()

    def delete_option_position(self, position_id: str) -> None:
        """Delete an option position without recording a realized gain."""
        session = self.Session()
        try:
            record = session.query(OptionPosition).filter_by(id=position_id).first()
            if record:
                session.delete(record)
                session.commit()
                logger.info(f"Deleted option position {position_id}")
            else:
                logger.warning(f"Option position {position_id} not found for deletion")
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting option position {position_id}: {e}")
            raise
        finally:
            session.close()

    def close_option_position(
        self, position_id: str, close_price: float
    ) -> Optional[float]:
        """Close an option position at a per-share premium and record the realized gain.

        Gains are measured against the opening premium, so an option opened before
        Jan 1 contributes its whole lifetime gain to this year's realized total.
        Pass a close price of 0 for an option that expired worthless.
        """
        session = self.Session()
        try:
            record = session.query(OptionPosition).filter_by(id=position_id).first()
            if not record:
                logger.warning(f"Option position {position_id} not found for close")
                return None

            share_equivalent = record.contracts * OPTION_CONTRACT_MULTIPLIER
            premium_per_share = (
                (record.cost_basis or 0.0) / abs(share_equivalent)
                if share_equivalent
                else 0.0
            )
            realized_gain = (close_price - premium_per_share) * share_equivalent
            symbol = option_contract_symbol(
                record.underlying, record.expiry, record.option_type, record.strike
            )

            session.add(
                RealizedGains(
                    id=str(uuid.uuid4()),
                    ticker=symbol,
                    shares_sold=share_equivalent,
                    cost_basis=premium_per_share,
                    sale_price=close_price,
                    realized_gain=realized_gain,
                    sold_at=datetime.utcnow(),
                )
            )
            session.delete(record)
            session.commit()
            logger.info(
                f"Closed {record.contracts} contracts of {symbol} at ${close_price:.2f}, realized ${realized_gain:.2f}"
            )
            return realized_gain
        except Exception as e:
            session.rollback()
            logger.error(f"Error closing option position {position_id}: {e}")
            raise
        finally:
            session.close()

    # ── Alert CRUD ──────────────────────────────────────────────────────

    def create_alert(self, ticker: str, alert_type: str, threshold: float) -> str:
        """Create a new alert rule. Returns the alert id."""
        session = self.Session()
        try:
            alert_id = str(uuid.uuid4())
            alert = Alert(
                id=alert_id,
                ticker=ticker.upper(),
                alert_type=alert_type,
                threshold=threshold,
                is_active=True,
                is_triggered=False,
                created_at=datetime.utcnow(),
            )
            session.add(alert)
            session.commit()
            logger.info(
                f"Created alert {alert_id}: {ticker} {alert_type} @ {threshold}"
            )
            return alert_id
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating alert: {e}")
            raise
        finally:
            session.close()

    def get_active_alerts(self) -> List[dict]:
        """Get all active (non-triggered) alerts."""
        session = self.Session()
        try:
            alerts = session.query(Alert).filter(Alert.is_active == True).all()
            return [
                {
                    "id": a.id,
                    "ticker": a.ticker,
                    "alert_type": a.alert_type,
                    "threshold": a.threshold,
                    "is_triggered": a.is_triggered,
                    "triggered_at": a.triggered_at,
                    "created_at": a.created_at,
                }
                for a in alerts
            ]
        finally:
            session.close()

    def get_triggered_alerts(self) -> List[dict]:
        """Get all triggered alerts."""
        session = self.Session()
        try:
            alerts = (
                session.query(Alert)
                .filter(Alert.is_triggered == True, Alert.is_active == True)
                .all()
            )
            return [
                {
                    "id": a.id,
                    "ticker": a.ticker,
                    "alert_type": a.alert_type,
                    "threshold": a.threshold,
                    "triggered_at": a.triggered_at,
                    "created_at": a.created_at,
                }
                for a in alerts
            ]
        finally:
            session.close()

    def mark_alerts_triggered(self, alert_ids: List[str]) -> None:
        """Mark a list of alerts as triggered."""
        session = self.Session()
        try:
            now = datetime.utcnow()
            session.query(Alert).filter(Alert.id.in_(alert_ids)).update(
                {"is_triggered": True, "triggered_at": now}, synchronize_session="fetch"
            )
            session.commit()
            logger.info(f"Marked {len(alert_ids)} alerts as triggered")
        except Exception as e:
            session.rollback()
            logger.error(f"Error marking alerts triggered: {e}")
            raise
        finally:
            session.close()

    def dismiss_alert(self, alert_id: str) -> None:
        """Dismiss (deactivate) an alert."""
        session = self.Session()
        try:
            alert = session.query(Alert).filter_by(id=alert_id).first()
            if alert:
                alert.is_active = False
                session.commit()
                logger.info(f"Dismissed alert {alert_id}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error dismissing alert: {e}")
            raise
        finally:
            session.close()

    def delete_alert(self, alert_id: str) -> None:
        """Permanently delete an alert."""
        session = self.Session()
        try:
            alert = session.query(Alert).filter_by(id=alert_id).first()
            if alert:
                session.delete(alert)
                session.commit()
                logger.info(f"Deleted alert {alert_id}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting alert: {e}")
            raise
        finally:
            session.close()

    # ── Broker fills ──────────────────────────────────────────────────

    def add_broker_fills(self, fills: List[Fill]) -> int:
        """Store fills not already imported; returns how many were new."""
        session = self.Session()
        try:
            existing = {row[0] for row in session.query(BrokerFill.id).all()}
            new_rows = {}
            for fill in fills:
                if fill.dedupe_key in existing or fill.dedupe_key in new_rows:
                    continue
                new_rows[fill.dedupe_key] = BrokerFill(
                    id=fill.dedupe_key,
                    account=fill.account,
                    trade_date=fill.trade_date,
                    symbol=fill.symbol,
                    side=fill.side.value,
                    quantity=fill.quantity,
                    price=fill.price,
                    fees=fill.fees,
                    source=fill.source,
                )
            session.add_all(new_rows.values())
            session.commit()
            logger.info(
                f"Imported {len(new_rows)} new broker fills ({len(fills) - len(new_rows)} already present)"
            )
            return len(new_rows)
        except Exception as e:
            session.rollback()
            logger.error(f"Error importing broker fills: {e}")
            raise
        finally:
            session.close()

    def get_broker_fills(self) -> List[Fill]:
        session = self.Session()
        try:
            return [
                Fill(
                    account=row.account,
                    trade_date=row.trade_date,
                    symbol=row.symbol,
                    side=Side(row.side),
                    quantity=row.quantity,
                    price=row.price,
                    fees=row.fees or 0.0,
                    source=row.source or "",
                )
                for row in session.query(BrokerFill)
                .order_by(BrokerFill.trade_date)
                .all()
            ]
        finally:
            session.close()

    def delete_broker_fills(self) -> None:
        session = self.Session()
        try:
            session.query(BrokerFill).delete()
            session.commit()
        finally:
            session.close()

    # ── Trend watchlist ───────────────────────────────────────────────

    def get_watchlist(self) -> List[str]:
        session = self.Session()
        try:
            return [
                row.ticker
                for row in session.query(WatchlistEntry)
                .order_by(WatchlistEntry.ticker)
                .all()
            ]
        finally:
            session.close()

    def add_to_watchlist(self, tickers: List[str]) -> None:
        session = self.Session()
        try:
            for ticker in tickers:
                session.merge(
                    WatchlistEntry(ticker=ticker.upper(), added_at=datetime.utcnow())
                )
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding to watchlist: {e}")
            raise
        finally:
            session.close()

    def remove_from_watchlist(self, ticker: str) -> None:
        session = self.Session()
        try:
            session.query(WatchlistEntry).filter_by(ticker=ticker.upper()).delete()
            session.commit()
        finally:
            session.close()

    # ── Analytics helpers ─────────────────────────────────────────────

    def get_all_metrics_df(self) -> pd.DataFrame:
        """Return the full stock metrics table as a DataFrame."""
        session = self.Session()
        try:
            query = session.query(StockMetrics)
            return pd.read_sql(query.statement, session.bind)
        finally:
            session.close()

    def get_sector_summary(self) -> pd.DataFrame:
        """Return sector-level aggregates (count, avg pct_above_ma_100, total market_cap)."""
        session = self.Session()
        try:
            query = (
                session.query(
                    StockMetrics.sector,
                    func.count(StockMetrics.ticker).label("stock_count"),
                    func.avg(StockMetrics.pct_above_ma_100).label("avg_pct_above_ma"),
                    func.sum(StockMetrics.market_cap).label("total_market_cap"),
                    func.avg(StockMetrics.rsi_14).label("avg_rsi"),
                )
                .filter(StockMetrics.sector.isnot(None), StockMetrics.sector != "")
                .group_by(StockMetrics.sector)
            )
            return pd.read_sql(query.statement, session.bind)
        finally:
            session.close()


# Create global database instance
db = Database()
