from typing import Any, List, Optional
from collections import defaultdict
from collections.abc import Iterable, Iterator
from enum import Enum
from pathlib import Path
import json
import sqlite3
import uuid
import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    Float,
    Integer,
    String,
    Text,
    Date,
    DateTime,
    Boolean,
    ForeignKey,
    UniqueConstraint,
    case,
    func,
    text,
    inspect,
    select,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import CreateTable
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
    # When the .info fundamentals were last fetched; technicals-only refreshes leave it alone
    fundamentals_updated_at = Column(DateTime)
    asset_type = Column(String)  # 'stock' or 'etf'
    # Mean close x volume over UNIVERSE_LIQUIDITY_LOOKBACK_DAYS bars, for the liquidity filter
    avg_dollar_volume_50d = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow)


class Account(Base):
    __tablename__ = "accounts"
    name = Column(String, primary_key=True)
    equity = Column(Float, nullable=False, default=0.0)
    cash = Column(Float, nullable=False, default=0.0)
    allow_short = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, default=datetime.utcnow)


class Portfolio(Base):
    __tablename__ = "portfolio"
    # One row per (account, ticker): the same ticker can be held in several accounts.
    account = Column(String, ForeignKey("accounts.name"), primary_key=True)
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
    account = Column(String, nullable=True)


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
    account = Column(String, nullable=True)


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


class TickerStatus(Base):
    """Fetch health per ticker, so symbols that keep failing are skipped for a while."""

    __tablename__ = "ticker_status"
    ticker = Column(String, primary_key=True)
    last_ok = Column(Date)
    consecutive_failures = Column(Integer, nullable=False, default=0)
    skip_until = Column(Date)  # skipped while today < skip_until


class RecommendationRun(Base):
    __tablename__ = "recommendation_runs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    job = Column(String, nullable=False)  # 'weekly', 'daily' or 'manual'
    equity = Column(Float)
    config_json = Column(Text)
    regime_json = Column(Text)


class RecommendationRecord(Base):
    """One stored recommendation of a run (the analysis-side type is ``Recommendation``)."""

    __tablename__ = "recommendations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(
        Integer, ForeignKey("recommendation_runs.id"), nullable=False, index=True
    )
    signal_key = Column(String, nullable=False, index=True)
    ticker = Column(String, nullable=False)
    account = Column(String, nullable=True)
    direction = Column(String, nullable=False)  # 'LONG' or 'SHORT'
    action = Column(String, nullable=False)
    status = Column(String, nullable=False)  # 'NEW' or 'STILL_VALID'
    first_seen_at = Column(DateTime, nullable=False)
    trigger_price = Column(Float)
    stop_price = Column(Float)
    shares = Column(Integer)
    risk_dollars = Column(Float)
    risk_pct_equity = Column(Float)
    distance_n = Column(Float)
    trend_score = Column(Float)
    rs_rank = Column(Float)
    asset_type = Column(String)
    is_late = Column(Boolean, nullable=False, default=False)
    reasons_json = Column(Text)
    blocked_by_json = Column(Text)
    extra_json = Column(Text)  # free-form order-ticket extras


class SignalDecision(Base):
    """What was done about a signal, keyed by the signal and when it was first seen."""

    __tablename__ = "signal_decisions"
    __table_args__ = (UniqueConstraint("signal_key", "first_seen_at"),)
    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_key = Column(String, nullable=False)
    first_seen_at = Column(DateTime, nullable=False)
    status = Column(String, nullable=False, default="pending")
    note = Column(Text)
    decided_at = Column(DateTime)


RECOMMENDATION_JOBS = ("weekly", "daily", "manual")
RECOMMENDATION_DIRECTIONS = ("LONG", "SHORT")
RECOMMENDATION_STATUSES = ("NEW", "STILL_VALID")
SIGNAL_DECISION_STATUSES = ("pending", "taken", "skipped")

# Keys accepted in a create_recommendation_run row; reasons/blocked_by/extra are JSON-encoded
_RECOMMENDATION_ROW_KEYS = frozenset(
    {
        "signal_key",
        "ticker",
        "account",
        "direction",
        "action",
        "status",
        "first_seen_at",
        "trigger_price",
        "stop_price",
        "shares",
        "risk_dollars",
        "risk_pct_equity",
        "distance_n",
        "trend_score",
        "rs_rank",
        "asset_type",
        "is_late",
        "reasons",
        "blocked_by",
        "extra",
    }
)
_RECOMMENDATION_REQUIRED_KEYS = frozenset(
    {"signal_key", "ticker", "direction", "action", "status"}
)

# SQLite allows at most 32766 bound variables per statement
_UPSERT_CHUNK_ROWS = 500


def _chunks(items: list, size: int = _UPSERT_CHUNK_ROWS) -> Iterator[list]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _json_default(value: Any) -> Any:
    """JSON-encode dates as ISO strings and enums by value; anything else is an error."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _to_json(value: Any) -> str:
    return json.dumps(value, default=_json_default)


def _enum_name(value: Any) -> Any:
    """Store enums (Direction, RecommendationAction, ...) by name, e.g. Direction.LONG -> 'LONG'."""
    return value.name if isinstance(value, Enum) else value


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
        self._migrate_metrics_pipeline_columns()
        # The default account must exist before portfolio rows are moved into it
        self._seed_default_account()
        self._migrate_portfolio_accounts()
        self._migrate_account_columns()
        logger.info(f"Database initialized at {db_path}")

    def _table_columns(self, table: str) -> set[str]:
        """Column names of a table, or an empty set if it doesn't exist."""
        inspector = inspect(self.engine)
        if table not in inspector.get_table_names():
            return set()
        return {c["name"] for c in inspector.get_columns(table)}

    def _migrate_metrics_pipeline_columns(self) -> None:
        """Add fundamentals_updated_at, asset_type and avg_dollar_volume_50d columns to the stock metrics table."""
        columns = self._table_columns(settings.TABLE_NAME)
        with self.engine.connect() as conn:
            if "fundamentals_updated_at" not in columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.TABLE_NAME} ADD COLUMN fundamentals_updated_at TIMESTAMP"
                    )
                )
                # Every row so far came from a full fetch including .info, so its
                # fundamentals are as old as the row itself
                conn.execute(
                    text(
                        f"UPDATE {settings.TABLE_NAME} SET fundamentals_updated_at = updated_at"
                    )
                )
                logger.info(
                    f"Migrated {settings.TABLE_NAME} table: added 'fundamentals_updated_at' column"
                )
            if "asset_type" not in columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.TABLE_NAME} ADD COLUMN asset_type TEXT"
                    )
                )
                logger.info(
                    f"Migrated {settings.TABLE_NAME} table: added 'asset_type' column"
                )
            if "avg_dollar_volume_50d" not in columns:
                conn.execute(
                    text(
                        f"ALTER TABLE {settings.TABLE_NAME} ADD COLUMN avg_dollar_volume_50d REAL"
                    )
                )
                logger.info(
                    f"Migrated {settings.TABLE_NAME} table: added 'avg_dollar_volume_50d' column"
                )
            conn.commit()

    def _seed_default_account(self) -> None:
        """Create the DEFAULT_ACCOUNT row when no accounts exist, so existing positions have a home."""
        # Only seed an empty table: once the user has set up (or renamed) accounts,
        # re-creating an empty default account on every start would be noise.
        with self.engine.connect() as conn:
            has_accounts = (
                conn.execute(select(Account.name).limit(1)).first() is not None
            )
        if has_accounts:
            return
        # allow_short is True because existing positions (and callers that don't pass
        # an account yet) include shorts; set up a long-only account for the Roth.
        stmt = sqlite_insert(Account.__table__).values(
            name=settings.DEFAULT_ACCOUNT,
            equity=0.0,
            cash=0.0,
            allow_short=True,
            updated_at=datetime.utcnow(),
        )
        with self.engine.connect() as conn:
            result = conn.execute(stmt.on_conflict_do_nothing(index_elements=["name"]))
            conn.commit()
        if result.rowcount:
            logger.info(f"Seeded account '{settings.DEFAULT_ACCOUNT}'")

    def _migrate_portfolio_accounts(self) -> None:
        """Rebuild portfolio with an (account, ticker) key, moving every row to DEFAULT_ACCOUNT.

        SQLite can't change a primary key in place, so the old table is renamed, the
        new one is created from the model and refilled, all in one transaction, after a
        backup of the DB to ``<db>.pre-accounts.bak``. Skipped when the account column
        already exists, which includes fresh DBs built by create_all.
        """
        if "account" in self._table_columns("portfolio"):
            return

        db_path = self._active_db_path
        backup_path = db_path.with_name(f"{db_path.name}.pre-accounts.bak")
        create_portfolio = str(CreateTable(Portfolio.__table__).compile(self.engine))
        # isolation_level=None so BEGIN/COMMIT are ours: pysqlite would otherwise run
        # the DDL outside the transaction
        conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
        try:
            # BEGIN IMMEDIATE takes the write lock, so a second process waits here and
            # then sees the migrated table instead of rebuilding it again
            conn.execute("BEGIN IMMEDIATE")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(portfolio)")}
            if "account" in columns:
                conn.execute("ROLLBACK")
                return
            self._backup_database(db_path, backup_path)
            conn.execute("ALTER TABLE portfolio RENAME TO portfolio_pre_accounts")
            conn.execute(create_portfolio)
            # Columns are named: older DBs have shares/added_at/cost_basis in another order
            conn.execute(
                "INSERT INTO portfolio (account, ticker, shares, cost_basis, added_at) "
                "SELECT ?, ticker, shares, cost_basis, added_at FROM portfolio_pre_accounts",
                (settings.DEFAULT_ACCOUNT,),
            )
            moved = conn.execute("SELECT COUNT(*) FROM portfolio").fetchone()[0]
            conn.execute("DROP TABLE portfolio_pre_accounts")
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            logger.error("Portfolio account migration failed; rolled back")
            raise
        finally:
            conn.close()
        logger.info(
            f"Migrated portfolio table to (account, ticker) key: {moved} rows moved to "
            f"'{settings.DEFAULT_ACCOUNT}', backup at {backup_path}"
        )

    @staticmethod
    def _backup_database(db_path: Path, backup_path: Path) -> None:
        """Write a consistent copy of the DB with SQLite's online backup API.

        A plain file copy would miss pages still in the WAL file.
        """
        source = sqlite3.connect(str(db_path))
        target = sqlite3.connect(str(backup_path))
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        logger.info(f"Backed up {db_path} to {backup_path}")

    def _migrate_account_columns(self) -> None:
        """Add a nullable account column to realized_gains and option_positions."""
        missing = [
            table
            for table in ("realized_gains", "option_positions")
            if "account" not in self._table_columns(table)
        ]
        with self.engine.connect() as conn:
            for table in missing:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN account TEXT"))
                # Everything recorded before accounts existed belongs to the default one
                conn.execute(
                    text(f"UPDATE {table} SET account = :account"),
                    {"account": settings.DEFAULT_ACCOUNT},
                )
                logger.info(f"Migrated {table} table: added 'account' column")
            conn.commit()

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

    @staticmethod
    def _bulk_upsert(session: Session, model: Any, rows: List[dict], key: str) -> None:
        """INSERT ... ON CONFLICT DO UPDATE in chunks of about 500 rows per statement.

        Rows are grouped by their key set, and each group updates only the columns its
        rows carry. A column missing from a row's dict therefore keeps its stored value
        (an explicit None still clears it), which is what lets a technicals-only refresh
        keep the stored fundamentals.
        """
        table = model.__table__
        known = set(table.columns.keys())
        groups: dict[frozenset[str], list[dict]] = defaultdict(list)
        for row in rows:
            unknown = row.keys() - known
            if unknown:
                raise ValueError(f"Unknown {table.name} columns: {sorted(unknown)}")
            if row.get(key) is None:
                raise ValueError(f"{table.name} row without '{key}': {row}")
            groups[frozenset(row)].append(row)

        for columns, group in groups.items():
            for chunk in _chunks(group):
                stmt = sqlite_insert(table).values(chunk)
                update_cols = {c: stmt.excluded[c] for c in columns if c != key}
                session.execute(
                    stmt.on_conflict_do_update(index_elements=[key], set_=update_cols)
                    if update_cols
                    else stmt.on_conflict_do_nothing(index_elements=[key])
                )

    def store_metrics(self, metrics_list: List[dict]) -> None:
        """Bulk-upsert metrics; a column missing from a row's dict keeps its stored value."""
        if not metrics_list:
            logger.warning("No metrics to store")
            return

        now = datetime.utcnow()
        rows = [{**metrics, "updated_at": now} for metrics in metrics_list]
        session = self.Session()
        try:
            self._bulk_upsert(session, StockMetrics, rows, "ticker")
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

    def get_recent_tickers(self, cutoff: datetime) -> set[str]:
        """Tickers whose metrics were updated at or after ``cutoff`` (UTC)."""
        session = self.Session()
        try:
            return {
                row.ticker
                for row in session.query(StockMetrics.ticker).filter(
                    StockMetrics.updated_at >= cutoff
                )
            }
        finally:
            session.close()

    def get_fundamentals_ages(self) -> dict[str, datetime | None]:
        """When each stored ticker's fundamentals were last fetched (None if never)."""
        session = self.Session()
        try:
            return {
                row.ticker: row.fundamentals_updated_at
                for row in session.query(
                    StockMetrics.ticker, StockMetrics.fundamentals_updated_at
                )
            }
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

    @staticmethod
    def _require_account(session: Session, name: str) -> Account:
        account = session.get(Account, name)
        if account is None:
            raise ValueError(f"Unknown account '{name}'")
        return account

    @classmethod
    def _check_short_allowed(
        cls, session: Session, account: str, old_shares: float, new_shares: float
    ) -> None:
        """Refuse opening or increasing a short in an account without allow_short."""
        if new_shares < 0 and new_shares < min(old_shares, 0.0):
            if not cls._require_account(session, account).allow_short:
                raise ValueError(f"Account '{account}' does not allow short positions")

    def get_portfolio_tickers(self, account: Optional[str] = None) -> List[str]:
        """Get the distinct portfolio tickers, across all accounts unless one is given."""
        session = self.Session()
        try:
            query = session.query(Portfolio.ticker).distinct()
            if account is not None:
                query = query.filter(Portfolio.account == account)
            return [p.ticker for p in query.order_by(Portfolio.ticker).all()]
        finally:
            session.close()

    def add_portfolio_ticker(
        self,
        ticker: str,
        cost_basis: float = 0.0,
        shares: float = 0.0,
        account: str = settings.DEFAULT_ACCOUNT,
    ) -> None:
        """Add a ticker to an account's portfolio with optional cost basis and shares.

        Raises ValueError for an unknown account, or a short (negative shares) in an
        account that doesn't allow shorts.
        """
        session = self.Session()
        try:
            existing = session.get(Portfolio, (account, ticker))
            if not existing:
                self._require_account(session, account)
                self._check_short_allowed(session, account, 0.0, shares)
                session.add(
                    Portfolio(
                        account=account,
                        ticker=ticker,
                        shares=shares,
                        cost_basis=cost_basis,
                        added_at=datetime.utcnow(),
                    )
                )
                session.commit()
                logger.info(
                    f"Added {ticker} to portfolio ({account}) with cost basis ${cost_basis:.2f}"
                )
            else:
                logger.warning(f"Ticker {ticker} already in portfolio ({account}).")
        except Exception as e:
            session.rollback()
            logger.error(f"Error adding {ticker} to portfolio ({account}): {e}")
            raise
        finally:
            session.close()

    def delete_portfolio_ticker(
        self, ticker: str, account: str = settings.DEFAULT_ACCOUNT
    ) -> bool:
        """Delete a ticker from an account's portfolio; False when it wasn't held there."""
        session = self.Session()
        try:
            record = session.get(Portfolio, (account, ticker))
            if record:
                session.delete(record)
                session.commit()
                logger.info(f"Deleted {ticker} from portfolio ({account}).")
                return True
            else:
                logger.warning(
                    f"Ticker {ticker} not found in portfolio ({account}) for deletion."
                )
                return False
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting {ticker} from portfolio ({account}): {e}")
            raise
        finally:
            session.close()

    def update_portfolio_shares(
        self, ticker: str, shares: float, account: str = settings.DEFAULT_ACCOUNT
    ) -> bool:
        """Update the number of shares for a portfolio position.

        Returns False when the account holds no such position. Raises ValueError
        when this opens or increases a short in an account that doesn't allow shorts.
        """
        session = self.Session()
        try:
            record = session.get(Portfolio, (account, ticker))
            if record:
                self._check_short_allowed(
                    session, account, record.shares or 0.0, shares
                )
                record.shares = shares
                session.commit()
                logger.info(f"Updated {ticker} ({account}) shares to {shares}")
                return True
            else:
                logger.warning(f"Ticker {ticker} not found in portfolio ({account})")
                return False
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating shares for {ticker} ({account}): {e}")
            raise
        finally:
            session.close()

    def get_portfolio_with_shares(self, account: Optional[str] = None) -> List[dict]:
        """Get portfolio positions with account, shares, cost basis, and add dates.

        Returns every account's positions unless ``account`` is given.
        """
        session = self.Session()
        try:
            query = session.query(Portfolio)
            if account is not None:
                query = query.filter(Portfolio.account == account)
            positions = query.order_by(Portfolio.account, Portfolio.ticker).all()
            return [
                {
                    "account": p.account,
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
        """Bulk-upsert crypto metrics; a column missing from a row's dict keeps its stored value."""
        if not metrics_list:
            logger.warning("No crypto metrics to store")
            return

        now = datetime.utcnow()
        rows = [{**metrics, "updated_at": now} for metrics in metrics_list]
        session = self.Session()
        try:
            self._bulk_upsert(session, CryptoMetrics, rows, "instrument")
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

    def update_portfolio_cost_basis(
        self, ticker: str, cost_basis: float, account: str = settings.DEFAULT_ACCOUNT
    ) -> bool:
        """Update the cost basis for a portfolio position; False when it isn't held there."""
        session = self.Session()
        try:
            record = session.get(Portfolio, (account, ticker))
            if record:
                record.cost_basis = cost_basis
                session.commit()
                logger.info(
                    f"Updated {ticker} ({account}) cost basis to ${cost_basis:.2f}"
                )
                return True
            else:
                logger.warning(f"Ticker {ticker} not found in portfolio ({account})")
                return False
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating cost basis for {ticker} ({account}): {e}")
            raise
        finally:
            session.close()

    def record_realized_gain(
        self,
        ticker: str,
        shares_sold: float,
        cost_basis: float,
        sale_price: float,
        account: str = settings.DEFAULT_ACCOUNT,
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
                account=account,
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

    def get_ytd_realized_gains(self, account: Optional[str] = None) -> dict:
        """Get total realized gains for the current year, for all accounts unless one is given."""
        session = self.Session()
        try:
            start_of_year = datetime(datetime.utcnow().year, 1, 1)
            query = session.query(RealizedGains).filter(
                RealizedGains.sold_at >= start_of_year
            )
            if account is not None:
                query = query.filter(RealizedGains.account == account)
            gains = query.all()

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
                        "account": g.account,
                    }
                    for g in gains
                ],
            }
        finally:
            session.close()

    def sell_position(
        self,
        ticker: str,
        sale_price: float,
        year_start_price: Optional[float] = None,
        account: str = settings.DEFAULT_ACCOUNT,
    ) -> Optional[float]:
        """Sell a long position (or cover a short) and record the realized gain for YTD tracking.

        Args:
            ticker: Stock ticker symbol
            sale_price: Price per share at time of sale, or buy-to-cover price for a short
            year_start_price: Price per share at start of year (Jan 1), used only for
                positions held since before Jan 1 or without a cost basis
            account: Account holding the position

        Returns:
            The YTD realized gain (sale_price - reference_price) * shares, where
            reference_price is the purchase (or short-sale) price for positions opened
            this year. Shares are negative for a short, so a cover below the short-sale
            price is a gain.
        """
        session = self.Session()
        try:
            record = session.get(Portfolio, (account, ticker))
            if not record:
                logger.warning(
                    f"Ticker {ticker} not found in portfolio ({account}) for sale"
                )
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
                account=account,
            )
            session.add(gain_record)
            session.delete(record)
            session.commit()

            logger.info(
                f"Sold {shares} shares of {ticker} ({account}) at ${sale_price:.2f}, "
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
        account: str = settings.DEFAULT_ACCOUNT,
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
                    account=account,
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

    def get_option_positions(self, account: Optional[str] = None) -> List[dict]:
        """Get open option positions (all accounts unless one is given), nearest expiry first."""
        session = self.Session()
        try:
            query = session.query(OptionPosition)
            if account is not None:
                query = query.filter(OptionPosition.account == account)
            positions = query.order_by(
                OptionPosition.expiry,
                OptionPosition.underlying,
                OptionPosition.strike,
            ).all()
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
                    "account": p.account,
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
                    account=record.account,
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

    # ── Accounts ──────────────────────────────────────────────────────

    def get_accounts(self) -> list[dict]:
        """All accounts, by name."""
        session = self.Session()
        try:
            return [
                {
                    "name": a.name,
                    "equity": a.equity or 0.0,
                    "cash": a.cash or 0.0,
                    "allow_short": bool(a.allow_short),
                    "updated_at": a.updated_at,
                }
                for a in session.query(Account).order_by(Account.name).all()
            ]
        finally:
            session.close()

    def upsert_account(
        self, name: str, equity: float, cash: float, allow_short: bool
    ) -> None:
        """Create an account or update its equity, cash and short permission."""
        if not name or not name.strip():
            raise ValueError("Account name must not be empty")
        stmt = sqlite_insert(Account.__table__).values(
            name=name,
            equity=equity,
            cash=cash,
            allow_short=allow_short,
            updated_at=datetime.utcnow(),
        )
        session = self.Session()
        try:
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["name"],
                    set_={
                        c: stmt.excluded[c]
                        for c in ("equity", "cash", "allow_short", "updated_at")
                    },
                )
            )
            session.commit()
            logger.info(
                f"Saved account '{name}': equity ${equity:,.2f}, cash ${cash:,.2f}, "
                f"allow_short={allow_short}"
            )
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving account '{name}': {e}")
            raise
        finally:
            session.close()

    def delete_account(self, name: str) -> None:
        """Delete an account; raises ValueError while stock or option positions reference it."""
        session = self.Session()
        try:
            in_use = (
                session.query(Portfolio).filter_by(account=name).count()
                + session.query(OptionPosition).filter_by(account=name).count()
            )
            if in_use:
                raise ValueError(
                    f"Account '{name}' still holds {in_use} position(s); move or close them first"
                )
            deleted = session.query(Account).filter_by(name=name).delete()
            session.commit()
            if deleted:
                logger.info(f"Deleted account '{name}'")
            else:
                logger.warning(f"Account '{name}' not found for deletion")
        except Exception as e:
            session.rollback()
            logger.error(f"Error deleting account '{name}': {e}")
            raise
        finally:
            session.close()

    def rename_account(self, old: str, new: str) -> None:
        """Rename an account, repointing its stock, option and realized-gain rows, in one transaction.

        Raises ValueError if ``old`` is missing, ``new`` is empty or ``new`` already exists.
        """
        new = (new or "").strip()
        if not new:
            raise ValueError("Account name must not be empty")
        session = self.Session()
        try:
            account = session.get(Account, old)
            if account is None:
                raise ValueError(f"Unknown account '{old}'")
            if session.get(Account, new) is not None:
                raise ValueError(f"Account '{new}' already exists")
            # Insert the new row first so the portfolio foreign key always has a target
            session.add(
                Account(
                    name=new,
                    equity=account.equity,
                    cash=account.cash,
                    allow_short=account.allow_short,
                    updated_at=datetime.utcnow(),
                )
            )
            session.flush()
            for model in (Portfolio, OptionPosition, RealizedGains):
                session.query(model).filter(model.account == old).update(
                    {model.account: new}, synchronize_session=False
                )
            session.query(Account).filter(Account.name == old).delete(
                synchronize_session=False
            )
            session.commit()
            logger.info(f"Renamed account '{old}' to '{new}'")
        except Exception as e:
            session.rollback()
            logger.error(f"Error renaming account '{old}' to '{new}': {e}")
            raise
        finally:
            session.close()

    def total_equity(self) -> float:
        """Sum of equity across all accounts: the single risk budget for sizing."""
        session = self.Session()
        try:
            return float(
                session.query(func.coalesce(func.sum(Account.equity), 0.0)).scalar()
            )
        finally:
            session.close()

    # ── Ticker fetch status ───────────────────────────────────────────

    def record_ticker_results(
        self,
        succeeded: Iterable[str],
        failed: Iterable[str],
        today: date | None = None,
    ) -> None:
        """Record a fetch run: a success resets the failure count, a failure adds one.

        Once failures reach TICKER_MAX_CONSECUTIVE_FAILURES the ticker is skipped until
        today + TICKER_SKIP_DAYS; each failure after that (when it's retried) starts a
        new skip. A ticker in both sets counts as a success.
        """
        today = today or date.today()
        ok = sorted(set(succeeded))
        bad = sorted(set(failed) - set(ok))
        max_failures = settings.TICKER_MAX_CONSECUTIVE_FAILURES
        skip_date = today + timedelta(days=settings.TICKER_SKIP_DAYS)
        table = TickerStatus.__table__
        failures = table.c.consecutive_failures + 1

        session = self.Session()
        try:
            for chunk in _chunks(ok):
                stmt = sqlite_insert(table).values(
                    [
                        {
                            "ticker": t,
                            "last_ok": today,
                            "consecutive_failures": 0,
                            "skip_until": None,
                        }
                        for t in chunk
                    ]
                )
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["ticker"],
                        set_={
                            "last_ok": stmt.excluded.last_ok,
                            "consecutive_failures": 0,
                            "skip_until": None,
                        },
                    )
                )
            for chunk in _chunks(bad):
                stmt = sqlite_insert(table).values(
                    [
                        {
                            "ticker": t,
                            "consecutive_failures": 1,
                            "skip_until": skip_date if max_failures <= 1 else None,
                        }
                        for t in chunk
                    ]
                )
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["ticker"],
                        set_={
                            "consecutive_failures": failures,
                            "skip_until": case(
                                (failures >= max_failures, skip_date),
                                else_=table.c.skip_until,
                            ),
                        },
                    )
                )
            session.commit()
            logger.info(
                f"Recorded ticker status: {len(ok)} succeeded, {len(bad)} failed"
            )
        except Exception as e:
            session.rollback()
            logger.error(f"Error recording ticker status: {e}")
            raise
        finally:
            session.close()

    def get_skipped_tickers(self, today: date | None = None) -> set[str]:
        """Tickers to skip today because they kept failing."""
        today = today or date.today()
        session = self.Session()
        try:
            return {
                row.ticker
                for row in session.query(TickerStatus.ticker).filter(
                    TickerStatus.skip_until > today
                )
            }
        finally:
            session.close()

    # ── Recommendation runs and signal journal ────────────────────────

    @staticmethod
    def _recommendation_record(row: dict, run_at: datetime) -> RecommendationRecord:
        """Validate one create_recommendation_run row and build its ORM record."""
        unknown = row.keys() - _RECOMMENDATION_ROW_KEYS
        if unknown:
            raise ValueError(f"Unknown recommendation fields: {sorted(unknown)}")
        missing = _RECOMMENDATION_REQUIRED_KEYS - row.keys()
        if missing:
            raise ValueError(f"Recommendation row missing fields: {sorted(missing)}")
        direction = _enum_name(row["direction"])
        status = _enum_name(row["status"])
        if direction not in RECOMMENDATION_DIRECTIONS:
            raise ValueError(f"Invalid recommendation direction: {direction!r}")
        if status not in RECOMMENDATION_STATUSES:
            raise ValueError(f"Invalid recommendation status: {status!r}")
        shares = row.get("shares")
        return RecommendationRecord(
            signal_key=row["signal_key"],
            ticker=row["ticker"],
            account=row.get("account"),
            direction=direction,
            action=_enum_name(row["action"]),
            status=status,
            # A signal without a carried-forward first_seen_at is first seen now
            first_seen_at=row.get("first_seen_at") or run_at,
            trigger_price=row.get("trigger_price"),
            stop_price=row.get("stop_price"),
            shares=int(shares) if shares is not None else None,
            risk_dollars=row.get("risk_dollars"),
            risk_pct_equity=row.get("risk_pct_equity"),
            distance_n=row.get("distance_n"),
            trend_score=row.get("trend_score"),
            rs_rank=row.get("rs_rank"),
            asset_type=row.get("asset_type"),
            is_late=bool(row.get("is_late", False)),
            reasons_json=_to_json(list(row.get("reasons") or [])),
            blocked_by_json=_to_json(list(row.get("blocked_by") or [])),
            extra_json=_to_json(dict(row.get("extra") or {})),
        )

    @staticmethod
    def _run_to_dict(run: RecommendationRun) -> dict:
        return {
            "id": run.id,
            "run_at": run.run_at,
            "job": run.job,
            "equity": run.equity,
            "config": json.loads(run.config_json) if run.config_json else {},
            "regime": json.loads(run.regime_json) if run.regime_json else {},
        }

    def create_recommendation_run(
        self,
        job: str,
        equity: float,
        config: dict,
        regime: dict,
        rows: list[dict],
    ) -> int:
        """Store a run and its recommendations in one transaction; returns the run id.

        Each row uses the ``recommendations`` column names, with ``reasons`` and
        ``blocked_by`` as lists and ``extra`` as a dict. Enum values for direction,
        action and status are stored by name. A row without ``first_seen_at`` gets
        the run time. Raises ValueError for an invalid job or row.
        """
        if job not in RECOMMENDATION_JOBS:
            raise ValueError(f"Invalid recommendation job: {job!r}")
        run_at = datetime.utcnow()
        records = [self._recommendation_record(row, run_at) for row in rows]

        session = self.Session()
        try:
            run = RecommendationRun(
                run_at=run_at,
                job=job,
                equity=equity,
                config_json=_to_json(config),
                regime_json=_to_json(regime),
            )
            session.add(run)
            session.flush()
            run_id = run.id
            for record in records:
                record.run_id = run_id
            session.add_all(records)
            session.commit()
            logger.info(
                f"Stored {job} recommendation run {run_id} with {len(records)} rows"
            )
            return run_id
        except Exception as e:
            session.rollback()
            logger.error(f"Error storing {job} recommendation run: {e}")
            raise
        finally:
            session.close()

    def get_latest_run(self, job: str | None = None) -> dict | None:
        """The most recent run, optionally of one job."""
        session = self.Session()
        try:
            query = session.query(RecommendationRun)
            if job is not None:
                query = query.filter(RecommendationRun.job == job)
            run = query.order_by(RecommendationRun.id.desc()).first()
            return self._run_to_dict(run) if run else None
        finally:
            session.close()

    def get_previous_run(self, run_id: int, job: str | None = None) -> dict | None:
        """The run stored just before ``run_id``, optionally of one job."""
        session = self.Session()
        try:
            query = session.query(RecommendationRun).filter(
                RecommendationRun.id < run_id
            )
            if job is not None:
                query = query.filter(RecommendationRun.job == job)
            run = query.order_by(RecommendationRun.id.desc()).first()
            return self._run_to_dict(run) if run else None
        finally:
            session.close()

    def get_run_recommendations(self, run_id: int) -> list[dict]:
        """A run's recommendations in the order they were stored, JSON fields decoded."""
        session = self.Session()
        try:
            records = (
                session.query(RecommendationRecord)
                .filter(RecommendationRecord.run_id == run_id)
                .order_by(RecommendationRecord.id)
                .all()
            )
            return [
                {
                    "id": r.id,
                    "run_id": r.run_id,
                    "signal_key": r.signal_key,
                    "ticker": r.ticker,
                    "account": r.account,
                    "direction": r.direction,
                    "action": r.action,
                    "status": r.status,
                    "first_seen_at": r.first_seen_at,
                    "trigger_price": r.trigger_price,
                    "stop_price": r.stop_price,
                    "shares": r.shares,
                    "risk_dollars": r.risk_dollars,
                    "risk_pct_equity": r.risk_pct_equity,
                    "distance_n": r.distance_n,
                    "trend_score": r.trend_score,
                    "rs_rank": r.rs_rank,
                    "asset_type": r.asset_type,
                    "is_late": bool(r.is_late),
                    "reasons": json.loads(r.reasons_json) if r.reasons_json else [],
                    "blocked_by": (
                        json.loads(r.blocked_by_json) if r.blocked_by_json else []
                    ),
                    "extra": json.loads(r.extra_json) if r.extra_json else {},
                }
                for r in records
            ]
        finally:
            session.close()

    def upsert_signal_decision(
        self,
        signal_key: str,
        first_seen_at: datetime,
        status: str,
        note: str | None = None,
    ) -> None:
        """Record what was done about a signal.

        ``note=None`` keeps an existing note. ``decided_at`` is set to now, or cleared
        when the status goes back to 'pending'.
        """
        if status not in SIGNAL_DECISION_STATUSES:
            raise ValueError(f"Invalid signal decision status: {status!r}")
        table = SignalDecision.__table__
        stmt = sqlite_insert(table).values(
            signal_key=signal_key,
            first_seen_at=first_seen_at,
            status=status,
            note=note,
            decided_at=None if status == "pending" else datetime.utcnow(),
        )
        session = self.Session()
        try:
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["signal_key", "first_seen_at"],
                    set_={
                        "status": stmt.excluded.status,
                        "note": func.coalesce(stmt.excluded.note, table.c.note),
                        "decided_at": stmt.excluded.decided_at,
                    },
                )
            )
            session.commit()
            logger.info(f"Signal {signal_key} ({first_seen_at}) marked {status}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving decision for signal {signal_key}: {e}")
            raise
        finally:
            session.close()

    def get_signal_decisions(
        self, signal_keys: Iterable[str] | None = None
    ) -> list[dict]:
        """Signal decisions, all of them or only for the given signal keys."""
        session = self.Session()
        try:
            query = session.query(SignalDecision)
            if signal_keys is None:
                decisions = query.all()
            else:
                keys = sorted(set(signal_keys))
                decisions = [
                    d
                    for chunk in _chunks(keys)
                    for d in query.filter(SignalDecision.signal_key.in_(chunk)).all()
                ]
            decisions.sort(key=lambda d: (d.signal_key, d.first_seen_at))
            return [
                {
                    "id": d.id,
                    "signal_key": d.signal_key,
                    "first_seen_at": d.first_seen_at,
                    "status": d.status,
                    "note": d.note,
                    "decided_at": d.decided_at,
                }
                for d in decisions
            ]
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
