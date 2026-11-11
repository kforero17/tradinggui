from dataclasses import dataclass
from typing import List
import pandas as pd
from pathlib import Path
from loguru import logger
from ..config.settings import settings
from .universe_sources import LISTED_COLUMNS, to_yfinance_symbol
from tenacity import retry, stop_after_attempt, wait_exponential

# Column names the legacy single-column ticker CSVs may use
TICKER_COLUMN_CANDIDATES = ["Symbol", "Name", "Ticker", "symbol", "name", "ticker"]


@dataclass(frozen=True)
class TickerInfo:
    symbol: str
    name: str
    exchange: str
    is_etf: bool
    in_sp500: bool


def is_valid_ticker(ticker: object) -> bool:
    """Letters and digits, optionally separated by dots or dashes (``BRK-B``)."""
    return (
        isinstance(ticker, str) and ticker.replace(".", "").replace("-", "").isalnum()
    )


class TickerLoader:
    # Paths are read from settings on every access so tests can patch them.
    @property
    def sp500_path(self) -> Path:
        return settings.SP500_CSV_PATH

    @property
    def nasdaq_path(self) -> Path:
        return settings.NASDAQ_CSV_PATH

    @property
    def listed_path(self) -> Path:
        return settings.LISTED_CSV_PATH

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _load_csv(self, path: Path) -> pd.DataFrame:
        """Load CSV file with retry logic."""
        try:
            # utf-8-sig strips the BOM the legacy sp500.csv starts with
            return pd.read_csv(
                path, dtype=str, keep_default_na=False, encoding="utf-8-sig"
            )
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")
            raise

    def _ticker_column(self, frame: pd.DataFrame, label: str) -> List[str]:
        # Handle different column names that might exist
        for col_name in TICKER_COLUMN_CANDIDATES:
            if col_name in frame.columns:
                logger.info(f"Found {label} tickers in column '{col_name}'")
                return [t for t in frame[col_name].tolist() if t]
        logger.warning(
            f"No ticker column found in {label} file. Available columns: {list(frame.columns)}"
        )
        return []

    def _load_listings(self) -> pd.DataFrame:
        """``symbol, name, exchange, is_etf`` rows from us_listed.csv, else the legacy nasdaq.csv."""
        if self.listed_path.exists():
            listed = self._load_csv(self.listed_path).reindex(
                columns=LISTED_COLUMNS, fill_value=""
            )
            listed["is_etf"] = listed["is_etf"].str.lower().eq("true")
            return listed
        # Fallback until the first universe refresh: symbols only, ETF status unknown
        logger.info(f"{self.listed_path} not found; falling back to {self.nasdaq_path}")
        symbols = self._ticker_column(self._load_csv(self.nasdaq_path), "NASDAQ")
        return pd.DataFrame(
            {"symbol": symbols, "name": "", "exchange": "", "is_etf": False}
        )

    def load_universe(self) -> dict[str, TickerInfo]:
        """Every valid ticker from the listings and the S&P 500, keyed by yfinance symbol.

        S&P 500 members come first. ETFs are left out unless ``UNIVERSE_INCLUDE_ETFS``.
        """
        sp500 = self._load_csv(self.sp500_path)
        sp500_names = sp500["Security"] if "Security" in sp500.columns else None
        sp500_symbols = self._ticker_column(sp500, "SP500")
        listings = self._load_listings()

        universe: dict[str, TickerInfo] = {}
        for i, raw in enumerate(sp500_symbols):
            symbol = to_yfinance_symbol(raw)
            if is_valid_ticker(symbol) and symbol not in universe:
                name = str(sp500_names.iloc[i]) if sp500_names is not None else ""
                universe[symbol] = TickerInfo(symbol, name, "", False, True)

        for row in listings.itertuples(index=False):
            symbol = to_yfinance_symbol(str(row.symbol))
            if not is_valid_ticker(symbol):
                continue
            in_sp500 = symbol in universe
            # The listing has the exchange and ETF flag, so it wins over the S&P row
            universe[symbol] = TickerInfo(
                symbol, str(row.name), str(row.exchange), bool(row.is_etf), in_sp500
            )

        if not settings.UNIVERSE_INCLUDE_ETFS:
            universe = {s: info for s, info in universe.items() if not info.is_etf}
        if not universe:
            raise ValueError("No ticker symbols found in either dataset")

        logger.info(
            f"Loaded universe of {len(universe)} tickers "
            f"({len(sp500_symbols)} from SP500, {len(listings)} listings, "
            f"{sum(i.is_etf for i in universe.values())} ETFs)"
        )
        return universe

    def load_unique_tickers(self) -> List[str]:
        """Load and deduplicate tickers from the S&P 500 and the US listings."""
        try:
            return list(self.load_universe())
        except Exception as e:
            logger.error(f"Error loading tickers: {e}")
            raise

    def validate_ticker_files(self) -> bool:
        """Validate that required ticker files exist."""
        missing_files = [f for f in [self.sp500_path] if not f.exists()]
        if not self.listed_path.exists() and not self.nasdaq_path.exists():
            missing_files.append(self.listed_path)

        if missing_files:
            logger.error(f"Missing required files: {missing_files}")
            return False

        return True


# Create global ticker loader instance
ticker_loader = TickerLoader()


def load_universe() -> dict[str, TickerInfo]:
    return ticker_loader.load_universe()
