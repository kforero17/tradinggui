from typing import List
import pandas as pd
from pathlib import Path
from loguru import logger
from ..config.settings import settings
from tenacity import retry, stop_after_attempt, wait_exponential

class TickerLoader:
    def __init__(self):
        self.sp500_path = settings.SP500_CSV_PATH
        self.nasdaq_path = settings.NASDAQ_CSV_PATH

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _load_csv(self, path: Path) -> pd.DataFrame:
        """Load CSV file with retry logic."""
        try:
            return pd.read_csv(path)
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")
            raise

    def load_unique_tickers(self) -> List[str]:
        """Load and deduplicate tickers from S&P 500 and NASDAQ."""
        try:
            # Load both datasets
            sp500 = self._load_csv(self.sp500_path)
            nasdaq = self._load_csv(self.nasdaq_path)

            # Extract ticker symbols from each dataset
            # Handle different column names that might exist
            sp500_tickers = []
            nasdaq_tickers = []
            
            # For SP500 - try different possible column names
            for col_name in ['Symbol', 'Name', 'Ticker', 'symbol', 'name', 'ticker']:
                if col_name in sp500.columns:
                    sp500_tickers = sp500[col_name].dropna().tolist()
                    logger.info(f"Found SP500 tickers in column '{col_name}'")
                    break
            
            # For NASDAQ - try different possible column names  
            for col_name in ['Symbol', 'Name', 'Ticker', 'symbol', 'name', 'ticker']:
                if col_name in nasdaq.columns:
                    nasdaq_tickers = nasdaq[col_name].dropna().tolist()
                    logger.info(f"Found NASDAQ tickers in column '{col_name}'")
                    break
            
            if not sp500_tickers:
                logger.warning(f"No ticker column found in SP500 file. Available columns: {list(sp500.columns)}")
            if not nasdaq_tickers:
                logger.warning(f"No ticker column found in NASDAQ file. Available columns: {list(nasdaq.columns)}")

            # Combine and clean tickers
            all_tickers = sp500_tickers + nasdaq_tickers
            if not all_tickers:
                raise ValueError("No ticker symbols found in either dataset")
            
            tickers = pd.Series(all_tickers)
            
            # Clean and validate tickers
            tickers = (
                tickers[tickers.notnull()]
                .str.upper()
                .str.strip()
                .drop_duplicates()
                .tolist()
            )

            # Validate ticker format
            valid_tickers = [
                ticker for ticker in tickers 
                if isinstance(ticker, str) and ticker.replace('.', '').replace('-', '').isalnum()
            ]

            logger.info(f"Loaded {len(valid_tickers)} unique tickers ({len(sp500_tickers)} from SP500, {len(nasdaq_tickers)} from NASDAQ)")
            return valid_tickers

        except Exception as e:
            logger.error(f"Error loading tickers: {e}")
            raise

    def validate_ticker_files(self) -> bool:
        """Validate that required ticker files exist."""
        required_files = [self.sp500_path, self.nasdaq_path]
        missing_files = [f for f in required_files if not f.exists()]
        
        if missing_files:
            logger.error(f"Missing required files: {missing_files}")
            return False
        
        return True

# Create global ticker loader instance
ticker_loader = TickerLoader() 