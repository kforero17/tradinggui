# Configure SSL verification BEFORE importing yfinance
# yfinance uses curl_cffi which requires environment variables set before import
from ..config.ssl_config import configure_ssl_verification
configure_ssl_verification()

from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
import time
import threading
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor

from ..config.settings import settings
from ..data.database import db


class RateLimiter:
    """Thread-safe rate limiter to prevent API throttling."""

    def __init__(self, calls_per_second: float = 0.5):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

class StockdexAPIError(Exception):
    """Custom exception for Stockdex API errors."""
    pass

class StockMetricsCalculator:
    def __init__(self, use_mock_data: bool = False):
        self.lookback_days = settings.HISTORICAL_LOOKBACK_DAYS
        self.use_mock_data = use_mock_data
        self.recent_data_age_limit_days = settings.RECENT_DATA_AGE_LIMIT_DAYS
        self.rate_limiter = RateLimiter(calls_per_second=settings.REQUESTS_PER_SECOND)
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        retry=retry_if_exception_type(StockdexAPIError)
    )
    def _get_historical_data_from_yfinance(self, ticker: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """Fetch historical data from yfinance (via Yahoo Finance)."""
        self.rate_limiter.wait()

        try:
            stock = yf.Ticker(ticker)
            df = stock.history(start=start_date, end=end_date, auto_adjust=True)

            if df.empty:
                logger.warning(f"No historical data for {ticker} from {start_date.date()} to {end_date.date()}.")
                return None

            df.index.name = 't'
            df.columns = df.columns.str.lower()

            required_cols = ['open', 'high', 'low', 'close', 'volume']
            available_cols = [col for col in required_cols if col in df.columns]

            if not available_cols:
                logger.warning(f"No required columns found in data for {ticker}")
                return None

            return df[available_cols]

        except Exception as e:
            error_msg = str(e)
            if "Too Many Requests" in error_msg or "Rate limited" in error_msg:
                logger.warning(f"Rate limited for {ticker}, will retry...")
                raise StockdexAPIError(f"Rate limited for {ticker}") from e
            logger.error(f"Failed to fetch historical data for {ticker} using yfinance: {e}")
            raise StockdexAPIError(f"Could not fetch historical data for {ticker}") from e

    def _get_historical_data_from_stockdex(self, ticker: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """Fallback: Fetch historical data from StockDex library."""
        self.rate_limiter.wait()

        try:
            from stockdex import Ticker as StockdexTicker
            stock = StockdexTicker(ticker=ticker)
            df = stock.price(range='1y')

            if df is None or df.empty:
                logger.warning(f"No StockDex data for {ticker}")
                return None

            df.columns = df.columns.str.lower()

            required_cols = ['open', 'high', 'low', 'close', 'volume']
            available_cols = [col for col in required_cols if col in df.columns]

            if not available_cols:
                return None

            return df[available_cols]

        except Exception as e:
            logger.error(f"StockDex also failed for {ticker}: {e}")
            return None

    def _get_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get historical data with yfinance primary and StockDex fallback."""
        if self.use_mock_data:
            return self._generate_mock_historical_data(ticker)

        today = datetime.utcnow()
        start_date = today - timedelta(days=self.lookback_days)

        logger.info(f"Fetching historical data for {ticker} from {start_date.date()} to {today.date()}")

        # Try yfinance first
        try:
            df = self._get_historical_data_from_yfinance(ticker, start_date, today)
            if df is not None:
                return df
        except (StockdexAPIError, RetryError) as e:
            logger.warning(f"yfinance failed for {ticker}: {e}")

        # Fallback to StockDex
        logger.info(f"Trying StockDex fallback for {ticker}")
        return self._get_historical_data_from_stockdex(ticker, start_date, today)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        retry=retry_if_exception_type(StockdexAPIError)
    )
    def _get_valuation_metrics(self, ticker: str, last_price: Optional[float] = None) -> Dict[str, Any]:
        """Fetch and compute valuation metrics from yfinance (via Yahoo Finance)."""
        metrics = {
            "market_cap": None, "pe_ratio": None, "pb_ratio": None,
            "enterprise_value": None, "ebitda": None, "ebitda_ev": None,
            "ps_ratio": None,
        }

        if self.use_mock_data:
            mock_data = self._generate_mock_valuation_data(ticker)
            metrics.update(mock_data)
            return metrics

        if last_price is None:
            logger.warning(f"Cannot calculate valuation metrics for {ticker} without a share price.")
            return metrics

        self.rate_limiter.wait()

        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            if not info:
                logger.warning(f"Could not fetch info for {ticker}")
                return metrics
            
            # --- 1. Get Market Cap and Quote Type ---
            quote_type = info.get('quoteType', '')
            market_cap = info.get('marketCap', None)
            metrics["market_cap"] = market_cap

            if quote_type == 'ETF':
                logger.info(f"{ticker} is an ETF. Standard valuation metrics are not applicable.")
                return metrics  # Return with just market_cap and Nones for the rest

            # --- 2. Get valuation metrics directly from info ---
            pe_ratio = info.get('trailingPE', None)
            pb_ratio = info.get('priceToBook', None)
            ps_ratio = info.get('priceToSalesTrailing12Months', None)
            enterprise_value = info.get('enterpriseValue', None)
            ebitda = info.get('ebitda', None)
            
            # Calculate EV/EBITDA if both are available
            ev_ebitda = None
            if enterprise_value and ebitda and ebitda > 0:
                ev_ebitda = enterprise_value / ebitda

            metrics.update({
                "pe_ratio": pe_ratio,
                "pb_ratio": pb_ratio,
                "enterprise_value": enterprise_value,
                "ebitda": ebitda,
                "ebitda_ev": ev_ebitda,
                "ps_ratio": ps_ratio,
            })
            return metrics
        except Exception as e:
            error_msg = str(e)
            if "Too Many Requests" in error_msg or "Rate limited" in error_msg:
                logger.warning(f"Rate limited for {ticker} valuation, will retry...")
                raise StockdexAPIError(f"Rate limited for {ticker}") from e
            logger.error(f"Error calculating valuation metrics for {ticker}: {e}", exc_info=False)
            return metrics

    def _parse_financial_number(self, value: Any) -> Optional[float]:
        """Convert string like '8.71B' or '439.26M' to float."""
        if value is None or (isinstance(value, str) and value.strip() in ['N/A', '']):
            return None
        
        if isinstance(value, (int, float)) and not np.isnan(value):
            return float(value)

        if isinstance(value, str):
            value = value.strip()
            multipliers = {'T': 1e12, 'B': 1e9, 'M': 1e6, 'K': 1e3}
            suffix = value[-1].upper()

            if suffix in multipliers:
                try:
                    return float(value[:-1]) * multipliers[suffix]
                except (ValueError, TypeError):
                    return None
            try:
                return float(value.replace(',', ''))
            except (ValueError, TypeError):
                return None
        
        return None

    def calculate_momentum_metrics(self, hist_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate momentum-based metrics."""
        if hist_data is None or hist_data.shape[0] < 100:
            raise ValueError("Insufficient historical data for momentum calculation.")

        close_prices = hist_data['close']
        ma_100 = close_prices.rolling(window=100).mean().iloc[-1]
        ema_100 = close_prices.ewm(span=100, adjust=False).mean().iloc[-1]
        last_price = close_prices.iloc[-1]

        return {
            "last_price": last_price,
            "ma_100": ma_100,
            "ema_100": ema_100,
            "pct_above_ma_100": (last_price - ma_100) / ma_100 * 100 if ma_100 else 0,
            "pct_above_ema_100": (last_price - ema_100) / ema_100 * 100 if ema_100 else 0
        }

    def get_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get all metrics for a ticker."""
        try:
            hist_data = self._get_historical_data(ticker)
            if hist_data is None:
                return None

            momentum_metrics = self.calculate_momentum_metrics(hist_data)
            valuation_metrics = self._get_valuation_metrics(
                ticker, last_price=momentum_metrics.get("last_price")
            )

            metrics = {
                "ticker": ticker,
                **momentum_metrics,
                **valuation_metrics
            }

            if not self._validate_momentum_metrics(metrics):
                logger.warning(f"Invalid momentum metrics for {ticker}")
                return None

            logger.success(f"Successfully generated metrics for {ticker}")
            return metrics
        except ValueError as e:
            logger.warning(f"Could not get metrics for {ticker}: {e}")
            return None
        except StockdexAPIError as e:
            logger.error(f"Yahoo Finance API error for {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing {ticker}: {e}")
            return None

    def get_metrics_batch(self, tickers: List[str], max_workers: int = None) -> List[Dict[str, Any]]:
        """Get metrics for multiple tickers in parallel with rate limiting."""
        max_workers = max_workers or settings.MAX_CONCURRENT_WORKERS
        all_metrics = []
        total_tickers = len(tickers)

        logger.info(f"Processing {total_tickers} tickers with {max_workers} workers (rate: {settings.REQUESTS_PER_SECOND}/sec).")

        # 1. Filter out tickers that have been updated recently
        tickers_to_process = [
            t for t in tickers 
            if not db.has_recent_metrics(t, self.recent_data_age_limit_days)
        ]
        
        skipped_count = total_tickers - len(tickers_to_process)
        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} tickers with recent data.")

        if not tickers_to_process:
            logger.info("No tickers to process after filtering.")
            return []

        # 2. Process remaining tickers in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(self.get_metrics, tickers_to_process)
            
            successful_count = 0
            for metrics in results:
                if metrics:
                    all_metrics.append(metrics)
                    successful_count += 1

        logger.info(f"Batch processing complete. Successfully fetched metrics for {successful_count}/{len(tickers_to_process)} tickers.")
        return all_metrics

    def _validate_momentum_metrics(self, metrics: Dict[str, Any]) -> bool:
        """Validate calculated momentum metrics."""
        required_fields = ["ticker", "last_price", "ma_100", "ema_100"]
        
        if not all(field in metrics and metrics[field] is not None for field in required_fields):
            return False

        for field in required_fields:
            if field != "ticker":
                value = metrics[field]
                if isinstance(value, (int, float)) and (np.isnan(value) or np.isinf(value)):
                    return False
        return True
    
    def _generate_mock_historical_data(self, ticker: str) -> pd.DataFrame:
        """Generate mock historical data for testing."""
        logger.info(f"Generating mock historical data for {ticker}")
        dates = pd.date_range(end=datetime.today(), periods=150, freq='D')
        base_price = 100 + (hash(ticker) % 50)
        prices = [base_price]
        for _ in range(149):
            prices.append(prices[-1] * (1 + np.random.normal(0.001, 0.02)))
        
        df = pd.DataFrame({'close': prices, 'open': prices, 'high': prices, 'low': prices, 'volume': prices}, index=dates)
        return df

    def _generate_mock_valuation_data(self, ticker: str) -> Dict[str, Any]:
        """Generate mock valuation data for testing."""
        logger.info(f"Generating mock valuation data for {ticker}")
        return {
            "market_cap": 2e12,
            "pe_ratio": 25.0,
            "pb_ratio": 5.0,
            "enterprise_value": 2.1e12,
            "ebitda": 1.5e11,
            "ebitda_ev": 14.0,
            "ps_ratio": 10.0,
        }


def calculate_period_gains(
    hist_data: pd.DataFrame,
    shares: float,
    added_at: Optional[datetime] = None
) -> Dict[str, Dict[str, Optional[float]]]:
    """Calculate gains for multiple time periods from historical data.

    Args:
        hist_data: DataFrame with 'close' prices indexed by date
        shares: Number of shares held
        added_at: When the stock was added to portfolio (used for YTD if after Jan 1)

    Returns:
        Dictionary with period gains:
        {'1D': {'gain_dollars': float, 'gain_percent': float}, ...}
    """
    empty_gain = {'gain_dollars': None, 'gain_percent': None}
    empty_gains = {'1D': empty_gain.copy(), '1W': empty_gain.copy(),
                   '1M': empty_gain.copy(), 'YTD': empty_gain.copy()}

    if hist_data is None or hist_data.empty:
        return empty_gains

    close_prices = hist_data['close'].dropna()
    if close_prices.empty:
        return empty_gains

    current_price = float(close_prices.iloc[-1])
    current_date = close_prices.index[-1]

    gains = {}

    # 1D: today vs yesterday close
    gains['1D'] = _calculate_gain_by_days(close_prices, current_price, shares, days_back=1)

    # 1W: today vs ~7 calendar days ago (find closest trading day)
    gains['1W'] = _calculate_gain_by_days(close_prices, current_price, shares, days_back=5)

    # Handle timezone - match the index timezone if present
    tz = close_prices.index.tz

    # 1M: today vs first trading day of current month
    first_of_month = pd.Timestamp(current_date.year, current_date.month, 1, tz=tz)
    gains['1M'] = _calculate_gain_from_date(close_prices, current_price, shares, first_of_month)

    # YTD: use added_at date if stock was added after Jan 1, otherwise use Jan 1
    first_of_year = pd.Timestamp(current_date.year, 1, 1, tz=tz)
    if added_at is not None:
        added_ts = pd.Timestamp(added_at, tz=tz)
        if added_ts > first_of_year:
            first_of_year = added_ts
    gains['YTD'] = _calculate_gain_from_date(close_prices, current_price, shares, first_of_year)

    return gains


def _calculate_gain_by_days(close_prices: pd.Series, current_price: float,
                            shares: float, days_back: int) -> Dict[str, Optional[float]]:
    """Calculate gain from N trading days back."""
    if len(close_prices) <= days_back:
        return {'gain_dollars': None, 'gain_percent': None}

    reference_price = float(close_prices.iloc[-(days_back + 1)])
    return _compute_gain_values(current_price, reference_price, shares)


def _calculate_gain_from_date(close_prices: pd.Series, current_price: float,
                              shares: float, reference_date: pd.Timestamp) -> Dict[str, Optional[float]]:
    """Calculate gain from a specific reference date (finds closest trading day on or after)."""
    available_dates = close_prices.index[close_prices.index >= reference_date]
    if available_dates.empty:
        return {'gain_dollars': None, 'gain_percent': None}

    reference_price = float(close_prices.loc[available_dates[0]])
    return _compute_gain_values(current_price, reference_price, shares)


def _compute_gain_values(current_price: float, reference_price: float,
                         shares: float) -> Dict[str, Optional[float]]:
    """Compute dollar and percentage gains."""
    if reference_price == 0:
        return {'gain_dollars': None, 'gain_percent': None}

    price_change = current_price - reference_price
    gain_percent = (price_change / reference_price) * 100
    gain_dollars = price_change * shares

    return {
        'gain_dollars': round(gain_dollars, 2),
        'gain_percent': round(gain_percent, 2)
    }


def calculate_portfolio_summary(positions: List[Dict]) -> Dict[str, Any]:
    """Calculate aggregate portfolio metrics.

    Args:
        positions: List of position dicts with 'current_value' and 'gains' keys

    Returns:
        Dictionary with total_value, day/ytd gain dollars and percentages
    """
    if not positions:
        return {
            'total_value': 0.0,
            'day_gain_dollars': 0.0,
            'day_gain_percent': 0.0,
            'ytd_gain_dollars': 0.0,
            'ytd_gain_percent': 0.0
        }

    total_value = sum(p.get('current_value', 0) or 0 for p in positions)

    day_gain_dollars = sum(
        p.get('gains', {}).get('1D', {}).get('gain_dollars', 0) or 0
        for p in positions
    )

    ytd_gain_dollars = sum(
        p.get('gains', {}).get('YTD', {}).get('gain_dollars', 0) or 0
        for p in positions
    )

    yesterday_value = total_value - day_gain_dollars
    ytd_start_value = total_value - ytd_gain_dollars

    day_gain_percent = (day_gain_dollars / yesterday_value * 100) if yesterday_value > 0 else 0
    ytd_gain_percent = (ytd_gain_dollars / ytd_start_value * 100) if ytd_start_value > 0 else 0

    return {
        'total_value': round(total_value, 2),
        'day_gain_dollars': round(day_gain_dollars, 2),
        'day_gain_percent': round(day_gain_percent, 2),
        'ytd_gain_dollars': round(ytd_gain_dollars, 2),
        'ytd_gain_percent': round(ytd_gain_percent, 2)
    }


# Create global metrics calculator instance
metrics_calculator = StockMetricsCalculator(use_mock_data=False) 