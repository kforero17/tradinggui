from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from yahoo_fin import stock_info as si
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
import random
import requests.exceptions
from ..config.settings import settings

class StockMetricsCalculator:
    def __init__(self, use_mock_data: bool = False):
        self.lookback_days = settings.HISTORICAL_LOOKBACK_DAYS
        self.rate_limit = settings.YAHOO_FIN_RATE_LIMIT
        self._last_request_time = 0
        self._min_request_interval = 1.0  # Minimum seconds between requests
        self.use_mock_data = use_mock_data

    def _throttle_request(self):
        """Implement random throttling between requests to avoid rate limits."""
        # Random delay between 2.5 and 4.5 seconds
        delay = random.uniform(2.5, 4.5)
        logger.debug(f"Throttling request: sleeping for {delay:.2f} seconds")
        time.sleep(delay)

    def _is_rate_limited_error(self, exception: Exception) -> bool:
        """Check if the exception is due to rate limiting."""
        if isinstance(exception, requests.exceptions.HTTPError):
            return exception.response.status_code == 429
        if isinstance(exception, AssertionError):
            # Yahoo_fin raises AssertionError for HTTP 429
            return "429" in str(exception) or "Too Many Requests" in str(exception)
        if isinstance(exception, requests.exceptions.JSONDecodeError):
            # Check if the response text indicates rate limiting
            return "Too Many Requests" in str(exception)
        return False

    @retry(
        stop=stop_after_attempt(5),  # Increased retry attempts
        wait=wait_exponential(multiplier=3, min=8, max=60),  # Longer backoff
        retry=retry_if_exception_type((requests.exceptions.RequestException, AssertionError, ValueError))
    )
    def _get_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch historical data with throttling and improved error handling."""
        if self.use_mock_data:
            return self._generate_mock_historical_data(ticker)
            
        self._throttle_request()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        try:
            start_date = datetime.today() - timedelta(days=self.lookback_days)
            df = si.get_data(ticker, start_date=start_date, headers=headers)
            if df is None or df.empty:
                logger.warning(f"No historical data found for {ticker}")
                return None
            logger.info(f"Successfully fetched historical data for {ticker}")
            return df
        except Exception as e:
            if self._is_rate_limited_error(e):
                logger.warning(f"Rate limited while fetching historical data for {ticker}, will retry with backoff")
                # Add extra delay for rate limit errors
                time.sleep(random.uniform(5, 10))
            else:
                logger.error(f"Error fetching historical data for {ticker}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=3, min=8, max=60),
        retry=retry_if_exception_type((requests.exceptions.RequestException, AssertionError, ValueError))
    )
    def _get_stock_stats(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch stock stats with throttling and improved error handling."""
        if self.use_mock_data:
            logger.info(f"Mock mode: skipping stock stats for {ticker}")
            return None
            
        self._throttle_request()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        try:
            stats = si.get_stats(ticker, headers=headers)
            if stats is None or stats.empty:
                logger.warning(f"No stock stats found for {ticker}")
                return None
            logger.info(f"Successfully fetched stock stats for {ticker}")
            return stats
        except Exception as e:
            if self._is_rate_limited_error(e):
                logger.warning(f"Rate limited while fetching stock stats for {ticker}, will retry with backoff")
                time.sleep(random.uniform(5, 10))
            else:
                logger.error(f"Error fetching stock stats for {ticker}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=3, min=8, max=60),
        retry=retry_if_exception_type((requests.exceptions.RequestException, AssertionError, ValueError))
    )
    def _get_valuation_stats(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch valuation stats with throttling and improved error handling."""
        if self.use_mock_data:
            return self._generate_mock_valuation_data(ticker)
            
        self._throttle_request()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }
        
        try:
            valuation_stats = si.get_stats_valuation(ticker, headers=headers)
            if valuation_stats is None or valuation_stats.empty:
                logger.warning(f"No valuation stats found for {ticker}")
                return None
            logger.info(f"Successfully fetched valuation stats for {ticker}")
            return valuation_stats
        except Exception as e:
            if self._is_rate_limited_error(e):
                logger.warning(f"Rate limited while fetching valuation stats for {ticker}, will retry with backoff")
                time.sleep(random.uniform(5, 10))
            else:
                logger.error(f"Error fetching valuation stats for {ticker}: {e}")
            raise

    def calculate_momentum_metrics(self, hist_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate momentum-based metrics. Warn if fallback price is used."""
        if hist_data.shape[0] < 100:
            raise ValueError("Insufficient historical data")

        close_prices = hist_data['close']
        ma_100 = close_prices.rolling(window=100).mean().iloc[-1]
        ema_100 = close_prices.ewm(span=100).mean().iloc[-1]
        last_price = close_prices.iloc[-1]

        # Warn if fallback price is used
        if last_price == -999999:
            logger.warning("Fallback price (-999999) used for last_price. Data quality issue.")

        return {
            "last_price": last_price,
            "ma_100": ma_100,
            "ema_100": ema_100,
            "pct_above_ma_100": (last_price - ma_100) / ma_100 * 100,
            "pct_above_ema_100": (last_price - ema_100) / ema_100 * 100
        }

    def calculate_valuation_metrics(self, ticker: str) -> Dict[str, Optional[float]]:
        """Calculate valuation-based metrics using get_stats_valuation."""
        try:
            # Get valuation stats
            valuation_stats = self._get_valuation_stats(ticker)
            
            metrics = {
                "pe_ratio": None,
                "pb_ratio": None,
                "ps_ratio": None,
                "peg_ratio": None,
                "forward_pe": None,
                "market_cap": None,
                "enterprise_value": None,
                "ebitda": None,
                "ebitda_ev": None
            }

            if valuation_stats is not None and not valuation_stats.empty:
                # Process the valuation stats DataFrame
                val = valuation_stats.iloc[:, :2]
                val.columns = ["Attribute", "Recent"]

                try:
                    # Extract P/E ratio
                    pe_row = val[val.Attribute.str.contains("Trailing P/E")]
                    if not pe_row.empty:
                        metrics["pe_ratio"] = float(pe_row.iloc[0, 1])

                    # Extract P/B ratio
                    pb_row = val[val.Attribute.str.contains("Price/Book")]
                    if not pb_row.empty:
                        metrics["pb_ratio"] = float(pb_row.iloc[0, 1])

                    # Extract P/S ratio
                    ps_row = val[val.Attribute.str.contains("Price/Sales")]
                    if not ps_row.empty:
                        metrics["ps_ratio"] = float(ps_row.iloc[0, 1])

                    # Extract PEG ratio
                    peg_row = val[val.Attribute.str.contains("PEG")]
                    if not peg_row.empty:
                        metrics["peg_ratio"] = float(peg_row.iloc[0, 1])

                    # Extract Forward P/E ratio
                    forward_pe_row = val[val.Attribute.str.contains("Forward P/E")]
                    if not forward_pe_row.empty:
                        metrics["forward_pe"] = float(forward_pe_row.iloc[0, 1])

                    # Extract Market Cap
                    mc_row = val[val.Attribute.str.contains("Market Cap")]
                    if not mc_row.empty:
                        market_cap_str = mc_row.iloc[0, 1]
                        metrics["market_cap"] = self._convert_market_cap(market_cap_str)

                    # Extract Enterprise Value
                    ev_row = val[val.Attribute.str.contains("Enterprise Value")]
                    if not ev_row.empty:
                        ev_str = ev_row.iloc[0, 1]
                        metrics["enterprise_value"] = self._convert_market_cap(ev_str)

                    # Extract EBITDA
                    ebitda_row = val[val.Attribute.str.contains("EBITDA")]
                    if not ebitda_row.empty:
                        ebitda_str = ebitda_row.iloc[0, 1]
                        metrics["ebitda"] = self._convert_market_cap(ebitda_str)

                    # Calculate EBITDA/EV ratio if both values are available
                    if metrics["ebitda"] and metrics["enterprise_value"] and metrics["enterprise_value"] != 0:
                        metrics["ebitda_ev"] = metrics["ebitda"] / metrics["enterprise_value"]

                except (ValueError, IndexError, KeyError) as e:
                    logger.warning(f"Error extracting specific valuation metric for {ticker}: {e}")

            return metrics

        except Exception as e:
            logger.exception(f"Error calculating valuation metrics for {ticker}")
            return {
                "pe_ratio": None,
                "pb_ratio": None,
                "ps_ratio": None,
                "peg_ratio": None,
                "forward_pe": None,
                "market_cap": None,
                "enterprise_value": None,
                "ebitda": None,
                "ebitda_ev": None
            }

    def _convert_market_cap(self, value_str: str) -> Optional[float]:
        """Convert market cap string (e.g., '3.02T', '52.4B') to float."""
        if not value_str or value_str == 'N/A':
            return None
        
        try:
            value_str = str(value_str).strip().replace(',', '')
            
            if value_str.endswith('T'):
                return float(value_str[:-1]) * 1e12
            elif value_str.endswith('B'):
                return float(value_str[:-1]) * 1e9
            elif value_str.endswith('M'):
                return float(value_str[:-1]) * 1e6
            elif value_str.endswith('K'):
                return float(value_str[:-1]) * 1e3
            else:
                return float(value_str)
        except (ValueError, AttributeError):
            return None

    def get_metrics(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get all metrics for a ticker."""
        try:
            # Get historical data
            hist_data = self._get_historical_data(ticker)
            if hist_data is None or hist_data.shape[0] < 100:
                logger.warning(f"Insufficient data for {ticker}")
                return None

            # Calculate momentum metrics (required)
            momentum_metrics = self.calculate_momentum_metrics(hist_data)
            
            # Try to calculate valuation metrics (optional)
            try:
                valuation_metrics = self.calculate_valuation_metrics(ticker)
            except Exception as e:
                logger.warning(f"Valuation metrics failed for {ticker}, proceeding with momentum metrics only: {e}")
                valuation_metrics = {
                    "pe_ratio": None,
                    "pb_ratio": None,
                    "ps_ratio": None,
                    "peg_ratio": None,
                    "forward_pe": None,
                    "market_cap": None,
                    "enterprise_value": None,
                    "ebitda": None,
                    "ebitda_ev": None
                }

            # Combine all metrics
            metrics = {
                "ticker": ticker,
                **momentum_metrics,
                **valuation_metrics
            }

            # Validate metrics (only check required momentum metrics)
            if not self._validate_momentum_metrics(metrics):
                logger.warning(f"Invalid momentum metrics for {ticker}")
                return None

            return metrics

        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            return None

    def get_metrics_batch(self, tickers: List[str], batch_size: int = 10) -> List[Dict[str, Any]]:
        """Get metrics for multiple tickers in batches to reduce API pressure."""
        all_metrics = []
        total_tickers = len(tickers)
        
        logger.info(f"Processing {total_tickers} tickers in batches of {batch_size}")
        
        for i in range(0, total_tickers, batch_size):
            batch = tickers[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (total_tickers + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_number}/{total_batches}: {batch}")
            
            batch_metrics = []
            for ticker in batch:
                try:
                    metrics = self.get_metrics(ticker)
                    if metrics:
                        batch_metrics.append(metrics)
                        logger.success(f"Successfully processed {ticker}")
                    else:
                        logger.warning(f"Failed to get metrics for {ticker}")
                except Exception as e:
                    logger.error(f"Error processing {ticker} in batch: {e}")
                    continue
            
            all_metrics.extend(batch_metrics)
            
            # Add longer delay between batches to avoid overwhelming the API
            if i + batch_size < total_tickers:  # Don't sleep after the last batch
                batch_delay = random.uniform(10, 15)
                logger.info(f"Batch {batch_number} complete. Waiting {batch_delay:.1f}s before next batch...")
                time.sleep(batch_delay)
        
        logger.info(f"Batch processing complete. Successfully processed {len(all_metrics)}/{total_tickers} tickers")
        return all_metrics

    def _validate_momentum_metrics(self, metrics: Dict[str, Any]) -> bool:
        """Validate calculated momentum metrics only."""
        required_fields = [
            "ticker", "last_price", "ma_100", "ema_100",
            "pct_above_ma_100", "pct_above_ema_100"
        ]
        
        # Check required fields
        if not all(field in metrics for field in required_fields):
            return False

        # Check for invalid values in momentum metrics only
        for field in required_fields:
            value = metrics[field]
            if field != "ticker" and isinstance(value, (int, float)) and (
                np.isnan(value) or np.isinf(value)
            ):
                return False

        return True

    def _generate_mock_historical_data(self, ticker: str) -> pd.DataFrame:
        """Generate mock historical data for testing."""
        logger.info(f"Generating mock historical data for {ticker}")
        
        # Generate 150 days of mock price data
        dates = pd.date_range(end=datetime.today(), periods=150, freq='D')
        
        # Base price varies by ticker
        base_prices = {
            'AAPL': 180,
            'MSFT': 400,
            'NVDA': 140,
            'TSLA': 250,
            'GLD': 200,
            'SPY': 450,
            'QQQ': 380
        }
        
        base_price = base_prices.get(ticker, 100)
        
        # Generate realistic price movements
        np.random.seed(hash(ticker) % 2**32)  # Consistent data for same ticker
        returns = np.random.normal(0.001, 0.02, 150)  # Daily returns
        prices = [base_price]
        
        for ret in returns[1:]:
            new_price = prices[-1] * (1 + ret)
            prices.append(max(new_price, 1.0))  # Ensure price doesn't go negative
        
        df = pd.DataFrame({
            'open': [p * random.uniform(0.98, 1.02) for p in prices],
            'high': [p * random.uniform(1.00, 1.05) for p in prices],
            'low': [p * random.uniform(0.95, 1.00) for p in prices],
            'close': prices,
            'adjclose': prices,
            'volume': [random.randint(1000000, 50000000) for _ in prices]
        }, index=dates)
        
        return df

    def _generate_mock_valuation_data(self, ticker: str) -> pd.DataFrame:
        """Generate mock valuation data for testing."""
        logger.info(f"Generating mock valuation data for {ticker}")
        
        # Base valuations vary by ticker
        mock_data = {
            'AAPL': {
                'Market Cap (intraday)': '2.85T',
                'Enterprise Value': '2.79T',
                'Trailing P/E': '28.45',
                'Forward P/E (1y)': '25.20',
                'PEG Ratio (5 yr expected)': '2.85',
                'Price/Sales (ttm)': '8.12',
                'Price/Book (mrq)': '39.85',
                'EBITDA': '130.5B'
            },
            'MSFT': {
                'Market Cap (intraday)': '3.12T',
                'Enterprise Value': '3.05T',
                'Trailing P/E': '32.15',
                'Forward P/E (1y)': '28.90',
                'PEG Ratio (5 yr expected)': '2.45',
                'Price/Sales (ttm)': '12.85',
                'Price/Book (mrq)': '12.25',
                'EBITDA': '118.2B'
            },
            'NVDA': {
                'Market Cap (intraday)': '3.58T',
                'Enterprise Value': '3.52T',
                'Trailing P/E': '65.45',
                'Forward P/E (1y)': '45.20',
                'PEG Ratio (5 yr expected)': '1.85',
                'Price/Sales (ttm)': '55.85',
                'Price/Book (mrq)': '48.25',
                'EBITDA': '85.6B'
            }
        }
        
        # Default values for unknown tickers
        default_data = {
            'Market Cap (intraday)': '50.0B',
            'Enterprise Value': '48.5B',
            'Trailing P/E': '22.50',
            'Forward P/E (1y)': '20.15',
            'PEG Ratio (5 yr expected)': '1.95',
            'Price/Sales (ttm)': '4.25',
            'Price/Book (mrq)': '3.85',
            'EBITDA': '8.5B'
        }
        
        data = mock_data.get(ticker, default_data)
        
        # Create DataFrame in the format expected by get_stats_valuation
        df = pd.DataFrame({
            0: list(data.keys()),
            1: list(data.values())
        })
        
        return df

# Create global metrics calculator instance
metrics_calculator = StockMetricsCalculator(use_mock_data=False)  # Use mock data by default due to network issues 