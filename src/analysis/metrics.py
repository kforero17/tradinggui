from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time
import random
import requests
from stockdex import Ticker

from ..config.settings import settings

class StockdexAPIError(Exception):
    """Custom exception for Stockdex API errors."""
    pass

class StockMetricsCalculator:
    def __init__(self, use_mock_data: bool = False):
        self.lookback_days = settings.HISTORICAL_LOOKBACK_DAYS
        self.use_mock_data = use_mock_data
        
    def _get_historical_data_from_stockdex(self, ticker: str, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """Fetch historical data from Stockdex (via Yahoo Finance)."""
        try:
            # Determine the appropriate range string for the stockdex API call
            delta_days = (end_date - start_date).days
            if delta_days <= 30:
                api_range = '1mo'
            elif delta_days <= 90:
                api_range = '3mo'
            elif delta_days <= 180:
                api_range = '6mo'
            elif delta_days <= 365:
                api_range = '1y'
            elif delta_days <= 730:
                api_range = '2y'
            else:
                api_range = '5y'

            stock = Ticker(ticker)
            df = stock.yahoo_api_price(range=api_range, dataGranularity='1d')
            
            if df.empty:
                logger.warning(f"No new historical data for {ticker} from {start_date.date()} to {end_date.date()}.")
                return None

            # The `timestamp` column should be datetime. Filter based on it.
            df = df[df['timestamp'] >= start_date]
            
            if df.empty:
                logger.warning(f"No historical data for {ticker} in the specified date range.")
                return None

            df.set_index('timestamp', inplace=True)
            df.index.name = 't'
            
            # Select only the needed columns
            return df[['open', 'high', 'low', 'close', 'volume']]

        except Exception as e:
            logger.error(f"Failed to fetch historical data for {ticker} using Stockdex: {e}")
            raise StockdexAPIError(f"Could not fetch historical data for {ticker}") from e

    def _get_historical_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get historical data directly from the API."""
        if self.use_mock_data:
            return self._generate_mock_historical_data(ticker)

        today = datetime.utcnow()
        start_date = today - timedelta(days=self.lookback_days)
        
        logger.info(f"Fetching historical data for {ticker} from {start_date.date()} to {today.date()}")
        return self._get_historical_data_from_stockdex(ticker, start_date, today)

    def _get_valuation_metrics(self, ticker: str, last_price: Optional[float] = None) -> Dict[str, Any]:
        """Fetch and compute valuation metrics from Stockdex (via Yahoo Finance)."""
        if self.use_mock_data:
            return self._generate_mock_valuation_data(ticker)
        
        if last_price is None:
            logger.warning(f"Cannot calculate valuation metrics for {ticker} without a share price.")
            return {}

        try:
            stock = Ticker(ticker)
            
            # --- 1. Get Market Cap and P/E from Summary ---
            raw_summary = stock.yahoo_web_summary
            summary = {}
            market_cap, pe_ratio = None, None

            if isinstance(raw_summary, dict):
                summary = raw_summary
            elif isinstance(raw_summary, pd.DataFrame) and not raw_summary.empty:
                summary = raw_summary.iloc[:, 0].to_dict()

            if summary:
                market_cap_data = summary.get('marketCap')
                if isinstance(market_cap_data, dict):
                    market_cap = self._parse_financial_number(market_cap_data.get('raw'))
                else:
                    market_cap = self._parse_financial_number(market_cap_data)
            else:
                logger.warning(f"Could not parse summary data for {ticker}.")

            # --- 2. Get other metrics from financials and balance sheet ---
            financials_df = stock.yahoo_api_financials(frequency='annual')
            balance_sheet_df = stock.yahoo_api_balance_sheet(frequency='annual')

            if financials_df.empty or balance_sheet_df.empty:
                logger.warning(f"Could not retrieve full financial or balance sheet data for {ticker}. Metrics will be incomplete.")
                return {
                    "market_cap": market_cap,
                    "pe_ratio": pe_ratio,
                    "pb_ratio": None, "enterprise_value": None, "ebitda": None,
                    "ebitda_ev": None, "ps_ratio": None, "peg_ratio": None, "forward_pe": None
                }
            
            financials = financials_df.iloc[0].to_dict()
            balance_sheet = balance_sheet_df.iloc[0].to_dict()

            revenue = self._parse_financial_number(financials.get('annualTotalRevenue'))
            ebit = self._parse_financial_number(financials.get('annualEBIT'))
            depreciation = self._parse_financial_number(financials.get('annualReconciledDepreciation'))
            annual_diluted_eps = self._parse_financial_number(financials.get('annualDilutedEPS'))

            cash = self._parse_financial_number(balance_sheet.get('annualCashAndCashEquivalents'))
            long_term_debt = self._parse_financial_number(balance_sheet.get('annualLongTermDebt'))
            short_term_debt = self._parse_financial_number(balance_sheet.get('annualCurrentDebtAndCapitalLeaseObligation'))
            book_value = self._parse_financial_number(balance_sheet.get('annualTotalEquityGrossMinorityInterest'))

            # --- 3. Calculate Derived Metrics ---
            pe_ratio = last_price / annual_diluted_eps if all(v is not None for v in [last_price, annual_diluted_eps]) and annual_diluted_eps > 0 else None
            ebitda = (ebit + depreciation) if all(v is not None for v in [ebit, depreciation]) else None
            ev = (market_cap + long_term_debt + short_term_debt - cash) if all(v is not None for v in [market_cap, long_term_debt, short_term_debt, cash]) else None
            
            pb_ratio = market_cap / book_value if all(v is not None for v in [market_cap, book_value]) and book_value > 0 else None
            ev_ebitda = ev / ebitda if all(v is not None for v in [ev, ebitda]) and ebitda > 0 else None
            ps_ratio = market_cap / revenue if all(v is not None for v in [market_cap, revenue]) and revenue > 0 else None

            return {
                "market_cap": market_cap,
                "pe_ratio": pe_ratio,
                "pb_ratio": pb_ratio,
                "enterprise_value": ev,
                "ebitda": ebitda,
                "ebitda_ev": ev_ebitda,
                "ps_ratio": ps_ratio,
            }
        except Exception as e:
            logger.error(f"Error calculating valuation metrics for {ticker}: {e}", exc_info=True)
            return {}

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

            return metrics
        except ValueError as e:
            logger.warning(f"Could not get metrics for {ticker}: {e}")
            return None
        except StockdexAPIError as e:
            logger.error(f"Stockdex API error for {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing {ticker}: {e}")
            return None

    def get_metrics_batch(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """Get metrics for multiple tickers."""
        all_metrics = []
        total_tickers = len(tickers)
        
        logger.info(f"Processing {total_tickers} tickers.")

        for i, ticker in enumerate(tickers):
            logger.info(f"[{i+1}/{total_tickers}] Processing {ticker}")
            try:
                metrics = self.get_metrics(ticker)
                if metrics:
                    all_metrics.append(metrics)
                    logger.success(f"Successfully processed {ticker}")
                else:
                    logger.warning(f"Failed to get metrics for {ticker}")
            except Exception as e:
                logger.error(f"Error processing {ticker} in batch: {e}")
        
        logger.info(f"Batch processing complete. Successfully processed {len(all_metrics)}/{total_tickers} tickers")
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
        }

# Create global metrics calculator instance
metrics_calculator = StockMetricsCalculator(use_mock_data=False) 