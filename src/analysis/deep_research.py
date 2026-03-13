from ..config.ssl_config import configure_ssl_verification
configure_ssl_verification()

import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger

from ..config.settings import settings
from .metrics import RateLimiter


class DeepResearchAnalyzer:

    def __init__(self):
        self.rate_limiter = RateLimiter(calls_per_second=0.5)

    def analyze(self, ticker: str) -> dict | None:
        try:
            self.rate_limiter.wait()
            stock = yf.Ticker(ticker)
            info = stock.info or {}

            if not info.get("longName"):
                logger.warning(f"No company info found for {ticker}")
                return None

            income_stmt = stock.income_stmt
            balance_sheet = stock.balance_sheet
            cashflow = stock.cashflow

            market_cap = info.get("marketCap")
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")

            revenue_history = self._build_revenue_history(income_stmt)
            margin_history = self._build_margin_history(income_stmt)
            balance_sheet_data = self._build_balance_sheet(balance_sheet)
            fcf_data = self._build_fcf(cashflow, income_stmt, market_cap)
            valuation_data = self._build_valuation(info)
            analyst_data = self._build_analyst(info, current_price)
            quality_data = self._build_quality(
                info, income_stmt, balance_sheet, margin_history, revenue_history
            )
            dividend_data = self._build_dividend(info)

            result = {
                "company_name": info.get("longName"),
                "ticker": ticker,
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "business_summary": info.get("longBusinessSummary"),
                "employee_count": info.get("fullTimeEmployees"),
                "website": info.get("website"),
                "revenue_history": revenue_history,
                "margin_history": margin_history,
                "balance_sheet": balance_sheet_data,
                "fcf": fcf_data,
                "valuation": valuation_data,
                "analyst": analyst_data,
                "quality": quality_data,
                "dividend": dividend_data,
            }

            logger.success(f"Deep research analysis complete for {ticker}")
            return result

        except Exception as e:
            logger.error(f"Deep research analysis failed for {ticker}: {e}")
            return None

    def _safe_get_row(self, df: pd.DataFrame, row_names: list[str]) -> pd.Series | None:
        if df is None or df.empty:
            return None
        for name in row_names:
            if name in df.index:
                return df.loc[name]
        return None

    def _safe_float(self, value) -> float | None:
        if value is None:
            return None
        try:
            val = float(value)
            if np.isnan(val) or np.isinf(val):
                return None
            return round(val, 2)
        except (ValueError, TypeError):
            return None

    def _build_revenue_history(self, income_stmt: pd.DataFrame) -> list[dict]:
        revenue_row = self._safe_get_row(income_stmt, ["Total Revenue", "Revenue"])
        if revenue_row is None:
            return []

        entries = []
        revenues = []
        for col in revenue_row.index:
            rev = self._safe_float(revenue_row[col])
            if rev is not None:
                revenues.append((str(col.year) if hasattr(col, "year") else str(col), rev))

        for i, (year, revenue) in enumerate(revenues):
            growth_pct = None
            if i + 1 < len(revenues) and revenues[i + 1][1] != 0:
                previous_revenue = revenues[i + 1][1]
                growth_pct = round((revenue - previous_revenue) / abs(previous_revenue) * 100, 2)
            entries.append({"year": year, "revenue": revenue, "growth_pct": growth_pct})

        return entries

    def _build_margin_history(self, income_stmt: pd.DataFrame) -> list[dict]:
        revenue_row = self._safe_get_row(income_stmt, ["Total Revenue", "Revenue"])
        gross_profit_row = self._safe_get_row(income_stmt, ["Gross Profit"])
        operating_income_row = self._safe_get_row(
            income_stmt, ["Operating Income", "EBIT", "Operating Revenue"]
        )
        net_income_row = self._safe_get_row(
            income_stmt, ["Net Income", "Net Income Common Stockholders"]
        )

        if revenue_row is None:
            return []

        entries = []
        for col in revenue_row.index:
            revenue = self._safe_float(revenue_row[col])
            if revenue is None or revenue == 0:
                continue

            gross_profit = self._safe_float(gross_profit_row[col]) if gross_profit_row is not None else None
            operating_income = self._safe_float(operating_income_row[col]) if operating_income_row is not None else None
            net_income = self._safe_float(net_income_row[col]) if net_income_row is not None else None

            year = str(col.year) if hasattr(col, "year") else str(col)
            entries.append({
                "year": year,
                "gross_margin": round(gross_profit / revenue * 100, 2) if gross_profit is not None else None,
                "operating_margin": round(operating_income / revenue * 100, 2) if operating_income is not None else None,
                "net_margin": round(net_income / revenue * 100, 2) if net_income is not None else None,
            })

        return entries

    def _build_balance_sheet(self, balance_sheet: pd.DataFrame) -> dict:
        result = {
            "total_debt": None,
            "total_equity": None,
            "debt_to_equity": None,
            "current_assets": None,
            "current_liabilities": None,
            "current_ratio": None,
            "cash_and_equivalents": None,
            "net_cash": None,
        }

        if balance_sheet is None or balance_sheet.empty:
            return result

        latest = balance_sheet.iloc[:, 0]

        def get_val(names: list[str]) -> float | None:
            for name in names:
                if name in latest.index:
                    return self._safe_float(latest[name])
            return None

        total_debt = get_val(["Total Debt", "Long Term Debt", "Total Non Current Liabilities Net Minority Interest"])
        total_equity = get_val(["Stockholders Equity", "Total Equity Gross Minority Interest", "Common Stock Equity"])
        current_assets = get_val(["Current Assets", "Total Current Assets"])
        current_liabilities = get_val(["Current Liabilities", "Total Current Liabilities"])
        cash = get_val([
            "Cash And Cash Equivalents",
            "Cash Cash Equivalents And Short Term Investments",
            "Cash Financial",
            "Cash And Cash Equivalents And Short Term Investments",
        ])

        result["total_debt"] = total_debt
        result["total_equity"] = total_equity
        result["current_assets"] = current_assets
        result["current_liabilities"] = current_liabilities
        result["cash_and_equivalents"] = cash

        if total_debt is not None and total_equity is not None and total_equity != 0:
            result["debt_to_equity"] = round(total_debt / total_equity, 2)

        if current_assets is not None and current_liabilities is not None and current_liabilities != 0:
            result["current_ratio"] = round(current_assets / current_liabilities, 2)

        if cash is not None and total_debt is not None:
            result["net_cash"] = round(cash - total_debt, 2)

        return result

    def _build_fcf(
        self, cashflow: pd.DataFrame, income_stmt: pd.DataFrame, market_cap: float | None
    ) -> dict:
        result = {
            "fcf_history": [],
            "fcf_yield": None,
            "capex_to_revenue": None,
        }

        if cashflow is None or cashflow.empty:
            return result

        fcf_row = self._safe_get_row(cashflow, ["Free Cash Flow"])
        ocf_row = self._safe_get_row(
            cashflow, ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities"]
        )
        capex_row = self._safe_get_row(cashflow, ["Capital Expenditure", "Capital Expenditures"])

        fcf_values = []
        for col in cashflow.columns:
            fcf_val = None
            if fcf_row is not None:
                fcf_val = self._safe_float(fcf_row[col])

            if fcf_val is None and ocf_row is not None and capex_row is not None:
                ocf = self._safe_float(ocf_row[col])
                capex = self._safe_float(capex_row[col])
                if ocf is not None and capex is not None:
                    fcf_val = round(ocf + capex, 2)

            if fcf_val is not None:
                year = str(col.year) if hasattr(col, "year") else str(col)
                fcf_values.append((year, fcf_val))

        entries = []
        for i, (year, fcf) in enumerate(fcf_values):
            growth_pct = None
            if i + 1 < len(fcf_values) and fcf_values[i + 1][1] != 0:
                prev_fcf = fcf_values[i + 1][1]
                growth_pct = round((fcf - prev_fcf) / abs(prev_fcf) * 100, 2)
            entries.append({"year": year, "fcf": fcf, "growth_pct": growth_pct})

        result["fcf_history"] = entries

        if entries and market_cap and market_cap > 0:
            latest_fcf = entries[0]["fcf"]
            result["fcf_yield"] = round(latest_fcf / market_cap * 100, 2)

        revenue_row = self._safe_get_row(income_stmt, ["Total Revenue", "Revenue"])
        if capex_row is not None and revenue_row is not None and not cashflow.columns.empty:
            latest_col = cashflow.columns[0]
            capex = self._safe_float(capex_row[latest_col])
            rev_col = None
            if revenue_row is not None:
                for c in revenue_row.index:
                    if hasattr(c, "year") and hasattr(latest_col, "year") and c.year == latest_col.year:
                        rev_col = c
                        break
                if rev_col is None and len(revenue_row.index) > 0:
                    rev_col = revenue_row.index[0]
            if rev_col is not None:
                revenue = self._safe_float(revenue_row[rev_col])
                if capex is not None and revenue is not None and revenue != 0:
                    result["capex_to_revenue"] = round(abs(capex) / revenue * 100, 2)

        return result

    def _build_valuation(self, info: dict) -> dict:
        return {
            "pe_trailing": self._safe_float(info.get("trailingPE")),
            "pe_forward": self._safe_float(info.get("forwardPE")),
            "ps_ratio": self._safe_float(info.get("priceToSalesTrailing12Months")),
            "pb_ratio": self._safe_float(info.get("priceToBook")),
            "ev_ebitda": self._safe_float(info.get("enterpriseToEbitda")),
            "peg_ratio": self._safe_float(info.get("pegRatio")),
            "market_cap": self._safe_float(info.get("marketCap")),
            "enterprise_value": self._safe_float(info.get("enterpriseValue")),
        }

    def _build_analyst(self, info: dict, current_price: float | None) -> dict:
        target_mean = self._safe_float(info.get("targetMeanPrice"))
        price = self._safe_float(current_price)

        upside_pct = None
        if target_mean is not None and price is not None and price != 0:
            upside_pct = round((target_mean - price) / price * 100, 2)

        return {
            "target_low": self._safe_float(info.get("targetLowPrice")),
            "target_mean": target_mean,
            "target_median": self._safe_float(info.get("targetMedianPrice")),
            "target_high": self._safe_float(info.get("targetHighPrice")),
            "recommendation": info.get("recommendationKey"),
            "num_analysts": info.get("numberOfAnalystOpinions"),
            "current_price": price,
            "upside_pct": upside_pct,
        }

    def _build_quality(
        self,
        info: dict,
        income_stmt: pd.DataFrame,
        balance_sheet: pd.DataFrame,
        margin_history: list[dict],
        revenue_history: list[dict],
    ) -> dict:
        result = {
            "gross_margin_avg": None,
            "gross_margin_trend": None,
            "revenue_cagr": None,
            "roic": None,
            "roe": None,
            "roa": None,
            "insider_pct": None,
            "institution_pct": None,
        }

        gross_margins = [
            m["gross_margin"] for m in margin_history if m.get("gross_margin") is not None
        ]
        if gross_margins:
            result["gross_margin_avg"] = round(sum(gross_margins) / len(gross_margins), 2)
            result["gross_margin_trend"] = self._determine_trend(gross_margins)

        if len(revenue_history) >= 2:
            latest_rev = revenue_history[0]["revenue"]
            oldest_rev = revenue_history[-1]["revenue"]
            num_years = len(revenue_history) - 1
            if oldest_rev > 0 and latest_rev > 0 and num_years > 0:
                cagr = ((latest_rev / oldest_rev) ** (1 / num_years) - 1) * 100
                result["revenue_cagr"] = round(cagr, 2)

        result["roic"] = self._compute_roic(income_stmt, balance_sheet)
        result["roe"] = self._safe_float(info.get("returnOnEquity"))
        if result["roe"] is not None:
            result["roe"] = round(result["roe"] * 100, 2)
        result["roa"] = self._safe_float(info.get("returnOnAssets"))
        if result["roa"] is not None:
            result["roa"] = round(result["roa"] * 100, 2)
        result["insider_pct"] = self._safe_float(info.get("heldPercentInsiders"))
        if result["insider_pct"] is not None:
            result["insider_pct"] = round(result["insider_pct"] * 100, 2)
        result["institution_pct"] = self._safe_float(info.get("heldPercentInstitutions"))
        if result["institution_pct"] is not None:
            result["institution_pct"] = round(result["institution_pct"] * 100, 2)

        return result

    def _determine_trend(self, values: list[float]) -> str:
        if len(values) < 2:
            return "stable"
        mid = len(values) // 2
        first_half_avg = sum(values[:mid]) / mid if mid > 0 else 0
        second_half_avg = sum(values[mid:]) / len(values[mid:]) if len(values[mid:]) > 0 else 0
        # values are ordered newest first, so first_half is recent, second_half is older
        diff = first_half_avg - second_half_avg
        threshold = 2.0
        if diff > threshold:
            return "improving"
        elif diff < -threshold:
            return "declining"
        return "stable"

    def _compute_roic(self, income_stmt: pd.DataFrame, balance_sheet: pd.DataFrame) -> float | None:
        operating_income_row = self._safe_get_row(
            income_stmt, ["Operating Income", "EBIT"]
        )
        if operating_income_row is None:
            return None

        if balance_sheet is None or balance_sheet.empty:
            return None

        latest_col_income = income_stmt.columns[0] if not income_stmt.empty else None
        latest_col_bs = balance_sheet.columns[0]

        if latest_col_income is None:
            return None

        operating_income = self._safe_float(operating_income_row[latest_col_income])
        if operating_income is None:
            return None

        latest_bs = balance_sheet.iloc[:, 0]

        def get_bs_val(names: list[str]) -> float | None:
            for name in names:
                if name in latest_bs.index:
                    return self._safe_float(latest_bs[name])
            return None

        total_equity = get_bs_val(["Stockholders Equity", "Total Equity Gross Minority Interest", "Common Stock Equity"])
        total_debt = get_bs_val(["Total Debt", "Long Term Debt"])
        cash = get_bs_val([
            "Cash And Cash Equivalents",
            "Cash Cash Equivalents And Short Term Investments",
        ])

        if total_equity is None or total_debt is None:
            return None

        cash = cash or 0
        tax_rate = 0.21
        nopat = operating_income * (1 - tax_rate)
        invested_capital = total_equity + total_debt - cash

        if invested_capital == 0:
            return None

        return round(nopat / invested_capital * 100, 2)

    def _build_dividend(self, info: dict) -> dict:
        dividend_yield = self._safe_float(info.get("dividendYield"))
        if dividend_yield is not None:
            dividend_yield = round(dividend_yield * 100, 2)
        return {
            "dividend_yield": dividend_yield,
            "payout_ratio": self._safe_float(info.get("payoutRatio")),
        }


deep_research_analyzer = DeepResearchAnalyzer()
