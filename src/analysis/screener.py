from ..config.ssl_config import configure_ssl_verification
configure_ssl_verification()

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading

import numpy as np
import pandas as pd
from loguru import logger

from ..config.settings import settings
from ..data.database import db
from ..analysis.deep_research import deep_research_analyzer
from ..analysis.metrics import metrics_calculator

FACTOR_WEIGHTS = {
    "value": 0.25, "quality": 0.25, "momentum": 0.20,
    "growth": 0.15, "sentiment": 0.15,
}
MAX_SECTOR_PCT = 0.30
TOP_N = 10
WATCHLIST_N = 10
MIN_MARKET_CAP = 1_000_000_000


def _safe(row: pd.Series | dict, key: str) -> float | None:
    val = row.get(key) if isinstance(row, dict) else row.get(key, None)
    if val is None:
        return None
    try:
        fval = float(val)
        return None if (np.isnan(fval) or np.isinf(fval)) else fval
    except (ValueError, TypeError):
        return None


def _safe_nested(data: dict | None, *keys: str) -> float | None:
    if data is None:
        return None
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    try:
        fval = float(current)
        return None if (np.isnan(fval) or np.isinf(fval)) else fval
    except (ValueError, TypeError):
        return None


def _avg(scores: list[float]) -> float:
    return float(np.mean(scores)) if scores else 50.0


def _fwd_trail_ratio(deep: dict | None) -> float | None:
    if deep is None:
        return None
    pe_fwd = _safe_nested(deep, "valuation", "pe_forward")
    pe_trail = _safe_nested(deep, "valuation", "pe_trailing")
    if pe_fwd is not None and pe_trail is not None and pe_trail > 0:
        return pe_fwd / pe_trail
    return None


class QuantScreener:

    def __init__(self):
        self._cache: dict | None = None
        self._cache_ts: datetime | None = None
        self._lock = threading.Lock()

    # ── Public API ─────────────────────────────────────────────────────

    def run_screen(self) -> dict:
        logger.info("Starting quant screen pipeline")
        df = db.get_all_metrics_df()
        candidates = self._prefilter(df)
        logger.info(f"Pre-filter passed {len(candidates)} candidates from {len(df)} total")

        sector_medians = self._compute_sector_medians(df)
        tickers = candidates["ticker"].tolist()
        db_rows = {row["ticker"]: row for _, row in candidates.iterrows()}
        deep_data_map, hist_data_map = self._fetch_tier2_data(tickers)

        scored: list[dict] = []
        for ticker in tickers:
            result = self._score_stock(
                ticker, db_rows[ticker],
                deep_data_map.get(ticker), hist_data_map.get(ticker),
                sector_medians,
            )
            if result is not None:
                scored.append(result)

        scored.sort(key=lambda s: s["composite_score"], reverse=True)
        top_10 = self._apply_sector_diversification(scored)
        for i, stock in enumerate(top_10, 1):
            stock["rank"] = i
        remaining = [s for s in scored if s not in top_10]
        watchlist = self._build_watchlist(remaining[:WATCHLIST_N])
        for i, stock in enumerate(watchlist, TOP_N + 1):
            stock["rank"] = i
        all_scores = [s["composite_score"] for s in scored]

        backtest_raw = self._compute_backtest_context([s["ticker"] for s in top_10])
        backtest = {
            "picks_6mo_avg": (backtest_raw.get("6mo") or {}).get("avg_pick_return"),
            "picks_12mo_avg": (backtest_raw.get("12mo") or {}).get("avg_pick_return"),
            "spy_6mo": (backtest_raw.get("6mo") or {}).get("spy_return"),
            "spy_12mo": (backtest_raw.get("12mo") or {}).get("spy_return"),
        }

        result = {
            "top_10": top_10,
            "watchlist": watchlist,
            "sector_distribution": self._sector_distribution(top_10),
            "backtest": backtest,
            "metadata": {
                "screened_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                "universe_size": len(df),
                "candidates": len(candidates),
            },
            "score_distribution": all_scores,
        }
        with self._lock:
            self._cache = result
            self._cache_ts = datetime.utcnow()
        logger.success(f"Quant screen complete: {len(top_10)} top picks, {len(watchlist)} watchlist")
        return result

    def get_cached_results(self) -> dict | None:
        with self._lock:
            if self._cache is None:
                return None
            age = (datetime.utcnow() - self._cache_ts).total_seconds() / 60
            return {**self._cache, "cache_age_minutes": round(age, 1)}

    # ── Tier 1: Pre-filter ─────────────────────────────────────────────

    def _prefilter(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        filtered = df[df["market_cap"] >= MIN_MARKET_CAP].copy()
        filtered = filtered[filtered["sector"].notna() & (filtered["sector"] != "")]
        filtered = filtered[filtered["pe_ratio"].notna() | filtered["ebitda_ev"].notna()]

        for col in ["pe_ratio", "pb_ratio", "ebitda_ev"]:
            if col not in filtered.columns:
                continue
            series = filtered[col].dropna()
            if len(series) > 10:
                q01, q99 = series.quantile(0.01), series.quantile(0.99)
                filtered = filtered[filtered[col].isna() | filtered[col].between(q01, q99)]
        return filtered.reset_index(drop=True)

    # ── Tier 2: Deep data fetch ────────────────────────────────────────

    def _fetch_tier2_data(
        self, tickers: list[str]
    ) -> tuple[dict[str, dict | None], dict[str, pd.DataFrame | None]]:
        deep_map: dict[str, dict | None] = {}
        hist_map: dict[str, pd.DataFrame | None] = {}

        def fetch_one(t: str) -> tuple[str, dict | None, pd.DataFrame | None]:
            return t, deep_research_analyzer.analyze(t), metrics_calculator._get_historical_data(t)

        logger.info(f"Fetching tier-2 data for {len(tickers)} tickers")
        with ThreadPoolExecutor(max_workers=settings.MAX_CONCURRENT_WORKERS) as pool:
            for ticker, deep, hist in pool.map(fetch_one, tickers):
                deep_map[ticker] = deep
                hist_map[ticker] = hist
        return deep_map, hist_map

    # ── Sector medians ─────────────────────────────────────────────────

    @staticmethod
    def _compute_sector_medians(df: pd.DataFrame) -> dict[str, dict[str, float]]:
        medians: dict[str, dict[str, float]] = {}
        cols = ["pe_ratio", "pb_ratio", "ebitda_ev", "ps_ratio"]
        for sector, group in df.groupby("sector"):
            if not sector:
                continue
            medians[sector] = {
                c: float(group[c].dropna().median())
                for c in cols if not group[c].dropna().empty
            }
        return medians

    # ── Scoring engine ─────────────────────────────────────────────────

    def _score_stock(
        self, ticker: str, db_row: pd.Series | dict,
        deep_data: dict | None, hist_data: pd.DataFrame | None,
        sector_medians: dict[str, dict[str, float]],
    ) -> dict | None:
        try:
            sector = db_row.get("sector") or (deep_data.get("sector") if deep_data else None)
            if not sector:
                return None
            s_medians = sector_medians.get(sector, {})
            scorers = {
                "value": self._score_value(db_row, deep_data, s_medians),
                "quality": self._score_quality(deep_data),
                "momentum": self._score_momentum(db_row, deep_data, hist_data),
                "growth": self._score_growth(deep_data),
                "sentiment": self._score_sentiment(deep_data),
            }
            factors = {k: v[0] for k, v in scorers.items()}
            sub_factors = {k: v[1] for k, v in scorers.items()}
            composite = sum(factors[f] * FACTOR_WEIGHTS[f] for f in FACTOR_WEIGHTS)
            return {
                "ticker": ticker, "sector": sector,
                "composite_score": round(composite, 2),
                "factor_scores": {k: round(v, 2) for k, v in factors.items()},
                "sub_factor_scores": sub_factors,
                **self._extract_key_metrics(db_row, deep_data),
            }
        except Exception:
            logger.exception(f"Scoring failed for {ticker}")
            return None

    # ── Factor: Value ──────────────────────────────────────────────────

    def _score_value(
        self, db_row: pd.Series | dict, deep: dict | None, s_medians: dict[str, float]
    ) -> tuple[float, dict[str, float]]:
        subs: dict[str, float] = {}
        pe = _safe(db_row, "pe_ratio")
        median_pe = s_medians.get("pe_ratio")
        if pe is not None and median_pe and median_pe > 0:
            subs["pe_vs_sector"] = round(float(np.clip(100 - (pe / median_pe - 0.5) * 100, 0, 100)), 1)
        if deep:
            fcf_yield = _safe_nested(deep, "fcf", "fcf_yield")
            if fcf_yield is not None:
                subs["fcf_yield"] = round(float(np.clip(fcf_yield * 10, 0, 100)), 1)
        ev_ebitda = _safe(db_row, "ebitda_ev")
        if ev_ebitda is not None and ev_ebitda > 0:
            subs["ev_ebitda"] = round(float(np.clip(100 - ev_ebitda * 3, 0, 100)), 1)
        pb = _safe(db_row, "pb_ratio")
        if pb is not None and pb > 0:
            subs["pb_ratio"] = round(float(np.clip(100 - pb * 10, 0, 100)), 1)
        ratio = _fwd_trail_ratio(deep)
        if ratio is not None:
            subs["fwd_pe_discount"] = round(float(np.clip(50 + (1.0 - ratio) * 200, 0, 100)), 1)
        return _avg(list(subs.values())), subs

    # ── Factor: Quality ────────────────────────────────────────────────

    def _score_quality(self, deep: dict | None) -> tuple[float, dict[str, float]]:
        if deep is None:
            return 50.0, {}
        subs: dict[str, float] = {}
        roe = _safe_nested(deep, "quality", "roe")
        if roe is not None:
            subs["roe"] = round(float(np.clip(roe * 2.5, 0, 100)), 1)
        margin_avg = _safe_nested(deep, "quality", "gross_margin_avg")
        if margin_avg is not None:
            trend = deep.get("quality", {}).get("gross_margin_trend")
            bonus = 20 if trend == "improving" else (10 if trend == "stable" else 0)
            subs["margin_stability"] = round(float(min(np.clip(margin_avg * 1.5, 0, 80) + bonus, 100)), 1)
        dte = _safe_nested(deep, "balance_sheet", "debt_to_equity")
        if dte is not None:
            subs["debt_to_equity"] = round(float(np.clip(100 - dte * 20, 0, 100)), 1)
        cr = _safe_nested(deep, "balance_sheet", "current_ratio")
        if cr is not None:
            if 1.0 <= cr <= 3.0:
                subs["current_ratio"] = round(80 + (1.0 - abs(cr - 2.0)) * 20, 1)
            else:
                subs["current_ratio"] = round(float(60 if cr > 3.0 else np.clip(cr * 40, 0, 100)), 1)
        roic = _safe_nested(deep, "quality", "roic")
        if roic is not None:
            subs["roic"] = round(float(np.clip(roic * 3, 0, 100)), 1)
        net_cash = _safe_nested(deep, "balance_sheet", "net_cash")
        if net_cash is not None:
            subs["net_cash"] = round(85.0 if net_cash > 0 else 35.0, 1)
        return _avg(list(subs.values())), subs

    # ── Factor: Momentum ───────────────────────────────────────────────

    def _score_momentum(
        self, db_row: pd.Series | dict, deep: dict | None,
        hist_data: pd.DataFrame | None,
    ) -> tuple[float, dict[str, float]]:
        subs: dict[str, float] = {}
        if hist_data is not None and not hist_data.empty and "close" in hist_data.columns:
            close = hist_data["close"]
            if len(close) >= 200:
                ma200 = float(close.rolling(200).mean().iloc[-1])
                if ma200 > 0:
                    pct = (float(close.iloc[-1]) - ma200) / ma200 * 100
                    subs["price_vs_200ma"] = round(float(np.clip(50 + pct * 2, 0, 100)), 1)
        pct_ma100 = _safe(db_row, "pct_above_ma_100")
        if pct_ma100 is not None:
            subs["relative_strength"] = round(float(np.clip(50 + pct_ma100 * 2, 0, 100)), 1)
        rsi = _safe(db_row, "rsi_14")
        if rsi is not None:
            if 50 <= rsi <= 65:
                subs["rsi_zone"] = 90.0
            elif 40 <= rsi < 50 or 65 < rsi <= 70:
                subs["rsi_zone"] = 70.0
            elif 30 <= rsi < 40 or 70 < rsi <= 80:
                subs["rsi_zone"] = 45.0
            else:
                subs["rsi_zone"] = 15.0
        macd_hist = _safe(db_row, "macd_histogram")
        if macd_hist is not None:
            subs["macd_histogram"] = round(float(np.clip(50 + macd_hist * 500, 0, 100)), 1)
        ratio = _fwd_trail_ratio(deep)
        if ratio is not None:
            subs["earnings_revision"] = round(float(np.clip((1.0 - ratio) * 200 + 50, 0, 100)), 1)
        return _avg(list(subs.values())), subs

    # ── Factor: Growth ─────────────────────────────────────────────────

    def _score_growth(self, deep: dict | None) -> tuple[float, dict[str, float]]:
        if deep is None:
            return 50.0, {}
        subs: dict[str, float] = {}
        rev_hist = deep.get("revenue_history") or []
        if rev_hist and rev_hist[0].get("growth_pct") is not None:
            subs["revenue_yoy"] = round(float(np.clip(50 + rev_hist[0]["growth_pct"] * 2, 0, 100)), 1)
        ratio = _fwd_trail_ratio(deep)
        if ratio is not None:
            subs["eps_acceleration"] = round(float(np.clip((1.0 - ratio) * 200 + 50, 0, 100)), 1)
        margin_hist = deep.get("margin_history") or []
        if len(margin_hist) >= 2:
            recent = margin_hist[0].get("operating_margin")
            older = margin_hist[-1].get("operating_margin")
            if recent is not None and older is not None:
                subs["margin_expansion"] = round(float(np.clip(50 + (recent - older) * 3, 0, 100)), 1)
        cagr = _safe_nested(deep, "quality", "revenue_cagr")
        if cagr is not None:
            subs["revenue_cagr"] = round(float(np.clip(cagr * 3, 0, 100)), 1)
        return _avg(list(subs.values())), subs

    # ── Factor: Sentiment ──────────────────────────────────────────────

    def _score_sentiment(self, deep: dict | None) -> tuple[float, dict[str, float]]:
        if deep is None:
            return 50.0, {}
        subs: dict[str, float] = {}
        insider = _safe_nested(deep, "quality", "insider_pct")
        if insider is not None:
            if 5 <= insider <= 25:
                subs["insider_ownership"] = 85.0
            elif insider < 5:
                subs["insider_ownership"] = round(40 + insider * 8, 1)
            else:
                subs["insider_ownership"] = round(max(85 - (insider - 25) * 2, 30), 1)
        inst = _safe_nested(deep, "quality", "institution_pct")
        if inst is not None:
            if 40 <= inst <= 80:
                subs["institutional_ownership"] = 85.0
            elif inst < 40:
                subs["institutional_ownership"] = round(float(np.clip(inst * 2, 0, 100)), 1)
            else:
                subs["institutional_ownership"] = round(max(85 - (inst - 80) * 3, 30), 1)
        upside = _safe_nested(deep, "analyst", "upside_pct")
        if upside is not None:
            subs["analyst_upside"] = round(float(np.clip(50 + upside * 1.5, 0, 100)), 1)
        rec = deep.get("analyst", {}).get("recommendation")
        if rec:
            rec_map = {"strongbuy": 95, "buy": 80, "hold": 50, "sell": 20, "strongsell": 5}
            normalized = rec.lower().replace("_", "").replace(" ", "")
            if normalized in rec_map:
                subs["recommendation"] = float(rec_map[normalized])
        return _avg(list(subs.values())), subs

    # ── Sector diversification ─────────────────────────────────────────

    @staticmethod
    def _apply_sector_diversification(scored: list[dict]) -> list[dict]:
        selected: list[dict] = []
        sector_counts: dict[str, int] = {}
        max_per_sector = int(TOP_N * MAX_SECTOR_PCT)
        for stock in scored:
            if len(selected) >= TOP_N:
                break
            sector = stock.get("sector", "Unknown")
            if sector_counts.get(sector, 0) >= max_per_sector:
                continue
            selected.append(stock)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
        return selected

    # ── Watchlist ──────────────────────────────────────────────────────

    @staticmethod
    def _build_watchlist(stocks: list[dict]) -> list[dict]:
        result: list[dict] = []
        for stock in stocks:
            factors = stock.get("factor_scores", {})
            weakest = min(factors, key=factors.get) if factors else "N/A"
            result.append({**stock, "weakest_factor": weakest})
        return result

    # ── Backtest context ───────────────────────────────────────────────

    def _compute_backtest_context(self, tickers: list[str]) -> dict:
        spy_hist = metrics_calculator._get_historical_data("SPY")
        if spy_hist is None or spy_hist.empty:
            return {"6mo": {}, "12mo": {}}
        spy_close = spy_hist["close"]
        result: dict[str, dict] = {}
        for label, days in [("6mo", 126), ("12mo", 252)]:
            empty = {"spy_return": None, "avg_pick_return": None, "excess_return": None}
            if len(spy_close) < days:
                result[label] = empty
                continue
            spy_ret = (float(spy_close.iloc[-1]) / float(spy_close.iloc[-days]) - 1) * 100
            pick_rets: list[float] = []
            for ticker in tickers:
                hist = metrics_calculator._get_historical_data(ticker)
                if hist is None or hist.empty or len(hist) < days:
                    continue
                pick_rets.append(
                    (float(hist["close"].iloc[-1]) / float(hist["close"].iloc[-days]) - 1) * 100
                )
            avg = float(np.mean(pick_rets)) if pick_rets else None
            result[label] = {
                "spy_return": round(spy_ret, 2),
                "avg_pick_return": round(avg, 2) if avg is not None else None,
                "excess_return": round(avg - spy_ret, 2) if avg is not None else None,
            }
        return result

    # ── Helpers ─────────────────────────────────────────────────────────

    @staticmethod
    def _extract_key_metrics(db_row: pd.Series | dict, deep: dict | None) -> dict:
        km: dict[str, float | str | None] = {
            "market_cap": _safe(db_row, "market_cap"),
            "pe_ratio": _safe(db_row, "pe_ratio"),
            "rsi_14": _safe(db_row, "rsi_14"),
            "pct_above_ma_100": _safe(db_row, "pct_above_ma_100"),
        }
        if deep:
            km["roe"] = _safe_nested(deep, "quality", "roe")
            km["revenue_cagr"] = _safe_nested(deep, "quality", "revenue_cagr")
            km["debt_to_equity"] = _safe_nested(deep, "balance_sheet", "debt_to_equity")
            km["upside_pct"] = _safe_nested(deep, "analyst", "upside_pct")
        return km

    @staticmethod
    def _sector_distribution(stocks: list[dict]) -> dict[str, int]:
        dist: dict[str, int] = {}
        for s in stocks:
            sector = s.get("sector", "Unknown")
            dist[sector] = dist.get(sector, 0) + 1
        return dist


quant_screener = QuantScreener()
