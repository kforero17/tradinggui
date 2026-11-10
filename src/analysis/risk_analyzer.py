from ..config.ssl_config import configure_ssl_verification
configure_ssl_verification()

from typing import Optional
from datetime import datetime
import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
import threading
import time

from ..data.database import db
from .metrics import metrics_calculator, RateLimiter


class PortfolioRiskAnalyzer:

    def __init__(self):
        self.rate_limiter = RateLimiter(calls_per_second=0.3)
        self._long_history_cache: dict[str, pd.DataFrame] = {}
        self._cache_lock = threading.Lock()

    def _get_long_history(self, ticker: str, years: int = 10) -> Optional[pd.DataFrame]:
        with self._cache_lock:
            if ticker in self._long_history_cache:
                return self._long_history_cache[ticker]
        try:
            self.rate_limiter.wait()
            df = yf.Ticker(ticker).history(period=f"{years}y")
            if df is None or df.empty:
                logger.warning(f"No long history data for {ticker}")
                return None
            df.columns = df.columns.str.lower()
            cols = [c for c in ['open', 'high', 'low', 'close', 'volume'] if c in df.columns]
            if not cols:
                logger.warning(f"No required columns in long history for {ticker}")
                return None
            df = df[cols]
            with self._cache_lock:
                self._long_history_cache[ticker] = df
            return df
        except Exception:
            logger.exception(f"Failed to fetch long history for {ticker}")
            return None

    def _build_returns_df(self, tickers: list[str], data_source: dict[str, Optional[pd.DataFrame]] = None) -> pd.DataFrame:
        frames: dict[str, pd.Series] = {}
        for t in tickers:
            if data_source is not None:
                hist = data_source.get(t)
            else:
                hist = metrics_calculator._get_historical_data(t)
            if hist is not None and not hist.empty and 'close' in hist.columns:
                frames[t] = hist['close'].pct_change().dropna()
        return pd.DataFrame(frames).dropna() if frames else pd.DataFrame()

    def _get_portfolio_context(self) -> dict:
        positions = db.get_portfolio_with_shares()
        all_metrics_df = db.get_all_metrics_df()

        tickers, position_values = [], {}
        sectors: dict[str, str] = {}
        industries: dict[str, str] = {}

        for pos in positions:
            ticker, shares = pos['ticker'], pos.get('shares', 0) or 0
            if shares <= 0:
                continue
            hist = metrics_calculator._get_historical_data(ticker)
            if hist is None or hist.empty:
                continue
            position_values[ticker] = float(hist['close'].iloc[-1]) * shares
            tickers.append(ticker)
            row = all_metrics_df[all_metrics_df['ticker'] == ticker]
            if not row.empty:
                sectors[ticker] = str(row.iloc[0].get('sector', 'Unknown') or 'Unknown')
                industries[ticker] = str(row.iloc[0].get('industry', 'Unknown') or 'Unknown')
            else:
                sectors[ticker] = 'Unknown'
                industries[ticker] = 'Unknown'

        total_value = sum(position_values.values())
        weights = {t: v / total_value for t, v in position_values.items()} if total_value > 0 else {t: 0.0 for t in tickers}

        all_fetch = list(set(tickers + ['SPY', '^TNX']))
        with ThreadPoolExecutor(max_workers=3) as executor:
            long_hist = dict(zip(all_fetch, executor.map(self._get_long_history, all_fetch)))

        spy_long = long_hist.get('SPY')
        tnx_long = long_hist.get('^TNX')
        long_close = {t: long_hist[t]['close'] for t in tickers if long_hist.get(t) is not None and 'close' in long_hist[t].columns}

        return {
            'positions': positions, 'tickers': tickers, 'weights': weights,
            'sectors': sectors, 'industries': industries,
            'short_returns': self._build_returns_df(tickers),
            'long_returns': self._build_returns_df(tickers, long_hist),
            'spy_returns': spy_long['close'].pct_change().dropna() if spy_long is not None else pd.Series(dtype=float),
            'tnx_returns': tnx_long['close'].pct_change().dropna() if tnx_long is not None else pd.Series(dtype=float),
            'long_close': long_close, 'total_value': round(total_value, 2),
        }

    # ── Core Risk Methods ──────────────────────────────────────────────

    def _compute_volatility_profile(self, ctx: dict) -> dict:
        short_returns, weights, tickers = ctx['short_returns'], ctx['weights'], ctx['tickers']
        spy_hist = metrics_calculator._get_historical_data('SPY')
        spy_vol = round(float(spy_hist['close'].pct_change().dropna().std() * np.sqrt(252)), 4) if spy_hist is not None else 0.0

        per_ticker: dict[str, dict] = {}
        for t in tickers:
            if t in short_returns.columns:
                vol = round(float(short_returns[t].std() * np.sqrt(252)), 4)
                per_ticker[t] = {'vol': vol, 'vol_ratio_vs_spy': round(vol / spy_vol, 4) if spy_vol > 0 else 0.0}
            else:
                per_ticker[t] = {'vol': 0.0, 'vol_ratio_vs_spy': 0.0}

        available = [t for t in tickers if t in short_returns.columns]
        portfolio_vol, diversification_ratio = 0.0, 1.0
        if available:
            w = np.array([weights.get(t, 0.0) for t in available])
            portfolio_var = float(w @ short_returns[available].cov().values @ w)
            portfolio_vol = round(float(np.sqrt(portfolio_var) * np.sqrt(252)), 4)
            weighted_vol_sum = sum(weights.get(t, 0.0) * per_ticker[t]['vol'] for t in available)
            diversification_ratio = round(weighted_vol_sum / portfolio_vol, 4) if portfolio_vol > 0 else 1.0

        return {'per_ticker': per_ticker, 'portfolio_vol': portfolio_vol, 'spy_vol': spy_vol, 'diversification_ratio': diversification_ratio}

    def _compute_beta_analysis(self, ctx: dict) -> dict:
        short_returns, weights, tickers = ctx['short_returns'], ctx['weights'], ctx['tickers']
        spy_hist = metrics_calculator._get_historical_data('SPY')
        if spy_hist is None:
            return {'per_ticker': {}, 'portfolio_beta': 0.0, 'portfolio_down_beta': 0.0}

        spy_daily = spy_hist['close'].pct_change().dropna()
        per_ticker: dict[str, dict] = {}
        zero_beta = {'up_beta': 0.0, 'down_beta': 0.0, 'asymmetry': 0.0}

        for t in tickers:
            if t not in short_returns.columns:
                per_ticker[t] = zero_beta.copy()
                continue
            aligned = pd.DataFrame({'ticker': short_returns[t], 'spy': spy_daily}).dropna()
            if len(aligned) < 30:
                per_ticker[t] = zero_beta.copy()
                continue
            up_beta = self._calc_beta(aligned[aligned['spy'] > 0], 'ticker', 'spy')
            down_beta = self._calc_beta(aligned[aligned['spy'] < 0], 'ticker', 'spy')
            per_ticker[t] = {'up_beta': round(up_beta, 4), 'down_beta': round(down_beta, 4), 'asymmetry': round(down_beta - up_beta, 4)}

        return {
            'per_ticker': per_ticker,
            'portfolio_beta': round(sum(weights.get(t, 0.0) * per_ticker[t]['up_beta'] for t in tickers), 4),
            'portfolio_down_beta': round(sum(weights.get(t, 0.0) * per_ticker[t]['down_beta'] for t in tickers), 4),
        }

    @staticmethod
    def _calc_beta(df: pd.DataFrame, ticker_col: str, market_col: str) -> float:
        if len(df) < 10:
            return 0.0
        market_var = df[market_col].var()
        return float(df[ticker_col].cov(df[market_col]) / market_var) if market_var != 0 else 0.0

    def _compute_drawdown_history(self, ctx: dict) -> dict:
        tickers, weights, long_close = ctx['tickers'], ctx['weights'], ctx['long_close']

        per_ticker: dict[str, dict] = {}
        for t in tickers:
            if t not in long_close:
                per_ticker[t] = {'max_dd': 0.0, 'current_dd': 0.0}
                continue
            dd = ((long_close[t] - long_close[t].cummax()) / long_close[t].cummax()) * 100
            per_ticker[t] = {'max_dd': round(float(dd.min()), 2), 'current_dd': round(float(dd.iloc[-1]), 2)}

        # Build normalized portfolio price series
        frames = {t: (long_close[t] / long_close[t].iloc[0]) * weights[t]
                  for t in tickers if t in long_close and weights.get(t, 0) > 0}
        if not frames:
            return {'portfolio_drawdown_series': {'dates': [], 'values': []}, 'worst_drawdowns': [],
                    'per_ticker': per_ticker, 'portfolio_max_dd': 0.0, 'portfolio_current_dd': 0.0}

        portfolio = pd.DataFrame(frames).dropna().sum(axis=1)
        p_dd = ((portfolio - portfolio.cummax()) / portfolio.cummax()) * 100

        return {
            'portfolio_drawdown_series': {
                'dates': [d.strftime('%Y-%m-%d') for d in p_dd.index],
                'values': [round(float(v), 2) for v in p_dd.values],
            },
            'worst_drawdowns': self._find_worst_drawdowns(p_dd),
            'per_ticker': per_ticker,
            'portfolio_max_dd': round(float(p_dd.min()), 2),
            'portfolio_current_dd': round(float(p_dd.iloc[-1]), 2),
        }

    @staticmethod
    def _find_worst_drawdowns(dd_series: pd.Series, threshold: float = -5.0, top_n: int = 5) -> list[dict]:
        if dd_series.empty:
            return []
        drawdowns: list[dict] = []
        in_dd, start, worst = False, None, 0.0

        for i in range(len(dd_series)):
            val = dd_series.iloc[i]
            if val < threshold and not in_dd:
                in_dd, start, worst = True, dd_series.index[i], val
            elif in_dd:
                worst = min(worst, val)
                if val >= -0.5 or i == len(dd_series) - 1:
                    end = dd_series.index[i]
                    drawdowns.append({
                        'depth': round(float(worst), 2), 'start': start.strftime('%Y-%m-%d'),
                        'end': end.strftime('%Y-%m-%d'),
                        'recovery': end.strftime('%Y-%m-%d') if val >= -0.5 else 'Ongoing',
                        'duration_days': (end - start).days,
                    })
                    in_dd, worst = False, 0.0

        drawdowns.sort(key=lambda x: x['depth'])
        return drawdowns[:top_n]

    def _compute_correlation_analysis(self, ctx: dict) -> dict:
        short_returns, tickers = ctx['short_returns'], ctx['tickers']
        available = [t for t in tickers if t in short_returns.columns]
        if len(available) < 2:
            return {'matrix': {}, 'avg_correlation': 0.0, 'high_pairs': [], 'tickers': available}

        corr = short_returns[available].corr()
        matrix = {t: {t2: round(float(corr.loc[t, t2]), 4) for t2 in available} for t in available}

        upper, high_pairs = [], []
        for i in range(len(available)):
            for j in range(i + 1, len(available)):
                val = float(corr.iloc[i, j])
                upper.append(val)
                if abs(val) > 0.7:
                    high_pairs.append({'pair': f"{available[i]}/{available[j]}", 'corr': round(val, 4)})

        high_pairs.sort(key=lambda x: abs(x['corr']), reverse=True)
        return {'matrix': matrix, 'avg_correlation': round(float(np.mean(upper)), 4) if upper else 0.0,
                'high_pairs': high_pairs, 'tickers': available}

    def _compute_sector_concentration(self, ctx: dict) -> dict:
        weights, sectors, industries = ctx['weights'], ctx['sectors'], ctx['industries']
        sector_w: dict[str, float] = {}
        industry_w: dict[str, float] = {}
        for t, w in weights.items():
            sector_w[sectors.get(t, 'Unknown')] = sector_w.get(sectors.get(t, 'Unknown'), 0.0) + w
            industry_w[industries.get(t, 'Unknown')] = industry_w.get(industries.get(t, 'Unknown'), 0.0) + w

        sector_pct = {s: round(w * 100, 2) for s, w in sector_w.items()}
        industry_pct = {i: round(w * 100, 2) for i, w in industry_w.items()}
        hhi = round(sum(v ** 2 for v in sector_pct.values()), 2)
        concentration = 'Low' if hhi < 1500 else ('Moderate' if hhi < 2500 else 'High')

        return {
            'sector_weights': sector_pct, 'industry_weights': industry_pct, 'hhi': hhi,
            'concentration_level': concentration,
            'overweight_sectors': [s for s, w in sector_pct.items() if w > 30],
            'single_stock_risk': [{'ticker': t, 'weight': round(w * 100, 2)} for t, w in weights.items() if w > 0.20],
        }

    def _compute_interest_rate_sensitivity(self, ctx: dict) -> dict:
        short_returns, tnx_returns = ctx['short_returns'], ctx['tnx_returns']
        tickers, weights = ctx['tickers'], ctx['weights']
        if tnx_returns.empty:
            return {'per_ticker': {}, 'portfolio_tnx_corr': 0.0}

        zero_rate = {'tnx_corr': 0.0, 'sensitivity': 'Low', 'impact_per_100bps': 0.0}
        per_ticker: dict[str, dict] = {}
        for t in tickers:
            if t not in short_returns.columns:
                per_ticker[t] = zero_rate.copy()
                continue
            aligned = pd.DataFrame({'ticker': short_returns[t], 'tnx': tnx_returns}).dropna()
            if len(aligned) < 30:
                per_ticker[t] = zero_rate.copy()
                continue
            corr_val = float(aligned['ticker'].corr(aligned['tnx']))
            ticker_vol = float(aligned['ticker'].std() * np.sqrt(252))
            tnx_vol = float(aligned['tnx'].std() * np.sqrt(252))
            vol_ratio = ticker_vol / tnx_vol if tnx_vol > 0 else 0.0
            label = ('High Negative' if corr_val < -0.3 else 'Moderate Negative' if corr_val < -0.15
                     else 'Low' if abs(corr_val) < 0.15 else 'High Positive' if corr_val > 0.3 else 'Moderate Positive')
            per_ticker[t] = {'tnx_corr': round(corr_val, 4), 'sensitivity': label, 'impact_per_100bps': round(corr_val * vol_ratio, 4)}

        return {'per_ticker': per_ticker, 'portfolio_tnx_corr': round(sum(weights.get(t, 0.0) * per_ticker[t]['tnx_corr'] for t in tickers), 4)}

    def _compute_liquidity_risk(self, ctx: dict) -> dict:
        tickers, weights, total_value = ctx['tickers'], ctx['weights'], ctx['total_value']
        empty = {'avg_volume_30d': 0.0, 'avg_dollar_volume': 0.0, 'pct_of_adv': 0.0, 'days_to_liquidate': 999.0, 'liquidity_tier': 'Low'}
        per_ticker: dict[str, dict] = {}

        for t in tickers:
            try:
                hist = metrics_calculator._get_historical_data(t)
                if hist is None or hist.empty or 'volume' not in hist.columns:
                    per_ticker[t] = empty.copy()
                    continue
                avg_vol = float(hist.tail(30)['volume'].mean())
                last_price = float(hist['close'].iloc[-1])
                dollar_vol = avg_vol * last_price
                pos_val = weights.get(t, 0.0) * total_value
                tier = 'High' if dollar_vol > 50_000_000 else ('Medium' if dollar_vol > 10_000_000 else 'Low')
                per_ticker[t] = {
                    'avg_volume_30d': round(avg_vol, 0), 'avg_dollar_volume': round(dollar_vol, 0),
                    'pct_of_adv': round(pos_val / dollar_vol * 100, 4) if dollar_vol > 0 else 0.0,
                    'days_to_liquidate': round(pos_val / (dollar_vol * 0.25), 2) if dollar_vol > 0 else 999.0,
                    'liquidity_tier': tier,
                }
            except Exception:
                logger.exception(f"Liquidity analysis failed for {t}")
                per_ticker[t] = empty.copy()
        return {'per_ticker': per_ticker}

    def _compute_risk_score(self, core_data: dict) -> dict:
        vol = core_data['volatility']['portfolio_vol']
        d_beta = core_data['beta']['portfolio_down_beta']
        max_dd = core_data['drawdown']['portfolio_max_dd']
        hhi = core_data['sector']['hhi']
        avg_corr = core_data['correlation']['avg_correlation']
        liq = core_data['liquidity']['per_ticker']
        tnx = core_data['rate_sensitivity']['portfolio_tnx_corr']
        ss = core_data['sector']['single_stock_risk']

        # Each component scored 0-100 then weighted
        s_vol = min(100, vol / 0.40 * 100)
        s_beta = min(100, d_beta / 2.0 * 100)
        s_dd = min(100, abs(max_dd) / 60.0 * 100)
        s_hhi = min(100, hhi / 5000.0 * 100)
        s_corr = min(100, avg_corr / 1.0 * 100)
        s_liq = min(100, sum(1 for v in liq.values() if v.get('liquidity_tier') == 'Low') / max(len(liq), 1) * 100) if liq else 0.0
        s_rate = min(100, abs(tnx) / 0.5 * 100)
        s_ss = min(100, max((i['weight'] for i in ss), default=0) / 50.0 * 100)

        components = {
            'volatility': round(s_vol, 2), 'down_beta': round(s_beta, 2), 'max_drawdown': round(s_dd, 2),
            'sector_hhi': round(s_hhi, 2), 'avg_correlation': round(s_corr, 2), 'liquidity': round(s_liq, 2),
            'rate_sensitivity': round(s_rate, 2), 'single_stock': round(s_ss, 2),
        }
        score = int(min(100, max(1, round(
            s_vol * 0.20 + s_beta * 0.15 + s_dd * 0.15 + s_hhi * 0.15 + s_corr * 0.10 + s_liq * 0.10 + s_rate * 0.10 + s_ss * 0.05
        ))))
        label = 'Low Risk' if score <= 25 else ('Moderate' if score <= 50 else ('Elevated' if score <= 75 else 'High Risk'))
        return {'score': score, 'components': components, 'label': label}

    def _build_summary_table(self, ctx: dict, core_data: dict) -> list[dict]:
        tickers, weights, short_returns = ctx['tickers'], ctx['weights'], ctx['short_returns']
        p_vol = core_data['volatility']['portfolio_vol']
        available = [t for t in tickers if t in short_returns.columns]

        portfolio_returns = None
        if available and p_vol > 0:
            portfolio_returns = sum(short_returns[t] * weights.get(t, 0.0) for t in available)

        rows: list[dict] = []
        for t in tickers:
            w = weights.get(t, 0.0)
            t_vol = core_data['volatility']['per_ticker'].get(t, {}).get('vol', 0.0)
            risk_contrib = 0.0
            if portfolio_returns is not None and t in short_returns.columns and p_vol > 0:
                risk_contrib = (w * t_vol * float(short_returns[t].corr(portfolio_returns))) / p_vol * 100
            rows.append({
                'ticker': t, 'weight': round(w * 100, 2), 'vol': round(t_vol * 100, 2),
                'up_beta': round(core_data['beta']['per_ticker'].get(t, {}).get('up_beta', 0.0), 4),
                'down_beta': round(core_data['beta']['per_ticker'].get(t, {}).get('down_beta', 0.0), 4),
                'max_dd': round(core_data['drawdown']['per_ticker'].get(t, {}).get('max_dd', 0.0), 2),
                'tnx_corr': round(core_data['rate_sensitivity']['per_ticker'].get(t, {}).get('tnx_corr', 0.0), 4),
                'liquidity_tier': core_data['liquidity']['per_ticker'].get(t, {}).get('liquidity_tier', 'Low'),
                'risk_contrib_pct': round(risk_contrib, 2),
            })
        return rows

    # ── Entry Point ────────────────────────────────────────────────────

    def compute_core_risk(self) -> dict:
        ctx = self._get_portfolio_context()
        if not ctx['tickers']:
            return {'error': 'No portfolio data'}

        volatility = self._compute_volatility_profile(ctx)
        beta = self._compute_beta_analysis(ctx)
        drawdown = self._compute_drawdown_history(ctx)
        correlation = self._compute_correlation_analysis(ctx)
        sector = self._compute_sector_concentration(ctx)
        rate_sensitivity = self._compute_interest_rate_sensitivity(ctx)
        liquidity = self._compute_liquidity_risk(ctx)

        core_data = {
            'volatility': volatility, 'beta': beta, 'drawdown': drawdown,
            'correlation': correlation, 'sector': sector,
            'rate_sensitivity': rate_sensitivity, 'liquidity': liquidity,
        }

        core_data['risk_score'] = self._compute_risk_score(core_data)
        core_data['summary_table'] = self._build_summary_table(ctx, core_data)
        core_data['total_value'] = ctx['total_value']
        return core_data

    # ── Async Methods ──────────────────────────────────────────────────

    def compute_implied_volatility(self) -> dict:
        ctx = self._get_portfolio_context()
        tickers, short_returns = ctx['tickers'], ctx['short_returns']
        per_ticker: dict[str, dict] = {}
        iv_hv_ratios: list[float] = []
        empty_iv = {'atm_call_iv': None, 'atm_put_iv': None, 'iv_hv_ratio': None, 'put_call_skew': None}

        for t in tickers:
            try:
                self.rate_limiter.wait()
                stock = yf.Ticker(t)
                if not stock.options:
                    per_ticker[t] = empty_iv.copy()
                    continue
                chain = stock.option_chain(stock.options[0])
                price = float(ctx['long_close'][t].iloc[-1]) if t in ctx['long_close'] else None
                if price is None:
                    per_ticker[t] = empty_iv.copy()
                    continue

                call_iv = self._find_atm_iv(chain.calls, price)
                put_iv = self._find_atm_iv(chain.puts, price)
                realized = float(short_returns[t].std() * np.sqrt(252)) if t in short_returns.columns else None
                ratio = round(call_iv / realized, 4) if call_iv and realized and realized > 0 else None
                if ratio:
                    iv_hv_ratios.append(ratio)
                skew = round(put_iv - call_iv, 4) if put_iv is not None and call_iv is not None else None
                per_ticker[t] = {
                    'atm_call_iv': round(call_iv, 4) if call_iv else None,
                    'atm_put_iv': round(put_iv, 4) if put_iv else None,
                    'iv_hv_ratio': ratio, 'put_call_skew': skew,
                }
            except Exception:
                logger.exception(f"IV analysis failed for {t}")
                per_ticker[t] = empty_iv.copy()

        return {'per_ticker': per_ticker, 'avg_iv_hv_ratio': round(float(np.mean(iv_hv_ratios)), 4) if iv_hv_ratios else 0.0}

    @staticmethod
    def _find_atm_iv(options_df: pd.DataFrame, current_price: float) -> Optional[float]:
        if options_df is None or options_df.empty:
            return None
        if 'strike' not in options_df.columns or 'impliedVolatility' not in options_df.columns:
            return None
        idx = (options_df['strike'] - current_price).abs().idxmin()
        iv = options_df.loc[idx, 'impliedVolatility']
        return None if pd.isna(iv) else float(iv)

    def compute_stress_tests(self) -> dict:
        ctx = self._get_portfolio_context()
        tickers, weights, total_value = ctx['tickers'], ctx['weights'], ctx['total_value']
        beta_data = self._compute_beta_analysis(ctx)
        rate_data = self._compute_interest_rate_sensitivity(ctx)
        vol_data = self._compute_volatility_profile(ctx)

        def rate_impact_for_ticker(t: str) -> float:
            tnx_corr = rate_data['per_ticker'].get(t, {}).get('tnx_corr', 0.0)
            t_vol = vol_data['per_ticker'].get(t, {}).get('vol', 0.0)
            return tnx_corr * t_vol * 2 * 100

        def build_scenario(name: str, desc: str, mkt_decline: float, include_equity: bool, include_rates: bool) -> dict:
            per_t, total_impact = {}, 0.0
            for t in tickers:
                pct = 0.0
                if include_equity:
                    pct += beta_data['per_ticker'].get(t, {}).get('down_beta', 1.0) * mkt_decline
                if include_rates:
                    pct += rate_impact_for_ticker(t)
                pct = round(pct, 2)
                dollars = round(pct / 100 * weights.get(t, 0.0) * total_value, 2)
                per_t[t] = {'impact_pct': pct, 'impact_dollars': dollars}
                total_impact += dollars
            return {'name': name, 'description': desc, 'market_decline': mkt_decline if include_equity else 0.0,
                    'portfolio_impact_pct': round(total_impact / total_value * 100, 2) if total_value > 0 else 0.0,
                    'portfolio_impact_dollars': round(total_impact, 2), 'per_ticker': per_t}

        return {'scenarios': [
            build_scenario('GFC 2008', '2008 Financial Crisis equivalent', -56.8, True, False),
            build_scenario('COVID 2020', 'COVID-19 March 2020 crash', -33.9, True, False),
            build_scenario('Rate Shock (+200bps)', 'Sudden 200 basis point increase in interest rates', 0.0, False, True),
            build_scenario('Stagflation', 'Market decline of 20% combined with +200bps rate shock', -20.0, True, True),
        ]}

    def compute_earnings_risk(self) -> dict:
        ctx = self._get_portfolio_context()
        tickers, weights, total_value = ctx['tickers'], ctx['weights'], ctx['total_value']
        per_ticker: dict[str, dict] = {}
        catalysts: list[dict] = []
        now = datetime.utcnow()
        empty_earnings = {'next_earnings': None, 'avg_earnings_move': None, 'max_earnings_move': None, 'dollar_at_risk': None}

        for t in tickers:
            try:
                self.rate_limiter.wait()
                stock = yf.Ticker(t)
                next_date = self._extract_next_earnings_date(stock)
                avg_move, max_move = self._compute_earnings_moves(stock, t, ctx)
                pos_val = weights.get(t, 0.0) * total_value
                dar = round(avg_move / 100 * pos_val, 2) if avg_move else None

                per_ticker[t] = {
                    'next_earnings': next_date.strftime('%Y-%m-%d') if next_date else None,
                    'avg_earnings_move': round(avg_move, 2) if avg_move else None,
                    'max_earnings_move': round(max_move, 2) if max_move else None,
                    'dollar_at_risk': dar,
                }
                if next_date:
                    days = (next_date - now).days
                    if 0 <= days <= 60:
                        catalysts.append({'ticker': t, 'date': next_date.strftime('%Y-%m-%d'), 'days_until': days, 'dollar_at_risk': dar or 0.0})
            except Exception:
                logger.exception(f"Earnings risk analysis failed for {t}")
                per_ticker[t] = empty_earnings.copy()

        catalysts.sort(key=lambda x: x['date'])
        return {'per_ticker': per_ticker, 'upcoming_catalysts': catalysts}

    @staticmethod
    def _extract_next_earnings_date(stock: yf.Ticker) -> Optional[datetime]:
        try:
            cal = stock.calendar
            if cal is None:
                return None
            if isinstance(cal, pd.DataFrame) and 'Earnings Date' in cal.columns:
                if not cal['Earnings Date'].empty:
                    return pd.Timestamp(cal['Earnings Date'].iloc[0]).to_pydatetime()
            elif isinstance(cal, dict):
                ed = cal.get('Earnings Date')
                if ed:
                    return pd.Timestamp(ed[0] if isinstance(ed, list) else ed).to_pydatetime()
        except Exception:
            pass
        return None

    def _compute_earnings_moves(self, stock: yf.Ticker, ticker: str, ctx: dict) -> tuple[Optional[float], Optional[float]]:
        try:
            ed_df = stock.earnings_dates
            if ed_df is None or ed_df.empty:
                return None, None
            close = ctx['long_close'].get(ticker)
            if close is None or close.empty:
                return None, None

            close_idx = close.index.tz_localize(None) if close.index.tz is not None else close.index
            moves: list[float] = []
            for ed in ed_df.index:
                ed_ts = pd.Timestamp(ed).tz_localize(None) if pd.Timestamp(ed).tz is not None else pd.Timestamp(ed)
                after = close_idx[close_idx >= ed_ts]
                before = close_idx[close_idx < ed_ts]
                if after.empty or before.empty:
                    continue
                p_after = float(close.iloc[close_idx.get_loc(after[0])])
                p_before = float(close.iloc[close_idx.get_loc(before[-1])])
                if p_before > 0:
                    moves.append(abs((p_after - p_before) / p_before * 100))
            return (float(np.mean(moves)), float(np.max(moves))) if moves else (None, None)
        except Exception:
            logger.exception(f"Failed to compute earnings moves for {ticker}")
            return None, None

    def compute_hedging_recommendations(self) -> dict:
        core_data = self.compute_core_risk()
        if 'error' in core_data:
            return {'recommendations': [], 'risk_budget_summary': 'No portfolio data available.'}

        recs: list[dict] = []
        down_beta = core_data['beta']['portfolio_down_beta']
        p_vol = core_data['volatility']['portfolio_vol']
        tnx_corr = core_data['rate_sensitivity']['portfolio_tnx_corr']
        avg_corr = core_data['correlation']['avg_correlation']
        hhi = core_data['sector']['hhi']

        if down_beta > 1.2:
            recs.append({'priority': 1, 'type': 'Protective Put', 'instrument': 'SPY Put',
                         'action': 'Buy SPY puts at 95% strike',
                         'rationale': f'Portfolio down-beta of {down_beta:.2f} indicates above-average downside exposure',
                         'estimated_cost': 'Medium'})

        for item in core_data['sector'].get('single_stock_risk', []):
            if item['weight'] > 15:
                recs.append({'priority': 2, 'type': 'Collar', 'instrument': f"{item['ticker']} Options",
                             'action': f"Buy put / sell call collar on {item['ticker']}",
                             'rationale': f"{item['ticker']} is {item['weight']:.1f}% of portfolio - single stock concentration risk",
                             'estimated_cost': 'Low'})

        for sector in core_data['sector'].get('overweight_sectors', []):
            recs.append({'priority': 3, 'type': 'Sector Hedge', 'instrument': f'Inverse {sector} ETF',
                         'action': f'Consider inverse ETF for {sector} sector exposure reduction',
                         'rationale': f'{sector} sector weight exceeds 30% - consider diversification',
                         'estimated_cost': 'Low'})

        if p_vol > 0.25:
            recs.append({'priority': 4, 'type': 'Tail Risk', 'instrument': 'VIX Call',
                         'action': 'Buy OTM VIX calls for tail risk protection',
                         'rationale': f'Portfolio volatility of {p_vol*100:.1f}% is elevated',
                         'estimated_cost': 'Medium'})

        if tnx_corr < -0.3:
            recs.append({'priority': 5, 'type': 'Rate Hedge', 'instrument': 'TBF',
                         'action': 'Allocate to TBF (short treasury ETF) to hedge rate sensitivity',
                         'rationale': f'Portfolio has high negative rate correlation ({tnx_corr:.2f})',
                         'estimated_cost': 'Low'})

        if avg_corr > 0.6 or hhi > 2500:
            recs.append({'priority': 6, 'type': 'Diversification', 'instrument': 'Uncorrelated Assets',
                         'action': 'Add uncorrelated asset classes (bonds, commodities, international)',
                         'rationale': f'High avg correlation ({avg_corr:.2f}) or concentrated HHI ({hhi:.0f})',
                         'estimated_cost': 'Low'})

        recs.sort(key=lambda x: x['priority'])
        score = core_data['risk_score']
        return {
            'recommendations': recs,
            'risk_budget_summary': f"Portfolio risk score: {score['score']}/100 ({score['label']}). {len(recs)} hedging action(s) recommended.",
        }


portfolio_risk_analyzer = PortfolioRiskAnalyzer()
