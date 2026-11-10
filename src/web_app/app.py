from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from loguru import logger
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

from ..data.database import db
from ..data.crypto_feed import crypto_feed
from ..analysis.metrics import (
    metrics_calculator,
    calculate_period_gains,
    calculate_portfolio_summary,
    get_year_start_price,
    get_option_quote,
    value_option_position,
)
from ..analysis.crypto_metrics import crypto_metrics_calculator
from ..analysis.deep_research import deep_research_analyzer
from ..analysis.risk_analyzer import portfolio_risk_analyzer
from ..analysis.screener import quant_screener
from ..analysis.trade_review import run_review
from ..analysis.turtle import (
    PositionStatus,
    TurtleConfig,
    TurtleSystem,
    backtest,
    correlation_matrix,
    evaluate_position,
    scan_breakouts,
    unit_limit_breaches,
)
from ..data.broker_import import UnsupportedFormatError, parse_export
from ..data.price_history import get_daily_ohlc, get_histories
from ..config.security import security_config
from ..config.settings import settings

app = Flask(__name__)
app.secret_key = security_config.get_secret_key()


# ── Context processor: inject navbar state into every template ────
@app.context_processor
def inject_nav_context():
    """Inject active_page and alert count into all templates."""
    triggered = db.get_triggered_alerts()
    return {
        "active_page": request.endpoint or "",
        "triggered_alert_count": len(triggered),
    }


def _build_stock_figure(hist_data: pd.DataFrame, ticker: str) -> Optional[go.Figure]:
    """Build a Plotly figure for a stock's historical data."""
    if hist_data is None or hist_data.empty:
        logger.warning(f"No data provided for {ticker} plot")
        return None

    if "close" not in hist_data.columns:
        logger.error(f"Missing 'close' column in data for {ticker}")
        return None

    close_prices = hist_data["close"].dropna()
    if close_prices.empty:
        logger.error(f"No valid close prices for {ticker}")
        return None

    dates = [d.strftime("%Y-%m-%d") for d in close_prices.index]
    prices = close_prices.values.tolist()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=dates,
            y=prices,
            mode="lines",
            name="Close Price",
            line=dict(color="#00D4AA", width=2),
        )
    )

    if len(close_prices) >= 100:
        ma_100 = close_prices.rolling(window=100).mean().dropna()
        ma_dates = [d.strftime("%Y-%m-%d") for d in ma_100.index]
        fig.add_trace(
            go.Scatter(
                x=ma_dates,
                y=ma_100.values.tolist(),
                mode="lines",
                name="100-Day MA",
                line=dict(color="#FF6B6B", width=1.5),
            )
        )

        ema_100 = close_prices.ewm(span=100, adjust=False).mean().dropna()
        ema_dates = [d.strftime("%Y-%m-%d") for d in ema_100.index]
        fig.add_trace(
            go.Scatter(
                x=ema_dates,
                y=ema_100.values.tolist(),
                mode="lines",
                name="100-Day EMA",
                line=dict(color="#4ECDC4", width=1.5),
            )
        )

    fig.update_layout(
        title=f"{ticker.upper()} Price Action",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        template="plotly_dark",
        height=500,
        margin=dict(t=50, b=50, l=50, r=50),
    )

    return fig


def _build_advanced_stock_figure(
    hist_data: pd.DataFrame, ticker: str
) -> Optional[go.Figure]:
    """Build a multi-panel Plotly chart: Candlestick+BB / Volume / RSI+MACD."""
    if hist_data is None or hist_data.empty:
        return None

    required = ["open", "high", "low", "close", "volume"]
    if not all(c in hist_data.columns for c in required):
        # Fall back to simple chart if OHLCV not available
        return _build_stock_figure(hist_data, ticker)

    close = hist_data["close"].dropna()
    if close.empty:
        return None

    dates = [d.strftime("%Y-%m-%d") for d in hist_data.index]

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.50, 0.20, 0.30],
        vertical_spacing=0.04,
        specs=[
            [{"secondary_y": False}],
            [{"secondary_y": False}],
            [{"secondary_y": True}],
        ],
        subplot_titles=[f"{ticker.upper()} Price Action", "Volume", "RSI / MACD"],
    )

    # ── Panel 1: Candlestick + Bollinger Bands + MAs ──
    fig.add_trace(
        go.Candlestick(
            x=dates,
            open=hist_data["open"],
            high=hist_data["high"],
            low=hist_data["low"],
            close=hist_data["close"],
            name="OHLC",
            increasing_line_color="#00D4AA",
            decreasing_line_color="#FF6B6B",
        ),
        row=1,
        col=1,
    )

    if len(close) >= 100:
        ma_100 = close.rolling(window=100).mean()
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=ma_100,
                mode="lines",
                name="MA-100",
                line=dict(color="#FF6B6B", width=1.2),
            ),
            row=1,
            col=1,
        )
        ema_100 = close.ewm(span=100, adjust=False).mean()
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=ema_100,
                mode="lines",
                name="EMA-100",
                line=dict(color="#4ECDC4", width=1.2),
            ),
            row=1,
            col=1,
        )

    if len(close) >= 20:
        bb_sma = close.rolling(window=20).mean()
        bb_std = close.rolling(window=20).std()
        bb_upper = bb_sma + 2 * bb_std
        bb_lower = bb_sma - 2 * bb_std
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=bb_upper,
                mode="lines",
                name="BB Upper",
                line=dict(color="#888", width=0.8, dash="dot"),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=bb_lower,
                mode="lines",
                name="BB Lower",
                line=dict(color="#888", width=0.8, dash="dot"),
                fill="tonexty",
                fillcolor="rgba(136,136,136,0.08)",
            ),
            row=1,
            col=1,
        )

    # ── Panel 2: Volume ──
    colors = [
        "#00D4AA" if c >= o else "#FF6B6B"
        for c, o in zip(hist_data["close"], hist_data["open"])
    ]
    fig.add_trace(
        go.Bar(
            x=dates,
            y=hist_data["volume"],
            name="Volume",
            marker_color=colors,
            opacity=0.7,
        ),
        row=2,
        col=1,
    )

    # ── Panel 3: RSI + MACD ──
    if len(close) >= 15:
        delta = close.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        avg_loss = losses.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=rsi,
                mode="lines",
                name="RSI (14)",
                line=dict(color="#FFD700", width=1.5),
            ),
            row=3,
            col=1,
        )
        fig.add_hline(
            y=70, line_dash="dash", line_color="#dc3545", line_width=0.8, row=3, col=1
        )
        fig.add_hline(
            y=30, line_dash="dash", line_color="#28a745", line_width=0.8, row=3, col=1
        )

    if len(close) >= 35:
        fast_ema = close.ewm(span=12, adjust=False).mean()
        slow_ema = close.ewm(span=26, adjust=False).mean()
        macd_line = fast_ema - slow_ema
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - signal_line
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=macd_line,
                mode="lines",
                name="MACD",
                line=dict(color="#00BFFF", width=1.2),
            ),
            row=3,
            col=1,
            secondary_y=True,
        )
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=signal_line,
                mode="lines",
                name="Signal",
                line=dict(color="#FF69B4", width=1.2),
            ),
            row=3,
            col=1,
            secondary_y=True,
        )
        hist_colors = ["#00D4AA" if v >= 0 else "#FF6B6B" for v in macd_hist]
        fig.add_trace(
            go.Bar(
                x=dates,
                y=macd_hist,
                name="MACD Hist",
                marker_color=hist_colors,
                opacity=0.5,
            ),
            row=3,
            col=1,
            secondary_y=True,
        )

    fig.update_layout(
        template="plotly_dark",
        height=900,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40, l=50, r=50),
        xaxis_rangeslider_visible=False,
    )
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.update_yaxes(title_text="RSI", row=3, col=1, range=[0, 100])
    fig.update_yaxes(title_text="MACD", row=3, col=1, secondary_y=True)

    return fig


def create_advanced_stock_plot_json(
    hist_data: pd.DataFrame, ticker: str
) -> Optional[str]:
    """Generate advanced multi-panel Plotly figure as JSON."""
    try:
        fig = _build_advanced_stock_figure(hist_data, ticker)
        if fig is None:
            return None
        return fig.to_json()
    except Exception as e:
        logger.error(f"Error creating advanced plot for {ticker}: {e}", exc_info=True)
        return None


def create_stock_plot(hist_data: pd.DataFrame, ticker: str) -> Optional[str]:
    """Generate an HTML plot for embedding in pages (includes Plotly CDN)."""
    try:
        fig = _build_stock_figure(hist_data, ticker)
        if fig is None:
            return None

        plot_html = pio.to_html(
            fig,
            full_html=False,
            include_plotlyjs="cdn",
            config={"displayModeBar": True, "responsive": True},
        )
        logger.info(f"Successfully created plot for {ticker}")
        return plot_html
    except Exception as e:
        logger.error(f"Error creating plot for {ticker}: {e}", exc_info=True)
        return None


def create_stock_plot_json(hist_data: pd.DataFrame, ticker: str) -> Optional[str]:
    """Generate Plotly figure data as JSON string for dynamic rendering in modals."""
    try:
        fig = _build_stock_figure(hist_data, ticker)
        if fig is None:
            return None

        logger.info(f"Successfully created plot JSON for {ticker}")
        return fig.to_json()
    except Exception as e:
        logger.error(f"Error creating plot JSON for {ticker}: {e}", exc_info=True)
        return None


class MomentumDirection(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"


def filter_momentum_stocks(
    min_market_cap: float = 2e9,
    min_momentum_pct: float = 0.0,
    direction: MomentumDirection = MomentumDirection.BULLISH,
) -> pd.DataFrame:
    """Filter stocks based on momentum and market cap criteria.

    ``min_momentum_pct`` is the momentum strength in the chosen direction:
    bullish keeps stocks at least that far above their 100-day MA, bearish
    keeps stocks at least that far below it. Strongest momentum is listed first.
    """
    try:
        df = db.get_latest_metrics()
        if df.empty:
            return df

        is_bearish = direction == MomentumDirection.BEARISH
        pct_above_ma = df["pct_above_ma_100"]
        meets_momentum = (
            pct_above_ma <= -min_momentum_pct
            if is_bearish
            else pct_above_ma >= min_momentum_pct
        )

        # Apply filters
        filtered_df = df[
            (df["market_cap"].notna())
            & (df["market_cap"] >= min_market_cap)
            & (pct_above_ma.notna())
            & meets_momentum
        ].copy()

        # Sort by momentum percentage, strongest in the chosen direction first
        filtered_df = filtered_df.sort_values("pct_above_ma_100", ascending=is_bearish)

        return filtered_df
    except Exception as e:
        logger.error(f"Error filtering momentum stocks: {e}")
        return pd.DataFrame()


def _build_crypto_figure(
    hist_data: pd.DataFrame, instrument: str
) -> Optional[go.Figure]:
    """Build a Plotly figure for crypto historical data."""
    if hist_data is None or hist_data.empty:
        logger.warning(f"No data provided for {instrument} crypto plot")
        return None

    if "close" not in hist_data.columns:
        logger.error(f"Missing 'close' column in crypto data for {instrument}")
        return None

    close_prices = hist_data["close"].dropna()
    if close_prices.empty:
        logger.error(f"No valid close prices for crypto {instrument}")
        return None

    timestamps = [d.strftime("%Y-%m-%d %H:%M") for d in close_prices.index]
    prices = close_prices.values.tolist()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=timestamps,
            y=prices,
            mode="lines",
            name="Close Price",
            line=dict(color="#00D4AA", width=2),
        )
    )

    ma_period = settings.CRYPTO_MA_PERIOD
    if len(close_prices) >= ma_period:
        ma = close_prices.rolling(window=ma_period).mean().dropna()
        ma_timestamps = [d.strftime("%Y-%m-%d %H:%M") for d in ma.index]
        fig.add_trace(
            go.Scatter(
                x=ma_timestamps,
                y=ma.values.tolist(),
                mode="lines",
                name=f"{ma_period}-Period MA",
                line=dict(color="#FF6B6B", width=1.5),
            )
        )

        ema = close_prices.ewm(span=ma_period, adjust=False).mean().dropna()
        ema_timestamps = [d.strftime("%Y-%m-%d %H:%M") for d in ema.index]
        fig.add_trace(
            go.Scatter(
                x=ema_timestamps,
                y=ema.values.tolist(),
                mode="lines",
                name=f"{ma_period}-Period EMA",
                line=dict(color="#4ECDC4", width=1.5),
            )
        )

    fig.update_layout(
        title=f"{instrument} Price Action",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        template="plotly_dark",
        height=500,
        margin=dict(t=50, b=50, l=50, r=50),
    )

    return fig


def create_crypto_plot_json(hist_data: pd.DataFrame, instrument: str) -> Optional[str]:
    """Generate Plotly figure data as JSON string for crypto charts."""
    try:
        fig = _build_crypto_figure(hist_data, instrument)
        if fig is None:
            return None

        logger.info(f"Successfully created crypto plot JSON for {instrument}")
        return fig.to_json()
    except Exception as e:
        logger.error(
            f"Error creating crypto plot JSON for {instrument}: {e}", exc_info=True
        )
        return None


def filter_crypto_instruments(min_volume: float = 0.0) -> pd.DataFrame:
    """Filter crypto instruments based on volume criteria."""
    try:
        df = db.get_latest_crypto_metrics()
        if df.empty:
            return df

        if min_volume > 0:
            df = df[df["volume_24h"].notna() & (df["volume_24h"] >= min_volume)].copy()

        df = df.sort_values("volume_24h", ascending=False)
        return df
    except Exception as e:
        logger.error(f"Error filtering crypto instruments: {e}")
        return pd.DataFrame()


def _parse_momentum_direction(raw_direction: Optional[str]) -> MomentumDirection:
    """Parse the direction query param, falling back to bullish for missing or unknown values."""
    try:
        return MomentumDirection(
            (raw_direction or MomentumDirection.BULLISH.value).lower()
        )
    except ValueError:
        return MomentumDirection.BULLISH


@app.route("/momentum")
def momentum():
    """Momentum stocks page."""
    try:
        # Get filter parameters from query string
        min_market_cap = float(request.args.get("min_market_cap", 2e9))  # Default $2B
        min_momentum_pct = float(
            request.args.get("min_momentum_pct", 0.0)
        )  # Default 0%
        direction = _parse_momentum_direction(request.args.get("direction"))

        # Filter stocks
        momentum_stocks = filter_momentum_stocks(
            min_market_cap, min_momentum_pct, direction
        )

        # Convert to list of dictionaries for template
        stocks_list = []
        if not momentum_stocks.empty:
            stocks_list = momentum_stocks.to_dict("records")

        return render_template(
            "momentum.html",
            stocks=stocks_list,
            min_market_cap=min_market_cap,
            min_momentum_pct=min_momentum_pct,
            direction=direction.value,
            total_count=len(stocks_list),
        )
    except Exception as e:
        logger.error(f"Error loading momentum page: {e}")
        flash(f"An error occurred: {e}", "error")
        return render_template(
            "momentum.html",
            stocks=[],
            min_market_cap=2e9,
            min_momentum_pct=0.0,
            direction=MomentumDirection.BULLISH.value,
            total_count=0,
        )


@app.route("/get_stock_plot/<ticker>")
def get_stock_plot(ticker: str):
    """API endpoint to get stock plot for modal display."""
    try:
        logger.info(f"==== Starting get_stock_plot for {ticker} ====")

        logger.info(f"Step 1: Fetching historical data for {ticker}")
        try:
            hist_data = metrics_calculator._get_historical_data(ticker)
            logger.info(
                f"Historical data fetch completed. Data is None: {hist_data is None}"
            )
            if hist_data is not None:
                logger.info(f"Historical data is empty: {hist_data.empty}")
        except Exception as fetch_error:
            logger.error(
                f"Exception while fetching historical data: {fetch_error}",
                exc_info=True,
            )
            return jsonify(
                {
                    "success": False,
                    "error": f"Failed to fetch historical data: {str(fetch_error)}",
                }
            )

        if hist_data is None or hist_data.empty:
            logger.warning(f"No historical data available for {ticker}")
            return jsonify(
                {
                    "success": False,
                    "error": f"No historical data available for {ticker}",
                }
            )

        logger.info(f"Historical data shape for {ticker}: {hist_data.shape}")
        logger.info(f"Historical data columns: {list(hist_data.columns)}")
        logger.info(f"Date range: {hist_data.index.min()} to {hist_data.index.max()}")

        logger.info(f"Step 2: Creating plot for {ticker}")
        try:
            plot_data = create_stock_plot_json(hist_data, ticker)
            logger.info(f"Plot creation completed. Plot is None: {plot_data is None}")
        except Exception as plot_error:
            logger.error(f"Exception while creating plot: {plot_error}", exc_info=True)
            return jsonify(
                {"success": False, "error": f"Failed to create plot: {str(plot_error)}"}
            )

        if not plot_data:
            logger.error(f"Failed to generate plot data for {ticker}")
            return jsonify({"success": False, "error": "Failed to generate plot"})

        logger.info(f"Step 3: Fetching metrics for {ticker}")
        try:
            metrics_df = db.get_latest_metrics(ticker=ticker)
            logger.info(
                f"Metrics fetch completed. DataFrame is empty: {metrics_df.empty}"
            )
        except Exception as metrics_error:
            logger.error(
                f"Exception while fetching metrics: {metrics_error}", exc_info=True
            )
            metrics_df = pd.DataFrame()

        metrics = None
        if not metrics_df.empty:
            metrics = metrics_df.iloc[0].to_dict()
            logger.info(f"Found metrics for {ticker}: {list(metrics.keys())}")
        else:
            logger.warning(f"No metrics found in database for {ticker}")

        logger.success(f"Successfully generated response for {ticker}")
        response_data = {"success": True, "plot_data": plot_data, "metrics": metrics}
        logger.info(f"Response keys: {list(response_data.keys())}")
        return jsonify(response_data)
    except Exception as e:
        logger.error(
            f"Unexpected error in get_stock_plot for {ticker}: {e}", exc_info=True
        )
        return jsonify({"success": False, "error": f"Unexpected error: {str(e)}"})


@app.route("/research", methods=["GET", "POST"])
def research():
    stock_data = None
    deep_research = None
    ticker = ""

    if request.method == "POST":
        ticker = request.form.get("ticker", "").upper().strip()
        if not ticker:
            flash("Ticker symbol cannot be empty.", "warning")
            return redirect(url_for("research"))
    elif request.method == "GET":
        ticker = request.args.get("ticker", "").upper().strip()

    if ticker:
        try:
            metrics_df = db.get_latest_metrics(ticker=ticker)

            if not metrics_df.empty:
                logger.info(f"Found {ticker} data in the database.")
                stock_data = metrics_df.iloc[0].to_dict()
            else:
                logger.info(f"No data for {ticker} in DB. Fetching in real-time...")
                fetched_metrics = metrics_calculator.get_metrics(ticker)

                if fetched_metrics:
                    stock_data = fetched_metrics
                    if request.method == "POST":
                        flash(f"Fetched real-time data for {ticker}.", "success")
                elif request.method == "POST":
                    flash(
                        f"Could not fetch data for {ticker}. It might be an invalid symbol.",
                        "error",
                    )

            if stock_data:
                hist_data = metrics_calculator._get_historical_data(ticker)
                stock_data["plot"] = create_stock_plot(hist_data, ticker)

                deep_research = deep_research_analyzer.analyze(ticker)

        except Exception as e:
            logger.error(f"Error during research for {ticker}: {e}", exc_info=True)
            flash(f"An error occurred while researching {ticker}.", "error")

    portfolio_tickers = db.get_portfolio_tickers()
    return render_template(
        "research.html",
        stock_data=stock_data,
        deep_research=deep_research,
        ticker=ticker,
        portfolio_tickers=portfolio_tickers,
    )


@app.route("/")
def portfolio():
    """Main portfolio page with value and gain calculations."""
    try:
        positions = db.get_portfolio_with_shares()
        ytd_realized = db.get_ytd_realized_gains()
        option_metrics = [
            value_option_position(
                option,
                get_option_quote(
                    option["underlying"],
                    option["expiry"],
                    option["option_type"],
                    option["strike"],
                ),
            )
            for option in db.get_option_positions()
        ]

        if not positions:
            summary = calculate_portfolio_summary(
                option_metrics, ytd_realized["ytd_realized_gain_dollars"]
            )
            return render_template(
                "portfolio.html",
                portfolio=[],
                options=option_metrics,
                summary=summary,
                realized_gains=ytd_realized,
            )

        all_metrics_df = db.get_latest_metrics()

        portfolio_metrics = []

        if "ticker" not in all_metrics_df.columns:
            flash("Metrics table is empty or missing 'ticker' column.", "warning")
            return render_template(
                "portfolio.html",
                portfolio=[],
                options=option_metrics,
                summary=None,
                realized_gains=ytd_realized,
            )

        for position in positions:
            ticker = position["ticker"]
            shares = position["shares"] or 0.0
            cost_basis = position.get("cost_basis", 0.0) or 0.0

            ticker_df = all_metrics_df[all_metrics_df["ticker"] == ticker]
            if ticker_df.empty:
                continue

            stock_data = ticker_df.iloc[0].to_dict()

            hist_data = metrics_calculator._get_historical_data(ticker)

            gains = calculate_period_gains(hist_data, shares, position.get("added_at"))

            live_price = None
            if hist_data is not None and not hist_data.empty:
                close_prices = hist_data["close"].dropna()
                if not close_prices.empty:
                    live_price = float(close_prices.iloc[-1])

            current_price = (
                live_price
                if live_price is not None
                else (stock_data.get("last_price", 0) or 0)
            )
            current_value = current_price * shares

            stock_data["last_price"] = current_price
            stock_data["plot"] = create_stock_plot(hist_data, ticker)
            stock_data["shares"] = shares
            stock_data["cost_basis"] = cost_basis
            stock_data["current_value"] = current_value
            stock_data["unrealized"] = (
                (current_price - cost_basis / abs(shares)) * shares
                if shares and cost_basis
                else None
            )
            stock_data["gains"] = gains

            portfolio_metrics.append(stock_data)

        summary = calculate_portfolio_summary(
            portfolio_metrics + option_metrics,
            ytd_realized["ytd_realized_gain_dollars"],
        )

        return render_template(
            "portfolio.html",
            portfolio=portfolio_metrics,
            options=option_metrics,
            summary=summary,
            realized_gains=ytd_realized,
        )
    except Exception as e:
        logger.error(f"Error loading portfolio page: {e}")
        flash(f"An error occurred: {e}", "error")
        return render_template(
            "portfolio.html",
            portfolio=[],
            options=[],
            summary=None,
            realized_gains=None,
        )


@app.route("/add_stock", methods=["POST"])
def add_stock():
    """Add a stock to the portfolio with optional share quantity and cost basis."""
    ticker = request.form.get("ticker", "").upper().strip()
    shares_str = request.form.get("shares", "0")
    cost_basis_str = request.form.get("cost_basis", "0")
    side = request.form.get("side", "long")
    source_page = request.form.get("source_page", "portfolio")

    if not ticker:
        flash("Ticker symbol cannot be empty.", "warning")
        return redirect(url_for("portfolio"))

    try:
        shares = abs(float(shares_str)) if shares_str else 0.0
        cost_basis = float(cost_basis_str) if cost_basis_str else 0.0
        if side == "short":
            shares = -shares

        if ticker in db.get_portfolio_tickers():
            flash(f"{ticker} is already in your portfolio.", "info")
        else:
            if db.get_latest_metrics(ticker=ticker).empty:
                logger.info(
                    f"No data for {ticker} in DB. Fetching before adding to portfolio..."
                )
                metrics = metrics_calculator.get_metrics(ticker)
                if metrics:
                    db.store_metrics([metrics])
                else:
                    flash(
                        f"Could not fetch data for {ticker}. Cannot add to portfolio.",
                        "error",
                    )
                    return redirect(
                        url_for(
                            "research" if source_page == "research" else "portfolio",
                            ticker=ticker,
                        )
                    )

            db.add_portfolio_ticker(ticker, cost_basis=cost_basis)
            if shares != 0:
                db.update_portfolio_shares(ticker, shares)
            flash(f"{ticker} has been added to your portfolio.", "success")

    except ValueError:
        flash("Invalid share quantity or cost basis.", "error")
    except Exception as e:
        logger.error(f"Error adding stock {ticker}: {e}")
        flash(f"An error occurred while adding {ticker}.", "error")

    if source_page == "research":
        return redirect(url_for("research", ticker=ticker))

    return redirect(url_for("portfolio"))


@app.route("/update_shares", methods=["POST"])
def update_shares():
    """Update share quantity for a portfolio position; negative shares are a short."""
    ticker = request.form.get("ticker", "").upper().strip()
    shares_str = request.form.get("shares", "0")

    try:
        shares = float(shares_str)
        db.update_portfolio_shares(ticker, shares)
        flash(f"Updated {ticker} to {shares} shares.", "success")
    except ValueError:
        flash("Invalid share quantity.", "error")
    except Exception as e:
        logger.error(f"Error updating shares for {ticker}: {e}")
        flash(f"Error updating shares: {e}", "error")

    return redirect(url_for("portfolio"))


@app.route("/update_cost_basis", methods=["POST"])
def update_cost_basis():
    """Update cost basis for a portfolio position."""
    ticker = request.form.get("ticker", "").upper().strip()
    cost_basis_str = request.form.get("cost_basis", "0")

    try:
        cost_basis = float(cost_basis_str)
        if cost_basis < 0:
            flash("Cost basis cannot be negative.", "warning")
            return redirect(url_for("portfolio"))

        db.update_portfolio_cost_basis(ticker, cost_basis)
        flash(f"Updated {ticker} cost basis to ${cost_basis:.2f}.", "success")
    except ValueError:
        flash("Invalid cost basis.", "error")
    except Exception as e:
        logger.error(f"Error updating cost basis for {ticker}: {e}")
        flash(f"Error updating cost basis: {e}", "error")

    return redirect(url_for("portfolio"))


@app.route("/sell_stock", methods=["POST"])
def sell_stock():
    """Sell a stock from the portfolio and record YTD realized gain."""
    ticker = request.form.get("ticker", "").upper().strip()
    sale_price_str = request.form.get("sale_price", "")

    if not ticker:
        flash("Ticker symbol cannot be empty.", "warning")
        return redirect(url_for("portfolio"))

    try:
        sale_price = float(sale_price_str)
        if sale_price < 0:
            flash("Sale price cannot be negative.", "warning")
            return redirect(url_for("portfolio"))

        hist_data = metrics_calculator._get_historical_data(ticker)
        year_start_price = get_year_start_price(hist_data)

        realized_gain = db.sell_position(ticker, sale_price, year_start_price)

        if realized_gain is not None:
            gain_word = "gain" if realized_gain >= 0 else "loss"
            year_start_msg = (
                f" (Jan 1 price: ${year_start_price:.2f})" if year_start_price else ""
            )
            flash(
                f"Sold {ticker}. YTD realized {gain_word}: ${abs(realized_gain):.2f}{year_start_msg}",
                "success",
            )
        else:
            flash(f"{ticker} not found in portfolio.", "warning")

    except ValueError:
        flash("Invalid sale price.", "error")
    except Exception as e:
        logger.error(f"Error selling stock {ticker}: {e}")
        flash(f"An error occurred while selling {ticker}.", "error")

    return redirect(url_for("portfolio"))


@app.route("/delete_stock/<ticker>")
def delete_stock(ticker: str):
    """Delete a stock from the portfolio without recording realized gain."""
    try:
        db.delete_portfolio_ticker(ticker)
        flash(f"{ticker} has been removed from your portfolio.", "success")
    except Exception as e:
        logger.error(f"Error deleting stock {ticker}: {e}")
        flash(f"An error occurred while removing {ticker}.", "error")

    return redirect(url_for("portfolio"))


@app.route("/add_option", methods=["POST"])
def add_option():
    """Open an option position; the premium is the total paid, or received when writing."""
    underlying = request.form.get("underlying", "").upper().strip()
    option_type = request.form.get("option_type", "")
    side = request.form.get("side", "long")

    if not underlying or option_type not in ("call", "put"):
        flash("Underlying and option type (call/put) are required.", "warning")
        return redirect(url_for("portfolio"))

    try:
        strike = float(request.form["strike"])
        expiry = datetime.strptime(request.form["expiry"], "%Y-%m-%d").date()
        contracts = abs(float(request.form["contracts"]))
        premium = float(request.form.get("cost_basis") or 0)
        if strike <= 0 or contracts == 0 or premium < 0:
            flash(
                "Strike and contracts must be positive and premium cannot be negative.",
                "warning",
            )
            return redirect(url_for("portfolio"))
        if side == "short":
            contracts = -contracts

        db.add_option_position(
            underlying, option_type, strike, expiry, contracts, premium
        )
        flash(
            f"Added {contracts:g} {underlying} {expiry} ${strike:g} {option_type} contracts.",
            "success",
        )
    except (KeyError, ValueError):
        flash("Invalid strike, expiry, contracts, or premium.", "error")
    except Exception as e:
        logger.error(f"Error adding option on {underlying}: {e}")
        flash(f"An error occurred while adding the {underlying} option.", "error")

    return redirect(url_for("portfolio"))


@app.route("/close_option", methods=["POST"])
def close_option():
    """Close an option position at a per-share premium and record the realized gain."""
    position_id = request.form.get("position_id", "")

    try:
        close_price = float(request.form.get("close_price", ""))
        if close_price < 0:
            flash("Close price cannot be negative.", "warning")
            return redirect(url_for("portfolio"))

        realized_gain = db.close_option_position(position_id, close_price)
        if realized_gain is None:
            flash("Option position not found.", "warning")
        else:
            gain_word = "gain" if realized_gain >= 0 else "loss"
            flash(
                f"Closed option. Realized {gain_word}: ${abs(realized_gain):,.2f}",
                "success",
            )
    except ValueError:
        flash("Invalid close price.", "error")
    except Exception as e:
        logger.error(f"Error closing option position {position_id}: {e}")
        flash("An error occurred while closing the option.", "error")

    return redirect(url_for("portfolio"))


@app.route("/delete_option/<position_id>")
def delete_option(position_id: str):
    """Delete an option position without recording a realized gain."""
    try:
        db.delete_option_position(position_id)
        flash("Option position removed.", "success")
    except Exception as e:
        logger.error(f"Error deleting option position {position_id}: {e}")
        flash("An error occurred while removing the option.", "error")

    return redirect(url_for("portfolio"))


@app.route("/crypto")
def crypto():
    """Crypto trends page."""
    crypto_feed.start()
    return render_template("crypto.html")


@app.route("/api/crypto/quotes")
def crypto_quotes():
    crypto_feed.start()
    frame = filter_crypto_instruments()
    # pandas serializes missing values as JSON null and timestamps as ISO strings.
    import json

    if not frame.empty:
        frame = frame[frame["instrument"].str.fullmatch(r"[A-Z0-9]+USD")]
    records = json.loads(frame.to_json(orient="records", date_format="iso"))
    return jsonify(instruments=records, **crypto_feed.status())


@app.route("/get_crypto_plot/<instrument>")
def get_crypto_plot(instrument: str):
    """API endpoint to get crypto plot for modal display."""
    try:
        logger.info(f"Fetching crypto plot for {instrument}")

        metrics_df = db.get_latest_crypto_metrics(instrument=instrument)

        metrics = None
        if not metrics_df.empty:
            metrics = metrics_df.iloc[0].to_dict()
            logger.info(f"Found crypto metrics for {instrument}")
        else:
            logger.warning(f"No crypto metrics found for {instrument}")
            return jsonify(
                {"success": False, "error": f"No data available for {instrument}"}
            )

        timeframe = request.args.get("timeframe", "1D")
        if timeframe not in ("1h", "1D"):
            return jsonify(success=False, error="Unsupported timeframe"), 400
        history, stale = crypto_feed.candles(instrument, timeframe)
        return jsonify(
            {
                "success": True,
                "plot_data": create_crypto_plot_json(history, instrument),
                "stale": stale,
                "last_candle": history.index[-1].isoformat(),
                "timeframe": timeframe,
            }
        )
    except Exception as e:
        logger.error(f"Error in get_crypto_plot for {instrument}: {e}", exc_info=True)
        return jsonify({"success": False, "error": f"Unexpected error: {str(e)}"})


@app.route("/api/crypto/store", methods=["POST"])
def store_crypto_data():
    """API endpoint to store crypto metrics from MCP data.

    Expects JSON payload with ticker_data and candlestick_data for an instrument.
    This allows Claude to fetch MCP data and push it to the database.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No JSON data provided"}), 400

        instrument = data.get("instrument")
        ticker_data = data.get("ticker_data")
        candlestick_data = data.get("candlestick_data", [])
        timeframe = data.get("timeframe", settings.CRYPTO_DEFAULT_TIMEFRAME)

        if not instrument or not ticker_data:
            return jsonify(
                {"success": False, "error": "Missing instrument or ticker_data"}
            ), 400

        metrics = crypto_metrics_calculator.get_metrics(
            instrument=instrument,
            candlestick_data=candlestick_data,
            ticker_data=ticker_data,
            timeframe=timeframe,
        )

        if metrics:
            db.store_crypto_metrics([metrics])
            logger.info(f"Stored crypto metrics for {instrument}")
            return jsonify(
                {"success": True, "instrument": instrument, "metrics": metrics}
            )
        else:
            return jsonify(
                {
                    "success": False,
                    "error": f"Failed to calculate metrics for {instrument}",
                }
            ), 400

    except Exception as e:
        logger.error(f"Error storing crypto data: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/crypto/symbols")
def get_crypto_symbols():
    """Return the list of default crypto symbols to track."""
    crypto_feed.start()
    markets = db.get_latest_crypto_metrics()
    symbols = (
        sorted(markets["instrument"].tolist())
        if not markets.empty
        else settings.CRYPTO_DEFAULT_SYMBOLS
    )
    return jsonify(
        {
            "success": True,
            "symbols": symbols,
            "ma_period": settings.CRYPTO_MA_PERIOD,
            "timeframe": settings.CRYPTO_DEFAULT_TIMEFRAME,
        }
    )


# ═══════════════════════════════════════════════════════════════════
# ADVANCED CHART API
# ═══════════════════════════════════════════════════════════════════


@app.route("/api/stock/chart/<ticker>")
def api_stock_chart(ticker: str):
    """API endpoint returning advanced or simple chart JSON."""
    view = request.args.get("view", "advanced")
    try:
        hist_data = metrics_calculator._get_historical_data(ticker)
        if hist_data is None or hist_data.empty:
            return jsonify({"success": False, "error": f"No data for {ticker}"})

        if view == "advanced":
            plot_json = create_advanced_stock_plot_json(hist_data, ticker)
        else:
            plot_json = create_stock_plot_json(hist_data, ticker)

        metrics_df = db.get_latest_metrics(ticker=ticker)
        metrics = metrics_df.iloc[0].to_dict() if not metrics_df.empty else None

        return jsonify({"success": True, "plot_data": plot_json, "metrics": metrics})
    except Exception as e:
        logger.error(f"Error in api_stock_chart for {ticker}: {e}")
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════
# ANALYTICS PAGE
# ═══════════════════════════════════════════════════════════════════


@app.route("/analytics")
def analytics():
    """Market analytics page with heatmap, breadth, movers."""
    try:
        df = db.get_all_metrics_df()
        sector_df = db.get_sector_summary()

        # Market breadth stats
        breadth = {}
        if not df.empty:
            total = len(df)
            has_ma = df["pct_above_ma_100"].notna()
            breadth["pct_above_ma"] = (
                round(
                    (df.loc[has_ma, "pct_above_ma_100"] > 0).sum() / has_ma.sum() * 100,
                    1,
                )
                if has_ma.sum() > 0
                else 0
            )
            has_rsi = df["rsi_14"].notna()
            if has_rsi.sum() > 0:
                rsi_vals = df.loc[has_rsi, "rsi_14"]
                breadth["pct_rsi_above_50"] = round(
                    (rsi_vals > 50).sum() / has_rsi.sum() * 100, 1
                )
                breadth["pct_overbought"] = round(
                    (rsi_vals >= 70).sum() / has_rsi.sum() * 100, 1
                )
                breadth["pct_oversold"] = round(
                    (rsi_vals <= 30).sum() / has_rsi.sum() * 100, 1
                )
            else:
                breadth["pct_rsi_above_50"] = 0
                breadth["pct_overbought"] = 0
                breadth["pct_oversold"] = 0
            breadth["total_stocks"] = total

        # Top movers (by pct_above_ma_100)
        top_gainers = []
        top_losers = []
        if not df.empty and "pct_above_ma_100" in df.columns:
            valid = df[
                df["pct_above_ma_100"].notna()
                & df["market_cap"].notna()
                & (df["market_cap"] >= 1e9)
            ]
            sorted_df = valid.sort_values("pct_above_ma_100", ascending=False)
            top_gainers = sorted_df.head(10).to_dict("records")
            top_losers = (
                sorted_df.tail(10).sort_values("pct_above_ma_100").to_dict("records")
            )

        # Sector heatmap data
        sector_data = []
        if not sector_df.empty:
            sector_data = sector_df.to_dict("records")

        return render_template(
            "analytics.html",
            breadth=breadth,
            top_gainers=top_gainers,
            top_losers=top_losers,
            sector_data=sector_data,
        )
    except Exception as e:
        logger.error(f"Error loading analytics page: {e}")
        flash(f"An error occurred: {e}", "error")
        return render_template(
            "analytics.html", breadth={}, top_gainers=[], top_losers=[], sector_data=[]
        )


@app.route("/api/analytics/comparative")
def api_comparative_chart():
    """API: comparative normalized price chart for multiple tickers."""
    tickers_param = request.args.get("tickers", "")
    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()]
    if not tickers or len(tickers) > 5:
        return jsonify(
            {"success": False, "error": "Provide 1-5 comma-separated tickers"}
        )

    try:
        fig = go.Figure()
        colors = ["#00D4AA", "#FF6B6B", "#4ECDC4", "#FFD700", "#FF69B4"]

        for i, ticker in enumerate(tickers):
            hist = metrics_calculator._get_historical_data(ticker)
            if hist is None or hist.empty or "close" not in hist.columns:
                continue
            close = hist["close"].dropna()
            if close.empty:
                continue
            # Normalize to 100 at start
            normalized = (close / close.iloc[0]) * 100
            dates = [d.strftime("%Y-%m-%d") for d in normalized.index]
            fig.add_trace(
                go.Scatter(
                    x=dates,
                    y=normalized.values.tolist(),
                    mode="lines",
                    name=ticker,
                    line=dict(color=colors[i % len(colors)], width=2),
                )
            )

        fig.update_layout(
            title="Comparative Performance (Normalized to 100)",
            yaxis_title="Indexed Price (%)",
            xaxis_title="Date",
            template="plotly_dark",
            height=500,
            margin=dict(t=50, b=50, l=50, r=50),
        )

        return jsonify({"success": True, "plot_data": fig.to_json()})
    except Exception as e:
        logger.error(f"Error building comparative chart: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/analytics/sector-heatmap")
def api_sector_heatmap():
    """API: sector treemap as Plotly JSON."""
    try:
        sector_df = db.get_sector_summary()
        if sector_df.empty:
            return jsonify({"success": False, "error": "No sector data available"})

        sector_df = sector_df[
            sector_df["total_market_cap"].notna() & (sector_df["total_market_cap"] > 0)
        ]

        colors = sector_df["avg_pct_above_ma"].fillna(0).tolist()

        fig = go.Figure(
            go.Treemap(
                labels=sector_df["sector"].tolist(),
                parents=[""] * len(sector_df),
                values=sector_df["total_market_cap"].tolist(),
                marker=dict(
                    colors=colors,
                    colorscale="RdYlGn",
                    cmid=0,
                    showscale=True,
                    colorbar=dict(title="Avg % Above MA"),
                ),
                textinfo="label+value+percent parent",
                hovertemplate="<b>%{label}</b><br>Market Cap: $%{value:,.0f}<br>Avg %% Above MA: %{color:.1f}%<br>Stocks: %{customdata}<extra></extra>",
                customdata=sector_df["stock_count"].tolist(),
            )
        )
        fig.update_layout(
            template="plotly_dark", height=500, margin=dict(t=30, b=10, l=10, r=10)
        )

        return jsonify({"success": True, "plot_data": fig.to_json()})
    except Exception as e:
        logger.error(f"Error building sector heatmap: {e}")
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════
# ALERTS PAGE
# ═══════════════════════════════════════════════════════════════════


@app.route("/alerts")
def alerts_page():
    """Alert management page."""
    try:
        active = db.get_active_alerts()
        triggered = [a for a in active if a["is_triggered"]]
        pending = [a for a in active if not a["is_triggered"]]
        return render_template(
            "alerts.html", pending_alerts=pending, triggered_alerts=triggered
        )
    except Exception as e:
        logger.error(f"Error loading alerts page: {e}")
        flash(f"An error occurred: {e}", "error")
        return render_template("alerts.html", pending_alerts=[], triggered_alerts=[])


@app.route("/alerts/create", methods=["POST"])
def create_alert():
    """Create a new alert rule."""
    ticker = request.form.get("ticker", "").upper().strip()
    alert_type = request.form.get("alert_type", "").strip()
    threshold_str = request.form.get("threshold", "")

    valid_types = [
        "price_above",
        "price_below",
        "rsi_overbought",
        "rsi_oversold",
        "macd_crossover",
        "pct_change",
    ]

    if not ticker or alert_type not in valid_types or not threshold_str:
        flash("Invalid alert parameters.", "warning")
        return redirect(url_for("alerts_page"))

    try:
        threshold = float(threshold_str)
        db.create_alert(ticker, alert_type, threshold)
        flash(f"Alert created: {ticker} {alert_type} @ {threshold}", "success")
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        flash(f"Error creating alert: {e}", "error")

    return redirect(url_for("alerts_page"))


@app.route("/alerts/<alert_id>/dismiss", methods=["POST"])
def dismiss_alert(alert_id: str):
    """Dismiss (deactivate) an alert."""
    try:
        db.dismiss_alert(alert_id)
        flash("Alert dismissed.", "success")
    except Exception as e:
        logger.error(f"Error dismissing alert: {e}")
        flash(f"Error: {e}", "error")
    return redirect(url_for("alerts_page"))


@app.route("/alerts/<alert_id>/delete", methods=["POST"])
def delete_alert(alert_id: str):
    """Delete an alert permanently."""
    try:
        db.delete_alert(alert_id)
        flash("Alert deleted.", "success")
    except Exception as e:
        logger.error(f"Error deleting alert: {e}")
        flash(f"Error: {e}", "error")
    return redirect(url_for("alerts_page"))


# ═══════════════════════════════════════════════════════════════════
# SCREENER
# ═══════════════════════════════════════════════════════════════════


@app.route("/screener")
def screener():
    results = quant_screener.get_cached_results()
    return render_template("screener.html", results=results)


@app.route("/api/screener/run", methods=["POST"])
def api_screener_run():
    try:
        results = quant_screener.run_screen()
        return jsonify({"success": True, **results})
    except Exception as e:
        logger.exception("Error running screener")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/screener/stock/<ticker>")
def api_screener_stock(ticker: str):
    results = quant_screener.get_cached_results()
    if not results:
        return jsonify({"error": "No screening results available"}), 404

    all_stocks = results.get("top_10", []) + results.get("watchlist", [])
    match = next((s for s in all_stocks if s["ticker"] == ticker.upper()), None)
    if not match:
        return jsonify({"error": f"{ticker} not found in screening results"}), 404

    return jsonify(
        {
            "ticker": match["ticker"],
            "sector": match.get("sector"),
            "composite_score": match.get("composite_score"),
            "factor_scores": match.get("factor_scores", {}),
            "sub_factor_scores": match.get("sub_factor_scores", {}),
        }
    )


# ═══════════════════════════════════════════════════════════════════
# PORTFOLIO RISK API
# ═══════════════════════════════════════════════════════════════════


@app.route("/api/portfolio/risk")
def api_portfolio_risk():
    """Calculate and return portfolio risk metrics: beta, Sharpe, correlation, drawdown."""
    try:
        positions = db.get_portfolio_with_shares()
        if not positions:
            return jsonify({"success": False, "error": "Portfolio is empty"})

        # Fetch historical data for all holdings + SPY
        tickers = [p["ticker"] for p in positions]
        weights = {}
        hist_dict = {}
        gross_exposure = 0.0

        for p in positions:
            t = p["ticker"]
            hist = metrics_calculator._get_historical_data(t)
            if hist is not None and not hist.empty and "close" in hist.columns:
                hist_dict[t] = hist["close"].dropna()
                last_px = float(hist_dict[t].iloc[-1])
                val = last_px * (p["shares"] or 0)
                weights[t] = val
                gross_exposure += abs(val)

        if gross_exposure == 0 or not hist_dict:
            return jsonify({"success": False, "error": "No price data for portfolio"})

        # Normalize by gross exposure; short positions keep a negative weight
        for t in weights:
            weights[t] /= gross_exposure

        # Build aligned returns DataFrame
        returns_df = pd.DataFrame(
            {t: s.pct_change().dropna() for t, s in hist_dict.items()}
        )
        returns_df = returns_df.dropna()

        if returns_df.empty:
            return jsonify({"success": False, "error": "Insufficient return data"})

        # Portfolio daily returns (weighted sum)
        portfolio_returns = sum(
            returns_df[t] * weights.get(t, 0)
            for t in returns_df.columns
            if t in weights
        )

        # Beta vs SPY
        beta = None
        spy_hist = metrics_calculator._get_historical_data("SPY")
        if spy_hist is not None and not spy_hist.empty and "close" in spy_hist.columns:
            spy_returns = spy_hist["close"].pct_change().dropna()
            # Align
            common = portfolio_returns.index.intersection(spy_returns.index)
            if len(common) > 30:
                pr = portfolio_returns.loc[common]
                sr = spy_returns.loc[common]
                cov = np.cov(pr, sr)
                if cov[1, 1] != 0:
                    beta = round(float(cov[0, 1] / cov[1, 1]), 3)

        # Sharpe ratio (annualized, rf = 5%)
        rf_daily = 0.05 / 252
        excess = portfolio_returns - rf_daily
        sharpe = None
        if excess.std() > 0:
            sharpe = round(float(excess.mean() / excess.std() * np.sqrt(252)), 3)

        # Max drawdown
        cumulative = (1 + portfolio_returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = round(float(drawdown.min() * 100), 2)
        current_drawdown = round(float(drawdown.iloc[-1] * 100), 2)

        # Correlation matrix
        corr = returns_df.corr().round(3)
        corr_dict = {
            t: {t2: corr.loc[t, t2] for t2 in corr.columns} for t in corr.index
        }

        # Sector exposure
        sector_exposure = {}
        all_metrics = db.get_all_metrics_df()
        for t, w in weights.items():
            row = all_metrics[all_metrics["ticker"] == t]
            sector = (
                row.iloc[0]["sector"]
                if not row.empty and pd.notna(row.iloc[0].get("sector"))
                else "Unknown"
            )
            sector_exposure[sector] = sector_exposure.get(sector, 0) + abs(w)
        sector_exposure = {k: round(v * 100, 1) for k, v in sector_exposure.items()}

        return jsonify(
            {
                "success": True,
                "beta": beta,
                "sharpe": sharpe,
                "max_drawdown": max_drawdown,
                "current_drawdown": current_drawdown,
                "correlation": corr_dict,
                "sector_exposure": sector_exposure,
                "drawdown_series": {
                    "dates": [d.strftime("%Y-%m-%d") for d in drawdown.index],
                    "values": [round(float(v) * 100, 2) for v in drawdown.values],
                },
            }
        )
    except Exception as e:
        logger.error(f"Error calculating portfolio risk: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════
# RISK DASHBOARD
# ═══════════════════════════════════════════════════════════════════


@app.route("/risk")
def risk_dashboard():
    try:
        core_risk = portfolio_risk_analyzer.compute_core_risk()
    except Exception as e:
        logger.exception("Error computing core risk")
        core_risk = {"error": f"Failed to compute risk metrics: {e}"}

    return render_template(
        "risk.html",
        core_risk=core_risk,
        report_date=datetime.now().strftime("%B %d, %Y"),
    )


@app.route("/api/risk/implied-volatility")
def api_risk_implied_volatility():
    try:
        data = portfolio_risk_analyzer.compute_implied_volatility()
        return jsonify({"success": True, **data})
    except Exception as e:
        logger.exception("Error computing implied volatility")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/risk/stress-tests")
def api_risk_stress_tests():
    try:
        data = portfolio_risk_analyzer.compute_stress_tests()
        return jsonify({"success": True, **data})
    except Exception as e:
        logger.exception("Error computing stress tests")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/risk/earnings-risk")
def api_risk_earnings_risk():
    try:
        data = portfolio_risk_analyzer.compute_earnings_risk()
        return jsonify({"success": True, **data})
    except Exception as e:
        logger.exception("Error computing earnings risk")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/risk/hedging")
def api_risk_hedging():
    try:
        data = portfolio_risk_analyzer.compute_hedging_recommendations()
        return jsonify({"success": True, **data})
    except Exception as e:
        logger.exception("Error computing hedging recommendations")
        return jsonify({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════
# TREND FOLLOWING (Turtle rules)
# ═══════════════════════════════════════════════════════════════════

DEFAULT_TREND_EQUITY = 100_000.0
DEFAULT_RISK_PER_UNIT_PCT = 0.5
DEFAULT_UNIT_CAP_PCT = 10.0
BACKTEST_START = "2015-01-01"
BENCHMARK_TICKER = "SPY"


def _json_safe(value: Any) -> Any:
    """Replace NaN/inf with None and timestamps/enums with strings so jsonify emits valid JSON."""
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (float, np.floating)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Enum):
        return value.value
    return value


def _float_arg(name: str, default: float) -> float:
    try:
        return float(request.args.get(name, default))
    except (TypeError, ValueError):
        return default


def _turtle_config_from_request() -> TurtleConfig:
    system = request.args.get("system", TurtleSystem.SYSTEM_2.value)
    overrides = {
        "risk_per_unit": _float_arg("risk_pct", DEFAULT_RISK_PER_UNIT_PCT) / 100,
        "max_unit_fraction": _float_arg("unit_cap_pct", DEFAULT_UNIT_CAP_PCT) / 100,
        "stop_n": _float_arg("stop_n", 2.0),
        "use_trend_filter": request.args.get("trend_filter") == "true",
    }
    exit_days = request.args.get("exit_days", "").strip()
    if exit_days.isdigit() and int(exit_days) > 0:
        overrides["exit_period"] = int(exit_days)
    if system == TurtleSystem.SYSTEM_1.value:
        return TurtleConfig.system_1(**overrides)
    return TurtleConfig.system_2(**overrides)


def _position_status_dict(status: PositionStatus) -> Dict[str, Any]:
    exit_event = status.first_exit
    return {
        "ticker": status.ticker,
        "last_date": status.last_date,
        "last_close": status.last_close,
        "shares": status.shares,
        "entry_price": status.entry_price,
        "entry_date": status.entry_date,
        "n_at_entry": status.n_at_entry,
        "n_now": status.n_now,
        "stop_price": status.stop_price,
        "exit_level": status.exit_level,
        "next_add_level": status.next_add_level,
        "distance_to_stop_n": status.distance_to_stop_n,
        "open_r_multiple": status.open_r_multiple,
        "open_risk_dollars": status.open_risk_dollars,
        "open_risk_pct_equity": status.open_risk_pct_equity,
        "units_held": status.units_held,
        "unit_shares_now": status.unit_shares_now,
        "action": status.action,
        "rule_fired_on": exit_event.exit_date if exit_event else None,
        "rule_fired_price": exit_event.price if exit_event else None,
        "rule_fired_reason": exit_event.reason if exit_event else None,
        "is_late": status.is_late,
        "unrealized_pnl": (status.last_close - status.entry_price) * status.shares,
    }


def _trend_watchlist() -> List[str]:
    """The saved watchlist, seeded on first use with held and previously traded tickers."""
    tickers = db.get_watchlist()
    if not tickers:
        seed = {p["ticker"] for p in db.get_portfolio_with_shares()}
        seed |= {f.symbol for f in db.get_broker_fills()}
        db.add_to_watchlist(sorted(seed))
        tickers = db.get_watchlist()
    return tickers


@app.route("/trend")
def trend():
    """Turtle trend-following rules for open positions and the watchlist."""
    return render_template(
        "trend.html",
        default_equity=DEFAULT_TREND_EQUITY,
        default_risk_pct=DEFAULT_RISK_PER_UNIT_PCT,
        default_unit_cap_pct=DEFAULT_UNIT_CAP_PCT,
    )


@app.route("/api/trend/positions")
def api_trend_positions():
    equity = _float_arg("equity", DEFAULT_TREND_EQUITY)
    config = _turtle_config_from_request()
    statuses, skipped, histories = [], [], {}
    for position in db.get_portfolio_with_shares():
        ticker, shares = position["ticker"], position["shares"]
        if shares <= 0 or not position["cost_basis"]:
            skipped.append(
                {
                    "ticker": ticker,
                    "reason": "Short or missing cost basis; rules are long-only",
                }
            )
            continue
        try:
            ohlc = get_daily_ohlc(ticker)
            histories[ticker] = ohlc
            status = evaluate_position(
                ticker,
                ohlc,
                shares,
                position["cost_basis"] / shares,
                position["added_at"] or datetime.utcnow(),
                equity,
                config,
            )
            statuses.append(status)
        except Exception as e:
            logger.exception(f"Could not evaluate {ticker}")
            skipped.append({"ticker": ticker, "reason": str(e)})

    units = {s.ticker: s.units_held for s in statuses if np.isfinite(s.units_held)}
    breaches = (
        unit_limit_breaches(units, correlation_matrix(histories), config)
        if histories
        else []
    )
    order = {"EXIT_STOP": 0, "EXIT_CHANNEL": 1, "ADD_UNIT": 2, "HOLD": 3}
    rows = sorted(
        (_position_status_dict(s) for s in statuses),
        key=lambda r: order[r["action"].value],
    )
    return jsonify(
        _json_safe(
            {
                "success": True,
                "positions": rows,
                "skipped": skipped,
                "unit_limit_breaches": breaches,
                "total_open_risk": sum(s.open_risk_dollars for s in statuses),
                "config": {
                    "system": config.system,
                    "entry_period": config.entry_period,
                    "exit_period": config.exit_period,
                    "stop_n": config.stop_n,
                },
            }
        )
    )


@app.route("/api/trend/scan")
def api_trend_scan():
    equity = _float_arg("equity", DEFAULT_TREND_EQUITY)
    config = _turtle_config_from_request()
    tickers = _trend_watchlist()
    histories = get_histories(tickers)
    signals = scan_breakouts(histories, equity, config)
    return jsonify(
        _json_safe(
            {
                "success": True,
                "signals": [s.__dict__ for s in signals],
                "missing": sorted(set(tickers) - set(histories)),
            }
        )
    )


@app.route("/api/trend/watchlist", methods=["GET", "POST"])
def api_trend_watchlist():
    if request.method == "POST":
        raw = (request.get_json(silent=True) or {}).get("tickers", "")
        tickers = [
            t.strip().upper() for t in raw.replace(" ", ",").split(",") if t.strip()
        ]
        db.add_to_watchlist(tickers)
    return jsonify({"success": True, "tickers": _trend_watchlist()})


@app.route("/api/trend/watchlist/<ticker>", methods=["DELETE"])
def api_trend_watchlist_delete(ticker: str):
    db.remove_from_watchlist(ticker)
    return jsonify({"success": True, "tickers": db.get_watchlist()})


@app.route("/api/trend/chart/<ticker>")
def api_trend_chart(ticker: str):
    """Price with the entry/exit channels and, for held positions, the 2N stop."""
    config = _turtle_config_from_request()
    try:
        ohlc = get_daily_ohlc(ticker).tail(260)
    except Exception as e:
        logger.exception(f"Could not load chart data for {ticker}")
        return jsonify({"success": False, "error": str(e)})
    full = get_daily_ohlc(ticker)
    entry_band = full["high"].rolling(config.entry_period).max().reindex(ohlc.index)
    exit_band = full["low"].rolling(config.exit_period).min().reindex(ohlc.index)
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=ohlc.index,
            open=ohlc["open"],
            high=ohlc["high"],
            low=ohlc["low"],
            close=ohlc["close"],
            name=ticker,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=ohlc.index,
            y=entry_band,
            name=f"{config.entry_period}-day high (entry)",
            line={"color": "#00D4AA", "width": 1.5},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=ohlc.index,
            y=exit_band,
            name=f"{config.exit_period}-day low (exit)",
            line={"color": "#ffc107", "width": 1.5},
        )
    )
    position = next(
        (p for p in db.get_portfolio_with_shares() if p["ticker"] == ticker.upper()),
        None,
    )
    if position and position["shares"] > 0 and position["cost_basis"]:
        status = evaluate_position(
            ticker,
            full,
            position["shares"],
            position["cost_basis"] / position["shares"],
            position["added_at"] or datetime.utcnow(),
            DEFAULT_TREND_EQUITY,
            config,
        )
        fig.add_hline(
            y=status.stop_price,
            line={"color": "#dc3545", "dash": "dash"},
            annotation_text=f"{config.stop_n:g}N stop {status.stop_price:.2f}",
        )
        fig.add_hline(
            y=status.entry_price,
            line={"color": "#aaa", "dash": "dot"},
            annotation_text=f"Entry {status.entry_price:.2f}",
        )
    fig.update_layout(
        template="plotly_dark",
        height=480,
        xaxis_rangeslider_visible=False,
        margin={"l": 40, "r": 20, "t": 30, "b": 30},
        legend={"orientation": "h"},
    )
    return jsonify({"success": True, "plot_data": fig.to_json()})


@app.route("/api/trend/backtest")
def api_trend_backtest():
    config = _turtle_config_from_request()
    starting_equity = _float_arg("equity", DEFAULT_TREND_EQUITY)
    histories = {
        t: h.loc[BACKTEST_START:] for t, h in get_histories(_trend_watchlist()).items()
    }
    if not histories:
        return jsonify(
            {"success": False, "error": "No price history for the watchlist"}
        )
    result = backtest(histories, config, starting_equity)
    curve = result.equity
    benchmark = get_daily_ohlc(BENCHMARK_TICKER).loc[curve.index[0] :, "close"]
    yearly = curve.resample("YE").last()
    yearly_returns = yearly.pct_change()
    yearly_returns.iloc[0] = yearly.iloc[0] / starting_equity - 1
    trades = sorted(result.trades, key=lambda t: t.exit_date, reverse=True)
    return jsonify(
        _json_safe(
            {
                "success": True,
                "stats": result.stats,
                "tickers": len(histories),
                "equity": {"dates": list(curve.index), "values": list(curve.values)},
                "benchmark": {
                    "dates": list(benchmark.index),
                    "values": list(
                        benchmark.values / benchmark.iloc[0] * starting_equity
                    ),
                },
                "yearly": [
                    {"year": d.year, "return": r} for d, r in yearly_returns.items()
                ],
                "trades": [
                    {**t.__dict__, "reason": t.reason.value if t.reason else "open"}
                    for t in trades[:100]
                ],
            }
        )
    )


# ═══════════════════════════════════════════════════════════════════
# TRADE REVIEW (broker CSV import)
# ═══════════════════════════════════════════════════════════════════


def _history_or_none(symbol: str) -> Optional[pd.DataFrame]:
    try:
        return get_daily_ohlc(symbol)
    except Exception:
        logger.exception(
            f"No price history for {symbol}; its trades are reviewed without Turtle replay"
        )
        return None


@app.route("/review")
def review():
    """Review of closed trades imported from broker CSV exports."""
    return render_template("review.html")


@app.route("/api/review/import", methods=["POST"])
def api_review_import():
    results = []
    account = request.form.get("account", "").strip() or None
    for upload in request.files.getlist("files"):
        try:
            text = upload.read().decode("utf-8-sig", errors="replace")
            fills = parse_export(text, upload.filename or "", account=account)
            added = db.add_broker_fills(fills)
            results.append({"file": upload.filename, "fills": len(fills), "new": added})
        except (UnsupportedFormatError, StopIteration, KeyError, ValueError) as e:
            logger.warning(f"Could not import {upload.filename}: {e}")
            results.append(
                {"file": upload.filename, "error": str(e) or "Unrecognised CSV layout"}
            )
    return jsonify(
        {"success": True, "files": results, "total_fills": len(db.get_broker_fills())}
    )


@app.route("/api/review/accounts")
def api_review_accounts():
    return jsonify(
        {
            "success": True,
            "accounts": sorted({f.account for f in db.get_broker_fills()}),
        }
    )


@app.route("/api/review/fills", methods=["DELETE"])
def api_review_clear():
    db.delete_broker_fills()
    return jsonify({"success": True})


@app.route("/api/review/summary")
def api_review_summary():
    fills = db.get_broker_fills()
    if not fills:
        return jsonify({"success": True, "empty": True})
    report = run_review(fills, _history_or_none, _turtle_config_from_request())
    return jsonify(
        _json_safe(
            {
                "success": True,
                "empty": False,
                "fills": len(fills),
                "summary": report.summary,
                "trades": [r.to_dict() for r in report.reviews],
                "open_positions": [
                    {
                        **o.__dict__,
                        "direction": o.direction.value,
                        "entry_date": o.entry_date.isoformat(),
                    }
                    for o in report.open_positions
                ],
            }
        )
    )
