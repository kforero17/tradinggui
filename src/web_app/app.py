from typing import Optional
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from loguru import logger
import plotly.graph_objects as go
import plotly.io as pio

from ..data.database import db
from ..analysis.metrics import metrics_calculator
from ..config.security import security_config

app = Flask(__name__)
app.secret_key = security_config.get_secret_key()

def _build_stock_figure(hist_data: pd.DataFrame, ticker: str) -> Optional[go.Figure]:
    """Build a Plotly figure for a stock's historical data."""
    if hist_data is None or hist_data.empty:
        logger.warning(f"No data provided for {ticker} plot")
        return None

    if 'close' not in hist_data.columns:
        logger.error(f"Missing 'close' column in data for {ticker}")
        return None

    close_prices = hist_data['close'].dropna()
    if close_prices.empty:
        logger.error(f"No valid close prices for {ticker}")
        return None

    dates = [d.strftime('%Y-%m-%d') for d in close_prices.index]
    prices = close_prices.values.tolist()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=dates,
        y=prices,
        mode='lines',
        name='Close Price',
        line=dict(color='#00D4AA', width=2)
    ))

    if len(close_prices) >= 100:
        ma_100 = close_prices.rolling(window=100).mean().dropna()
        ma_dates = [d.strftime('%Y-%m-%d') for d in ma_100.index]
        fig.add_trace(go.Scatter(
            x=ma_dates,
            y=ma_100.values.tolist(),
            mode='lines',
            name='100-Day MA',
            line=dict(color='#FF6B6B', width=1.5)
        ))

        ema_100 = close_prices.ewm(span=100, adjust=False).mean().dropna()
        ema_dates = [d.strftime('%Y-%m-%d') for d in ema_100.index]
        fig.add_trace(go.Scatter(
            x=ema_dates,
            y=ema_100.values.tolist(),
            mode='lines',
            name='100-Day EMA',
            line=dict(color='#4ECDC4', width=1.5)
        ))

    fig.update_layout(
        title=f'{ticker.upper()} Price Action',
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        template='plotly_dark',
        height=500,
        margin=dict(t=50, b=50, l=50, r=50)
    )

    return fig


def create_stock_plot(hist_data: pd.DataFrame, ticker: str) -> Optional[str]:
    """Generate an HTML plot for embedding in pages (includes Plotly CDN)."""
    try:
        fig = _build_stock_figure(hist_data, ticker)
        if fig is None:
            return None

        plot_html = pio.to_html(
            fig,
            full_html=False,
            include_plotlyjs='cdn',
            config={'displayModeBar': True, 'responsive': True}
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

def filter_momentum_stocks(min_market_cap: float = 2e9, min_momentum_pct: float = 0.0) -> pd.DataFrame:
    """Filter stocks based on momentum and market cap criteria."""
    try:
        df = db.get_latest_metrics()
        if df.empty:
            return df
        
        # Apply filters
        filtered_df = df[
            (df['market_cap'].notna()) & 
            (df['market_cap'] >= min_market_cap) &
            (df['pct_above_ma_100'].notna()) & 
            (df['pct_above_ma_100'] >= min_momentum_pct)
        ].copy()
        
        # Sort by momentum percentage (descending)
        filtered_df = filtered_df.sort_values('pct_above_ma_100', ascending=False)
        
        return filtered_df
    except Exception as e:
        logger.error(f"Error filtering momentum stocks: {e}")
        return pd.DataFrame()

@app.route('/momentum')
def momentum():
    """Momentum stocks page."""
    try:
        # Get filter parameters from query string
        min_market_cap = float(request.args.get('min_market_cap', 2e9))  # Default $2B
        min_momentum_pct = float(request.args.get('min_momentum_pct', 0.0))  # Default 0%
        
        # Filter stocks
        momentum_stocks = filter_momentum_stocks(min_market_cap, min_momentum_pct)
        
        # Convert to list of dictionaries for template
        stocks_list = []
        if not momentum_stocks.empty:
            stocks_list = momentum_stocks.to_dict('records')
        
        return render_template(
            'momentum.html', 
            stocks=stocks_list,
            min_market_cap=min_market_cap,
            min_momentum_pct=min_momentum_pct,
            total_count=len(stocks_list)
        )
    except Exception as e:
        logger.error(f"Error loading momentum page: {e}")
        flash(f"An error occurred: {e}", "error")
        return render_template('momentum.html', stocks=[], min_market_cap=2e9, min_momentum_pct=0.0, total_count=0)

@app.route('/get_stock_plot/<ticker>')
def get_stock_plot(ticker: str):
    """API endpoint to get stock plot for modal display."""
    try:
        logger.info(f"==== Starting get_stock_plot for {ticker} ====")

        logger.info(f"Step 1: Fetching historical data for {ticker}")
        try:
            hist_data = metrics_calculator._get_historical_data(ticker)
            logger.info(f"Historical data fetch completed. Data is None: {hist_data is None}")
            if hist_data is not None:
                logger.info(f"Historical data is empty: {hist_data.empty}")
        except Exception as fetch_error:
            logger.error(f"Exception while fetching historical data: {fetch_error}", exc_info=True)
            return jsonify({'success': False, 'error': f'Failed to fetch historical data: {str(fetch_error)}'})

        if hist_data is None or hist_data.empty:
            logger.warning(f"No historical data available for {ticker}")
            return jsonify({'success': False, 'error': f'No historical data available for {ticker}'})

        logger.info(f"Historical data shape for {ticker}: {hist_data.shape}")
        logger.info(f"Historical data columns: {list(hist_data.columns)}")
        logger.info(f"Date range: {hist_data.index.min()} to {hist_data.index.max()}")

        logger.info(f"Step 2: Creating plot for {ticker}")
        try:
            plot_data = create_stock_plot_json(hist_data, ticker)
            logger.info(f"Plot creation completed. Plot is None: {plot_data is None}")
        except Exception as plot_error:
            logger.error(f"Exception while creating plot: {plot_error}", exc_info=True)
            return jsonify({'success': False, 'error': f'Failed to create plot: {str(plot_error)}'})

        if not plot_data:
            logger.error(f"Failed to generate plot data for {ticker}")
            return jsonify({'success': False, 'error': 'Failed to generate plot'})

        logger.info(f"Step 3: Fetching metrics for {ticker}")
        try:
            metrics_df = db.get_latest_metrics(ticker=ticker)
            logger.info(f"Metrics fetch completed. DataFrame is empty: {metrics_df.empty}")
        except Exception as metrics_error:
            logger.error(f"Exception while fetching metrics: {metrics_error}", exc_info=True)
            metrics_df = pd.DataFrame()

        metrics = None
        if not metrics_df.empty:
            metrics = metrics_df.iloc[0].to_dict()
            logger.info(f"Found metrics for {ticker}: {list(metrics.keys())}")
        else:
            logger.warning(f"No metrics found in database for {ticker}")

        logger.success(f"Successfully generated response for {ticker}")
        response_data = {
            'success': True,
            'plot_data': plot_data,
            'metrics': metrics
        }
        logger.info(f"Response keys: {list(response_data.keys())}")
        return jsonify(response_data)
    except Exception as e:
        logger.error(f"Unexpected error in get_stock_plot for {ticker}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'Unexpected error: {str(e)}'})

@app.route('/research', methods=['GET', 'POST'])
def research():
    """Stock research page."""
    stock_data = None
    ticker = ""

    if request.method == 'POST':
        ticker = request.form.get('ticker', '').upper().strip()
        if not ticker:
            flash("Ticker symbol cannot be empty.", "warning")
            return redirect(url_for('research'))
    elif request.method == 'GET':
        ticker = request.args.get('ticker', '').upper().strip()

    if ticker:
        try:
            # 1. Try to get data from the database first
            metrics_df = db.get_latest_metrics(ticker=ticker)
            
            if not metrics_df.empty:
                logger.info(f"Found {ticker} data in the database.")
                stock_data = metrics_df.iloc[0].to_dict()
            else:
                # 2. If not in DB, fetch using the API
                logger.info(f"No data for {ticker} in DB. Fetching in real-time...")
                fetched_metrics = metrics_calculator.get_metrics(ticker)
                
                if fetched_metrics:
                    stock_data = fetched_metrics
                    if request.method == 'POST':
                        flash(f"Fetched real-time data for {ticker}.", "success")
                elif request.method == 'POST':
                    flash(f"Could not fetch data for {ticker}. It might be an invalid symbol.", "error")

            # 3. If we have data, get historical data for plot
            if stock_data:
                hist_data = metrics_calculator._get_historical_data(ticker)
                stock_data['plot'] = create_stock_plot(hist_data, ticker)

        except Exception as e:
            logger.error(f"Error during research for {ticker}: {e}", exc_info=True)
            flash(f"An error occurred while researching {ticker}.", "error")

    portfolio_tickers = db.get_portfolio_tickers()
    return render_template('research.html', stock_data=stock_data, ticker=ticker, portfolio_tickers=portfolio_tickers)

@app.route('/')
def portfolio():
    """Main portfolio page."""
    try:
        tickers = db.get_portfolio_tickers()
        
        portfolio_metrics = []
        if tickers:
            all_metrics_df = db.get_latest_metrics()
            
            # Ensure all_metrics_df has a 'ticker' column to check against
            if 'ticker' in all_metrics_df.columns:
                portfolio_df = all_metrics_df[all_metrics_df['ticker'].isin(tickers)]
                
                for _, row in portfolio_df.iterrows():
                    stock_data = row.to_dict()
                    
                    # Generate plot for each stock
                    hist_data = metrics_calculator._get_historical_data(row['ticker'])
                    stock_data['plot'] = create_stock_plot(hist_data, row['ticker'])
                    
                    portfolio_metrics.append(stock_data)
            else:
                 flash("Metrics table is empty or does not contain a 'ticker' column.", "warning")

        return render_template('portfolio.html', portfolio=portfolio_metrics)
    except Exception as e:
        logger.error(f"Error loading portfolio page: {e}")
        flash(f"An error occurred: {e}", "error")
        return render_template('portfolio.html', portfolio=[])

@app.route('/add_stock', methods=['POST'])
def add_stock():
    """Add a stock to the portfolio."""
    ticker = request.form.get('ticker', '').upper().strip()
    source_page = request.form.get('source_page', 'portfolio')

    if not ticker:
        flash("Ticker symbol cannot be empty.", "warning")
        return redirect(url_for('portfolio'))

    try:
        # Check if ticker is already in portfolio
        if ticker in db.get_portfolio_tickers():
            flash(f"{ticker} is already in your portfolio.", "info")
        else:
            # If not in portfolio, ensure we have data, then add
            if db.get_latest_metrics(ticker=ticker).empty:
                logger.info(f"No data for {ticker} in DB. Fetching before adding to portfolio...")
                metrics = metrics_calculator.get_metrics(ticker)
                if metrics:
                    db.store_metrics([metrics])
                else:
                    flash(f"Could not fetch data for {ticker}. Cannot add to portfolio.", "error")
                    return redirect(url_for('research' if source_page == 'research' else 'portfolio', ticker=ticker))
            
            db.add_portfolio_ticker(ticker)
            flash(f"{ticker} has been added to your portfolio.", "success")

    except Exception as e:
        logger.error(f"Error adding stock {ticker}: {e}")
        flash(f"An error occurred while adding {ticker}.", "error")
    
    if source_page == 'research':
        return redirect(url_for('research', ticker=ticker))
    
    return redirect(url_for('portfolio'))

@app.route('/delete_stock/<ticker>')
def delete_stock(ticker: str):
    """Delete a stock from the portfolio."""
    try:
        db.delete_portfolio_ticker(ticker)
        flash(f"{ticker} has been removed from your portfolio.", "success")
    except Exception as e:
        logger.error(f"Error deleting stock {ticker}: {e}")
        flash(f"An error occurred while removing {ticker}.", "error")
        
    return redirect(url_for('portfolio')) 