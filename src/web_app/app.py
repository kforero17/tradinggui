from typing import Optional
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from loguru import logger
import plotly.graph_objects as go
import plotly.io as pio

from ..data.database import db
from ..analysis.metrics import metrics_calculator

app = Flask(__name__)
app.secret_key = "supersecretkey"  # TODO: Use an environment variable for this

def create_stock_plot(hist_data: pd.DataFrame, ticker: str) -> Optional[str]:
    """Generate an interactive plot for a stock's historical data."""
    if hist_data is None or hist_data.empty:
        logger.warning(f"No data provided for {ticker} plot")
        return None
    
    try:
        fig = go.Figure()
        
        # Ensure we have the required columns
        if 'close' not in hist_data.columns:
            logger.error(f"Missing 'close' column in data for {ticker}")
            return None
        
        # Clean the data
        close_prices = hist_data['close'].dropna()
        if close_prices.empty:
            logger.error(f"No valid close prices for {ticker}")
            return None
        
        # Price Line
        fig.add_trace(go.Scatter(
            x=close_prices.index, 
            y=close_prices, 
            mode='lines', 
            name='Close Price',
            line=dict(color='#00D4AA', width=2)
        ))
        
        # 100-day MA (only if we have enough data)
        if len(close_prices) >= 100:
            ma_100 = close_prices.rolling(window=100).mean().dropna()
            fig.add_trace(go.Scatter(
                x=ma_100.index, 
                y=ma_100, 
                mode='lines', 
                name='100-Day MA',
                line=dict(color='#FF6B6B', width=1.5)
            ))
        
        # 100-day EMA (only if we have enough data)
        if len(close_prices) >= 100:
            ema_100 = close_prices.ewm(span=100, adjust=False).mean().dropna()
            fig.add_trace(go.Scatter(
                x=ema_100.index, 
                y=ema_100, 
                mode='lines', 
                name='100-Day EMA',
                line=dict(color='#4ECDC4', width=1.5)
            ))
        
        fig.update_layout(
            title=f'{ticker.upper()} Price Action',
            xaxis_title='Date',
            yaxis_title='Price (USD)',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            template='plotly_dark',
            height=500,
            margin=dict(t=50, b=50, l=50, r=50)
        )
        
        # Configure plot to include necessary dependencies
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
        logger.info(f"Fetching plot data for {ticker}")
        hist_data = metrics_calculator._get_historical_data(ticker)
        
        if hist_data is None or hist_data.empty:
            logger.warning(f"No historical data available for {ticker}")
            return jsonify({'success': False, 'error': f'No historical data available for {ticker}'})
        
        logger.info(f"Historical data shape for {ticker}: {hist_data.shape}")
        logger.info(f"Historical data columns: {list(hist_data.columns)}")
        logger.info(f"Date range: {hist_data.index.min()} to {hist_data.index.max()}")
        
        plot_html = create_stock_plot(hist_data, ticker)
        
        if plot_html:
            logger.success(f"Successfully generated plot for {ticker}")
            return jsonify({'success': True, 'plot': plot_html})
        else:
            logger.error(f"Failed to generate plot HTML for {ticker}")
            return jsonify({'success': False, 'error': 'Failed to generate plot'})
    except Exception as e:
        logger.error(f"Error generating plot for {ticker}: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

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