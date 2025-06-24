from typing import Optional
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash
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
        return None
    
    fig = go.Figure()
    
    # Price Line
    fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['close'], mode='lines', name='Close Price'))
    
    # 100-day MA
    fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['close'].rolling(window=100).mean(), mode='lines', name='100-Day MA'))
    
    # 100-day EMA
    fig.add_trace(go.Scatter(x=hist_data.index, y=hist_data['close'].ewm(span=100, adjust=False).mean(), mode='lines', name='100-Day EMA'))
    
    fig.update_layout(
        title=f'{ticker.upper()} Price Action',
        xaxis_title='Date',
        yaxis_title='Price (USD)',
        legend_title='Legend',
        template='plotly_dark'
    )
    
    return pio.to_html(fig, full_html=False)

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