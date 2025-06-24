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
    if not ticker:
        flash("Ticker symbol cannot be empty.", "warning")
        return redirect(url_for('portfolio'))

    try:
        # Check if we already have metrics. If not, fetch them.
        if db.get_latest_metrics(ticker=ticker).empty:
            logger.info(f"No data for {ticker} in DB. Fetching...")
            metrics = metrics_calculator.get_metrics(ticker)
            if metrics:
                db.store_metrics([metrics])
                flash(f"Successfully fetched and stored new data for {ticker}.", "success")
            else:
                flash(f"Could not fetch data for {ticker}. It might be an invalid symbol.", "error")
                return redirect(url_for('portfolio'))

        db.add_portfolio_ticker(ticker)
        flash(f"{ticker} has been added to your portfolio.", "success")
    except Exception as e:
        logger.error(f"Error adding stock {ticker}: {e}")
        flash(f"An error occurred while adding {ticker}.", "error")
    
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