# TradingGui

Personal trading dashboard combining stock screening, portfolio tracking, and crypto trend analysis in a Flask web app backed by SQLite.

## Architecture

```
                         ┌──────────────────────┐
                         │   Flask Web App       │
                         │   http://127.0.0.1:8051│
                         │                       │
                         │  /          Portfolio  │
                         │  /research  Stock Lookup│
                         │  /momentum  Screener   │
                         │  /crypto    Crypto Trends│
                         └────────┬──────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                     │
     ┌────────▼────────┐ ┌───────▼────────┐  ┌────────▼────────┐
     │  yfinance        │ │  SQLite DB     │  │  MCP Crypto     │
     │  (live quotes +  │ │  (persistence) │  │  (Claude-driven │
     │   400d history)  │ │                │  │   data ingest)  │
     └─────────────────┘ └────────────────┘  └─────────────────┘
```

## Data Pipeline

### Stock Data (batch)

Run `python -m src.main` to populate the database:

1. `TickerLoader` reads and deduplicates tickers from S&P 500 and NASDAQ CSVs
2. Tickers are batched (50 per group), each checked for freshness (skip if updated within 1 day)
3. `StockMetricsCalculator` fetches 400 days of daily OHLCV via `yfinance` (fallback: `stockdex`) plus fundamentals from `yfinance.Ticker.info`
4. Momentum + valuation metrics are calculated and upserted into SQLite
5. Rate limited to 0.5 req/s with exponential backoff retries (3 workers max)

### Crypto Data (Claude-driven)

The crypto pipeline has no autonomous scheduler. It requires Claude (or another agent) with MCP crypto market data tools:

1. Claude calls `get_ticker` and `get_candlestick` for each of the 48 configured USD pairs
2. Claude POSTs the raw response to `POST /api/crypto/store`
3. `CryptoMetricsCalculator` computes MA/EMA from candlestick data and stores results
4. Data is considered fresh for 5 minutes (configurable)

### Portfolio Data

Managed interactively through the web UI. Users add long or short stock positions and option positions, update shares/cost basis, and record sales, covers and option closes. Realized gains are tracked with YTD calculations.

## Data Model

SQLite database at `data/processed/stock_metrics.db` with these tables:

### `valuation_momentum`

Per-ticker stock metrics (one row per ticker, upserted on refresh):

| Column | Description |
|---|---|
| `ticker` (PK) | Symbol |
| `last_price` | Most recent close |
| `ma_100` / `ema_100` | 100-day simple / exponential moving average |
| `pct_above_ma_100` / `pct_above_ema_100` | % deviation from MA/EMA |
| `pe_ratio`, `pb_ratio`, `ps_ratio` | Trailing valuation ratios |
| `market_cap`, `enterprise_value`, `ebitda` | Fundamentals from Yahoo |
| `ebitda_ev` | EV / EBITDA (calculated) |
| `updated_at` | Last refresh timestamp (UTC) |

### `portfolio`

| Column | Description |
|---|---|
| `ticker` (PK) | Symbol |
| `shares` | Quantity held; negative for a short |
| `cost_basis` | Total cost, or total short-sale proceeds for a short (not per-share) |
| `added_at` | Position open date |

### `option_positions`

| Column | Description |
|---|---|
| `id` (PK) | UUID |
| `underlying` | Underlying symbol |
| `option_type` | `call` or `put` |
| `strike`, `expiry` | Contract terms |
| `contracts` | Contract count; negative for written options (100 shares per contract) |
| `cost_basis` | Total premium paid, or received for written options |
| `added_at` | Position open date |

### `realized_gains`

| Column | Description |
|---|---|
| `id` (PK) | UUID |
| `ticker`, `shares_sold` | What was sold. Negative `shares_sold` is a short cover or a written option bought back. Options use the OCC symbol (e.g. `SPCX270115P00140000`) and `shares_sold` = contracts × 100 |
| `cost_basis`, `sale_price` | Per-share cost and sale price (per-share premium for options) |
| `year_start_price` | Jan 1 price (for YTD gain calc) |
| `realized_gain` | `(sale_price - year_start_price) * shares` |
| `sold_at` | Sale timestamp |

### `crypto_metrics`

| Column | Description |
|---|---|
| `instrument` (PK) | e.g. "BTCUSD-PERP" |
| `last_price`, `price_change_24h` | Spot price and 24h % change |
| `high_24h`, `low_24h`, `volume_24h` | 24h range and volume |
| `ma_50` / `ema_50` | 50-period moving averages (from candlestick data) |
| `pct_above_ma_50` / `pct_above_ema_50` | % deviation from MA/EMA |
| `timeframe` | Candle interval (default "1D") |
| `updated_at` | Last refresh timestamp |

## Metrics Calculations

### Stock Momentum

From 400 days of daily closes (minimum 100 data points required):

- **MA-100**: Simple rolling mean over last 100 closes
- **EMA-100**: Exponential weighted mean (span=100)
- **% Above MA/EMA**: `(last_price - avg) / avg * 100`

### Stock Valuation

Sourced from `yfinance.Ticker.info`: trailing P/E, P/B, P/S, market cap, EV, EBITDA. ETFs only get market cap. `ebitda_ev` = `EV / EBITDA` when both are available and EBITDA > 0.

### Portfolio Period Gains

Calculated live per page load using yfinance historical data:

| Period | Reference Price |
|---|---|
| 1D | Previous trading day close |
| 1W | Close from 5 trading days ago |
| 1M | First close on or after the 1st of current month |
| YTD | First close on or after Jan 1 (or `added_at` if later) |

Each returns dollar gain (`price_change * shares`) and percent gain. For a short, shares are negative, so a falling price is a gain and the percent gain is inverted.

### Option Positions

Each page load fetches the contract from the yfinance option chain. The mark is the bid/ask midpoint, or the last trade when either side is zero. Market value is `mark * contracts * 100`, unrealized gain is measured against the opening premium, and day gain uses the chain's `change` field. YTD gain for an option is its unrealized gain since open, so it overstates YTD for an option opened before Jan 1. A contract missing from the chain (for example, after expiry) shows "Quote unavailable" and is left out of the totals; close it at its final value (0 if it expired worthless).

**Portfolio summary** aggregates stock and option positions into net market value, day gain, and YTD gain (unrealized + realized from `realized_gains` table). Day and YTD percentages are measured against gross exposure (the sum of absolute position values), so shorts don't shrink the base. The portfolio risk overview weights positions by gross exposure, with shorts carrying negative weights. Options are not included in the risk overview, and the `/risk` dashboard covers long stock positions only.

### Crypto Metrics

From MCP `get_candlestick` (max 50 candles) + `get_ticker`:

- **MA-50 / EMA-50**: Computed from 50 candle closes (the MCP limit means MA period = candle count)
- **24h stats**: price, change %, high, low, volume from ticker data

## Web App

Start with `python run_web_app.py` (default: `http://127.0.0.1:8051`).

### Pages

| Route | Page | Description |
|---|---|---|
| `/` | Portfolio | All holdings with period gains (1D/1W/1M/YTD), Plotly price charts, summary totals. Add/sell/cover/update stock positions; add/close option positions. |
| `/research` | Research | Single-stock lookup. Searches DB first, falls back to live yfinance fetch. Shows chart + metrics. |
| `/momentum` | Momentum | Screener table filterable by market cap and % above MA-100. Click for chart modal. |
| `/crypto` | Crypto | Crypto trends table filterable by 24h volume. Shows MA/EMA deviation and 24h stats. |

### API Endpoints

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/api/crypto/store` | Accepts MCP ticker + candlestick data, computes metrics, stores |
| `GET` | `/api/crypto/symbols` | Returns configured symbol list, MA period, timeframe |
| `GET` | `/get_stock_plot/<ticker>` | Returns Plotly JSON + metrics for modal chart |
| `GET` | `/get_crypto_plot/<instrument>` | Returns crypto metrics for modal |

Charts use Plotly (`plotly_dark` theme). Portfolio/research charts are rendered server-side as HTML; momentum/crypto modals are rendered client-side from JSON.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # configure settings
```

For corporate proxy environments, set `DISABLE_SSL_VERIFY=true` in `.env`.

## Usage

```bash
# Start the web dashboard
python run_web_app.py

# Batch-refresh stock data (S&P 500 + NASDAQ)
python -m src.main

# Check crypto DB status
python refresh_crypto.py

# Desktop DB browser (PyQt6)
python src/run_browser.py
```

## Configuration

Settings are managed via `pydantic-settings` and can be overridden in `.env`:

| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `data/processed/stock_metrics.db` | SQLite path |
| `HISTORICAL_LOOKBACK_DAYS` | `400` | Days of stock history to fetch |
| `RECENT_DATA_AGE_LIMIT_DAYS` | `1` | Stock freshness threshold |
| `MAX_CONCURRENT_WORKERS` | `3` | Parallel fetch workers |
| `REQUESTS_PER_SECOND` | `0.5` | yfinance rate limit |
| `CRYPTO_MA_PERIOD` | `50` | Crypto MA window |
| `CRYPTO_DEFAULT_TIMEFRAME` | `1D` | Candle interval |
| `CRYPTO_DATA_AGE_LIMIT_MINUTES` | `5` | Crypto freshness threshold |
| `WEB_APP_HOST` | `127.0.0.1` | Flask bind address |
| `WEB_APP_PORT` | `8051` | Flask port |
| `DISABLE_SSL_VERIFY` | `false` | Bypass SSL (for corporate proxies) |
| `FLASK_SECRET_KEY` | auto-generated | Session secret key |

## Dependencies

Core: `Flask`, `SQLAlchemy`, `pandas`, `yfinance`, `plotly`, `pydantic-settings`, `loguru`, `tenacity`

Optional: `PyQt6` (desktop browser), `stockdex` (yfinance fallback), `gunicorn` (production server)

### Crypto Trends live data

Open `/crypto` to start the public Crypto.com market feed. No API key is needed.
The server discovers USD spot pairs from the ticker feed and saves quotes every
60 seconds while the server remains running, including when the tab is closed.
The browser checks saved quotes every 15 seconds while visible. Each app worker
runs its own feed; a single worker avoids duplicate provider requests.

Search symbols, filter by USD volume, or sort by volume and 24-hour movers.
Select a market for a 180-candle hourly or daily price chart with 50-period
simple and exponential moving averages. Historical candles are cached for five
minutes and refreshed while the page is visible. USD volume uses the provider's
quote-currency turnover, not the number of coins traded.

Quote timestamps show when the server received data. Quotes older than three
minutes are marked stale. Provider failures retain saved quotes and cached
charts, display an error, and retry automatically. Chart history is cached in
memory and reloaded after a server restart. Public data source:
[Crypto.com Exchange API](https://exchange-developer.crypto.com/exchange/v1).
