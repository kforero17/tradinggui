# TradingGui

Personal trading dashboard combining trend-following recommendations (long and short), multi-account portfolio tracking, stock screening and crypto trend analysis in a Flask web app backed by SQLite. Scheduled jobs refresh the data weekly and re-check positions every weekday, and send a digest.

## Architecture

```
                         ┌──────────────────────┐
                         │   Flask Web App       │
                         │   http://127.0.0.1:8051│
                         │                       │
                         │  /          Today      │
                         │  /portfolio Portfolio  │
                         │  /trend     Trend      │
                         │  /review    Trade Review│
                         │  ...        (see Tabs) │
                         └────────┬──────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                     │
     ┌────────▼────────┐ ┌───────▼────────┐  ┌────────▼────────┐
     │  yfinance        │ │  SQLite DB     │  │  MCP Crypto     │
     │  (bars, quotes,  │ │  (persistence) │  │  (Claude-driven │
     │   fundamentals)  │ │                │  │   data ingest)  │
     └─────────────────┘ └────────────────┘  └─────────────────┘
              ▲                   ▲
              └──── python -m src.jobs weekly|daily (launchd) ────┘
```

## Jobs

Two scheduled jobs share an exclusive file lock (`data/.job.lock`), so they never overlap. The Today page's **Recheck holdings** button takes the same lock and answers "A scheduled job is running; try again shortly" while a job runs.

```bash
# Weekly: universe files -> bars and technicals -> rotating fundamentals -> alerts
#         -> recommendations with an entry scan -> digest (always notifies)
.venv/bin/python -m src.jobs weekly [--tickers A,B | --limit N] [--no-notify]

# Weekday: holdings and the latest ENTER/WATCH list on refreshed bars -> digest
.venv/bin/python -m src.jobs daily [--no-notify]

# Legacy entry point: the full weekly job
.venv/bin/python -m src.main
```

- **weekly** refreshes the universe CSVs, updates the bar cache and technical metrics for the universe plus holdings in batches of 500, fetches the fundamentals that are due, evaluates alerts, stores a recommendation run that scans for new entries, then writes the HTML digest and sends it.
- `--tickers A,B` or `--limit N` (mutually exclusive) make a smoke run on a subset, plus holdings: the universe-file refresh is skipped and only the subset (plus holdings and the watchlist) is scanned for entries.
- **daily** stores a run that re-evaluates positions (stops, channel exits, add-on units) and the latest ENTER/WATCH rows on freshly downloaded bars. The report is always written; it notifies only when something is actionable: an EXIT_STOP, EXIT_CHANNEL or ADD_UNIT row, or an ENTER that is new since the previous run. Rows already marked Taken or Skipped in the journal don't notify; undecided exits keep notifying every day.
- `--no-notify` writes the report but skips notifications.

Exit codes for `python -m src.jobs`: `0` success, `1` error, `2` another job holds the lock (argparse also uses `2` for bad arguments). `python -m src.main` exits `1` on missing ticker files, a held lock or an error.

### Notifications

The digest goes to `data/reports/digest_<job>_<YYYY-MM-DD>.html`, a macOS notification (`osascript`) and, if `SLACK_WEBHOOK_URL` is set, a Slack webhook. Delivery is best-effort: a failed channel is logged and doesn't fail the job.

## Scheduling (macOS launchd)

```bash
scripts/install_launchd.sh              # install or reinstall both LaunchAgents
scripts/install_launchd.sh --uninstall  # remove them
```

The script needs `.venv/bin/python`. It fills the repo path into the templates in `deploy/launchd/` and bootstraps them into `~/Library/LaunchAgents`:

| Label | Schedule (local time) | Command |
|---|---|---|
| `com.tradinggui.weekly` | Saturday 06:00 | `python -m src.jobs weekly` |
| `com.tradinggui.daily` | Monday to Friday 16:30 | `python -m src.jobs daily` |

A run missed while the Mac sleeps starts on wake. Logs go to `logs/weekly.out.log`, `logs/weekly.err.log`, `logs/daily.out.log`, `logs/daily.err.log`, and `logs/trading.log` (rotated daily, kept 7 days).

```bash
launchctl print gui/$(id -u)/com.tradinggui.weekly | grep -E 'state|last exit|runs'
launchctl print gui/$(id -u)/com.tradinggui.daily | grep -E 'state|last exit|runs'
launchctl kickstart -k gui/$(id -u)/com.tradinggui.daily   # run one now
```

## Data

- **Universe**: the weekly job downloads all US listings from the NASDAQ Trader symbol directory (`nasdaqlisted.txt` and `otherlisted.txt`) into `data/raw/us_listed.csv`, and the S&P 500 members from Wikipedia into `data/raw/sp500.csv`. A failed download, or fewer rows than `UNIVERSE_MIN_LISTED_ROWS` / `UNIVERSE_MIN_SP500_ROWS`, keeps the previous file. ETFs are included (`UNIVERSE_INCLUDE_ETFS`). Until the first refresh, the legacy `data/raw/nasdaq.csv` is used instead of `us_listed.csv`.
- **Recommendation universe**: liquid listings (last price at least `UNIVERSE_MIN_PRICE`, 50-day average dollar volume at least `UNIVERSE_MIN_DOLLAR_VOLUME`) plus holdings and the Trend watchlist. Leveraged and inverse ETFs are excluded by name (`UNIVERSE_EXCLUDE_LEVERAGED_ETFS`), because their daily reset breaks long-run trend rules.
- **Bar cache**: split-adjusted daily OHLC per ticker in `data/cache/ohlc/<TICKER>.csv`. The first download goes back to 2015-01-01; later runs download only the bars after the last cached one, 100 tickers per `yf.download` call. A new split re-downloads that ticker's full history.
- **Fundamentals** (`yfinance` `.info`) rotate instead of being fetched every run: holdings always, plus liquid non-ETF stocks whose last fetch is `FUNDAMENTALS_MAX_AGE_DAYS` or more days old, oldest first, at most `FUNDAMENTALS_MAX_PER_RUN` per run (holdings are never cut). The rest is deferred to later runs.
- **Dead-ticker skip list**: a ticker that fails `TICKER_MAX_CONSECUTIVE_FAILURES` runs in a row (no bars, stale bars or too little history) is skipped for `TICKER_SKIP_DAYS` days, then retried. Holdings are never skipped. Outcomes are in the `ticker_status` table.
- **Database**: `data/processed/stock_metrics_v2.db`. The migration that added accounts first wrote a backup to `data/processed/stock_metrics_v2.db.pre-accounts.bak`.
- **Reports**: HTML digests in `data/reports/`.

## Data Pipeline

### Crypto Data (Claude-driven)

The crypto pipeline has no autonomous scheduler. It requires Claude (or another agent) with MCP crypto market data tools:

1. Claude calls `get_ticker` and `get_candlestick` for each of the 48 configured USD pairs
2. Claude POSTs the raw response to `POST /api/crypto/store`
3. `CryptoMetricsCalculator` computes MA/EMA from candlestick data and stores results
4. Data is considered fresh for 5 minutes (configurable)

### Portfolio Data

Managed interactively through the web UI. Users add long or short stock positions and option positions, update shares/cost basis, and record sales, covers and option closes. Realized gains are tracked with YTD calculations.

## Data Model

SQLite database at `data/processed/stock_metrics_v2.db`. The main tables are below. The trend-following tables are `accounts` (name, equity, cash, `allow_short`), `recommendation_runs` and `recommendations` (one stored run per job, with its rows), `signal_decisions` (the journal: taken, skipped or pending per `signal_key` and `first_seen_at`), `trend_watchlist`, `ticker_status` (the skip list), `broker_fills` (Trade Review imports) and `alerts`.

### `valuation_momentum`

Per-ticker stock metrics (one row per ticker, upserted on refresh). Among the columns:

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
| `account` (PK) | Account name (`accounts.name`) |
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

From daily closes (the jobs use the cached bars; minimum 100 bars required):

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

### Tabs

| Route | Tab | Description |
|---|---|---|
| `/` | Today | Home page, read from the latest stored run: the action list (exits, add-ons, entries, watch) with copyable order tickets, open risk against the heat budget, the market regime (benchmark vs its 200-day average, breadth), the signal journal (Taken / Skip per row), expired signals, the Accounts editor and **Recheck holdings**. Filters by direction, asset type and account. |
| `/portfolio` | Portfolio | Holdings grouped by account, with period gains (1D/1W/1M/YTD), Plotly price charts and summary totals. Add/sell/cover/update stock positions; add/close option positions. |
| `/research` | Research | Single-stock lookup. Searches DB first, falls back to live yfinance fetch. Shows chart + metrics. |
| `/momentum` | Momentum | Screener table filterable by market cap and % above MA-100, with a bearish view. Click for chart modal. |
| `/trend` | Trend | Turtle rules (System 1 or 2) for open positions, long and short, plus watchlist breakouts and a watchlist backtest since 2015. |
| `/review` | Trade Review | Import E*TRADE and Fidelity exports; closed trades are FIFO-matched per account and replayed under the Turtle rules, with discipline checks. |
| `/crypto` | Crypto | Crypto trends table filterable by 24h volume. Shows MA/EMA deviation and 24h stats. |
| `/analytics` | Analytics | Market heatmap, breadth and movers. |
| `/risk` | Risk | Portfolio risk dashboard (long stock positions). |
| `/screener` | Screener | Cached quant screener results. |
| `/alerts` | Alerts | Create, dismiss and delete alerts (price above/below, RSI, MACD crossover, % change); the weekly job evaluates them. |

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
python -m venv .venv   # the launchd script expects .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # configure settings
```

For corporate proxy environments, set `DISABLE_SSL_VERIFY=true` in `.env`.

### First-time setup

1. Start the web app and open **Today → Accounts**. Create one account per brokerage account and enter its equity and cash: for example a Roth IRA with **Allow short** off and a taxable account with it on. Sizing and the heat budget use the total equity across accounts.
2. The DB seeds a `default` account (`DEFAULT_ACCOUNT`) that **allows shorts**, so existing positions have a home. Rename it to one of your real accounts (its positions move with it), or delete it once it holds no positions.
3. Run one full weekly job to warm the cache: `.venv/bin/python -m src.jobs weekly --no-notify`. The first run downloads daily history since 2015 for every ticker in the universe, so it takes much longer than later runs.
4. Install the schedule: `scripts/install_launchd.sh`.

## Usage

```bash
# Start the web dashboard
python run_web_app.py

# Run the jobs by hand (see Jobs)
python -m src.jobs weekly
python -m src.jobs daily

# Check crypto DB status
python refresh_crypto.py

# Desktop DB browser (PyQt6)
python src/run_browser.py
```

## Configuration

Settings are managed via `pydantic-settings` and can be overridden in `.env`:

| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `data/processed/stock_metrics_v2.db` | SQLite path |
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

Trend-following and job settings (`src/config/settings.py`, also overridable in `.env`):

| Variable | Default | Description |
|---|---|---|
| `UNIVERSE_INCLUDE_ETFS` | `true` | Keep ETFs in the universe |
| `UNIVERSE_MIN_PRICE` | `5.0` | Minimum last price to be liquid |
| `UNIVERSE_MIN_DOLLAR_VOLUME` | `10000000` | Minimum 50-day average dollar volume |
| `UNIVERSE_EXCLUDE_LEVERAGED_ETFS` | `true` | Drop leveraged and inverse ETFs from recommendations |
| `UNIVERSE_MIN_LISTED_ROWS` / `UNIVERSE_MIN_SP500_ROWS` | `5000` / `490` | Sanity floors for the universe refresh |
| `TREND_SYSTEM` | `S2` | Turtle system: `S1` (20-day breakouts, 10-day exits) or `S2` (55-day, 20-day) |
| `TREND_RISK_PER_UNIT` | `0.005` | Equity risked per unit (0.5%) |
| `TREND_MAX_UNIT_FRACTION` | `0.10` | Cap on one unit's value as a fraction of equity |
| `MAX_PORTFOLIO_HEAT_PCT` | `0.10` | Heat budget: total open risk as a fraction of equity |
| `MAX_WATCH_ROWS` | `30` | WATCH rows shown on Today (ENTER rows are never capped) |
| `REGIME_BENCHMARK` | `SPY` | Benchmark for the market regime |
| `FUNDAMENTALS_MAX_AGE_DAYS` | `28` | Refetch fundamentals older than this |
| `FUNDAMENTALS_MAX_PER_RUN` | `1000` | Fundamentals fetched per run, beyond holdings |
| `FUNDAMENTALS_REQUESTS_PER_SECOND` | `1.0` | Rate limit for `.info` calls |
| `DOWNLOAD_CHUNK_SIZE` | `100` | Tickers per batched bar download |
| `TICKER_MAX_CONSECUTIVE_FAILURES` / `TICKER_SKIP_DAYS` | `3` / `28` | Skip-list threshold and duration |
| `DEFAULT_ACCOUNT` | `default` | Name of the seeded account |
| `SLACK_WEBHOOK_URL` | unset | Slack incoming webhook for the digest (set in the environment or `.env`) |

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
