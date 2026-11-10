# Plan: Fast weekly metrics + trend-following recommendations + "Today" action dashboard

## Context
- **Refresh is slow and manual.** The full refresh (`python -m src.main`) took 7h18m on its last run (2026-10-06), and nothing schedules it. A global `RateLimiter` (0.5 req/s) is shared by all 3 workers, and each ticker makes 2 calls (`history` and `.info`). There is no incremental cache and no batching, and about 1,300 dead symbols are retried every run.
- **Trend signals are scattered.** `src/analysis/turtle.py` already has good trend logic (`evaluate_position`, `scan_breakouts`, unit limits, correlation). But it only runs per request, on a 3-ticker watchlist, and sizes from an equity number typed into the browser. Its results are never stored.
- **There's no single "what do I do now" view.** Nothing ranks the actions, and you aren't notified when something fires.

**Goal:** a weekly launchd job that refreshes metrics fast and stores a ranked recommendation run, plus a light weekday job for stops, exits and adds. A new **Today** tab shows one action list with order tickets, heat vs budget and a decision journal. A digest notification goes out after each run.

**Your decisions:** launchd scheduler · liquid universe (price ≥ $5, 50d avg $vol ≥ $10M, plus holdings and watchlist) · all 4 extras (action dashboard, stored equity, signal history + journal, digest) · weekly full run + daily stop check.

**Your answers to QUESTIONS.md (2026-11-10):**
- **Q1, shorts:** yes. Short-side trend signals are added, but only accounts with `allow_short` (taxable) get them. The Roth stays long-only.
- **Q2, accounts:** one ticker can be held in several accounts, so `Portfolio` moves to a composite `(account, ticker)` key.
- **Q3, home page:** Today replaces Portfolio at `/`, and Portfolio moves to `/portfolio`.
- **Q4, email:** not needed. The digest uses macOS notifications plus optional Slack.
- **Q7, universe files:** the universe CSVs refresh on a schedule (weekly, as the first step of the weekly job), and the universe now includes **ETFs**.
- **Q5 (heat budget) and Q6 (options in heat):** unanswered. The defaults stay: 10% heat, configurable, and options are shown but not counted in heat.

## Prerequisite: branching
The new code builds on **uncommitted, untracked** work on `feature/coderabbit-config-and-webapp-updates`: turtle.py, indicators.py, price_history.py, trade_review.py, broker_import.py, the trend/review UI and the tests. Branching from `origin/main` would leave all of it out of the PR. So, as the first steps of implementation:
1. Commit the current work in progress on its own branch, via `/commit`.
   - Leave `data/processed/*.db-wal` and `*.db-shm` out: add them to `.gitignore` and run `git rm --cached` (the local files are kept).
2. Create `weekly-trend-recommendations` off that branch.
3. Write `IMPLEMENTATION_SPEC.md`, `PLAN.md`, `ASSUMPTIONS.md` and `QUESTIONS.md` in the repo root from this plan, as the `/code` skill requires.

## Phase A: Fast metrics pipeline
**Target:** the weekly run takes minutes, not hours, once the cache is warm.

1. **Incremental, batched bar cache** in `src/data/price_history.py`. Keep the per-ticker CSV in `data/cache/ohlc/`.
   - **Fetch:** `_read_cache` returns cached bars whatever their age. A new `refresh_histories(tickers, chunk_size=100)` calls `yf.download(chunk, start=<min last bar − 5d>, group_by="ticker", auto_adjust=False, actions=True, threads=True)`. That's one rate-limited call per chunk, so about 67 calls for the whole universe.
   - **Merge:** append the new bars, deduping on the index.
   - **Splits:** if a new bar has `stock_splits > 0`, re-download that ticker's full history, because split-adjusted history changes after a split.
   - **Freshness:** `get_daily_ohlc` serves the cache when its last bar is ≥ the last completed session. Otherwise it refreshes incrementally, so the web app also gets faster.
   - **New settings:** `DOWNLOAD_CHUNK_SIZE` and `BATCH_REQUESTS_PER_SECOND`, separate from the `.info` rate.
2. **Technicals from cached bars.** Add a pure `compute_technical_metrics(ticker, ohlc) -> dict` to `src/analysis/metrics.py`.
   - It reuses the existing `calculate_rsi`, MACD, Bollinger and MA/EMA100 code.
   - `get_metrics_batch` stops calling `_get_historical_data` per ticker.
3. **Rotating fundamentals.** Add `fundamentals_updated_at` to `valuation_momentum` (ad-hoc ALTER, the pattern in `database.py`).
   - `.info` is fetched only for liquid-universe tickers older than `FUNDAMENTALS_MAX_AGE_DAYS` (28), so about a quarter of the universe per week. Holdings are always fetched.
   - It gets its own `FUNDAMENTALS_REQUESTS_PER_SECOND` (1.0) and keeps the existing tenacity retry. Fix `RetryError` so the ticker keeps its technicals instead of being dropped.
4. **Dead-ticker skip list.** New `ticker_status` table: `ticker` (PK), `last_ok`, `consecutive_failures`, `skip_until`.
   - After 3 failures, skip the ticker for 28 days. Holdings are never skipped.
5. **DB writes.**
   - `store_metrics` and `store_crypto_metrics` switch to bulk `sqlite insert().on_conflict_do_update`, replacing row-by-row `session.merge()`.
   - `has_recent_metrics` becomes one `get_recent_tickers(cutoff) -> set[str]` query.
6. **Timing.** Log `perf_counter` timings per stage (download / compute / fundamentals / store) so the speedup can be measured.

## Phase A0: Universe refresh, with ETFs (Q7)
1. **New `src/data/universe_sources.py`:** `refresh_universe_files() -> UniverseRefreshResult`.
   - **US listings:** download `https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` and `otherlisted.txt` (pipe-delimited; both reachable from the sandbox).
     - Drop `Test Issue == "Y"`, the trailing "File Creation Time" row, and warrants, rights and units (by name suffix or symbol pattern).
     - Map class-share dots to yfinance dashes (`BRK.B → BRK-B`).
     - Keep the `ETF` flag and the exchange.
     - Write `data/raw/us_listed.csv` with columns `symbol, name, exchange, is_etf`.
   - **S&P 500:** `pd.read_html` on Wikipedia's "List of S&P 500 companies" (lxml is installed), written to `data/raw/sp500.csv` (`Symbol` column, dots mapped to dashes).
   - **Safety:** write to a temp file, then rename. Sanity-check the row counts (S&P ≥ 490, listings ≥ 5,000). On any failure, keep the previous file and log the error, so a bad download never empties the universe.
2. **`src/data/ticker_loader.py`:**
   - `load_unique_tickers()` reads `us_listed.csv` (falling back to the old `nasdaq.csv` until the first refresh) plus `sp500.csv`.
   - New `load_universe() -> dict[str, TickerInfo]` returns `is_etf`, `exchange` and `in_sp500`.
   - New settings: `LISTED_CSV_PATH`, `UNIVERSE_INCLUDE_ETFS=True`.
3. **ETFs:**
   - Add an `asset_type` column (`stock` / `etf`) to `valuation_momentum`.
   - Fundamentals (`.info`) are skipped for ETFs.
   - ETFs go through the same liquidity filter and trend rules.
   - The Today page gets a Stocks / ETFs / All filter.
4. **Housekeeping:** delete the duplicate `src/data/sp500.csv` and `src/data/nasdaq.csv` if grep confirms nothing reads them.

## Phase B: Recommendation engine
0. **Short side in `src/analysis/turtle.py` (Q1).** Add a `Direction` enum (`LONG` / `SHORT`) and mirror the rules:
   - **Short entry:** close < `donchian_low(entry_period)`. The 55-day failsafe and S1 skip-after-winner rules are mirrored too.
   - **Stop:** entry + 2N.
   - **Channel exit:** `donchian_high(exit_period)`.
   - **Adds:** every ½N below entry.
   - **Trend filter:** EMA25 < EMA350.
   - **`evaluate_position` and `first_exit`:** infer the direction from the sign of `shares`. Negative shares are already the short convention in `Portfolio`.
   - **`scan_breakouts(..., directions=(LONG,))`:** the default keeps current behaviour. `BreakoutSignal` gets a `direction` field.
   - **Backtest:** `TurtleConfig.allow_short=False` by default, so existing backtests are unchanged. When it's True, the backtest also trades shorts.
   - **Tests:** existing turtle tests stay green. New mirrored tests use falling synthetic bars.
1. **Indicators** (`src/analysis/indicators.py`): add `adx(ohlc, 14)`, `rate_of_change(close, days)` and `pct_from_high(ohlc, 252)`. Same style as the existing functions: a DataFrame in, a Series out.
2. **`src/analysis/universe.py`:** `liquid_universe(histories, min_price, min_dollar_volume, lookback=50) -> list[str]`, then union with holdings and the watchlist. Settings: `UNIVERSE_MIN_PRICE=5`, `UNIVERSE_MIN_DOLLAR_VOLUME=10_000_000`.
3. **`src/analysis/recommendations.py`** (new module, fully hinted, `__all__`):
   - **Types:** `RecommendationAction` enum (`EXIT_STOP > EXIT_CHANNEL > ADD_UNIT > ENTER > WATCH > HOLD`, in priority order) and a frozen dataclass `Recommendation`. Its fields:
     - ticker, account, **direction**, action
     - entry/trigger price, stop price, shares, $ risk, % of equity
     - distance_n, trend_score, rs_rank, `reasons: list[str]`, `blocked_by: list[str]`, `is_late`
   - **`build_recommendations(histories, positions, equity, accounts, config, limits)`:**
     - Positions go through the existing `evaluate_position` and map to EXIT_* / ADD_UNIT / HOLD.
     - The universe goes through the existing `scan_breakouts`. Triggered breakouts become ENTER, and those within 1N become WATCH.
     - **Constraints:**
       - Held tickers are left out of ENTER.
       - The existing `unit_limit_breaches` and `correlation_matrix` are run with the prospective unit added; a breach goes into `blocked_by`.
       - Portfolio heat: open risk plus the new unit's risk must stay ≤ `MAX_PORTFOLIO_HEAT_PCT` (default 10%), checked in rank order.
       - **Account routing:**
         - SHORT entries go only to `allow_short` accounts (taxable).
         - LONG entries go to the account whose cash covers the position value, Roth first.
         - Entries no account can fund still appear, but `blocked_by` says "no account with sufficient cash" (or "no short-enabled account").
       - **Shared risk budget:** sizing and heat use **total equity** across accounts, so there's one risk budget. Unit limits count a ticker's units across all its accounts.
       - **Opposite positions:** a ticker held long anywhere is not recommended as a short entry, and the reverse.
     - **Ranking ENTER/WATCH:** `trend_score` (0–100) combines the EMA25/350 filter, ADX > 25, closeness to the 52-week high, and the cross-sectional percentile of 6- and 12-month ROC.
     - **Market regime flag:** SPY vs its 200-day line, plus breadth (% of the universe above its 200-day). Shown on the dashboard; it doesn't block anything.
   - **Default config:** `TurtleConfig.system_2()` (55/20), overridable in settings.
4. **Better entry dates.** When broker fills exist, `evaluate_position` gets the earliest open-lot `entry_date` from `broker_import` FIFO matching (`OpenPosition`) instead of `Portfolio.added_at`.

## Phase C: Accounts, equity and multi-account holdings (Q2)
- **New `accounts` table:** `name` (PK), `equity`, `cash`, `allow_short`, `updated_at`. It's seeded with one account named by `DEFAULT_ACCOUNT` ("default") so existing data has a home. You rename or add Roth and taxable in the UI.
- **Portfolio key becomes `(account, ticker)`.** SQLite can't change a primary key in place, so this is a one-time rebuild migration in `Database._migrate`:
  1. Copy the DB file to `stock_metrics_v2.db.pre-accounts.bak`.
  2. Create `portfolio_new` with `account` (FK to accounts) and the composite PK.
  3. `INSERT … SELECT` every existing row with `account = DEFAULT_ACCOUNT`.
  4. Drop the old table, rename the new one, all in one transaction.

  The migration is idempotent: it is skipped if the `account` column already exists.
- **Account columns on other tables:** `RealizedGains` and `OptionPosition` get a nullable `account` column (simple ALTER).
- **DB API:** every portfolio method that takes a ticker also takes `account`:
  - `add_stock`, `update_shares`, `update_cost_basis`, `sell_position`, the delete method
  - `get_portfolio_with_shares` returns `account`
  - Adding a short to an account that doesn't allow shorts is refused.
- **Other DB methods:** `get_accounts`, `upsert_account` and `total_equity()`.
- **Portfolio UI** (`portfolio.html`, `app.py`):
  - An Account column, with per-account subtotals and a grand total.
  - The add-stock form gets an account select.
  - Edit, sell and delete forms carry a hidden `account` field, and the delete route becomes `/delete_stock/<account>/<ticker>`.
  - `calculate_portfolio_summary` returns totals and a per-account breakdown.
- **Trend positions:** `/api/trend/positions` evaluates each `(account, ticker)` position separately, so an exit is actionable in a specific account. Unit and correlation limits aggregate by ticker.
- **Entry dates:** broker-fill entry dates are matched per account, since `BrokerFill.account` already exists.
- **API:** `GET/POST /api/accounts`.
- **Sizing:** uses `total_equity()`. The Trend page controls default to the stored equity instead of `DEFAULT_TREND_EQUITY`. Also fix `api_trend_chart`, which ignores the requested equity.

## Phase D: Signal history and journal
- **Tables:**
  - `recommendation_runs`: id, run_at, job (weekly/daily), equity, config_json, regime_json
  - `recommendations`: run_id FK, the Recommendation fields, and `first_seen_at`
  - `signal_decisions`: ticker, action, first_seen_at, status `pending|taken|skipped`, note, decided_at
- **`diff_runs(prev, curr)`** labels each signal NEW / STILL_VALID / EXPIRED, keyed on (ticker, action family). `first_seen_at` carries forward.
- **API:**
  - `GET /api/recommendations/latest`: the latest run plus expired items and decisions
  - `POST /api/recommendations/decision`: records taken, skipped or a note

## Phase E: Today dashboard
- **Page (Q3):** Today is served at `/` (endpoint `today`). Portfolio moves to `/portfolio` but keeps its endpoint name, so existing `url_for('portfolio')` redirects still work.
  - Hard-coded `"/"` links in templates and JS are updated.
  - The page is `today.html` plus `static/js/today.js`, written as a vanilla IIFE like `trend.js` and using the `utils.js` helpers. It's first in the `base.html` nav.
- **Filters:** direction (Long / Short / All), asset type (Stocks / ETFs / All) and account.
- **Top:** summary cards for equity, open risk and heat vs budget, action counts, last run time with a stale warning, and the market regime.
- **Action table, grouped Exits → Adds → New entries (ranked) → Watch:**
  - Each row has an order ticket (buy-stop at the entry level, shares, initial stop, $ risk, R to the 52-week high), a "copy ticket" button, NEW/STILL VALID badges, `blocked_by` reasons, and Taken / Skip / Note buttons.
  - Clicking a row opens the existing `/api/trend/chart/<t>` modal.
- **Expired signals:** a collapsible section.
- **Equity editor:** a small settings card for account equity.
- **Speed:** the page reads the **persisted latest run**, so it loads instantly. A "Recheck holdings" button runs the daily job's logic for holdings only.

## Phase F: Jobs, scheduling and digest
- **`src/jobs/`:**
  - **`__main__.py`:** argparse with subcommands `weekly` and `daily`, and flags `--tickers` / `--limit` for smoke runs.
  - **`pipeline.py`:**
    - `run_weekly()`: refresh universe files (A0; failure keeps the old files) → refresh bars for the full universe → metrics → rotating fundamentals → alerts (existing `evaluate_alerts`, vectorized over the frame instead of `iterrows`) → recommendations for the liquid universe → persist the run → digest.
    - `run_daily()`: incremental bars for holdings and the watchlist → position actions → persist the run (job=daily) → notify only if an EXIT or ADD fired.
  - `src/main.py:main()` delegates to `run_weekly()`, so the existing command still works.
- **`src/notify/digest.py`:**
  - Renders a Jinja HTML report to `data/reports/digest_<date>.html`.
  - Sends a macOS notification through `osascript display notification`. It works from a user LaunchAgent, but not from the sandbox.
  - Posts to Slack if `SLACK_WEBHOOK_URL` is set. No email (Q4).
- **launchd:**
  - Templates in `deploy/launchd/`: `com.tradinggui.weekly.plist` (Sat 06:00) and `com.tradinggui.daily.plist` (Mon–Fri 16:30 CT). Each sets the venv python, the WorkingDirectory, and StandardOut/Err to `logs/`.
  - `scripts/install_launchd.sh` fills in the paths and runs `launchctl bootstrap gui/$UID`. You run it yourself.
  - launchd runs a missed calendar job once on wake.
- **Locking:** a file lock (`data/.job.lock`) stops weekly and daily from overlapping.
- **README:** add Trend, Review, Today, jobs and launchd install, and fix the DB filename.

## Critical files
- **Modify:**
  - `src/data/price_history.py`, `src/data/ticker_loader.py`, `src/analysis/metrics.py`, `src/analysis/indicators.py`, `src/analysis/turtle.py`
  - `src/web_app/templates/portfolio.html`
  - `src/data/database.py`, `src/config/settings.py`, `src/main.py`
  - `src/web_app/app.py`, `src/web_app/templates/base.html`, `src/web_app/templates/trend.html` / `static/js/trend.js` (equity default)
  - `README.md`, `.gitignore`
- **New:**
  - `src/data/universe_sources.py`, `src/analysis/universe.py`, `src/analysis/recommendations.py`
  - `tests/fixtures/` (symbol-directory and S&P HTML samples, plus an old-schema portfolio DB built in the test)
  - `src/jobs/{__init__,__main__,pipeline}.py`, `src/notify/{__init__,digest}.py`, `src/notify/templates/digest.html`
  - `src/web_app/templates/today.html`, `src/web_app/static/js/today.js`
  - `deploy/launchd/*.plist`, `scripts/install_launchd.sh`
- **Reuse, don't rewrite:**
  - `turtle.evaluate_position`, `scan_breakouts`, `unit_shares`, `correlation_matrix`, `unit_limit_breaches`, `TurtleConfig.system_2`
  - `indicators.turtle_n` / `donchian_*` / `ema`
  - `metrics.calculate_rsi` and the MACD/BB helpers, `evaluate_alerts`
  - `broker_import` FIFO `OpenPosition`
  - `app._json_safe` and `_turtle_config_from_request`; `utils.js` helpers; the `.data-table` / `.metric-card` / `.rule-badge` CSS

## Implementation order (sub-agent units)
1. Prerequisite commit and branch; spec docs.
2. Phase A as three parallel units:
   - A0: universe sources + ticker_loader + `asset_type`
   - A1: bar cache + `refresh_histories`
   - A2: DB bulk upsert, `ticker_status`, `fundamentals_updated_at`
3. Then A3: metrics from bars + rotating fundamentals (skips ETFs).
4. Phase C (accounts, composite-key migration, portfolio UI/routes), in parallel with B0 (turtle short side).
5. Then B1–B4 (recommendations, which need accounts and short-side rules).
5. Phase D (DB plus diffing).
6. Phase F jobs and digest, then Phase E UI, as parallel units once D's API shape is fixed.
7. Tests, ruff and README throughout; final verify and self-review.

## Tests (pytest, synthetic bars as in `tests/test_turtle.py::_bars`, temp DB via a `monkeypatch` of `settings.DB_PATH`)
- **`test_price_history.py`:**
  - incremental merge appends without duplicates
  - a new split forces a full re-download
  - chunked `yf.download` results are split per ticker (monkeypatched `yf.download`)
  - a fresh cache makes no network call
- **`test_metrics_from_bars.py`:** `compute_technical_metrics` matches the old per-ticker results on the same bars; fundamentals rotation picks only stale tickers plus holdings.
- **`test_universe.py`:** liquidity thresholds and the union with holdings and the watchlist.
- **`test_universe_sources.py`:** parses fixture copies of `nasdaqlisted.txt` / `otherlisted.txt` / an S&P HTML table:
  - test issues, warrants and the footer row are dropped
  - `BRK.B → BRK-B`
  - the ETF flag is kept
  - a failed sanity check keeps the old file
- **`test_turtle.py` (extended):** mirrored short tests for entry, 2N stop above entry, channel exit at the N-day high, adds below entry, and the trend filter. A backtest with `allow_short=False` matches the current results.
- **`test_accounts.py` (extended):**
  - the composite-key migration on a DB in the old schema keeps all rows under `DEFAULT_ACCOUNT` and writes the backup
  - the migration is idempotent
  - the same ticker can be held in two accounts
  - a short in a non-short account is refused
- **`test_recommendations.py` (extended):**
  - shorts are routed only to `allow_short` accounts
  - long entries go to an account with enough cash
  - no opposite-direction entry on a held ticker
  - unit limits aggregate across accounts
- **`test_recommendations.py`:**
  - exits rank above adds, and adds above entries
  - held tickers are excluded from ENTER
  - the heat budget blocks lower-ranked entries
  - a correlation or unit breach fills `blocked_by`
  - a late signal sets `is_late`
- **`test_signal_history.py`:** NEW / STILL_VALID / EXPIRED diffing, `first_seen_at` carry-forward, and decisions persisted.
- **`test_accounts.py`:** account CRUD and `total_equity`.
- **`test_jobs.py`:** `run_daily` with stubbed bars persists a run and calls the notifier only when an EXIT or ADD fires; the lock prevents overlap.
- **Flask:** `/today`, `/api/recommendations/latest`, `/api/recommendations/decision` and `/api/accounts`, using `test_client` and a temp DB.
- **Checks:** run `ruff format` and `ruff check --fix` on changed files. The full `pytest` suite must stay green (57 existing tests plus the new ones).

## Verification
1. `pytest`, plus ruff on changed files.
2. Smoke run: `python -m src.jobs weekly --limit 200` (cold cache). Check the per-stage timing log, `recommendation_runs` and `recommendations` rows, and `data/reports/digest_*.html`.
3. Full `python -m src.jobs weekly` once to warm the cache, then a second run to measure warm time (target: well under 30 min). Compare against the 7h18m baseline.
4. `python -m src.jobs daily`: about a minute, and it notifies only if something fired.
5. `python run_web_app.py`, open `/today`. Check the order tickets, heat vs budget, journal buttons and the chart modal; the Trend page should default to the stored equity.
6. You run `scripts/install_launchd.sh` and then `launchctl print gui/$UID/com.tradinggui.weekly` (the sandbox blocks launchctl and osascript for me).

## Risks and notes
- **Values shift slightly.** Metrics switch from dividend-adjusted (`auto_adjust=True`) to the split-only cached bars, so MA, RSI and MACD values move a little. That's accepted, so that every tab uses one price source.
- **Cold first run.** The first warm-up downloads full history since 2015 for about 5.3k live tickers in chunks of 100. That's still minutes, but `yf.download` can throttle; chunk size and rate are configurable.
- **Portfolio migration.** The composite-key rebuild is the riskiest DB change. It writes a backup first, runs in one transaction, and is tested against an old-schema fixture DB.
- **Shorts.** Short signals don't know about borrow availability or hard-to-borrow fees. Treat a short ENTER as "check borrow first".
- **Bigger universe.** Adding ETFs and NYSE/ARCA listings grows the raw universe to about 11–12k symbols, so about 120 download chunks. The liquidity filter keeps recommendations manageable.
- **Debug reloader.** It doesn't matter here, because nothing schedules from inside Flask.

## Appendix: reference snippets for implementers

### A1: batched incremental refresh (`src/data/price_history.py`)
```python
def refresh_histories(tickers: list[str], chunk_size: int = settings.DOWNLOAD_CHUNK_SIZE) -> dict[str, pd.DataFrame]:
    """Bring each ticker's cached bars up to the last completed session, one download call per chunk."""
    session = last_completed_session()
    stale = [t for t in tickers if _last_bar(t) is None or _last_bar(t) < session]
    for chunk in itertools.batched(stale, chunk_size):  # py3.10: use a local _chunks helper
        start = min((_last_bar(t) or DEFAULT_HISTORY_START) for t in chunk) - timedelta(days=5)
        _batch_rate_limiter.wait()
        raw = yf.download(list(chunk), start=start.isoformat(), group_by="ticker",
                          auto_adjust=False, actions=True, threads=True, progress=False)
        for ticker in chunk:
            new_bars = _normalize(raw[ticker]) if ticker in raw.columns.get_level_values(0) else None
            _merge_into_cache(ticker, new_bars)  # full re-download if new_bars has stock_splits > 0
    ...
```
Python is 3.10, so `itertools.batched` is not available; write a small `_chunks` helper.

### A2: bulk upsert (`src/data/database.py`)
```python
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

stmt = sqlite_insert(StockMetrics).values(rows)
update_cols = {c.name: stmt.excluded[c.name] for c in StockMetrics.__table__.columns if c.name != "ticker"}
session.execute(stmt.on_conflict_do_update(index_elements=["ticker"], set_=update_cols))
```
SQLite allows at most 32766 bound variables per statement, so chunk the rows (about 500 per statement).

### B3: recommendation types (`src/analysis/recommendations.py`)
```python
class RecommendationAction(enum.Enum):
    EXIT_STOP = "EXIT_STOP"
    EXIT_CHANNEL = "EXIT_CHANNEL"
    ADD_UNIT = "ADD_UNIT"
    ENTER = "ENTER"
    WATCH = "WATCH"
    HOLD = "HOLD"

ACTION_PRIORITY: dict[RecommendationAction, int] = {a: i for i, a in enumerate(RecommendationAction)}

@dataclass(frozen=True)
class Recommendation:
    ticker: str
    action: RecommendationAction
    account: str | None
    trigger_price: float | None
    stop_price: float | None
    shares: int
    risk_dollars: float
    risk_pct_equity: float
    distance_n: float | None
    trend_score: float | None
    rs_rank: float | None
    is_late: bool = False
    reasons: tuple[str, ...] = ()
    blocked_by: tuple[str, ...] = ()
```

### F: launchd plist (`deploy/launchd/com.tradinggui.weekly.plist`; the install script fills in `__REPO__`)
```xml
<key>ProgramArguments</key>
<array><string>__REPO__/.venv/bin/python</string><string>-m</string><string>src.jobs</string><string>weekly</string></array>
<key>WorkingDirectory</key><string>__REPO__</string>
<key>StartCalendarInterval</key><dict><key>Weekday</key><integer>6</integer><key>Hour</key><integer>6</integer><key>Minute</key><integer>0</integer></dict>
<key>StandardOutPath</key><string>__REPO__/logs/weekly.out.log</string>
<key>StandardErrorPath</key><string>__REPO__/logs/weekly.err.log</string>
```
The daily plist uses an array of five `StartCalendarInterval` dicts, Weekday 1–5 at Hour 16, Minute 30.

### A0: parsing the symbol directory (`src/data/universe_sources.py`)
```python
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
# nasdaqlisted columns: Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
# otherlisted columns:  ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
# The last line of each file is "File Creation Time: ..."; drop it.
EXCLUDED_NAME_PATTERN = r"\b(Warrants?|Rights?|Units?)\b"

def to_yfinance_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")
```

### C: composite-key migration (`src/data/database.py`, inside `_migrate`)
```python
if "account" not in portfolio_columns:
    shutil.copy2(settings.DB_PATH, settings.DB_PATH.with_suffix(".db.pre-accounts.bak"))
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE portfolio_new (
            account TEXT NOT NULL REFERENCES accounts(name),
            ticker TEXT NOT NULL, shares FLOAT, cost_basis FLOAT, added_at DATETIME,
            PRIMARY KEY (account, ticker))"""))
        conn.execute(text("INSERT INTO portfolio_new SELECT :acct, ticker, shares, cost_basis, added_at FROM portfolio"),
                     {"acct": settings.DEFAULT_ACCOUNT})
        conn.execute(text("DROP TABLE portfolio"))
        conn.execute(text("ALTER TABLE portfolio_new RENAME TO portfolio"))
```
Create the `accounts` table and seed `DEFAULT_ACCOUNT` before this runs.

### B0: direction-aware turtle rules (`src/analysis/turtle.py`)
```python
class Direction(enum.Enum):
    LONG = 1
    SHORT = -1

def direction_of(shares: float) -> Direction:
    return Direction.SHORT if shares < 0 else Direction.LONG

# stop = entry - sign * stop_n * N ; add level k = entry + sign * k * add_step_n * N
# channel exit: LONG -> close < donchian_low(exit); SHORT -> close > donchian_high(exit)
```

## Implementation notes: changes from the plan
- **Daily watch-list rescan.** Daily and manual runs also re-scan the tickers in the latest weekly ENTER/WATCH list, so a breakout during the week shows up as ENTER that evening. Without this, a weekly scan only sees breakouts on Friday's bar. Daily runs still never expire entry signals.
- **Leveraged and inverse ETFs.** These are excluded from recommendations (`UNIVERSE_EXCLUDE_LEVERAGED_ETFS`) but still included in the metrics refresh.
- **Fundamentals cap.** `FUNDAMENTALS_MAX_PER_RUN=1000` caps fundamentals fetches per run, with holdings always included. Almost every row was more than 28 days stale, so without the cap the first full run would have spent about an hour on `.info` calls.
- **Stale-bar guard.** A ticker whose last cached bar is more than 10 days old counts as failed. This stops delisted tickers from passing the liquidity filter on frozen data.
- **Today's bar is never cached.** The in-progress session bar is never written to the cache; bars are cached only through `last_completed_session()`.
- **`default` account.** It is seeded with `allow_short=True`, so the existing short workflow keeps working, and only when the accounts table is empty, so renaming it sticks. Accounts can be renamed (`rename_account`), and positions move with the account.
- **Recheck lock and notifications.** "Recheck holdings" takes the same file lock as the scheduled jobs and returns 409 while one is running. Notifications skip rows already journaled as taken or skipped.
- **WATCH cap.** The dashboard shows only the top `MAX_WATCH_ROWS=30` WATCH rows; ENTER rows are never capped.
- **Sizing settings.** `TREND_RISK_PER_UNIT=0.5%` and `TREND_MAX_UNIT_FRACTION=10%` moved into settings, matching the Trend page defaults.

## Smoke results
Both runs used the real DB on 2026-11-10.
- **Weekly, `--limit 150` (153 tickers including holdings):**
  - 162s total: bar download 8.3s, compute 0.2s, fundamentals 151s (first rotation), recommendations 2.3s.
  - 39 recommendations: 2 EXIT_CHANNEL, 1 ADD_UNIT, 16 ENTER, 20 WATCH.
- **Daily:** 0.7s.
- **Baseline:** the old full refresh took 7h18m, at about 4s per ticker.
