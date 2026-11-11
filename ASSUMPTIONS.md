# Assumptions

Assumptions made while planning. Append any new ones during implementation.

1. **Branch base.** The new branch `weekly-trend-recommendations` is cut from `feature/coderabbit-config-and-webapp-updates` after its uncommitted work is committed, not from `origin/main`. The new features depend on turtle.py, price_history.py, indicators.py, broker_import.py and the trend UI, none of which are on main yet.
2. **One price source.** Metrics switch from yfinance `auto_adjust=True` (dividend-adjusted, 400 days) to the shared split-adjusted bar cache (since 2015). MA, RSI and MACD values will shift slightly.
3. **Liquidity filter.** The universe is tickers with last close ≥ $5 and 50-day average dollar volume ≥ $10M. Holdings and the watchlist are always included. Both thresholds are configurable.
4. **Rotating fundamentals.** `.info` is refreshed every 28 days, for liquid-universe tickers only; holdings are refreshed every run. Illiquid tickers keep technicals only.
5. **Dead tickers.** A ticker is skipped for 28 days after 3 consecutive failed fetches. Holdings are never skipped.
6. **Default rules.** Recommendations use `TurtleConfig.system_2()` (55-day entry, 20-day exit) on daily bars. Short signals are mirrored rules (entry on a break of the N-day low, stop at entry + 2N, exit on the N-day high) and go only to accounts with `allow_short`. The Roth stays long-only. The backtest keeps `allow_short=False` by default, so existing results don't change.
7. **Heat budget.** Total open risk plus new-entry risk is capped at 10% of total equity. Entries are admitted in rank order until the budget is used up.
8. **Sizing equity.** Sizing and heat use the **sum** of the stored account equities, so there's one risk budget across accounts. Accounts are a new table. `Portfolio` is rebuilt with a composite `(account, ticker)` PK, and existing rows migrate to `DEFAULT_ACCOUNT`; a backup is taken first. Unit limits count a ticker's units across all accounts.
9. **Entry dates.** A position's entry date is the earliest open broker lot when broker fills exist; otherwise it falls back to `Portfolio.added_at`.
10. **Schedule.** The weekly job runs Saturday 06:00 local time. The daily job runs Mon–Fri at 16:30 local time (CT, 90 minutes after the close, so yfinance has the daily bar).
11. **Notifications.** These are a macOS notification plus an HTML report under `data/reports/`, and Slack only if `SLACK_WEBHOOK_URL` is set. Email is out of scope.
12. **Home page.** Today is served at `/`. Portfolio moves to `/portfolio` and keeps its Flask endpoint name.
13. **Repo hygiene.** The SQLite `-wal` and `-shm` files are untracked (`git rm --cached`) and gitignored. The local files are not deleted.
14. **Account routing for entries.** LONG entries are suggested for the first account whose cash covers the position value, trying the Roth first. SHORT entries go only to `allow_short` accounts. Entries no account can fund are still listed, with a `blocked_by` reason.
15. **Universe sources.** The universe comes from the NASDAQ Trader symbol directory (`nasdaqlisted.txt` + `otherlisted.txt`, covering NASDAQ, NYSE and ARCA, with an ETF flag) plus the S&P 500 list scraped from Wikipedia. It refreshes weekly as the first step of the weekly job, and a failed or implausible download keeps the previous files.
16. **ETFs.** ETFs are included in the universe and the trend rules. They skip fundamentals and are tagged `asset_type = etf`.
17. **Shorts and borrow.** Short signals don't check borrow availability or fees. The dashboard labels them "check borrow".
18. **Options in heat.** Options are listed but not counted in heat or open risk until Q6 is answered.

## Added during implementation

### Turtle short side
19. **Signed units.** `PositionStatus.units_held` and the values in `unit_limit_breaches` are signed, negative for shorts. Longs and shorts are counted separately for the correlated-group limit and the 12-unit one-direction limit. The 4-per-ticker limit uses `abs(units)`.
20. **Where `allow_short` applies.** `TurtleConfig.allow_short` (default False) only affects `backtest`. `scan_breakouts` takes an explicit `directions=` argument instead. With the defaults, every existing result is identical, which is checked against a snapshot taken before the change.
21. **Backtest positions.** A ticker holds one position at a time in the backtest. If a bar breaks both channels, the long is taken.
22. **Skip-after-winner per direction.** The S1 skip-after-winner sequence is tracked separately for longs and shorts.

### Universe and price cache
23. **What's dropped from the universe.** Warrants and rights are dropped. SPAC units are dropped by name suffix, by "Units, each" wording, and by the symbol suffixes `.U`, `.UN`, `.W`, `.WS`, `.R`, `.RT`. MLP common units such as ET and PAA are kept, and so are tangible equity units. Preferred shares (symbols with `$`) are dropped.
24. **Today's bar.** The bar for the session in progress is never cached. Bars are cached only through `last_completed_session()`, which counts a weekday as complete after 17:00 New York time; holidays are not modelled.
25. **Checked sidecar.** `PRICE_CACHE_DIR/_checked.json` stops a holiday, when no new bar arrives, from triggering a re-download on every call. A chunk that fails or comes back empty is not marked checked, so it is retried.
26. **Universe files are committed.** `data/raw/us_listed.csv` is committed alongside `sp500.csv` so the universe used by a run is visible in git. `sp500.csv` now has `Symbol,Security` columns.
27. **Batched refresh in the weekly job.** `refresh_histories` returns every requested history in memory, about 140 KB per ticker, so the weekly job refreshes and computes in batches of about 500 tickers.
28. **Leveraged and inverse ETFs.** These are excluded from trend recommendations by a name pattern (2x/3x, Ultra, Inverse, Bear, -1x and similar), controlled by `UNIVERSE_EXCLUDE_LEVERAGED_ETFS`, default True. Their daily-reset decay makes trend rules unreliable on them. They are still included in the metrics refresh.

### Database layer
29. **Default account allows shorts.** The seeded `default` account has `allow_short=True`, so today's short workflow and the existing short test keep working. Set the Roth up as its own account with `allow_short=False`.
30. **Partial upserts.** Bulk upserts update only the columns present in each row; rows are grouped by key set. A missing key leaves the stored value alone, and an explicit `None` clears it.
31. **Getters cover all accounts.** Portfolio, option and gain getters default to all accounts (`account=None`). Write methods default to `DEFAULT_ACCOUNT`.
32. **Real DB migrated, with a backup.** The live DB was migrated the first time the app module was imported after the migration landed. The backup is `data/processed/stock_metrics_v2.db.pre-accounts.bak`, taken with the SQLite backup API so the WAL contents were included. After the migration, the 3 portfolio rows are under `default` and the 5,869 metrics rows are unchanged, and `integrity_check` passes.
33. **Fundamentals backfill.** Existing rows get `fundamentals_updated_at` set to their `updated_at`, so the first rotation doesn't refetch everything.
34. **Signal decision key.** `signal_decisions` is keyed on (`signal_key`, `first_seen_at`). `signal_key` is built as `ticker|direction|family` for entries and `account|ticker|direction|family` for position actions.

### Metrics pipeline
35. **Fundamentals age is in calendar days.** "Older than 28 days" compares dates, not 24-hour periods. Otherwise a weekly run that starts a little earlier in the day would push a ticker to a 5-week rotation.
36. **Stale-bar guard.** A ticker whose last bar is more than 10 days before the last completed session (`MAX_BAR_AGE_DAYS`) counts as failed. This stops a delisted ticker's frozen cache from passing the liquidity filter, and after repeated failures it goes on the skip list.
37. **ETF holdings still fetch `.info`.** Holdings are always due for fundamentals, ETF holdings included. For an ETF only `market_cap` gets filled.
38. **`get_metrics_batch` delegates.** It now calls `refresh_metrics`, which stores rows itself, and returns `[]`. `main.py` calls `refresh_metrics()` directly instead.
39. **Single-ticker paths unchanged.** `/research` and `/add_stock` still fetch history per ticker (dividend-adjusted, 0.5 req/s). Their `.info` fetch now uses the 1 req/s fundamentals limiter.

### Recommendation engine
40. **Trend score.** The score (0–100) is 40 × relative-strength percentile (6- and 12-month ROC), plus 20 if ADX > 25, plus up to 20 for being within 25% of the 52-week high (or low for shorts), plus 20 if the EMA25/350 filter agrees.
41. **What uses the risk budget.** Only unblocked ADD_UNIT and ENTER rows count against heat, unit limits and cash, admitted in the order adds first, then entries by score. WATCH rows are checked against those limits but don't use them up.
42. **Short routing.** Shorts go to the first account that allows shorting, with no cash or margin check. Admitted longs reduce the cash of the account they're routed to, so the Roth fills up first and later longs fall back to the taxable account.
43. **HOLD rows.** They are NEW only in the first run ever, then STILL_VALID, and they never expire.
44. **Signal keys.** WATCH and ENTER share a signal key (`TICKER|DIR|ENTRY`), so a watch that triggers keeps its `first_seen_at`. Daily runs never expire entry signals.
45. **Entries mid-week.** The weekly scan only sees breakouts on the last bar. To avoid missing mid-week breakouts, the daily job re-scans the tickers in the latest weekly ENTER/WATCH list, so a watch that triggers mid-week becomes ENTER the same evening. Treat WATCH rows as standing buy-stop or sell-stop orders for the week.
46. **Sizing settings.** Sizing uses `TREND_RISK_PER_UNIT=0.5%` and `TREND_MAX_UNIT_FRACTION=10%`, matching the Trend page defaults. The dashboard shows only the top `MAX_WATCH_ROWS=30` WATCH rows by score.

### Web UI
47. **Default account seeding.** The `default` account is seeded only when the accounts table is empty, so renaming it, for example to "taxable", sticks across restarts.
48. **Staleness.** The Today page flags STALE when the latest *weekly* run is more than 8 days old or doesn't exist. This is computed on the server.
49. **Decisions.** Clicking the active Taken or Skip again resets a signal to `pending`. A Note keeps the current status.
50. **Filters.** The count cards follow the active filters; heat and equity are always portfolio-wide. Rows with no `asset_type` count as stocks.
51. **Portfolio page prices.** The Portfolio page still uses live yfinance prices and doesn't use the cached bars, because the cache holds only completed sessions and would change the intraday price and day's gain. Each ticker is fetched once even when several accounts hold it.
52. **Account deletion and realized gains.** Deleting an account is refused while stock or option positions reference it. Its `realized_gains` rows are kept as history, still under the old account name.

### Jobs and digest
53. **Daily rescan scope.** Daily and manual runs re-scan only the (ticker, direction) pairs in the latest entries run. Trend score and RS rank carry over from the weekly run, because RS is ranked across the whole universe; breadth is left empty on daily runs.
54. **Which row the dashboard shows.** For each signal key, the row from the newest run that contains it wins. If a later re-scan drops a key, the older row stays.
55. **What notifies.** The daily job notifies for EXIT_STOP, EXIT_CHANNEL and ADD_UNIT rows that have no taken or skipped decision, and for ENTER rows that are NEW or weren't ENTER in the previous run. Undecided exits keep notifying every day on purpose.
56. **Fundamentals cap.** Each run fetches fundamentals for at most `FUNDAMENTALS_MAX_PER_RUN=1000` tickers, holdings first and then the oldest. This matters because the first runs find every row stale; the 150-ticker smoke run spent 151s of its 160s metrics stage on fundamentals.
57. **Alert failures fail loudly.** A failure while evaluating alerts now fails the run; the old `main.py` logged it and carried on. `python -m src.main` exits 1 if the job lock is held, and the `src.jobs` CLI exits 2.
58. **Recheck takes the job lock.** "Recheck holdings" in the UI takes the same lock as the scheduled jobs and returns 409 while a job is running.

### Review fixes
59. **Unit limits that are already breached.** A breach blocks a new unit whenever the unit raises the breach's count, even if the limit was already breached before. Breaches the unit doesn't change are ignored.
60. **Excluded from the entry scan.** Skip-listed tickers are dropped from the entry scan unless they are held or on the watchlist. Tickers whose last bar is more than 10 days before the last session are dropped too.
61. **Positions that can't be evaluated.** Holdings with no cost basis or no history still block entries in that ticker, and they are listed as "not evaluated" on Today and in the digest.
62. **Stale holdings.** If a holding's bars are older than the last completed session after a refresh, its rows are flagged, a notification is always sent, and the CLI exits with code 3.
63. **Cache markers.** A ticker, cached or not, is marked checked only when the download actually returned it, so a ticker dropped from a chunk is retried on the next call. Symbols that are really dead are retired by the skip list.
64. **Degraded `.info` responses.** A yfinance `.info` with no `quoteType` or no usable values counts as a failure. Stored fundamentals are kept and the ticker stays due for a refetch.
65. **Position routes without an account.** The legacy delete route and forms that omit `account` resolve the ticker's only holding account. If several accounts hold it, the action is refused. A change that matches nothing flashes a warning instead of a success message.
66. **Pre-run backups.** Only the weekly job backs up the DB, because it writes thousands of rows; the daily job only adds one run. Eight backups (about two months of weekly runs) are kept. A failed backup stops the job instead of writing without one.
67. **SSL for `requests`.** `DISABLE_SSL_VERIFY` already disabled verification for yfinance through a global ssl override, which `requests` ignores. The universe downloads and the Slack webhook now pass `verify=` from the same setting rather than adding a separate CA-bundle setting.
