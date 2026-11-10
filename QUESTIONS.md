# Open Questions

Answered questions are recorded here and are reflected in IMPLEMENTATION_SPEC.md and PLAN.md. Append any new ones during implementation.

## Answered (2026-11-10)
1. **Short-side trend signals for the taxable account?** **Yes.** `turtle.py` gets mirrored short rules, and short entries are routed only to `allow_short` accounts.
2. **Multiple accounts per ticker?** **Yes, one ticker can be held in several accounts.** `Portfolio` moves to a composite `(account, ticker)` key, with a migration.
3. **Today replaces Portfolio as the home page?** **Yes.** Today is at `/` and Portfolio moves to `/portfolio`.
4. **Email digest?** **No.** macOS notification, plus optional Slack.
7. **Refresh the universe CSVs on a schedule?** **Yes, and add ETFs.** The NASDAQ Trader symbol directory plus the S&P 500 list from Wikipedia, refreshed weekly.

## Still open (defaults used until answered)
5. **Heat budget.** Is 10% of equity the right default for portfolio heat? The Faith rules imply about 12 units × 1% risk at the loosest limit. *Default: 10%, configurable via `MAX_PORTFOLIO_HEAT_PCT`.*
6. **Options in open risk.** Should option positions count toward open risk and heat? *Default: listed but not counted.*
8. **Account routing.** Is "first account with enough cash, Roth first" the right rule for long entries? Or should some names, such as high-turnover trades, go to taxable on purpose? *Default: Roth first.*
9. **Short sizing.** Should short entries use the same 1%-of-equity risk per unit as longs, or a smaller fraction because of gap and squeeze risk? *Default: same as longs.*

## Raised during implementation
10. **Correlation limits with opposite directions.** How should a long in one ticker and a short in a positively correlated ticker count toward the correlated-group limits? Right now each direction counts only against itself, and negative correlation is ignored.
11. **Exchange code "F".** What is otherlisted exchange code "F" (20 rows, for example ET)? It is kept as the raw code.
12. **ETF share of the universe.** ETFs are 5,784 of the 12,026 listings (48%). Beyond dropping leveraged and inverse ETFs, should ETFs be capped or limited further, for example by AUM?
13. **Broker account names.** Broker CSV account names (for example "ETRADE-1234") may not match the names in the accounts table (for example "Roth IRA"), in which case entry dates fall back to `added_at`. Is a mapping between them needed?
14. **Short-side margin.** Short entries are routed to the first account that allows shorting, with no margin or borrow check. Is that enough?
15. **Research page account.** The Research page's "Add to Portfolio" still adds to `DEFAULT_ACCOUNT`. If that account was renamed, the add fails with an "unknown account" message. Should Research get an account picker?
16. **History after a rename.** Stored recommendation runs keep the old account name after a rename until the next run. Should history be rewritten?
17. **Equity not set yet.** No account equity is stored yet, so sizing falls back to $100k. The `default` account has 0 cash, so most long entries in the smoke run show "no account with sufficient cash". Set up accounts under Today → Accounts before relying on the order tickets.
