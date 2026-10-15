# Finnhub cache readers and retained consumers, October 15, 2026

## Status

Implemented locally on `codex/finnhub-paid-readers`, based on main `28c56693`. This package connects the five staged estimate/target scopes to actual ticker and research consumers. It does not establish live paid-provider coverage or complete analyst migration. `FINNHUB_PAID_READERS_ENABLED` defaults off; the reviewed-basis registry has no production entries. Paid collection, subscriptions, public source selection and schedules are unchanged.

## Product behavior

- The existing Financials contract receives annual/quarterly EPS and revenue forecasts from verified cached scopes. Exact period and fiscal year/quarter identity are retained; different dates never become one combined forecast. Annual null/zero quarter labels are equivalent. Zero/negative EPS and native revenue units survive. Missing averages/analyst counts remain unavailable, and historical periods never become the next forecast.
- Forecasts are a separate projection over SEC US GAAP USD statements. Stored SEC evidence, historical amounts, timestamps, summary ratios and valuation fields remain unchanged. Foreign reporting/ordinary-share panels are held. A source review must explicitly establish historical comparison compatibility; listing currency is insufficient.
- Premium analyst detail and comparison readers receive a current price-target range. Free summaries contain no range or target evidence. Monthly recommendation dates remain distinct from target update/observation dates; targets can display without inventing a recommendation snapshot. Mean and median remain distinct. No quote, upside, target event, score contribution or recommendation change is invented.
- Research uses the same forecast projection and retains its source hashes, observations and basis. Finnhub averages include proprietary estimates, so they do not establish pure consensus availability. An explicit valuation eligibility guard prevents deriving forward P/E from these forecasts before market/share compatibility is verified. Source-aware charts do not calculate forecast growth from unrelated array positions.
- Real React panels display the blended-estimate basis and independently dated USD target evidence. Existing layout and plan boundaries remain.

## Integrity and activation requirements

Readers never request a provider, enqueue paid collection or write canonical tables. Disabled or unreviewed scopes return before database lookup. A current cache is checked against its source version, 24-hour observation age, identity, complete raw response, deterministic saved projection and original observation timestamp. Period/target age is reevaluated without refreshing that timestamp. Stale, corrupt, conflicting or unsupported sources are held. Target source dates retain their precision and the existing 45-day age bound. Staging remains immutable and publication-ineligible; reviewed read projections are separate.

Each reviewed-basis entry is scoped to provider symbol, kind and frequency. It requires official Finnhub evidence URL/hash, bounded review validity, explicit USD/unit/accounting/share basis, and (for financial estimates) compatibility with `sec_statements_v1`. There are intentionally zero real entries until source evidence establishes these facts. The current compatibility contract accepts US GAAP USD forecasts per listed diluted share and USD targets per listed share. Adjusted EPS, foreign currencies, ADR/ordinary-share differences and unverified units need supported explicit contracts, not guessed conversions. Enabling the flag alone cannot publish unreviewed data.

Share-class cache reuse was repaired: BRK-B and BRK.B reuse the same verified provider scope without a paid refetch, while BRK.A remains distinct. Original caller identity and evidence remain unchanged.

## Validation

- Initial 37 focused checks passed on Windows Python 3.14.2.
- Expanded Linux Python 3.12.3 run: 206 passed, one existing `test_ticker_financials_normalizes_statement_earnings_and_summary` forward-P/E assertion failed. Loading the exact unchanged `28c56693` ticker reader reproduces the same `None` versus `25.0` failure. No full-suite pass is claimed.
- Final reader/research run: 49 passed, comprising 45 reader tests and four existing research regressions. Across the expanded and final runs, 215 distinct backend tests pass, with the one separately reproduced baseline failure.
- Nine frontend checks pass, including three real React server renders for paid target-only evidence, Free target hiding and a zero-EPS forecast. TypeScript passes with incremental output disabled, including the final repeat after the chart change.
- SQLite source-to-consumer tests preserve complete snapshot fingerprints across read/repeat/rollback, reject stale/tampered/foreign/unreviewed data, preserve fiscal identity and source clocks, verify Premium/free comparison projections and retain unavailable score inputs. Network calls are prohibited in the reader fixtures. These checks are not live paid-data or scheduled delivery evidence.

Initial Linux collection failed because the runner lacked the explicit in-memory DATABASE_URL and tried the application's `/data` default; the corrected isolated run is reported above. React test-harness resolution/named-export/wrapper-child mistakes were corrected before the successful render checks. WSL baseline loading could not resolve the Windows Git worktree path; the unchanged reader was captured with Windows Git and loaded into the same Linux environment instead. These were test harness repairs, not production failures.

## Remaining

Exact-head review/CI/release/runtime verification, live paid entitlement and source-basis review, response coverage, scheduled cache preparation and quote-compatible target upside/valuation remain. Earnings/net-income forecasts are not inferred from EPS multiplied by shares. Full institutional/current financial/transcript coverage, all scheduled watchlist/monitoring/daily/weekly/ranking consumers, sustained capacity, final Massive, zero global FMP traffic and billing retirement remain open. Pending production-data export approvals were not retried. No paid request, subscription, source activation, schedule/host change, email, video or landing redesign occurred.
