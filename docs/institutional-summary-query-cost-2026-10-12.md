# Institutional summary query cost, October 12, 2026

The [actual PostgreSQL publication measurement](institutional-bounded-writes-2026-10-12.md) found 30,108 SELECT statements and 82.172 process CPU seconds for 20 event-producing filings. Repeated per-ticker summary reads reload the same quarter's active filing list and independently fetch changes and current positions.

## Implementation

The existing whole-filing and symbol-batch processors now read summary inputs in groups of up to 50 symbols within each filing. The quarter's active filing IDs are read once. Each shared changes/positions query is capped at 10,001 rows to detect overflow; more than 10,000 rows invokes the original complete individual reads, without truncation. Noncanonical input symbols also retain the original reader behavior. No shared state survives the generator or filing transaction.

The existing summary calculation is factored without changing its body. Per-symbol summary updates, event generation, materialization order and flushes remain in their prior order. Superseded filings stay excluded. Source ownership, publication receipts, evidence, event identity, timing, rank/score rules, job limits and schedules are unchanged.

## Current validation

The targeted query regression fails on the prior implementation with 13 repeated current-quarter filing-list reads, then passes with one. Eight focused checks cover complete row/event parity for whole-filing and symbol-batch processing, both overflow paths, superseded holdings, exits, option rows and empty/noncanonical input. The broader run passes 153 checks and fails the existing `test_institution_profile_endpoints_are_locked_until_pro` holder-name assertion. An independent rerun using `origin/main` institutional service code reproduces the same failure. No full-suite pass is claimed.

An initial new fixture specified a nonexistent filing `holder_name` column; it was corrected before the meaningful red regression. An edit helper initially placed constants after a decorator; its compile-before-write check rejected the edit without modifying application code, and the helper was corrected. Neither failed attempt is counted as application validation.

Actual PostgreSQL baseline/candidate parity and performance measurements completed against the same 718 captured parsed originals and 579 references. The baseline uses the original individual reader and unchanged calculation; the candidate uses bounded input groups. Both use 20 actual current filings and their real prior evidence, exact current publication logic, no provider requests, no emails, disposable loopback-only schemas, and an all-table identical-repeat check. Cross-run canonical projections retain every source/value/date field except explicitly identified execution timestamps. Both processes completed successfully. Complete publication results, six canonical table projections and all table counts match exactly; both full-state repeats are identical with no new events, source requests, emails or production writes.

## Other cutover state

The full captured-cohort replay `46444` completed with 574 documents, 119/119 generated SEC event proofs, 53 continuity checks, identical full-state repeat, duplicate-alert refusal and identical 12/8/119 monitoring/signal/watchlist previews. This process imported the deployed implementation before this edit; it is not a validation of the new optimization.

Read-only receipt `financial-versions-1791770453.json` at 02:00:53 UTC verifies the ordinary 01:56 financial run: ten SEC scopes and eight Finnhub scopes, no quota stop, core still FMP. SEC v5 records rise to 221 and Finnhub v2 to 58, versus 211/52 at the previous observation. Complete current-version coverage and numerical/semantic checks remain open.

No application deployment, source activation, purchase, customer email, schedule/capacity change, landing-page change or FMP cancellation. Institutional publication stays disabled. Remaining source coverage, Senate, core financials, transcripts, all affected consumers and final Massive/global FMP/billing gates remain.


## Completed PostgreSQL comparison

PR596 application head `d7f9538a08b2cd1cad3cae6fad1c149c2fd85550`; local Python3.14.2/PostgreSQL16.15. The same20actual current filings and real priors publish3490positions and three SEC-proof events. SELECTs fall30108 to19617 (34.84%), with unchanged1619INSERTs/2803UPDATEs. Process CPU80.312 to78.688seconds (2.02%); wall93.119 to90.753seconds. SELECT cursor time18.395 to15.164seconds. These single-run measurements show substantially fewer queries but only modest CPU change, not a demonstrated production speedup. FinalRSS429.43/429.32MB is effectively unchanged; ~708MB lifetime peak includes restoration. Repeat CPU32.672/36.812seconds is not improved and does not invoke the changed summary path.

Evidence: `institutional-summary-baseline-postgres-oct12.json`, `institutional-summary-candidate-postgres-oct12.json`, `institutional-summary-comparison-oct12.json`. Each mode drops its own disposable schema. An additional isolated runtime fixture passes locally for ordinary reads and both overflow branches; deployed Python verification remains pending. Existing dependency-audit failures are in unchanged dependencies; secret scan and Vercel pass. No all-CI/full-suite or production-capacity claim.

Ordinary financial metadata at02:10:42UTC records241SECv5/74Finnhubv2 rows, versus221/58 at02:00. Three ordinary runs each complete10SEC/8Finnhub scopes without quota stops, public core remainsFMP. Adapter-version counts are not completeness or numeric-parity proof.


## Release and deployed verification, October 12, 02:20 UTC

PR596 merged as `d05acfa379d197e209fe11cd8c7f7a3246035a61`; workflow38195180766 succeeds. Automatic review reported one stale-checkpoint documentation finding, fixed in7898cb24; no application findings. An initial merge invocation failed argument parsing before any mutation; exact-head retry succeeded. The CI diagnostic request initially used the merge SHA where the tool requires the PR head, returned409, and succeeded with the correct PR head.

Receipt `summary-query-runtime-1791771527.json` at02:18:32UTC verifies all40 runtime hashes, API shared2CPU/2048MB and DB performance1CPU/2048MB unchanged, publication false, configured20-target hourly data-lane job unchanged, disabled job0.0136CPU without ORM/database imports, and no queued/running jobs. Eight public requests succeed: feeds0.370/0.384seconds, freshAAPL1.492/NVDA1.163seconds,50newsitems0.304seconds. These post-restart samples do not establish sustained capacity.

Deployed Python3.12.15 passes the isolated in-memory SQLite fixture with ordinary reads and both overflow paths producing identical complete states. Socket connects and requests are blocked; no production writes or emails. Receipt `summary-query-fixture-runtime-1791771593.json`. The first helper assumed the development-only httpx package existed and failed before fixture setup; diagnostics exposed the missing package, then the helper was corrected to block sockets without installing anything. Local Python3.14 repeat also passes.

Two-hour pre-deploy metrics `cpu-quota-1791771476.json` have121samples of zero throttle but CPU balance declines8129 to3235, with current2701. Current API busy CPU is about0.161 versus0.125baseline, including background nice0.1266. Intervening releases and credit decline prohibit a sustained-headroom claim. `scheduled-job-cpu-1791771484.json` records enrichment16.7876CPU/48.84wall seconds, reference1.4407CPU and prior collection1.3762CPU. Next investigate recurring cost and live bounded publication capacity before activating another workload. No hardware or schedule change.

All local rehearsal and verifier handles are terminal; the owned PostgreSQL instance is stopped and both schemas removed. Institutional selection staysFMP/publication false, public coreFMP. Source holds, broad financial parity, Senate access, transcripts, all scheduled consumers/rankings, finalMassive subscription/verification, zero global FMP traffic and billing cancellation remain open.
