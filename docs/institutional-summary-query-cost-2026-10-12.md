# Institutional summary query cost, October 12, 2026

The [actual PostgreSQL publication measurement](institutional-bounded-writes-2026-10-12.md) found 30,108 SELECT statements and 82.172 process CPU seconds for 20 event-producing filings. Repeated per-ticker summary reads reload the same quarter's active filing list and independently fetch changes and current positions.

## Implementation

The existing whole-filing and symbol-batch processors now read summary inputs in groups of up to 50 symbols within each filing. The quarter's active filing IDs are read once. Each shared changes/positions query is capped at 10,001 rows to detect overflow; more than 10,000 rows invokes the original complete individual reads, without truncation. Noncanonical input symbols also retain the original reader behavior. No shared state survives the generator or filing transaction.

The existing summary calculation is factored without changing its body. Per-symbol summary updates, event generation, materialization order and flushes remain in their prior order. Superseded filings stay excluded. Source ownership, publication receipts, evidence, event identity, timing, rank/score rules, job limits and schedules are unchanged.

## Current validation

The targeted query regression fails on the prior implementation with 13 repeated current-quarter filing-list reads, then passes with one. Eight focused checks cover complete row/event parity for whole-filing and symbol-batch processing, both overflow paths, superseded holdings, exits, option rows and empty/noncanonical input. The broader run passes 153 checks and fails the existing `test_institution_profile_endpoints_are_locked_until_pro` holder-name assertion. An independent rerun using `origin/main` institutional service code reproduces the same failure. No full-suite pass is claimed.

An initial new fixture specified a nonexistent filing `holder_name` column; it was corrected before the meaningful red regression. An edit helper initially placed constants after a decorator; its compile-before-write check rejected the edit without modifying application code, and the helper was corrected. Neither failed attempt is counted as application validation.

Actual PostgreSQL baseline/candidate parity and performance measurements are running against the same 718 captured parsed originals and 579 references. The baseline uses the original individual reader and unchanged calculation; the candidate uses bounded input groups. Both use 20 actual current filings and their real prior evidence, exact current publication logic, no provider requests, no emails, disposable loopback-only schemas, and an all-table identical-repeat check. Cross-run canonical projections retain every source/value/date field except explicitly identified execution timestamps. Results are pending; no performance saving or production readiness is claimed yet.

## Other cutover state

The full captured-cohort replay `46444` remains live after its 574-document first pass, with 119/119 generated SEC event proofs retained. Full repeat and consumers are not yet verified. This process imported the deployed implementation before this edit; it is not a validation of the new optimization.

Read-only receipt `financial-versions-1791770453.json` at 02:00:53 UTC verifies the ordinary 01:56 financial run: ten SEC scopes and eight Finnhub scopes, no quota stop, core still FMP. SEC v5 records rise to 221 and Finnhub v2 to 58, versus 211/52 at the previous observation. Complete current-version coverage and numerical/semantic checks remain open.

No application deployment, source activation, purchase, customer email, schedule/capacity change, landing-page change or FMP cancellation. Institutional publication stays disabled. Remaining source coverage, Senate, core financials, transcripts, all affected consumers and final Massive/global FMP/billing gates remain.
