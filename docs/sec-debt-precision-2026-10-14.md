# Original debt precision evidence

October 14, 2026. Implemented locally; release/runtime/ordinary verification pending. Core fundamentals and prices remain FMP, institutional publication remains disabled.

## Source and behavior

The [Comcast June 30 original](https://www.sec.gov/Archives/edgar/data/1166691/000162828026049360/cmcsa-20260630.htm) reports debt as90.4b with `decimals=-8`, and current6117m plus noncurrent84264m with `decimals=-6`. All three use context c-22. The precise sum90.381b fits inside the interval implied by the rounded summary under round-to-nearest. Source SHA256 `4ed89cc5104d722b9f3a3c112995b4ae1589704d99a91e759f7b49bf54277a69`.

Companyfacts omits these accuracy attributes. The core preparation path now requests at most one exact current original only when a compatible complete debt total differs from its current/noncurrent sum. Exact directory identity and availability, bounded SEC transport, original namespace/issuer/period validation and existing cache reuse apply. Public financial-statement acquisition keeps its original default behavior and does not request this extra document.

Optional acquisition failure preserves existing core facts and reports unavailable precision. A precision-only original is never merged into selected financial values. An annotation requires exact reported value, tag, period, filing, issuer and original hash. Raw decimals and source identity are retained. Existing inline supplements can also supply the same narrowly scoped debt evidence.

Leverage v8 includes the proof with explicit rounding assumption, intervals and limitations. The [XBRL Calculations1.1 specification](https://www.xbrl.org/Specification/calculation-1.1/REC-2023-02-22%2Bcorrected-errata-2024-02-14/calculation-1.1-REC-2023-02-22%2Bcorrected-errata-2024-02-14.html) distinguishes rounding from truncation; this feature is not a full conforming calculation validator. It requires full containment rather than mere overlap and rejects missing/invalid precision, excess digits, different originals and unbound values. It neither assumes a filing's rounding mode nor proves accounting scope. Lease-overlap and aggregate guards remain unchanged; no public ratio is selected.

## Validation

The actual complete original plus checksum-verified companyfacts and submissions reproduce all three debt observations and decimals. Reported-total interval is[90.350b,90.450b], while the maturity-sum interval is[90.380b,90.382b]. The full projection repeats identically and preserves all prior canonical values, non-leverage evidence, EBITDA34.131b, candidate amounts and existing holds. Whole-projection SHA256 `0c0a894f5cf5f4a1682728351e60cc917a511a1e5d80da174ca4552c7ff7a04b` on Python3.12.3.

A real isolated SQLite preparation test makes four fixture reads (directory, companyfacts, submissions, original), stores the evidence and repeats without any source request or payload change. No canonical FundamentalsCache or Event rows are created. Twenty-five separate original-companyfacts control pairs preserve canonical values, non-leverage evidence, EBITDA and full repeats under baselinev7/candidatev8. Eight known companyfacts-only coverage holds remain; this is not full fresh-universe coverage.

The source fixtures and hashes are tracked under backend/tests/fixtures/sec_debt_precision (367845bytes total compressed/manifest). Local replay receipts are artifacts/debt-precision-original-replay-oct14.json and debt-precision-controls-oct14.json. No replay makes network requests, production writes or customer deliveries. Focused tests cover optional failure, source/period identity, default statement isolation, complete-source preparation/cache reuse, invalid precision and unchanged double-counting guards. Expanded Linux Python3.12.3 group:312pass/one unchanged forward-P/E assertion; the identical assertion reproduces with all five modified existing application modules loaded from committed baseline. Final45 precision tests pass after the stale/future request guard. Earlier283 affected checks and the added actual-original cache test also pass; no full-suite claim.

One initial test expected malformed directory arrays to be ignored; the existing strict directory rejection correctly remains, and the test was corrected. An expanded test invocation named a nonexistent test file and ran no tests; corrected discovery uses the actual financial-cache/comparison/ticker tests. No coverage is claimed from that failed invocation.

## Remaining gates

Deploy and verify this exact package and ordinary propagation. Individual CPB/COO/VRSK corrected-cache rotation and CMCSA precision persistence remain unobserved. Original rounding evidence supports the later policy decision but does not finish canonical net-debt/EBITDA selection, missing interest/D&A coverage or whole-universe financial continuity. All score/ranking/monitoring/watchlist/daily/weekly consumers, institutions/transcripts, sustained capacity, final Massive, zero global FMP requests and billing remain gates. No provider selection, schedule/host/subscription, email, video, landing or cancellation change.

The initial baseline harness needed the workspace import path and repository-owner read permission; those setup failures did not execute the assertion. The corrected baseline reproduction confirms the exact same None-versus25.0 failure at test_ticker_financials.py:281. Baseline module hashes are retained in artifacts/debt-precision-baseline-oct14/manifest.json.
