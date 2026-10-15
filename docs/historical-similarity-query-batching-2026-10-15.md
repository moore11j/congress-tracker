# Historical similarity query batching, October 15, 2026

## Measured problem

Production c4eaa0f0 (PR653) retains slow newly generated ticker bundles. The00:18 AAPL warning reports3.754seconds inside the builder, including2.704seconds for confirmation/history; NVDA2.915seconds includes1.453seconds there. Cached00:21 reads are0.468/0.536seconds and fresh IBM1.493seconds. At00:21 the current API has CPU credits and zero sampled throttling. This identifies the measured stage without claiming that all latency has one cause.

A bounded, read-only profile excludes live snapshot capture and prohibits HTTP. AAPL confirmation context takes0.599seconds/42queries; historical similarity takes0.798seconds/214queries, including212 separate immutable outcome-entry lookups. The profiler adds overhead; it is attribution evidence, not a controlled end-to-end benchmark.

## Repair and boundaries

Reuse the existing outcome ledger bulk reader for the selected similarity matches, loading canonical entries and their horizon observations together. Pass those maps into the existing outcome renderer. Fetch historical prices only for allowed legacy snapshots without a canonical entry. There is no new cache, source selection, query limit, matching/scoring method, date policy, return calculation or response-field change. All point-in-time/live-only, episode deduplication, audited outcome and entitlement rules remain.

Implemented on `codex/ticker-context-latency`, not deployed at this checkpoint. PR651 precision remains held. FMP core/prices/transcripts and institutional-publication=false remain unchanged; two services and schedules remain unchanged.

## Verification

Linux Python3.12.3:59 similarity/integrity/Premium pagination checks and19 ticker projection/public cache/divergence checks pass. New mixed fixtures cover bullish/bearish canonical matured outcomes, missing canonical horizons, allowed synthetic legacy outcomes, forbidden unaudited outcomes, historical reconstruction exclusion and episode deduplication. Entire payloads and both public projection variants equal the prior individual-lookup path, with exactly one entry query and one observation query and no writes.

Initial new fixtures correctly failed the immutable-snapshot and complete-provenance guards; fixtures were corrected before insertion, without weakening production guards. The older similarity test failed identically against the unchanged deployed implementation because its synthetic prices lacked the explicit test source required by the existing legacy policy. Adding that fixture label restores its intended test coverage; no production legacy policy change.

An actual PostgreSQL comparison runs both readers and repeats in one read-only repeatable-read transaction, with snapshot writes excluded, HTTP prohibited and3second SQL timeouts. Full outputs match exactly:

| Scope | Matches | Old queries | New queries | Old wall seconds | New wall seconds |
|---|---:|---:|---:|---|---|
| AAPL |212|214|3|0.663 /0.759|0.407 /0.387|
| NVDA |99|101|3|0.503 /0.507|0.392 /0.381|

AAPL full-result SHA256:`4dcbeb7221040660dfb951c5564cf7e5e9862958e68be4d6b5273b8ca4db92f2`; NVDA:`ccdc537397c9413d6af9f96d81a9c4a06ff88bd95fa2d185cc518d291a4e74a4`. These matched cohorts had no canonical entries, so they preserve explicit reconstruction-required outcomes; the positive canonical case is covered by the local audited fixtures, not claimed as a live populated cohort. No production data writes, provider requests or emails occurred. Timing samples do not establish sustained performance or full-site recovery.

Local evidence: `artifacts/ticker-confirmation-profile-1792023749.json`, `historical-similarity-parity-1792023922.json`, `historical-similarity-baseline-test-oct15.log`, `historical-similarity-verified-tests-oct15.log`, `historical-similarity-consumers-oct15.log`; stage and CPU receipts in `artifacts/direct-feeds/free-replacements-2026-10-09/` with timestamps1792023689/1792023665. These artifacts are ignored; conclusions are retained here.

## Remaining checks

Review/deploy and verify the exact runtime, public ticker contract and ordinary operation. This read-only service change can be reverted independently while retaining the current continuation-aware scheduler and its queue. Full ordinary portfolio/external completion, sustained capacity, financial coverage/definitions, institutions, transcripts, all ranking/monitoring/watchlist/daily/weekly consumers, final Massive subscription/validation, zero FMP traffic and billing/access dates remain gates. Do not shut down FMP from these narrow performance checks.
