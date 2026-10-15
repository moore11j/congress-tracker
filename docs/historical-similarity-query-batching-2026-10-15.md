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

## Deployed release and current observation

October15,00:40UTC: PR654 source `64c1b4a856d38c1dd9cc99d03dd4b5fd3e38a88f` merged as `68c28d9aabe06db6c058498460a36ace539d6e05`. Existing backend workflow38660940431/job116181752688 completed successfully, including readiness and Premium privacy checks. Two transient GitHub log502 responses were followed through the same run; no duplicate deployment. Queue was empty before dispatch. Exact CI logs confirm the unchanged frontend five vulnerability groups (four high, one critical) and Pillow audit failure; secret scan passes. Dependency manifests were not changed. PR attachment failed at the app's100-attachment limit; no prior attachment was removed.

Runtime verification on Python3.12.15 compares the deployed module with the saved prechange module in one read-only repeatable-read transaction, with HTTP blocked and snapshot writes excluded. The UTF-8/LF-normalized source hash matches the reviewed implementation. Two verifier attempts stopped before comparison/database queries because of host text-decoding/hash normalization; explicit UTF-8 on both sides resolves this without changing application code. AAPL212matches and NVDA99matches retain the exact hashes above across both readers and repeats. AAPL214queries become3, old0.710/0.741seconds versus deployed0.397/0.405; NVDA101queries become3, old0.495/0.531 versus deployed0.406/0.408. No production writes or emails. Receipt: `artifacts/historical-similarity-runtime-1792024744.json`.

After that verifier finished, seven public requests succeeded: Feed0.394seconds, Insights0.748 with research and no unavailable fallback, news0.251 with12items, research0.451 with44briefs, AAPL2.270 and NVDA2.396, readiness0.468. These are immediate samples, not full browser-render or sustained-load proof. Receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/insights-incident-1792024760.json`; its obsolete hardcoded release boolean is not used.

Ordinary00:37 news completes23/23scopes with42fresh company caches, and earnings preparation5/5. Research20scopes remains17partial/3unavailable; fundamentals latest batch remains6partial/4unavailable. Reference scope reports a truncated117-scope window, not proof that the broader inventory shrank or completed. Both existing production machines retain their sizes, API image matches68c28d9a, institution publication isfalse and public core/prices/transcripts remainFMP. Receipt: `artifacts/preload-receipts-1792024772.json`.

The full ordinary portfolio run/external receipt still awaits the00:45scheduled trigger at this checkpoint. No manual full rerun was started. Ticker latency and sustained capacity remain open; this narrow deployed optimization does not complete migration or permit FMP shutdown. Codex currently permits ordinary usage with92%weekly consumed; the screenshot429's specific historical cause and permanent recovery are unproved.
