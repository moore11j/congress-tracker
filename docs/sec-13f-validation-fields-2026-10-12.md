# Shared SEC 13F validation fields, October 12, 2026

## Measured bottleneck and scoped repair

The current positive PostgreSQL rehearsal restores the same 718 captured originals and 579 references, prepares 20 real prior filings, then publishes 20 actual current filings. It produces three source-bound events and repeats with an identical complete database state, no extra positions/events, zero source calls/emails/production writes. The profiled run uses 100.141 CPU/109.601 wall seconds; instrumentation makes this unsuitable for direct speed comparisons. Its complete profile shows 671,103 XML findtext calls and 13.875 seconds in that path, with preparation consuming 51.358 of 109.535 cumulative seconds. Receipt institutional-positive-profile-candidate-oct12.json and its .profile retain the evidence. Owned local PostgreSQL was stopped and the schema removed after success.

The complete-submission validator now extracts each information-table row once, using the existing namespace-aware first-leaf index for both validation and normalization. The existing normalization body moves into a shared private helper. Exact decimal totals, row counts, missing/invalid numbers, identity/date/form checks, blank-first duplicate fields and split repeated parents retain their existing behavior. There is no acceptance tolerance or source/schedule/publication change.

## Validation

Python 3.14.2: 284 affected tests pass. A new traversal regression fails against the deployed parser with eight duplicate XPath reads for two rows, and passes with the repair. Required fields, negative/nonfinite/overflow/missing values, duplicate and split-parent XML, standalone XML normalization, SEC quarantine, collection, publication and provenance regressions pass. Initial test copy had a backend/backend destination and ran no tests; corrected before passing checks.

The same-process before/after comparison validates all 718 complete text/parsed/hold outputs, including 157,503 positions, against each other and the retained independent output hashes. CPU is 29.750 to 26.203 seconds (11.92% lower); this is parser-only local evidence, not total-job or production throughput. No provider calls, canonical writes or emails. Receipt institutional-validation-fields-parity-oct12.json. A complete unprofiled positive PostgreSQL batch and full-state repeat on the candidate is running in isolated schema bounded_13f_17643e38fbb342a3907a8c57720b457d; handle38920 must be observed to terminal before claims or cleanup. Never restart for silence.

## Current production boundaries

05:35 CPU metrics show no throttling in 121 two-hour samples and 8883 current burst balance, with current API busy CPU about 0.1207 versus 0.125 baseline. Intervening restarts and near-baseline utilization do not establish sustained capacity for additional work. Live institutional inventory at05:38:56 is unchanged:718parsed/17failed/34quarantined,769unvisited,82,758,139parsed original bytes, zero missing originals; ownershipFMP/generation0, publicationfalse. API shared2/2048MB and database performance1/2048MB remain unchanged. Recent recurring job CPU includes news2.1604s, SEC research1.3792s and SEC metadata1.6846s; partial statuses retain coverage holds.

Senate backup remains selected generation5 with two existing receipts and no added events; latest queuedrun34 is terminal/exit0. Next ordinary selected06:27 remains unobserved. SEC05:47 ordinary quarantine observation remains pending. Current production remainsce0ce857; this parser repair is not deployed. No landing edit, purchase, email, capacity or recurring schedule change, source activation or FMP cancellation.

The preceding landing-only turn did not advance migration. This turn supplies a new actual publication profile, implements a measured repair and proves exact original-output parity. Institutional sustained capacity, source coverage, free core/transcripts/all consumers, finalMassive, zero globalFMP traffic and billing cancellation remain open. Goal active.


## Completed publication rehearsal and review

2026-10-12 05:48 UTC: PR603 validation complete, release pending. 284tests and exact718-original/157503-position parity pass. Candidate PostgreSQL38920 terminal/pass:20positive filings, three source-bound events, full publication results/six canonical projections identical to profiled baseline, repeat creates nothing and entire state identical (fb17c2a0c95a8f4966f2cb42f47d230bca138fcfa916aef19e80a00254dad4d9). Candidate52.062CPU/62.066wall, repeat17.797/18.287; no causal whole-batch speedup claim from comparison with profiled or older runs. OwnedPG stopped/schema removed. SECordinary05:47run215 completes without errors but processes0, so quarantining previously failed originals remains unobserved; next06:47. Backendce0ce857/institutionsfalse; Senategeneration5 ordinary06:27 pending. Secret/Vercelpass, unchanged dependency audits fail. No purchase/source/schedule/capacity/email/landing change or cancellation. Goalactive; this turn concrete repair/evidence progress.
