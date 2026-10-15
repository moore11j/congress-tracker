# SEC submission scanning compatibility, October 15, 2026

Deployed and verified as `12fd3668` through PR655, October 15. Institutional preparation is verified against the current complete peer inventory; publication cost and sustained capacity remain open. Institutional publication remains disabled; core fundamentals, prices and transcripts remain FMP.

## Change and measured bottleneck

A fresh Linux Python 3.12.3 profile of complete peer preparation from all 718 checksum-verified saved originals takes 18.066 CPU seconds. Regular-expression search/findall calls account for approximately 4.56 seconds. The optimization replaces only literal DOCUMENT and XML section boundary scans with string searches. It preserves nonoverlapping section boundaries, first XML selection, whitespace trimming and complete originals. XML parsing and issuer, accession, date, amendment, count, value and numeric validation are unchanged. No new cache, truncation, source fallback or relaxed hold is introduced.

The saved baseline module exactly equals deployed `544b893b:backend/app/clients/direct_sources.py`; normalized SHA256 `434716ad0bb95541a59ddb6e94f8add5d2a8655a338ec34c397eb2549f4cc32d`.

## Local validation

170 focused checks pass on Linux Python 3.12.3. They cover submission delimiter behavior, malformed and duplicate attachments, Unicode whitespace, XML field validation, source collection/quarantine, complete peers, prepared evidence and institutional publication/batches. The new differential tests include 1,010 deterministic boundary cases and full accepted-result or rejected-error comparisons. No full-suite or production capacity claim is made.

Four complete offline passes, in baseline/candidate/candidate/baseline order, verify all 718 original documents and parsed fields/holds. Full result SHA256 is identical: `3edc19018c6f3040bd714ed70a2b6b40ad61f4015d408e9a8710eb1d938827f8`. Complete September peer preparation for 1,560 CUSIPs from the first 20 captured targets retains all 402 peer records, metadata, values and summary bytes. Its complete SHA256 is `6d5f901efd88e5353f17c4bb9efed790909a27d566166e48c62dde90345e82a9`, also matching the preceding profile receipt. These first 20 captured targets are not asserted to be the current production selection.

| Pass | Parsing CPU seconds | Complete preparation CPU seconds |
|---|---:|---:|
| Baseline 1 | 22.389 | 35.980 |
| Candidate 1 | 19.693 | 11.534 |
| Candidate 2 | 8.389 | 7.054 |
| Baseline 2 | 22.946 | 48.761 |

Both candidate passes use less CPU, but the large variation and initial overlap with the isolated test run prevent a dependable fixed saving estimate. These timings exclude database publication and do not establish production throughput or sustained capacity. The original profile and this replay use different measurement procedures and must not be mixed into one speedup calculation. Networking is blocked; there are zero source requests, production writes or customer deliveries.

Receipts in the primary workspace: `artifacts/submission-scan-tests-oct15.log`, `artifacts/submission-scan-replay-oct15.log`, and `artifacts/direct-feeds/free-replacements-2026-10-09/submission-scan-replay-oct15.json`. Original capture SHA256: `5762d171fbf53a617b9b0681cb488b376c89950065fadd95bb002c5f38ee066b`. The baseline profile is `stream-preparation-current-oct15.json` in the same artifact directory. All local test/replay processes are terminal; none was restarted for silence.

## Session recovery and remaining gates

The owner screenshot reports a historical Codex remote-turn HTTP429 retry failure. Current execution and tools work, and the account reports ordinary usage allowed with 95% of its weekly window consumed. No model, retry, account or subscription setting was changed. The historical cause and permanent service repair remain unverified. Official [error guidance](https://developers.openai.com/api/docs/guides/error-codes) describes multiple 429 causes; the screenshot alone does not identify which applied.

At 01:03 UTC, the read-only production queue is empty and has no new portfolio cycle or external receipt. Earlier failed receipts are preserved, not replayed. Receipt: `artifacts/portfolio-cycle-1792026191.json`.

Next: exact release review and runtime verification, then bounded institutional writer and scheduled capacity validation before publication. Full ordinary portfolio completion, financial definitions/coverage and propagation, transcripts, all monitoring/watchlist/daily/weekly/ranking consumers, final Massive subscription/validation, zero global FMP traffic and billing/access dates remain gates. No production deployment, source activation, recurring schedule, host/subscription change, customer email, video, landing change or FMP cancellation occurred in this validation turn.

## Release and current production preparation

[PR655](https://github.com/moore11j/congress-tracker/pull/655), source `936adac390a4a08925a7260f64e54613470668e0`, merged as `12fd3668c2d69e4ce485b432d16db8af7ec15937`. Deployment workflow38664235932/job116192340018 succeeds, including readiness and Premium-route privacy verification. A fresh pre-dispatch queue receipt confirms no active, pending or continuing jobs. Two transient diagnostic log HTTP502 responses were retried against the same run; no deployment was restarted.

Nine deployed source/schedule hashes match. Python3.12.15 reproduces the complete saved eight-position SEC original and identical repeat, with full result SHA256 `da44ead3d7e47429e376dc866a057c11dd26dabbf05d2beedc0c0aaf610e792c`; empty XML remains rejected. The fixture has blocked networking and zero production writes or emails. Runtime receipt: `artifacts/submission-scan-runtime-1792026761.json`.

One explicitly queued execution of existing `maintenance-institutional-capacity-v1`, target run2, finishes successfully. Its helper is idempotent for that run and does not add a calendar trigger. The job retains actual scheduler ownership, source lock, read-only repeatable-read PostgreSQL, three-second statement timeout, memory/CPU/wall guards and existing data-lane serialization. It never calls the publisher.

Current result: 20 targets, 688 complete September peer filings, 81,174,781 comparison bytes, 40 reference documents and12 identifier documents. Preparation takes17.9282CPU/26.1909wall seconds, peakRSS302.08MB. Complete-peer scanning accounts for15.3195CPU seconds. Source calls, canonical writes and email deliveries are all zero; source ownership remainsFMP generation0. The older October12 run took19.6547CPU seconds but covered402 peers/48.4MB, so this is not a controlled production saving percentage. Current receipts: `artifacts/capacity-scan-queue-1792026800.json` and `artifacts/direct-feeds/free-replacements-2026-10-09/capacity-live-1792026837.json`. The queue reports run2 terminal/exit0 with no pending or active job.

Fresh01:15 read-only inventory reports1,321 parsed originals and63 quarantined documents, all1,384 unvisited by publication. All parsed originals are retained:150,281,590 bytes total, zero missing, maximum3,328,754 bytes. This is materially larger than the718-original offline replay cohort. A fresh full-cohort writer/consumer replay remains necessary before activation; the older replay cannot prove current coverage. Receipt: `institutional-live-inventory-1792026928.json` in the same artifact directory.

Public reads around this check succeed: Feed0.425seconds, Insights0.553 with research/no unavailable fallback, news0.330/12items and research0.493/44briefs. Fresh AAPL2.365/NVDA2.212seconds remain slower than the desired experience; later cached reads0.443/0.304 do not prove fresh latency is fixed. Two unchanged hosts and image12fd3668 verify. News01:13 completes23/23; research preparation01:14 completes20 scopes with16partial/fourunavailable, proving ordinary work resumes after the diagnostic without claiming complete research coverage. Institutional reference remains partial/deferred. Receipts: `insights-incident-1792026830.json` and `artifacts/preload-receipts-1792026898.json`.

Current sampled CPU throttling is zero with credits about8,831, but the prior two-hour history still contains exhausted credits and throttling. Current five-minute busy CPU is about0.133 against0.125 baseline. This does not prove sustained capacity. Receipt: `cpu-quota-1792026847.json` in the source artifact directory. At01:15 there is still no new portfolio dispatch/cycle/external receipt; old failures remain intact and were not manually replayed (`artifacts/portfolio-cycle-1792026917.json`).

Exact source CI passes secret scan; unchanged dependency manifests still produce the frontend five-vulnerability audit failure and the backend Pillow audit failure. No all-CI/full-suite claim. Attaching the PR reaches the app's100-attachment limit; the ordinary PR link above remains available. Next: refresh complete institutional replay evidence for the grown cohort and verify bounded writer cost/consumers, ordinary full portfolio and sustained capacity. All financial/transcript/consumer/Massive/global-FMP/billing gates remain. No provider activation, schedule/host/subscription change, customer email, video, landing change or FMP cancellation.
