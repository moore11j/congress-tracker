# Retain conflicting SEC 13F originals, October 12, 2026

## Evidence and cause

Production collection run206 reported17 cover/table reconciliation failures. Three independently captured public SEC originals confirm matching row counts but different totals:

| Accession | Rows | Cover total USD | Table sum USD |
|---|---:|---:|---:|
| 0001737089-26-000006 | 1669 | 369178 | 3691783 |
| 0001802091-26-000004 | 101 | 588945018 | 588945021 |
| 0002139665-26-000002 | 1159 | 168905531 | 168905543 |

[SEC Form 13F special instruction 6(c)](https://www.sec.gov/files/form13f.pdf) defines the cover total from the information-table values. Exact reconciliation remains required; this repair does not infer a rounding tolerance or correct issuer values. These are unresolved coverage holds, not accepted complete filings.

Previously a parser exception rolled back collection before preserving its bytes, marked the document failed and retried after one hour. A typed cover mismatch now retains the exact immutable original with a SHA256, empty publishable positions and explicit declared/actual diagnostics. It marks the document quarantined under the existing collector review interval (168 hours in ordinary SEC collection). A changed original or amendment is still separately identifiable. Prior successful revisions remain immutable. Transport failures and persistence failures remain failed/retryable; no broad exception conversion.

## Local validation, 05:09 UTC

Python3.14.2:243 affected checks pass, including retained original/revision dedup, two-hour skip, eight-day recheck, previous-success preservation, rollback after failed storage, transport retry, publication and no-send alert consumers.31 focused checks pass independently. The first test copy used an incorrect backend/backend path and ran no tests; corrected before both successful runs.

All718 accepted saved originals produce exactly the same complete text/parsed/hold outputs as the prior baseline, including157503 positions. Parser CPU28.766seconds is local timing, not production throughput. Receipt institutional-cover-parser-parity-oct12.json. Three actual rejected originals pass an isolated collector replay:three quarantines, exact saved byte hashes, no second-hour fetch and zero canonical positions/filings/events/emails. Receipt sec-cover-actual-collector-oct12.json. No production rows exported or changed.

## Release state

Implementation is local and not yet deployed. Both source acceptance and institutional publication gates remain unchanged. Senate shadow collection's first manual run is already verified; ordinary collection/freshness and source selection remain separate. Institutions, public core financials, transcripts, all retained scheduled consumers, final Massive prices, global zero-FMP traffic and cancellation remain unfinished. No purchase, schedule or hardware change.


## Production release, October 12, 05:13 UTC

[PR601](https://github.com/moore11j/congress-tracker/pull/601), tested head d0023f8508157927ac416a2784abfa7e03d0e855, merged as ce0ce8570a6009da0344bcdc5c43c9a3ee877559. Backend workflow38206861642 succeeds, including readiness and Premium privacy.55 runtime files match; API shared2CPU/2048MB and database performance1CPU/2048MB are unchanged. Secret scan passes. Exact-head CI diagnostics show33 existing Pillow10.4.0 advisories and existing frontend dependency findings; no dependency manifests changed. Automated code review is unavailable because its quota is exhausted. This is not an all-CI or security-clear claim.

Deployed Python3.12.15 runs the actual101-position original in isolated in-memory SQLite: exact bytes retained under the expected SHA, source totals still held, second-hour run performs no source fetch, zero canonical writes/emails. Receipt sec-cover-deployed-fixture-1791782001.json. This proves runtime behavior, not an ordinary SEC collection receipt; next ordinary SEC run remains required.

Runtime receipt sec-cover-runtime-1791782014.json confirms zero pending/running queued jobs, institutional publicationfalse, Senate collectiontrue/publicationfalse and unchanged bounded schedules. News50items0.314s, feeds0.392/0.497s, freshAAPL1.569s/NVDA1.239s. These postrestart samples do not prove sustained capacity. No source selection, schedule, purchase or cancellation change.
