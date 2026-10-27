# Comparative statement captions and column periods

## Confirmed source defect

The ASC test discovered during PR739 validation fails on the deployed `ec74b5ff` lineage as well as the prior parser. Its complete SEC filing prints a shared caption covering three and six months in both 2026 and 2025, while each individual column has its own complete duration and date. The parser prepends the entire comparative caption to each column and then rejects the two years as an ambiguous single period. The cash-flow table has the same issue. This is a shared table-layout problem, not a ticker-specific accounting policy.

Original failure evidence: `artifacts/asc-existing-date-hold-oct27.json` and `artifacts/asc-existing-header-detail-oct27.json`. The complete originals and their independent tagged financial values are retained in `backend/tests/fixtures/sec_statement_currency_scope`; existing tests validate every fixture hash and byte length. The current tagged ASC financial path already works and is not being replaced with HTML values.

## Repair and boundaries

The existing combined-header interpretation remains first. When it fails specifically for ambiguous dates or durations, a fallback requires an independently complete column period. Every applicable shared caption must agree with the column's month/day, include its year if years are stated, and explicitly support its duration. Multiple different shared month/day values, unsupported durations, conflicting years, malformed or future column dates and missing column context remain held. No date is inferred from the filing date and no amounts, currencies or financial definitions are changed.

Discovery qualification version3 requires earlier qualification receipts to be rechecked on ordinary collection. No refresh is forced. Collection remains enabled, public discovery readers remain disabled, and core fundamentals/prices/transcripts remain FMP. No new schedule, host, subscription, source publication or customer send is introduced.

## Validation in progress

The original failing ASC test and16 initial comparative-period tests pass locally. An additional complete-source check compares both quarterly HTML revenues against independently tagged filing values, preserves separate half-year observations and verifies repeat identity. Explicit euro/pound checks protect the unchanged currency columns. The full affected Linux suites and a frozen49-source comparison against exact deployed `ec74b5ff8ad7ba4a390bcb50372abe2b2e6f6734` are running.

A local helper initially decoded UTF-8 as the Windows default encoding, changing currency symbols. The affected comparison/test processes were explicitly stopped, their incomplete results were discarded, and the original symbols were restored from the deployed Git bytes before starting fresh validation. This issue never reached a commit or deployment. All subsequent text reads/writes use explicit UTF-8.

## Ordinary collection and remaining gates

At11:22 UTC, the ordinary11:21 financial worker completed10/10 scopes. Discovery now has ten checkpoints, still with pending qualifications. The newest BBVA checkpoint inspected three covers and has45 covers remaining. This is incremental collection evidence, not complete source or public-reader acceptance. FMP remains selected; no source deadlines or scheduling order were bypassed.

Further qualification, selected-reader verification, broad financial definitions/coverage, all alert/ranking/digest consumers, sustained capacity, pending institutional capture/export approval, Meta's normal retry, Massive last and zero-FMP/billing closure remain open. The previous goal turn made progress by deploying PR739 with verified runtime/source parity; this turn repairs the newly isolated source defect and measures ordinary progress.


## Terminal source and consumer verification

The corrected frozen source passes all272 affected Linux Python3.12 checks, including the previously failing original ASC test, independent quarter/half-year binding, malformed and future dates, conflicting shared years/durations, Unicode currencies, source-to-score/watchlist transitions, repeat caches, discovery progression and scheduled-job bounds. There are no failures, errors or skips in `artifacts/column-period-linux-oct27.xml`. Earlier explicitly stopped runs are excluded.

All49 complete retained source cases preserve exact acquisition, panel, core, provenance, hold and request outputs against deployed `ec74b5ff`. Receipt `artifacts/column-period-release-parity-oct27.json` records270.125 process CPU seconds and zero provider requests or production writes. ASC's HTML quarter revenue116,214,000USD now independently matches its existing tagged panel, while comparative quarters and half-years remain separate. The source-hash proof pins15 code/test/fixture files for exact-release verification. This does not claim a new public ASC field: the tagged path already supplied it.

The 11:25:40UTC read-only actual-planner receipt `discovery-queue-order-1793100340.json` finds111 due scopes, still placing PAGS at105, compared with105 due scopes/PAGS105 at10:45. AMX/DAO/ADAG are now106/107/108. Installed code sorts by last attempt while combining20-minute retry and20-hour refresh deadlines. This explains the observed lack of retry advancement and changes the next action: evaluate ordering by actual eligible deadline while preserving priority, current batch/load guards and schedules. No queue-order edit or override has been applied in this release.

Release and installed/ordinary verification remain pending at this checkpoint. Broader source qualifications, reader activation, all consumers and the overall FMP exit remain incomplete.
