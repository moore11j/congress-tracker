# Revenue periods required by the calculation, October 27, 2026

## Problem and change

The shared revenue selector rejected GigaCloud's complete current contract-revenue history because an unused September 2025 period had no observation under that concept. The three actual trailing-year inputs are the 2025 annual total, January to June 2026 and January to June 2025. All three have complete original consolidated statement proof, and the two interim totals match the existing total-revenue concept exactly, including filing identity.

The selector now ignores an absent candidate period only when a valid existing observation exists and that period is not required by either selected current or prior trailing calculation. Present but ambiguous candidates, differing values, filing identities and missing required legs still hold. Every selected period still requires an original consolidated total; histories are not spliced into core calculations. No symbol-specific rule, new source allowance or scoring threshold is introduced.

Actual-cache testing also exposed missing persisted revenue-proof validation. Both public statements and core cache readers now reject malformed source identity/hash, period, total label, accounting basis, overlap and service-total reconciliation evidence without issuing network requests. Existing supported selections remain accepted.

## Complete-original result

Four original public SEC files are retained with byte counts and SHA256 hashes in `backend/tests/fixtures/sec_revenue_unused_periods/sources.json`: current companyfacts, submissions, FY2025 and June2026 interim statements, acquired October27 at05:34UTC. The actual adapter uses two distinct filing reads within its existing shared acquisition limit.

The resulting current revenue is $1,466,516,000: $1,289,897,000 plus $771,131,000 minus $594,512,000. Five additional supported metrics are free-cash-flow growth/margin and gross/net/operating margins. The three previously supported values remain identical. Operating margin is $170,800,000 divided by $1,466,516,000, expressed as a percentage. Revenue growth and net debt/EBITDA remain unavailable. Displayed history retains supported prior values. No GCT leverage admission is included: the income proof has a separate tax-presentation mismatch that remains held.

This is a shared selector repair, not evidence that all financial coverage is complete. The separate offline audit of 80 partial cases among280 retained pairs found no additional usable leverage ratio from merely exposing leverage through the partial-financial path; no such change was made.

## Validation and release state

The focused Windows suite passes62 checks, including four complete originals, actual preparation/cache reuse and rejection, retained history, real conflicts, duplicate/missing source evidence, and source-to-score/watchlist recovery, repeat, threshold crossing and rollback. The local runtime rehearsal verifies16 source/fixture hashes, four originals, two reads, five verified revenue periods and exact repeat:5.46875CPU seconds, no network, production records/writes or emails.

The corrected Linux3.12.3 regression passes203 checks in147.90seconds, including the source, prior revenue/primary-income, cache, preparation and affected consumer suites. Linux runtime rehearsal passes the same16 hashes and two-source result in4.968332CPU seconds. The final frozen49-source comparison remains in progress at this checkpoint. The first comparison was discarded because the cache-validation source changed during execution. An initial Linux invocation named a nonexistent test file and ran no tests; the corrected invocation uses the actual cache/preparation test files.

Application changes are local on `codex/revenue-unused-periods`, based on the exact application tree deployed as6906305b. Not released or activated. Core FMP remains selected. Required next gates are the final tests/parity, reviewable release, installed proof and ordinary scheduled propagation. Broader financial/consumer/institutional/transcript/paid/Massive/zero-FMP/billing acceptance remains open.

## Institutional validation discovery

Read-only inventory1793079649, observed05:40:40UTC, reports5,062 parsed originals,169 quarantined,625,915,858 parsed-source bytes, no missing originals and maximum original3,328,754 bytes. Institutional ownership remainsFMP/generation0; all5,231 documents are unvisited by publication. This aggregate contains no institutional positions or customer payloads and is separate from the previously rejected snapshot export.

The October15 snapshot runner's2,000-document/200MB bounds no longer cover this cohort. It was not executed or enlarged. Its `main()` does already lower CPU priority with `os.nice(19)`; the plain Python SSH command does not imply that protection is absent. Fresh bounded staging/load verification and explicit owner approval of the production payload and local OneDrive destination remain necessary before an export. No alternative transfer was attempted.
