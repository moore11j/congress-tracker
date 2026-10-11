# Fair retries for held institutional filings

October 11 UTC / October 10 local, 2026. Institutional publication remains disabled.

`publish_13f_batch(retry_waiting=True)` always selected the first100 held/waiting document IDs. Unresolved early filings could occupy that window forever and prevent later filings from being reconsidered when missing evidence arrives. Ordering by receipt timestamp would not fix this: unchanged retries intentionally preserve receipt timestamps and canonical state.

The repair saves a separate operational retry position in the existing InsightsSnapshot store. Each retry starts after the last completed document ID, wraps within the same bounded query when necessary and never selects the same document twice within a batch. The current limit1–200 and all source/original/value/publication checks remain. Unattempted filings retain their separate selection path.

The position is tied to the selected source generation. Completing a batch uses the existing feed writer lock and compares the previously read state before saving, so a late concurrent batch cannot rewind a newer position. Pauses and generation changes refuse stale updates. Same-position saves are no-ops. No schema migration, schedule, request budget, price lookup, source selection or customer email is introduced.

## Validation

A regression stages an early permanent source-identity hold and a later waiting filing, then supplies the later filing's actual prior. Before the fix, two limit-one retries both select document1 and never reach ready document2. With rotation, the second retry publishes document2 once; wrapping returns to the held document without changing its receipt or duplicating events.

86 focused evidence, prepared-evidence, batch, worker and publication checks pass on Python3.14.2. Additional cases cover wrapping, bounded unique IDs, compare-and-set stale writers, source generation reset, pause refusal and malformed operational state. The first test command used the repository root and failed during import; rerunning from backend reproduced the intended failure before implementation.

Actual disposable PostgreSQL16.15 passes four substantive publisher/rotation/state tests. The captured574-document replay backlog has321 eligible parsed held/waiting documents. Four bounded100-document selections reach all321; the old repeated first window reaches only100. Every publication receipt remains byte-for-byte equivalent under fingerprint `d9c93365ef59133200a482a6e318271ef846fb87d9e2b8360f29312bb249bd20`. A concurrent feed lock holder is refused. The fourth batch legitimately wraps to79 previously attempted IDs; no batch contains duplicates.

This captured backlog check validates selection and operational state, not321 new publications or resolved evidence gaps. No provider requests, production writes or emails occurred. Disposable schemas were removed and the local PostgreSQL server stopped. Receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/retry-rotation-postgres-oct11.json`.

The final full canonical/consumer replay93622 remains independently running on the earlier integrated institutional code. This change does not alter its source calculations or per-document publisher. Production capacity, complete peer growth bounds, unresolved identifiers/prior evidence, ordinary consumers and source activation remain separate gates.
