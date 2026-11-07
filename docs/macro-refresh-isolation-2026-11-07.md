# Macro refresh durability and request isolation

Read-only production evidence at 04:10 and 04:19 UTC finds 11 current CFTC asset
rows dated November 3, fetched November 6 at 21:46. All 1,976 ticker cache rows
are older, the latest dated September 22; the latest macro feed date is also
September 22. The existing weekly job covers 5,698 known symbols. Its last run
was interrupted, and its request audit records 785 failed ticker commentary
requests, all HTTP 429. This is current asset collection without completed
consumer refresh, not a failure to obtain the underlying CFTC data.

The existing loop calls optional model generation before committing its ticker
updates. The model failure handler rolls back the shared session. A regression
against unchanged deployed `46191327` reproduces 64 calls, each observing zero
durably saved ticker rows. The repaired path saves all 64 rows before its first
request, stops after the first failure, and retains identical numeric data on
repeat.

The repair commits the existing macro feed before ticker work, reads profiles and
prior caches in batches of 50, and commits each completed ticker batch. Metadata
precedence and the selected fundamentals provider are preserved. Case-ambiguous
records use the existing single-symbol lookup. All numerical mappings, weights,
report dates, feed identifiers and paywalls remain unchanged.

Optional ticker commentary runs after all deterministic rows are durable, with
at most three requests and a stop after the first unsuccessful result. A weekly
market summary may be prepared afterward, unless ticker commentary failed. This
is at most four optional requests per existing weekly run; no new schedule is
added. Matching cached text remains available. New text is attached only while
the row still belongs to the same refresh, under its row lock.

Insights requests now use a matching cached market summary or the existing
deterministic fallback. They do not request a model or write the summary cache.
Previously a summary cache miss could make a model request on the page path.

Initial source, durability, interruption, request and existing macro checks pass
33 cases. A broader development selection passes 174 and fails two unrelated
existing cases: the market-pressure fixture expects an equal-weight conflict
although the current weighted result is bullish, and a watchlist entitlement
fixture lacks its alert-rule table. Both failures reproduce with unchanged
deployed macro code; no production behavior or test expectation was altered to
hide them. The dedicated release suite covers the repaired paths, canonical
scores, provider selection, ticker consumers and macro access checks.

The final batched implementation passes 147 local checks, including two one-slot
connection tests that reproduced occupied database slots before the repair.
Settings lookup transactions now finish before optional external requests. One
local attempt could not create temporary directories and recorded 135 passes and
12 setup errors; the complete rerun used an isolated writable temporary directory
and passed all 147 cases. No assertions were weakened.

A complete captured-metadata comparison covers all 5,698 known scopes separately
under FMP, SEC and free-direct selection. Profiles, mappings and full numerical
projections are identical to unchanged 46191327. Each selector uses 342 profile
queries instead of the comparison baseline's 34,182. This is a measured query
comparison, not a claim of equivalent production latency or newly available
coverage. FMP maps 2,926 scopes; each replacement maps 2,676. Unmapped scopes remain
unmapped. The capture contains public instrument metadata, not customer records.

Exact Linux release, installed proof and bounded recovery of the interrupted
weekly job remain pending at this checkpoint.
The broad FMP migration, fresh market prices and institutional publication retain
their separate gates. No production writes or model requests were made by the
investigation or tests.

Evidence in the primary checkout: `macro-job-completion-1794024636.json`,
`macro-job-completion-1794025147.json`, `macro-baseline.xml`,
`macro-existing-baseline.xml`, `macro-isolation.xml` and `macro-release.xml`.
