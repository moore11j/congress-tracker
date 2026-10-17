# Revenue concept parity for AEVA and Allegion, October17,2026

## Observed problem and source evidence

Both issuers report current total revenue, but the generic reader chooses the current Revenues tag whose annual history is incomplete. It therefore withholds the entire current financial projection. The complete RevenueFromContractWithCustomerExcludingAssessedTax series contains current and annual data. Four current quarter/YTD pairs match the total exactly in each issuer's original10-Q; the reviewed annual10-K contains the complete annual basis using that same contract-revenue tag.

Eight full originals (companyfacts, submissions, current10-Q and annual10-K for each) are retained as deterministic compressed fixtures in backend/tests/fixtures/sec_revenue_concepts. sources.json binds URLs, hashes and statement identities. The compact production review config is derived from parsed complete annual originals; tests reparse the originals and require exact config equality. No production customer or provider-response export was used. The source diagnosis and remaining unrelated cases are in [the coverage report](revenue-coverage-diagnosis-2026-10-17.md).

Independent original-statement arithmetic: AEVA18,079,000+12,398,000-8,879,000=21,598,000USD trailing revenue; ALLE4,067,300,000+2,185,100,000-1,963,900,000=4,288,500,000USD. Eight AEVA and four ALLE financial metrics recover. Missing ALLE income/cash fields remain missing; this is not complete company or universe coverage.

## Selection and boundaries

sec_revenue_concept_parity applies only to exact reviewed symbol/CIK pairs. A complete ordinary total series retains precedence. Otherwise it selects a single unchanged contract-revenue history only when every overlapping total is identical in value, USD units, accession and filing date; current facts match the latest completed periodic filing; and every annual basis matches the reviewed original amount, period, accession and filing date. It never splices current totals into a different historical concept or treats including-tax revenue as equivalent.

Identity, stale/latest/ambiguous filings, conflicts, missing inputs, changed annuals and explicit nil remain held. Evidence retains selected/superseded concepts, matching original periods, source hashes and no-splicing basis. Existing inline provenance remains attached. Preparation version changes are restricted to the two repaired symbols; other issuers retain their existing rules. No new source requests, source switch, schedule, batch size, market input, preference, canonical write or email is introduced.

The next quarterly rollover is simulated and passes with a matching new total/contract report and unchanged reviewed annual basis. A new or restated annual basis that fails original equality remains held for source review; this test is not proof of future filings or indefinite unattended annual coverage.

## Validation and release state

Initial33 new Linux tests pass. Final expanded39 new cases plus adjacent fundamentals, preparation, continuity, bank, non-revenue and inline suites pass239 tests on Linux Python3.12.3 in48.23seconds. Complete source parsing validates the annual review and quarterly comparisons. Wrong currency/identity/accession, differing component totals, annual changes, missing current/prior periods, ambiguous/stale/new filings, nil history and unreviewed issuers are covered.

Actual quarterly acquisition feeds isolated preparation, exact cached repeats, score context and the real watchlist custom-alert evaluator for both revenue growth and operating margin. Recovery0false alerts, unchanged repeat0duplicates, later synthetic threshold crossing1alert, repeat0duplicates and rollback0alerts pass. No production records or EmailDelivery rows are written. These tests do not establish full live ranking/digest/source acceptance.

The bounded installed verifier is prepared and locally rehearsed:16 source/config/fixture hashes, two complete source projections and exact cache repeat pass on Windows Python3.14.2 with network forbidden and isolated SQLite. Production runtime and ordinary refresh are not yet verified. Branch codex/sec-revenue-concept-parity; review/merge/deployment pending. Core remainsFMP and institutions remain unpublished. Financial coverage, transitions/other revenue definitions, transcripts/analysts/valuation, all scheduled consumers/capacity, finalMassive, zero global FMP traffic and billing remain open.
