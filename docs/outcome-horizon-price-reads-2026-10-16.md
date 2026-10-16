# Exact cached prices for outcome horizons

October16,2026. Local implementation and PostgreSQL parity verified; release pending. FMP remains enabled.

## Problem and repair

`materialize_cached_outcome_horizons` loaded every daily price between the earliest outstanding horizon and today for all due symbols. Only exact target trading sessions can contribute immutable observations. A complete2018-2026fixture reproduces6404loaded price rows for five horizons requiring ten prices. This helper is called repeatedly by the price hydrator and its public repair path.

The repair derives every missing horizon's target session with the existing market-calendar rule, then reads the complete set of stock/benchmark keys through the existing composite primary key. Statements contain at most400pairs; this is a query batch size, not a coverage cap. Already observed horizons remain immutable. Missing sessions are not replaced by later cached dates, holidays are still advanced to the correct trading session, and unverified adjustment bases remain rejected. No provider request, source selection, schedule, window limit, table/index/schema or score/return formula changes.

This reduces unnecessary database/ORM work. It does not establish that every long job is fixed or that overall host capacity is sufficient. The separate portfolio and institutional performance jobs remain under observation.

## Evidence

- Before repair: two regression failures, one pass;6404rows loaded instead of10.
- After repair:115focused LinuxPython3.12.3 checks pass across exact-price reads, Outcomes ledger/integrity, hydration continuation and Premium pagination. Three additional holiday/missing-session/invalid-basis cases then pass with the three existing cases:118distinct passing checks overall. Existing FastAPI/Starlette deprecation warnings remain; no full-suite claim.
- Frozen prior implementation is the full-result oracle. All five observation projections match exactly except nondeterministic DB creation timestamps; repeat creates zero observations and leaves the rows unchanged. A405entry/406required-key case proves query chunking does not truncate the population.
- Disposable local PostgreSQL16.15 independently reproduces6404 versus10loaded price rows, exact five-observation parity and zero-insert repeat. `EXPLAIN ANALYZE` uses `price_cache_pkey` with0.193ms execution in this synthetic local dataset. This is not a production latency claim. Both owned temporary schemas were removed and the local server stopped. Receipt artifacts/horizon-postgres-1792109771.json. No production reads/writes, provider calls or customer emails.
- Unrelated frontend/tsconfig.tsbuildinfo SHA256A185B71626210F3E1B07F4EFC977408ADC8F7FB3103CAC7E726F2D5F95CC6DEA is preserved.

## Ordinary source operation

Normal SEC collector keyfc9fcb5090f3f5be0ed847d2 was confirmed actually running, then observed terminal at00:14:24UTC. Scheduler run125, collectionrun629, processed245documents:163parsed/82quarantined. CPU6.9453seconds; exit1/statuspartial remains explicit. This was the existing23:47scheduled trigger delayed by the data lane, not another manually queued validation.

Complete October14public index comparison still matches all543metadata records and retains the prior-day filing date. It now has273parsed/25quarantined Form4 and235parsed/10quarantined13F, with zero pending records for that index. Receipt sec-index-live-1792109694.json. This verifies the ordinary collector recovery for that index, not all-source completeness.

The three latest collection errors are two feed reports for the October15index HTTP403 and one13Fcover-total mismatch (sec-index-error-dates-1792109837.json). The source date and error categories are verified; unavailability cause is not. Do not waive the hold or call the October15index complete. Total eligible Form4 publication backlog at00:16 remains425unattempted/306held;282previous distinct-owner holds still await rotation. Ordinary publisher completion is unverified. No duplicated collector/publisher was enqueued and no canonical data was rewritten.

## Release and remaining gates

Local branchcodex/outcome-horizon-price-reads. Review/merge, guarded deployment, installed verification and actual scheduled savings remain pending. PR694/695 are merged but not deployed; the protected portfolio cycle was518/1135/zero recorded failures at00:17. At00:19 the actual data-lane process is app.refresh_institutional_performance_cache,275.6seconds/23.72group CPU,21queued. A continuing state is not proof of a live portfolio process; no release bypass or checkpoint clearing occurred.

Financial/institutional/transcript coverage, all alert/ranking consumers, sustained capacity, final Massive/paid-provider validation, actual zero-FMP requests and billing closure remain. Pending private-export approvals are untouched. Previous goal turn produced a merged repair; this turn adds ordinary collection evidence and a separately reproduced/validated database-read repair.
