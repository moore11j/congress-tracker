# Institutional cached price boundary reads, October 16, 2026

## Problem and change

The ordinary institutional performance refresh was observed running in the shared data lane. Its return reader aggregates minimum/maximum dates over every valid price in each holder/period, then joins those dates back to prices. A complete 24-symbol, 52,800-price local fixture reproduces substantial unnecessary work. This does not establish what proportion of the production job is attributable to this query.

The replacement performs two correlated, ordered, one-row boundary lookups per symbol and period through the existing `(symbol, date)` primary key. The symbol set still comes from all eligible holdings and the existing active-filing selection. No holding, symbol or period limit is added. Adjusted-only versus close selection, positive-price filtering, six-place rounding, nonfinite/missing boundaries, 65% coverage, weighted returns and cache publication remain unchanged. No schema, schedule, provider, source selector or host change.

## Verification

Before change, the query-work regression fails with 8,342 SQLite work units for both reference and candidate; five other cases pass. After change, the broader Linux Python 3.12.3 run has 82 passes and one unrelated existing profile-name failure. A further complete summary/cache/repeat test brings the focused file to seven passes, giving 83 distinct passing tests overall. The same profile-name test fails with the frozen original price reader: the unchanged router deliberately retains public holder identity, while the older test expects None. That test was not weakened or changed. This is not a full-suite pass.

Frozen prior implementation verifies exact price-pair dictionaries for all periods, adjusted and unadjusted prices, missing/zero/negative/nonfinite/rounded-zero boundaries, active/all-filing selection, 1,203 holdings, no-data periods and empty periods. Complete four-period holder summaries and persisted metric fields also match, including repeat. Volatile DB update timestamps are excluded from persisted comparison.

Actual local PostgreSQL 16.15 receipt `artifacts/institution-boundary-postgres-1792110599.json`:

| Measure across three periods | Prior | Replacement |
| --- | ---: | ---: |
| Price rows examined in EXPLAIN ANALYZE | 211,200 | 96 |
| Shared blocks | 2,255 | 531 |
| Query execution, ms | 284.853 | 3.697 |

Complete outputs and repeat match; the existing price_cache_pkey is used. These are synthetic local measurements, not production latency or sustained-capacity proof. An initial diagnostic recorder erroneously recorded its own EXPLAIN; that rehearsal failed and cleaned its schema/server. After correcting the recorder, the successful rehearsal again removed its owned schema and stopped its server. No production access, provider request or email.

Prepared `artifacts/verify-institution-boundary-runtime-oct16.py` passes local Python 3.14.2 rehearsal: two hashes, all six valid adjusted pairs/seven unadjusted pairs, missing-adjustment exclusion and exact repeat, network forbidden. Receipt `artifacts/institution-boundary-runtime-1792110713.json`. Installed production verification remains required after a successful guarded release.

## Ordinary source acceptance

Normal SEC Form4 publisher run125, triggered October15 23:57UTC, completed October16 00:21:03UTC. It attempts200filings, publishes188 with365transactions/events, and holds12 for review. All365receipt references have distinct IDs, existing events, correct SEC provenance and normalized source identities; none are backdated arrivals.123published filings have a prior different-owner overlap handled by the deployed predicate. Exit1 remains explicit partial status. Current remaining273unattempted/270held. Owner remains SECgeneration2. Read-only receipt `artifacts/form4-ordinary-1792110332.json` verifies actual ordinary publication, not inbox receipt or all-source completeness. No new manual job or email.

## Release state and remaining work

PR696 merged as7914518f at00:22:06UTC, confirmed from GitHub in this turn. PR694/695/696 remain undeployed. Protected portfolio cycle is526/1135 with zero recorded failures at00:25, while another ordinary process occupies the data lane. No release retry, interruption or guard bypass. New boundary reader is locally prepared on codex/institutional-price-boundary-reads; exact review/merge/release and installed/ordinary performance verification remain.

Previous percentage-only turn was no progress toward the goal. This continuation confirms actual scheduled insider publication and implements/measures a separate complete-data query repair. Goal stays active; broad financial/institutional/transcript coverage, all monitoring/watchlist/daily/weekly/ranking acceptance, sustained capacity, final Massive activation, actual zero-FMP requests and billing closure remain. Pending private exports are untouched.

## Merged, deployment deferred: October16 00:35UTC

PR697 https://github.com/moore11j/congress-tracker/pull/697 tested6bacb497ee1ee72b22fa0f42afd5016d94aa96d1 merged a85771126e332c9c32c1c51b4a47b97f24b327c7 at00:34:35UTC. All checks succeed: workflow38830118311 jobs116746722780/116746722946/116746723088 and Vercel preview/feedback. Fetched merged source/test tree matches the tested revision. Attachment was attempted but the chat has reached its100attachment identity cap; ordinary PR URL remains available.

No backend deployment was attempted. Read-only final release-blocker-1792110904.json records530/1135protected scopes, zero failures, continuing/no live portfolio process,12queued. active-work-1792110902.json independently confirms another app.ingest_run group actually live17seconds/1.22CPU. Neither queue state nor a previous succeeded external request clears the unfinished-cycle guard. PR694/695/696/697 await safe release and installed verification.

Public render receipt public-render-1792110814542.json at00:33:34: all four200/noRSCerrors, Feed0.645s, Insights0.153s, AAPL2.646s, NVDA3.713s. These responses confirm no sampled server error, not sustained capacity, fresh/full data or signed-in behavior. Broader performance remains open. Local test/process handles all terminal; ownedPGserver stopped and schema removed. No provider activation, new schedule, hardware change, purchase, customer email, video or landing edit.


## October17 installed release

This package is deployed in f4f9ff0f through successful guarded workflow38986463595, and its isolated installed-runtime proof passes. See the [central release and ordinary-operation report](migration-release-verification-2026-10-17.md) for exact receipts, verifier corrections, live-source distinctions and remaining gates. Earlier deferred-release notes above are historical. No provider selection changed; FMP retirement remains incomplete.
