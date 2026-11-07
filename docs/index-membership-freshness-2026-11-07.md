# Index membership freshness repair

## Problem and bounded repair

Read-only production evidence on November 7 found the selected free S&P 500 and Nasdaq-100 memberships still dated July 16. Wikipedia is already the default provider; this is an operational freshness repair, not a newly substituted FMP endpoint. No refresh invocation existed in the schedule. In addition, an unchanged source revision returned before renewing the successful check timestamp.

The existing market-pressure background job now checks only its selected Wikipedia index universes before preparing price snapshots, only when that job is allowed to run. A valid successful check suppresses another check for 24 hours. At most two requests occur per invocation, each capped at 4 MiB, with redirects and retries disabled in this path. Failed responses preserve the old membership and freshness and may retry at the next existing job invocation. The existing seven weekday invocations therefore permit at most fourteen failed source requests per weekday; there is no new cron entry, process, paid-provider fallback or page-request acquisition.

Verified unchanged revisions renew only the check timestamp, preserving row IDs, effective dates and original revision observation date. An older observation cannot rewind that timestamp. Provider, page, parser and source-kind metadata must agree before using the unchanged-revision shortcut. Changed memberships retain existing IDs, end-date removals and insert additions using the observation date, without inventing historical index entry dates. Dry runs never update freshness. Existing membership count, required symbols and sector-diversity checks remain.

## Complete evidence and validation

Two identified public requests at 06:32 UTC returned complete MediaWiki originals: 302,069 bytes for S&P 500 revision1383776342 and 101,276 bytes for Nasdaq-100 revision1381716526. Their retained gzip fixtures and `sources.json` verify exact byte counts and SHA-256 hashes. Both pass existing parser/coverage checks, yielding503 and101 securities. The separate read-only production capture contains606 active public membership rows and no customer data.

Compared with the July membership, S&P500 has ten additions and ten removals, while Nasdaq-100 has one addition and three removals. Full captured-baseline rehearsal verifies every current symbol, retained identity/effective date, newly observed entry, end-dated removal, market-pressure universe consumer and exact repeat. These are community-maintained current lists, not official historical effective-date evidence.

77 local checks pass, covering both complete originals, full production-reference replay, stale-to-fresh same-revision checks, older observation protection, dry runs, failed commits, provider revision collisions, freshness/provider/universe selection, market-hours job wiring, denied/malformed/oversized source responses, existing market-pressure consumers and FMP transport shutdown.

Two pre-existing test fixtures were repaired: the no-network page test omitted the securities/watchlist-item tables now used by the reader; its mixed-signal example used a signals allocation too small to constitute material conflicting evidence under the existing scoring policy. The latter now supplies material opposing Congress/price evidence and asserts the canonical mixed direction. No scoring or consumer implementation changed.

## Release and remaining acceptance

Local implementation only at preparation. Linux CI, reviewed exact release, installed hash/source/cache verification and ordinary scheduled freshness remain pending. No production membership update was performed during source capture or rehearsal. Core financials, transcript and price selectors remain unchanged; full financial coverage, institutional/consumer/alert acceptance, sustained operation, final Massive and global zero-FMP/billing closure remain separate gates.

Rollback is the prior job/service implementation. Existing index rows/history remain usable; failed refreshes never erase valid records or declare old data fresh.
