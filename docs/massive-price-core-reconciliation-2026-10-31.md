# Massive price core reconciliation, October 31, 2026

## Status and scope

Prepared on `codex/massive-price-core-reconciliation` from current fetched main `e71f58f7974f67511e7c2a75ba6be847beebf9fb`. This reconciles earlier primary-workspace changes as a narrow patch onto the current backend. Original primary-workspace application edits remain untouched. This is a default-off preparation package, not price activation or production deployment. Massive purchase and live entitlement validation remain last, after free-source and consumer acceptance.

Seven application files add selected quote/history/action routing, durable quote provenance, validation of provider symbol and split basis, and daily percent alert evidence. Malformed or conflicting source rows cannot silently overwrite valid observations. Quote metadata is bound to the exact price and observation time; restarting a worker or refreshing the cache cannot make an old quote newly observed. An older observation cannot replace a newer durable one. Foreground requests reuse cached data and queue existing background work instead of waiting on provider HTTP. Entitlement/source errors do not fall back to FMP in selected paths.

Daily history uses completed sessions, stores prior-session fallbacks under their actual dates and keeps share-class cache identities canonical. Partial writes clear incompatible legacy/raw OHLC metadata rather than relabel it as Massive. Raw bars and split actions preserve the existing single-adjustment contract; split-adjusted price returns are not total returns.

Market pressure uses the selected quote's own timestamp and prior close. Minute quotes no longer overwrite daily closing-price history, including in the default FMP mode. Daily percent alerts hold unverified legacy references, retain the delayed observation timestamp and preserve existing durable duplicate identities.

## Validation recovered October 31

The completed October 27 receipts were inspected after resume. All seven application source hashes still match the final PostgreSQL proof. Full hash manifest and exact test identifiers: `artifacts/massive-price-core-manifest-oct31.json`.

- Final affected Linux run: 279 checks, 277 passed and two failed. Both exact failures reproduce on unchanged deployed baseline e71f58f7: mixed market-pressure classification expects conflicted but is bullish; an outcome-horizon fixture expects materialization from weekend prices. Neither is represented as passing or repaired here. Receipts: `massive-core-final-linux-oct27.xml` and `massive-core-baseline-oct27/result.xml`.
- Separate selected quote-to-durable-alert Linux replay: one passed. Unverified legacy evidence holds; verified delayed source triggers once; repeated evaluation and a real same-session recross do not duplicate the alert. Zero Event or EmailDelivery rows; no provider HTTP or production data used. Receipt: `massive-alert-replay-linux-oct27.xml`.
- Final focused Windows run: 54 passed, `massive-core-final-windows-oct27.xml`. Counts overlap the Linux suite; they are not additive unique coverage.
- Local PostgreSQL 16.15: repeated additive schema upgrade preserves legacy rows; worker reuse, timezone binding, monotonic writes, legacy-writer rejection/clearing and daily metadata basis conditions pass. Owned database and server were cleaned up. Receipt: `massive-quote-postgres-oct27/result.json`.
- October 31 manual diff review and `git diff --check` pass. No full-suite, full consumer, live entitlement or production acceptance claim.

## Release ordering and remaining consumers

The October 27 installed audit found `quotes_cache.quote_metadata_json` absent. Before deploying updated ORM readers or workers, apply and verify the nullable TEXT column through the bounded idempotent quote schema helper. Recheck current schema first. This turn has not applied production DDL. The extra nullable column permits an old application rollback; mismatched retained metadata is rejected by new readers. Keep `STOCK_PRICE_PROVIDER=fmp` throughout a preparation release.

Still outstanding before activation: public API/ticker/chart metadata and labels, valuation price routing, absolute-price custom-alert provenance/freshness, all digest/ranking/watchlist consumers, complete price-history warming, splits/benchmark/entitlement coverage, exact installed release, scheduled observation and rollback. The current alert job ends at 16:10 New York; a 15-minute delayed 16:00 close can arrive later, requiring explicit close-window handling before acceptance. Five original public-caller tests were not copied into this package because those callers are not yet integrated; this is an explicit remaining gate, not a test exclusion claiming completion.

## Fresh reliability and free-source observations

October 31 19:54 UTC public rendered-response spot checks: Feed 0.578s, Insights 0.447s, AAPL 0.300s and NVDA 0.320s; all HTTP 200 without RSC/server-error markers. Receipt: `artifacts/public-render-1793476455431.json`. This does not measure full client data, authenticated consumers or sustained capacity.

Read-only ordinary discovery observation at19:54 UTC finds171 checkpoints and2205 covers:2008 pending qualifications,155 with no supported statement,39 held and3 qualified. Four recent scheduled batches completed10/10; BP and IAG report DiscoveryError. Qualified filings alone do not prove a complete current issuer selection. Core provider remains FMP; discovery collection is enabled and public discovery reading disabled. Receipt: `artifacts/discovery-observation-1793476457.json`. Investigate qualification holds/errors while preserving source verification and existing workload limits.

No provider switch, subscription, infrastructure change, schema mutation, customer message, video restart, institutional export or production release occurred. Institutional capture approval, free financial coverage, transcripts, all consumer checks, sustained capacity, final Massive activation and zero-FMP/billing closure remain open.
