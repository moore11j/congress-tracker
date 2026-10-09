# SEC earnings preparation release, October 9

The owner asked to continue the authorized migration after an unnecessary stop. Current work does not depend on permission for a recurring autonomous agent. Massive remains last.

This scoped backend package prepares retained SEC earnings filing bundles and separate press-panel caches behind `SEC_EARNINGS_WARMING_ENABLED=1`. Public press selection remains FMP; canonical research/event publication remains off. No model call, customer email, subscription purchase or FMP cancellation is part of preparation.

The scheduled worker rotates five symbols every five minutes across owned ticker watchlists, active research theses, recently viewed tickers and the established prewarm list. It uses the shared SEC collector lock, existing pressure guard and a durable lease with the deployed dead-owner recovery logic. Planning commits before source/cache work. Failed symbols rotate; source refusals stop the batch. Each symbol checks at most five Item 2.02 filings from the last year, up to eleven source requests excluding a directory miss. Six-hour caches prevent repeated downloads. Historical and unsupported material may be held; this is selected earnings-release coverage, not general press or transcripts.

The cache retains accession/source hashes, SEC acceptance and filing dates, with original release publication time null. Exact source bytes are retained in existing direct-feed tables. Legacy research matches are reused as reconciliation evidence; ambiguity is held. Preparation does not create ResearchSourceDocuments, Events, alerts or delivery records. Transient failures retain prior caches without reporting stale data as fresh. Public source flags remain independent.

Exact-release validation on Python 3.14.2: 79 focused parser/store/preparation/financial-warmer/collector tests pass. Real disposable PostgreSQL, pool size two with no overflow: four fixture reads, four overlapping collector refusals, zero extra repeat reads, lease cleared and private test schema removed. Saved public LEVI/HELE/TLRY/WDFC source replay: twelve documents and twelve revisions, four caches, three eligible links, zero research documents/events; repeat is identical without additional reads. These are isolated checks, not production coverage or alert-delivery evidence.

Runtime entry: `backend/app/jobs/warm_sec_earnings.py`; read-only verifier: `backend/scripts/verify_sec_earnings_rollout.py`. Source safety and earlier publication/consumer rehearsals are detailed in [the earnings-materials report](sec-earnings-materials-2026-10-08.md), currently maintained in the primary workspace. Public selection requires actual scheduled coverage, fresh legacy reconciliation and all reader/digest checks. No broad migration-complete claim.

Deployment and actual scheduled receipts are pending at this checkpoint. Existing dependency-audit and unrelated baseline test failures remain separate; no full-suite pass claimed.

## Preparation deployed and observed

PR499, final source `3e244372`, merged as `7c66a3ab`, deployed successfully through workflow 38005914318. Thirty-two runtime hashes match across the two API workers, cron and video. All confirm preparation enabled, public press/financial/metadata/fundamentals/prices still FMP, and the exact Finnhub news cutoff unchanged.

Actual scheduled batches at 23:47 and 23:52 UTC each complete five scopes and clear their leases. At 23:53:42, ten of the 63-symbol universe have been attempted: eight caches, 23 release links, 68 retained revisions, zero SEC press publication receipts and zero SEC press research documents. AL and BNPQY are unavailable in this capture; AJG has only held material. This is observed partial coverage, not full readiness. Source-reason investigation and the remaining rotation continue.

## Reader and canonical compatibility package

The next isolated package connects the existing press API/worker, watchlist materializer, research extractor and source labels to the prepared SEC feed. Public source selection remains off for this release. Research uses prepared-only evidence instead of forcing a new crawl. Original release publication times remain null; displayed dates explicitly say Filed. Existing research matches retain their IDs, attribution and processing state; ambiguity remains held.

Canonical SEC press publication additionally requires `SEC_EARNINGS_PUBLISH_AFTER`, an explicit aware activation timestamp, alongside the immutable filing-date control. Earlier same-day filings cannot become new alerts. The timestamp is bound into published receipts so configuration changes cannot reinterpret a published identity. All updated writers and a fresh reconciliation must precede source activation.

Validation found and fixed a missing intraday date-label change and equivalent PostgreSQL timezone representations being mistaken for changed events. The event hash now normalizes timestamps to UTC without changing their instants. The real PostgreSQL suite passes six transaction/concurrency/repeat cases, with the test schema removed and local server stopped. Final 56 focused press/research/notification checks pass. The initial broader group passed 139 checks and exposed the now-fixed intraday label failure. Thirteen frontend checks pass; one existing analyst-copy assertion targets an unchanged component and the expected phrase is absent from HEAD too. TypeScript passes after adding the provider field to the press response type. No full-suite claim.

Exact-release replay of actual prepared production caches verifies all eight captured panels, 23 source links, pagination and empty-state messages, without HTTP, queue calls, canonical writes or legacy digest backfill. A separate replay of the three recent positive SEC examples retains three research documents, three canonical Events and three MonitoringAlerts across two passes; all monitoring/daily/watchlist builders and intraday identities match, with zero model calls, ResearchEvidenceEvents or EmailDelivery rows. Saved replay hash: `d592ea4fe72588df2a172ae4795ea9be1af0569e6a078f8093faac9e95eb37dd`.

This package is being prepared for release. Production canonical publication, live extraction quality, broader source coverage and ordinary delivery-window observation remain gates. Massive remains last, and FMP remains active.
