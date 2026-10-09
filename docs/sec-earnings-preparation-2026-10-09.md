# SEC earnings preparation release, October 9

The owner asked to continue the authorized migration after an unnecessary stop. Current work does not depend on permission for a recurring autonomous agent. Massive remains last.

This scoped backend package prepares retained SEC earnings filing bundles and separate press-panel caches behind `SEC_EARNINGS_WARMING_ENABLED=1`. Public press selection remains FMP; canonical research/event publication remains off. No model call, customer email, subscription purchase or FMP cancellation is part of preparation.

The scheduled worker rotates five symbols every five minutes across owned ticker watchlists, active research theses, recently viewed tickers and the established prewarm list. It uses the shared SEC collector lock, existing pressure guard and a durable lease with the deployed dead-owner recovery logic. Planning commits before source/cache work. Failed symbols rotate; source refusals stop the batch. Each symbol checks at most five Item 2.02 filings from the last year, up to eleven source requests excluding a directory miss. Six-hour caches prevent repeated downloads. Historical and unsupported material may be held; this is selected earnings-release coverage, not general press or transcripts.

The cache retains accession/source hashes, SEC acceptance and filing dates, with original release publication time null. Exact source bytes are retained in existing direct-feed tables. Legacy research matches are reused as reconciliation evidence; ambiguity is held. Preparation does not create ResearchSourceDocuments, Events, alerts or delivery records. Transient failures retain prior caches without reporting stale data as fresh. Public source flags remain independent.

Exact-release validation on Python 3.14.2: 79 focused parser/store/preparation/financial-warmer/collector tests pass. Real disposable PostgreSQL, pool size two with no overflow: four fixture reads, four overlapping collector refusals, zero extra repeat reads, lease cleared and private test schema removed. Saved public LEVI/HELE/TLRY/WDFC source replay: twelve documents and twelve revisions, four caches, three eligible links, zero research documents/events; repeat is identical without additional reads. These are isolated checks, not production coverage or alert-delivery evidence.

Runtime entry: `backend/app/jobs/warm_sec_earnings.py`; read-only verifier: `backend/scripts/verify_sec_earnings_rollout.py`. Source safety and earlier publication/consumer rehearsals are detailed in [the earnings-materials report](sec-earnings-materials-2026-10-08.md), currently maintained in the primary workspace. Public selection requires actual scheduled coverage, fresh legacy reconciliation and all reader/digest checks. No broad migration-complete claim.

Deployment and actual scheduled receipts are pending at this checkpoint. Existing dependency-audit and unrelated baseline test failures remain separate; no full-suite pass claimed.
