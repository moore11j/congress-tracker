# Direct-feed shadow rollout

October 8, 2026. The owner requested continued work through a reliable FMP shutdown. This release starts scheduled official-source collection alongside FMP. It does not activate public replacement publishers or cancel billing.

## Release scope

Prepared on `codex/fmp-shadow-rollout` from production/GitHub main `b79891156d5200dfda1b56a9d5596d9f989a898d`, in a separate checkout to exclude unrelated local edits. Additive release migration creates eight isolated staging, source-control, publication-receipt and repair-audit tables. It changes no source selection or existing canonical rows. PostgreSQL DDL has bounded lock/statement timeouts; migration precedes updated workers.

`DIRECT_FEEDS_MODE=shadow` enables hourly House/Senate collection at minute 17, bounded to 50 documents per source; Form 4/13F collection runs at minute 47, bounded to 200 per source. A shared advisory lock serializes collection. Discovery covers the previous seven days through yesterday, preserving source bytes, revisions, identity, timestamps, holds and run receipts. Seven-day rechecks avoid downloading the same successful document every hour; failures remain visible and retry under bounded rules. The Senate collector uses the ordinary public-notice session. Scans and source conflicts remain held.

All three public-publication flags are false. No publisher schedule or source-switch command is included. Existing FMP schedules remain active. Shared SEC parsing and conservative official-disclosure parsing are included; the old row-only Congress promoter refuses unsafe publication. This release does not contain the later canonical repair, full legacy writer ownership rollout, Massive quote schema, SEC fundamentals schema or feature-retirement changes. Receipt/repair helper code is present for shared schema dependencies, not activated.

## Validation before deployment

100 focused tests pass on Python 3.14.2 across direct feeds, SEC collection/scheduling, House PDF, Senate session and official disclosure pipelines. Repeated migration against the same isolated database leaves exactly eight isolated tables, zero provider selections and no public tables. Fly configuration validates. No relevant secret overrides of the shadow/publication/provider flags were found. GitHub main and production both matched the base revision before rollout.

An additional existing ingestion test file, `test_institutional_ingest_job.py`, cannot collect because line 149 has a syntax error already present at HEAD; this release does not modify it. An additional 21 existing SEC snapshot, correction and buys-ingestion checks pass. No full-suite pass is claimed.

## Deployment and remaining gates

Prepared, not yet deployed at this checkpoint. Live scheduled receipts, public-row isolation, runtime/image health and collection coverage must be verified after deployment. FMP remains active. Massive Starter activation and delayed-price verification remain pending. Congress scan/identity exceptions, production-safe canonical repairs, Form 4/13F publication, fresh scoring/rankings and no-send monitoring/watchlist/daily/weekly parity remain cutover gates. Optional FMP research features still need replacement or explicit retirement before zero-egress and billing cancellation checks.
