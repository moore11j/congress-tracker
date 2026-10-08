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

## First live deployment and PDF runtime correction

Release `dbf0b1b9` deployed successfully via workflow [37842069681](https://github.com/moore11j/congress-tracker/actions/runs/37842069681). The additive migration completed before worker replacement, all four machines run that image, and readiness/database/Premium access checks passed. Python 3.12.15; live settings confirm shadow mode and all publishers false, with no source controls or publication receipts.

A bounded October 7 collection at 20:50 UTC discovered two House, one Senate, 325 Form 4 and 86 13F reports. It stored ten revisions: eight parsed and two quarantined. Two House PDFs failed and one 13F had an invalid share/principal type; 401 documents remain pending. Senate unattended discovery completed. Source hashes verify for all ten captured revisions, public writes remain zero. This is live staging, not completed scheduled reliability or public cutover.

The House failure was reproduced against the pinned production `pypdf==6.13.3`; local testing had used 6.19.0. Older visitor callbacks supply stale coordinates for table cells. The official [changelog](https://pypdf.readthedocs.io/en/6.19.0/meta/CHANGELOG.html#version-6-18-1-2026-09-11) records the visitor text-matrix fix in 6.18.1. Pinning the already locally tested 6.19.0 aligns runtimes. An exact source-PDF fixture now checks real extraction, both stock symbols/dates and joint-owner amount ranges, rather than only mocked text coordinates. Saved raw HTML fixtures retain source whitespace; code/document diff checks exclude that known source-byte whitespace.

109 focused release checks pass after the dependency correction, including existing annual-disclosure parsing and the real PDF. Follow-up deployment/live retry not yet verified at this checkpoint. FMP remains active and billing unchanged.

## Verified corrected release, 20:58 UTC

Corrective release `c2b97e80364768cad0771068fde19859a92ccd86` deployed successfully via [workflow 37842927148](https://github.com/moore11j/congress-tracker/actions/runs/37842927148). Repeat additive migration, all four started worker images and readiness/database/Premium access checks pass. Production reports Python 3.12.15 and pypdf 6.19.0.

The bounded retry extracts both previously failed House reports without transport/parser errors: one parsed, one quarantined because existing canonical records differ. Total revisions rise from ten to twelve; all twelve stored source-byte hashes verify. An immediate repeat processes zero documents and keeps twelve revisions. Senate discovery returns the same complete result and page hash across all three runs. Source selections and publication receipts remain zero; all publisher flags remain false. These are manual live checks. The first hourly cron receipts and broader backlog/exception coverage are still pending; 401 SEC documents await collection and one SEC share/principal-type failure remains held.

Saved local receipts: `artifacts/direct-feeds/shadow-rollout-2026-10-08/first-receipts.json` and `corrected-receipts.json`, with transport captures alongside them. A single authenticated AAPL Massive snapshot probe at 20:58:18 UTC still returns 403. Starter activation, fresh product/alert parity and public source cutover remain unfinished. No FMP cancellation, public canonical repair, replacement publication or test email was performed. Existing scheduled product/email jobs were retained.

Follow-up order: observe scheduled collection and drain/review held documents; deploy complete canonical writer/repair protections and bounded direct publication; finish fresh price/fundamental/ranking/monitoring parity and retire remaining optional FMP dependencies; then verify zero FMP requests and billing cancellation. The FMP shutdown goal remains active.
