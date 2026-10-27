# Issuer transcript freshness and Meta Q3 recovery, October 27, 2026

## Confirmed problems

The current generic issuer reader checked the original index that authorized a retained transcript, but did not require the current index to be successfully checked recently. Failed, pending, quarantined, stale, future-dated and never-checked current indexes could therefore leave an older transcript marked prepared. UL and SEC/MGA already had separate directory-health checks; their paths remain unchanged.

The failure is present in actual retained production evidence. Read-only receipt `artifacts/issuer-freshness-probe-1793096565.json` shows META's current index failed at06:13UTC, while the installed reader still returns its Q2 transcript as prepared. The candidate reader holds it as `latest_issuer_index_not_verified`. Production still selects FMP transcripts, so this is a preparation/cutover defect, not a claim that the new reader already changed customer research.

The current Meta collector failure is caused by a verified source format change: the Q3 earnings transcript uses the same tenant's `files/content_files/META-Q3-2026-Earnings-Call-Transcript.pdf` path and lowercase `file` document type. The existing parser only accepts its `doc_financials/year/quarter/` directory and capitalized `File` value.

## Repair

`issuer_discovery.current_index_hold` verifies the configured current index, parsed status, identity, actual successful check within48hours, immutable original hash/size and observation time before considering retained periods. It returns an explicit hold for missing/failed/stale coverage. It never refreshes or replaces originals. The existing hourly two-request collection budget, daily recheck minimum and36hour index priority remain unchanged. The added lookup is bounded to one current index and one revision; it does not scan the archive or open network connections.

`issuer_q4_pdf_discovery` accepts the independently reviewed Meta content directory only with the exact tenant, issuer, fiscal quarter/year and earnings-call filename. The allowed file-type case variant does not allow hyperlinks or HTML. Follow-up calls remain excluded. The complete PDF must still prove company, fiscal period, publication date and Q&A independently. Existing source/projection formats and canonical event/document identities are unchanged.

## Source and consumer evidence

Exactly two public originals were fetched once with no redirects, a2MiB bound and denial stop: the complete official index and its exact linked earnings transcript. Retained locally in `artifacts/meta-index-oct27/`; no full transcript is added to the public PR.

- Index:12,128bytes, SHA256 `c705565e5fac85d5c1302ee33ab1e123ed2538fb58a5f25a2f9dc64cf9339ca9`.
- PDF:139,167bytes, SHA256 `980c5e766b36f06c4d3d22b543cc35714da05c0abe400b60d36199f0c90759da`. Exact official link: https://s21.q4cdn.com/399680738/files/content_files/META-Q3-2026-Earnings-Call-Transcript.pdf .
- Complete parsed text:56,989characters, Q3 FY2026, publishedOctober21, Q&A present; SHA256 `3227be9e9e9be4cabead95dfaeebce246fa39cfedf9457c80f9eabc2f68bb8a4`. Full repeat is identical.

Actual collector and research-consumer replays pass on Windows and Linux with network blocked and an isolated in-memory database. Two one-request collection calls consume only the complete current index and Q3 PDF. The prior-year directory is explicitly unavailable in this bounded replay, not synthesized or counted as covered. A new-publication case creates exactly one document with an unchanged full-database repeat. A historical FMP case retains all original canonical document fields and again has an unchanged repeat. Both have zero evidence events, email rows and production writes. Receipts: `consumer-replay-win32.json` and `consumer-replay-linux.json` in the source folder.

A separate read-only in-process production comparison preserves the entire valid IBM Q3, Reddit Q2 and SoFi Q2 result exactly. It exports only status/hash metadata, not source text or customer records. CPU after imports was4.36seconds for IBM/Reddit and3.16seconds for Meta/SoFi. These bounded diagnostics do not prove sustained capacity or publication acceptance. Runtime source files and database records were not changed by the comparison.

## Tests and release state

The initial valid baseline reproduces six missing-directory-health failures, with the fresh-repeat control passing. Four existing fixture modules needed their persisted receipt timestamps aligned to their simulated clocks; the static Microsoft consumer fixture now includes a real fresh directory proof instead of silently bypassing discovery health. Production clocks and source observations were not changed.

Affected Linux suites pass413checks before the final Meta and no-fallback cases. The expanded424-case run has423passes and one new test caller-name typo; that test now uses the actual `_ingest_latest_transcript` entry point. The final15-case freshness/consumer rerun passes on the corrected test. These runs cover424distinct passing tests on the unchanged final application source; they are not438distinct tests or a single424-test green run. No full-suite or production cutover claim.

Review, guarded deployment, installed hashes and ordinary collection recovery remain pending. Transcript selection stays FMP; no new schedule, host, subscription, customer email, publication boundary or forced refresh is part of this package. META's next ordinary index attempt must precede ordinary PDF preparation; no source deadline is bypassed. UL's current403 remains a separate hold. Broad transcript coverage, institutional approval/publication, core financial coverage, all consumers, sustained capacity, Massive last and zero-FMP/billing closure remain open.
