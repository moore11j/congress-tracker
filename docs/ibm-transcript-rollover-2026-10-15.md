# IBM transcript discovery through event pages

October 15, 2026. PR664 deployed; follow-up collection-status correction is locally tested and awaiting release. Final runtime and ordinary collection remain unverified.

## Source contract

IBM's [investor homepage](https://www.ibm.com/investor) contains completed-quarter tiles linking to [earnings event pages](https://www.ibm.com/investor/events/earnings-2q26). Those pages distinguish Webcast transcript from Prepared remarks. The new policy follows only the exact completed-quarter event link and its exact transcript download. It excludes the separate upcoming-event banner and prepared remarks. No generated URL grants permission to fetch a PDF.

Both source hops retain immutable original bytes and checksums. Metadata is rebuilt from the original homepage and original event page before any PDF fetch or reader use. The PDF independently identifies IBM, fiscal quarter, call date and actual questions and answers. Only reviewed IBM download paths are admitted. Date, identity, link or format changes are held rather than silently accepted. Existing reviewed static periods retain their original proof.

Every homepage, event and PDF request shares the existing global two-page collection budget and minimum daily recheck interval. The collector releases its database transaction before fetching. A newly discovered unprepared quarter blocks fallback to an older transcript. The canonical reader and research document identity remain unchanged; publication remains separately gated.

## Public originals and isolated verification

Nine saved public sources cover the homepage and four event/PDF pairs for fiscal 2025 Q3/Q4 and 2026 Q1/Q2. All raw checksums match capture.json in artifacts/ibm-transcript-chain-oct15. Complete PDF texts, dates and Q&A match the independently retained text receipts: 58,198, 58,816, 58,188 and 55,218 characters respectively. Three originals have empty-password encryption with extraction allowed; existing PDF permission/resource guards remain unchanged.

The Linux Python 3.12.3 full collector replay uses nine simulated requests in batches 2/2/2/2/1/0, creates one latest canonical research document and zero events, and repeats with identical whole-database state and no additional fetches. All nine documents/revisions are retained. Network is blocked and storage is isolated SQLite, with no production reads or writes. Receipt: artifacts/ibm-transcript-chain-oct15/full-replay-linux.json. The 26.5041 CPU seconds are local replay cost, not production capacity evidence.

The affected issuer/research/monitoring/digest suite passes431 checks in95.94seconds on Linux Python3.12.3. The final IBM suite passes33 checks in12.85seconds, including seven additional source-change/revision/bound cases:438 distinct checks overall. No full-suite or sustained-capacity claim. Initial fixture failures were synthetic observation timestamps and an explicit SQLAlchemy join requirement; these were test setup errors, not evidence of production success or failure.

## Other sources and remaining gates

The normal public NVIDIA and Alphabet financial-report feeds each returned HTTP 403 on their first reviewed request. No retry, private endpoint, credential substitution or access-control workaround was used. Their automatic discovery remains unresolved; the captured public Q4 widget describes the public route but does not establish usable access. Receipts: artifacts/q4-transcript-indexes-oct15/financial-index-capture.json and protocol-capture.json.

IBM ordinary collection, broader transcript coverage and selected publication remain gates. Core financial and valuation coverage, institutional activation/current-cohort consumers, all scheduled alerts/rankings, sustained capacity, final Massive prices, global zero-FMP requests and billing remain incomplete. Core/prices/transcripts remain FMP; institutional publication remains disabled. Pending CAT/institutional payload permissions and valuation-model choice remain unchanged. No subscription, host, schedule, source selector, video, landing page, customer email or cancellation change.

## Collection-report correction

PR664 source bf6511231cabe57d4a0b7ce2dc40acee8dea2594 merged c0ce021c4efaea05527d9f91e086bc0cf6fec84d and backend workflow38682990185/job116251470933 succeeded with readiness/Premium privacy checks. Exact-head preview and secret scan passed; unchanged dependency audits failed (33 Pillow advisory rows and five frontend groups, four high/one critical).

Final outer-collector review found that unresolved event pages were absent from its status query. Reader publication already holds missing new periods, but an idle run could incorrectly report collected. The follow-up includes issuer_earnings_event in unresolved sources and adds three full outer-collector regressions for pending, failed and quarantined pages. All133 affected tests pass on Linux3.12.3 in32.19seconds. No retry, request-budget or publication change. Final combined runtime verification is held until this correction is deployed.
