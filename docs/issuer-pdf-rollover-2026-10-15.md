# Meta and SoFi transcript rollover

October 15, 2026 UTC. Implemented and validated locally; release/runtime and ordinary collection are pending.

## Official sources and semantic checks

The reviewed [Meta financials page](https://investor.atmeta.com/financials/) and [SoFi quarterly-results page](https://investors.sofi.com/financials/quarterly-results/default.aspx) use public financial-report indexes. Their linked public widget code documents the unauthenticated `/feed/FinancialReport.svc/GetFinancialReportList` route and report-type/year query. No API key, private service route, account or paid provider is used. Captured page/widget/index URLs, observation times and hashes are in artifacts/issuer-pdf-rollover-oct15/{capture,protocol-capture,reports-capture}.json.

Four exact official indexes and twelve linked PDFs cover Q1-Q4 2025 and Q1-Q2 2026 for both companies. Sources are pinned to their exact reviewed CDN tenants and fiscal paths. Index validation requires English report year/quarter/end date, one earnings transcript with the exact reviewed title/category/type, bounded documents and no duplicate periods. Meta's separate Follow Up Call Transcript is explicitly excluded. External hosts, sibling tenants, query/fragment URLs, wrong periods, conflicting records and future report periods are held.

PDF content independently establishes company, fiscal period, heading call date and the operator's Q&A opening. Meta's ordinal English dates and SoFi's numeric dates, Unicode separator and first-question ligature are supported without changing source text. Fourth-quarter2025 calls correctly retain January2026 publication dates. One actual SoFi original exposed a missing Your-versus-Our operator variant after the initial synthetic pass; the exact observed wording and regression cases were added. Missing Q&A and contradictory identities remain held. New discovered records say the transcript publisher is not identified rather than inferring authorship from the company heading; existing reviewed publisher metadata stays unchanged.

## Integration and budgets

The existing hourly collector shares its two-page budget and minimum daily recheck interval across all supported discovery policies and static records. Failed/interrupted attempts reserve their check time before HTTP; no transaction is held during downloads. New PDF links grant only the exact validated URL. The current reviewed Meta/SoFi Q2 records retain their original URL, metadata and source proof, including Meta's issuer-to-CDN redirect. Newly discovered pending quarters block fallback to an older prepared period; removed policies cannot fetch old staging records. Original index bytes/hashes stay pinned across subsequent index revisions. Cross-provider reconciliation, publication boundary and duplicate safeguards are unchanged.

This prepares unattended rollover for Meta and SoFi alongside Microsoft and Reddit. NVIDIA, Alphabet, IBM and Moderna still use fixed reviewed periods. The inspected current registry and discovered cohorts fit within the existing48page-attempt/day ceiling; this is an arithmetic bound, not measured production freshness or capacity. Source retries retain their existing transport limits. No schedule trigger, host, subscription, publication selector or daily budget changes.

## Validation

Final355 Linux Python3.12.3 checks pass in94.18seconds across all issuer policies/readers/PDFs, research warming, monitoring and email digests. Earlier source-only231pass and intermediate350pass results predate the final observed-format correction. This is focused coverage, not a full-suite pass.

All twelve checksum-verified public PDFs reproduce their complete original text and reviewed call dates. Re-parsing resolved metadata gives identical complete parsed output. The full isolated collector processes four indexes/twelve PDFs in eight two-request batches, then requests nothing; current canonical readers create exactly two research documents, no research events, and the entire database repeats identically. No provider/model/email/network or production database access occurs during replay. Windows3.14.2 receipt: artifacts/issuer-pdf-rollover-oct15/actual-cohort-replay-win32.json. Matching Linux3.12.3 actual-cohort replay also passes, with the same complete source and database equality (actual-cohort-replay-linux.json). Local parse CPU ranges1.0304 to5.4671seconds per PDF; the complete repeated-source/collector/reader rehearsal uses83.3481CPU seconds (84.5964wall seconds); this is not a production-capacity claim.

## Remaining work

Exact release/runtime verification and ordinary scheduled source collection remain pending. Broader transcript coverage/publication, four remaining issuer rollovers, full financial and institutional coverage, all consumers/rankings/alerts, sustained capacity, final Massive price routing/entitlements, global zero-FMP traffic and billing remain gates. Production remains PR661/4432fe8a with core/prices/transcripts FMP and institutions publication off. Pending CAT/institutional production-payload permissions and valuation-model choice remain pending; no rejected read was retried or bypassed. No customer email, video, landing or cancellation action.
