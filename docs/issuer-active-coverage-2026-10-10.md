# Active workload issuer coverage, October 10

Add reviewed IBM, SoFi and Moderna Q2 2026 transcripts for the observed22-ticker operational workload. Retain existing Microsoft, NVIDIA, Meta and Alphabet records unchanged. Public transcripts remain FMP. Scheduled preparation still reads at most two reviewed documents per run and rechecks a document no more than daily.

## Source and format compatibility

IBM official event https://www.ibm.com/investor/events/earnings-2q26 links https://www.ibm.com/downloads/documents/us-en/177ce39d1dc47bfb. July22,2026;18pages;55,218 normalized characters. Source SHA256c6b7bb0fb0f6321a4a3e92e31f71cd36394146c5658819248bcc2e001abbc3a7. Its PDF is encrypted but opens with an empty user password and explicitly grants extraction (permission bit16). The first actual-source replay correctly exposed the old blanket encryption rejection. Support only empty-password documents with extraction permission; keep password-required/extraction-prohibited cases rejected. No password guessing or permission bypass.

SoFi official overview https://investors.sofi.com/overview/default.aspx links the exact external PDF https://s27.q4cdn.com/749715820/files/doc_financials/2026/q2/SoFi-2Q-2026-Earnings-Transcript_FINAL.pdf. July29,2026;40pages;61,696characters. Source SHA25606effe1dc517bf9d89d539324ec586972defe290285f9ceb26dfbe2e1c7627ea. Official page SHA2565cc48caa03610d9bb2be6efaaa2744362a0e2a632faef7daaf85d719302a83e4.

Moderna official quarterly page https://investors.modernatx.com/quarterly-results embeds the exact transcriptURL https://irp.cdn-website.com/89dd0539/files/uploaded/MRNA-USQ_Transcript_2026-07-31.pdf. July31,2026;16pages;62,294characters. Source SHA2568422323b1781b23f945a870a7b1cffae1291752d42198cb0f92e46ca091a6efc. Official page SHA25666ae3c9d4a26e855e6ba50547cf0722d74bbabef1575916012d26a66e397186a. Preserve LSEG StreetEvents attribution; do not label this an issuer-authored transcript or republish the raw text publicly.

External document permissions require a reviewed source-controlled registry entry, official same-investor-host linking page and its checksum, HTTPS and PDF format. They grant only the exact document URL; sibling files, other CDN tenants, credentials, HTTP and unreviewed redirects remain rejected. No extra runtime discovery requests or broad CDN host permission. Prepared readers and canonical publication receipts retain linking-page URL/checksum and reject silent provenance changes.

## Verification and remaining gates

93 focused tests pass on local Python3.14.2, including exact-link isolation, public PDF permissions, identity, fiscal period, source revisions and schedules. A new provenance fixture initially attempted to replace metadata on an immutable revision; corrected the fixture to add a new source revision, preserving the production invariant.

Captured original replay uses isolated SQLite with source/model/email transports blocked. It verifies full company/date/quarter/publisher/Q&A, one research document per period, whole-state identical repeat, unchanged first evidence receipt after transport-only revision, zero evidence events and zero emails. Local artifacts are under artifacts/direct-feeds/free-replacements-2026-10-09/issuer-candidates-oct10 and issuer-active-provenance-replay-oct10.json. Deployment/hash/production source receipts remain required.

Registry coverage becomes seven issuers, five in the current22-ticker operational workload. GOOG association, broader current-period coverage and source reconciliation remain open; not a transcript cutover. TSMC direct original returns403 and is not registered. Reddit/Quest pages need further link discovery; no absence inferred from dynamic HTML alone. No subscription, public source switch, customer email or FMP cancellation.
