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

## Deployment and bounded live collection,21:24UTC

PR539/7da6ec4557ec794c4e27a4f68033475c0184d779 merged as2f399c05241c56dce806e4df3ae7225eb40425ca; backend workflow38087277310 succeeds. Ten exact runtime hashes match. Registry d72b967220aab1449cd3eba245ce3e53eae328e8a99081929077910b18bbc187; transport896e8d49386d8263d520f5af548db321b9f1369db6c31cca07e21089e6f8b9f3; collector6f934bb31be07152b94412d9c614160abd350dbc112ba4cbf9275867ea137be5; reader17c44091a91bf67fc50349e2799301eea6e7ad968c42039a608f50506228afc0. Secret scan passes; dependency audits retain unchanged Next.js ecosystem/Pillow failures.

Two explicit validation enqueues use the existing data lane and unchanged limit2/recheck24 command. Run92 collects IBM/SOFI, leaves one pending; run93 collects only MRNA. Both zero errors/public writes. IBM6595/revision6569, SOFI6596/revision6570, MRNA6597/revision6573 each have one attempt/one revision. Source hash, extracted text hash, publication date and publisher equal all three final captured-original replays. Older five source records retain attempt1/revision1 and unchanged timestamps. Receipt issuer-scheduled-receipt-1791667468.json: all eight documents parsed, queue exit0 and empty. These are explicit validation runs, not a new ordinary hourly cycle; existing hourly cadence unchanged. Public provider remainsFMP.

Read-only reconciliation receipt issuer-reconciliation-1791667405.json proves NVDA/META/IBM/SOFI each has one processed FMP research document for the reviewed quarter, with a different text hash. Existing guard requires reconciliation rather than duplicate publication. MSFT/GOOGL have no same-period candidate. MRNA was pending at that audit; its existing-record comparison remains unverified. Next work must preserve original research identity/history while binding official evidence and handle future-quarter discovery, not merely add static quarter URLs.

Two-service receipt1791667477 confirms correct release,1448MB available, empty queues and six successful public responses. Feeds0.56-0.69s/profile0.71s; new NVDA summary2.03s. One new AAPL summary6.35s, followed by two unchanged-generation reads0.357/0.275s. Cold-summary latency is still an open reliability issue; do not report universally fast ticker generation. No source switch, purchase, customer test email or FMP cancellation.
