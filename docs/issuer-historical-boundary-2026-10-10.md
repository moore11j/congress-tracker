# Issuer transcript historical boundary, October 10

Read-only production evidence finds one processed FMP document for each reviewed NVDA/META/IBM/SOFI/MRNA call. Each differs from the official source text. The five documents have232 evidence events and23 extraction chunks. Neither equivalent fiscal periods nor similar content establishes exact source equivalence.

## Compatibility behavior

For a call before the explicit issuer publication boundary, retain completed FMP research only when there is exactly one candidate with the canonical security/period/provider/external identity, a content hash, and the same publication date as the independently validated issuer source. Missing, unfinished, ambiguous or conflicting baselines remain held. Existing document IDs, evidence, excerpts, hashes, processing versions and timestamps are not rewritten. The retention receipt records the original document separately from issuer evidence and rejects later changes. HTML/PDF transport revisions retain and revalidate the first receipt.

Historical source text is deliberately absent from the extraction return value: no issuer text can be sent to extraction under a legacy document hash. Ordinary matching still uses the original document and event identities, preserving the existing matching budget/deduplication. It does not create a second set of source events. Public coverage remains partial, explicitly names the retained historical provider and does not claim text equivalence or broad issuer coverage. A new eligible fiscal period uses a separate issuer document once; repeated preparation reuses it.

Everything is behind the existing issuer selector and explicit date boundary. Public transcripts remain FMP in production. No activation boundary, schedule, model budget, customer email or subscription change is included.

## Validation

119 focused tests pass on Python3.14.2 across source parsing, registry, prepared evidence, operational consumers and existing daily/weekly alert parity. Two FastAPI startup deprecation warnings remain. Tests include conflicting date/identity/period/provider, incomplete/duplicate baselines, mutation after receipt, changed publication boundary, transport-only revisions, original receipt tampering, same-document matching and the next fiscal period. The first transport test exposed collector receipt reset; the collector now retains both immutable publication and historical receipts.

A read-only export contains only company-level source/evidence records, no user or thesis records. First isolated original-source replay preserves all five documents,232 events and23 chunks, complete repeat state and canonical fingerprint31ec313fc3f87e9148a4cab961fcf989192c59f0443fbcba4fd06fd0f2f7f2e3. It predates the final preservation of ordinary matching. The final matching-enabled replay also passes with the same canonical fingerprint and zero requests/emails. It executes ordinary matching against the232 real company events with no customer theses in the isolated database; it does not claim production customer matching certification. All37 targeted matching/issuer checks pass. Original PDFs, including publisher provenance, are checksum-matched to production. No production writes, HTTP/model requests or emails occur during replay.

Artifacts: issuer-history-baseline.json, issuer-history-replay-oct10.json; final issuer-history-consumer-replay-oct10.json passed. Source discovery for future quarters, wider coverage, GOOG association, remaining institutions/core/consumer gates and Massive subscription last remain open. This is historical compatibility, not transcript cutover or all-alert certification.

## Deployment verified,21:43UTC

PR541/e3ed1167 merged as a497d4ec3118e0364e0883224ac94f2baf5a3eea; workflow38088550823 succeeds. Eleven source/runtime hashes match, plus the five-file topology check. Public transcript provider remainsFMP; hourly minute13 schedule unchanged and all eight prepared sources remain one attempt/one revision. No production historical receipts applied. Existing Next.js/Pillow dependency audits remain failing in unchanged dependencies; secret scan passes. Receipt issuer-scheduled-receipt-1791668590.json.

Two-service receipt1791668625 retains correct machines,1533MB available and empty queues. Six public responses200; feed0.55-0.61s/profile0.55s, new NVDA1.83s but AAPL7.84s. Immediate timing capture isolates AAPL signals5.54s, quote0.20s, confirmation0.81s; see cold-build report. The release is verified, but universal latency and provider cutover are not.

## Next issuer source discovered

Reddit's official quarterly page uses a public Q4 financial-report API. Captured HTML SHA8ab69b54648360c7b33f5c8bcc701ea4f820b4a3c383c789fff2fff6b80678c6 and API JSON SHAd28b8820b24e50aba02f3cf8e3312d0cf728ca2e5cf972c2689e7488c1b9378a establish the exact transcript link, rather than guessing its filename. The issuer-linked [Q2 transcript](https://s203.q4cdn.com/380862485/files/doc_financials/2026/q2/Reddit-Q2-26-Earnings-Call_Transcript.pdf) has24 pages,410149bytes and SHAb817c1508a458fa7228ea0fb5cdb089d32b5987bd21aa4769b4221a5725c49c6. Heading datesJuly30,2026; normalized text57647characters includes operator-led analyst questions. It lacks the parser's generic question-and-answer marker, requiring a reviewed exact Q&A pattern before registration. Source preparation/registration is not yet implemented. The API supports year-based discovery, but unattended future-quarter validation is not built.

Artifacts in issuer-candidates-oct10: reddit-quarterly-results.html, reddit-q4-api.js, reddit-quarterly-api.json, reddit-q2-transcript.pdf/text. Initial optional bs4 import was unavailable; standard library parsing completed without installation. No new paid feed, public selection, customer email or FMP cancellation.
