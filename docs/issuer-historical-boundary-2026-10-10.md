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
