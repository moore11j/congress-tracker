# House scan retention and preparation holds

October 12, 2026. Repair deployed as PR621/d2793db6; runtime verified. Ordinary collection and scanned transaction review remain pending.

## Observed gaps

Production read-only receipt `preparation-holds-1791832995.json` explains the sampled scheduled partial results. SEC statement warming returned existing partial panels and unavailable SPY/CX/DRAM/IBIT scopes, without transport exceptions in that sampled batch. Free research held ASML, BRK-B and CX for `recommendation_symbol_mismatch`; do not weaken identity checks. Institutional reference collection staged one verified and one unavailable identity and reported a bounded universe window. Senate backup run270 succeeded; House run269 had four no-text-layer errors. These are scoped receipts, not complete coverage or delivery proof.

Read-only `house-scan-holds-1791833242.json` identifies four failed public PDFs with no content hashes or retained revisions: House9116355,9116356,9116357 each52 attempts; House9116376 has39. Their failures currently receive hourly retries. Official bytes were downloaded to primary `artifacts/direct-feeds/free-replacements-2026-10-09/house-scans-oct12/` for offline review. The first three have two pages each; the last has31. No transaction transcription has been approved by this repair.

## Repair

`HouseScanReviewRequired`, a specific `DirectSourceError`, is raised only after a valid PDF has no extracted text and no existing exact-byte reviewed transcription. The collector retains that original in existing revision storage, with empty extracted text, zero inferred transactions, explicit held reason and page count. It skips canonical reconciliation. Existing quarantine recheck bounds apply; network failures, invalid PDFs, mismatched reviews and storage failures remain retryable failures. No job timing, provider selection, source flag, schema or public consumer is changed.

Repeated identical source bytes do not add revisions; changed bytes preserve the previous immutable original. This improves evidence retention and avoids futile hourly reparsing while keeping missing coverage explicit. It does not complete the scanned disclosures or prove overall CPU capacity.

## Validation

Three new regressions failed on the original code. After repair,75 focused tests pass on Python3.14.2 across House quarantine/review, direct feeds, SEC cover quarantine and SEC collection. An initial test-copy command used a duplicated backend path and ran no tests; corrected before these results.

Actual four-original SQLite replay verifies four quarantines, exact bytes/SHA256, zero refetches at two hours, four unchanged revisions after a due daily repeat, zero canonical reconciliation/events/emails. Receipt `house-scans-oct12/receipt.json` contains all hashes. No production data was modified.

Current CPU receipt `role-cpu-1791833189.json` still has API two-hour mean0.19523 CPU against0.125 shared allowance; latest five-minute0.18773, zero current throttle,4197.01 credits. DB two-hour mean0.10853. This does not establish sustained recovery after PR620. Production remains99c2bf49 pending this release; coreFMP and institutional publicationfalse remain gated.

Next: release and verify retention under deployed Python3.12, then ordinary collection and actual scan review, financial source/identity coverage, institutional capacity/publication, transcripts and all consumer checks. Massive remains the final price subscription step, followed by zero global FMP traffic and billing cancellation. No new subscription, schedule, host, customer email, video or landing change.


## Deployed verification, October12 at19:34UTC

[PR621](https://github.com/moore11j/congress-tracker/pull/621) sourceccaf0228 merged asd2793db64ebba654ac002c6f55fbc1410ca55b3c. Backend workflow38298233999 succeeds. Exact-head CI38298104975 secret scan passes; unchanged Pillow10.4.0/33-advisory and frontend five-production-vulnerability audits fail. Logs read with the required CI tool, dependency manifests unchanged; no full-CI pass claim. PR attachment attempted but app100-attachment limit rejected it; normal PR URL remains usable.

Additional33 Congress publication/targeted repair tests pass, for108 focused passes across the two nonoverlapping groups. `house-scan-release-1791833593.json` verifies75 deployed hashes with no mismatch and the productionPython3.12.15 isolated-memory fixture: exact-byte retention, no two-hour refetch, one immutable revision after due daily repeat, no production writes/network/emails. This is not an ordinary collection receipt; the four production failed documents still need their next due scheduled collection. Previous99c2bf49 remains the rollback release.

Public receipt `issuer-staging-public-1791833590.json` verifies readiness0.442s, feeds0.535/0.564s, AAPL1.492s, NVDA1.361s and AZOpress0.252s/four identities retained. These immediate postrelease samples do not prove sustained capacity or all-consumer completeness. CoreFMP and institutional publicationfalse are unchanged.

Read-only PDF skill review rendered and inspected all six pages of the three Harshbarger forms and the cover/first attachment page of the31-page Khanna form. The Harshbarger pages show municipal/Treasury debt and private-fund names, with repeated lots and some degraded lettering; Khanna page2 mixes an index-linked instrument, common stock, hybrid securities and an option. Do not map private KKR funds to public KKR stock or hybrids/options to ordinary shares. This is triage evidence, not a complete verified transcription or canonical publication approval. Remaining Khanna pages require review. Local PNGs and original hashes stay together in house-scans-oct12.

No new source selection, subscription, scheduled trigger, host/storage configuration, customer email, video/landing change or FMP cancellation. All new tool handles are terminal; no local PostgreSQL. Next ordinary retention, complete scan transcription/classification/reconciliation, and remaining financial/source/institutional/transcript/all-consumer gates before finalMassive and zero-FMP/cancellation.
