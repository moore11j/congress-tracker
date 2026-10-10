# Independently selected SEC company filing lists

October 9 Pacific / October 10 UTC, 2026. The ticker filing-list reader can select `SEC_FILINGS_PROVIDER=sec_edgar` independently of core financial ratios and prices. It uses the existing verified issuer submissions parser and normalized public contract, including accession identity, filing versus acceptance date, pagination, source hash and explicit recent-only coverage. The default remains FMP until activation.

Direct lists now occupy `sec_company_filings` shared caches, preserving legacy `sec_filings` rows for rollback. Public cache misses enqueue the existing filing job without synchronous source transport. The same job calls the selected adapter. FMP cannot satisfy a selected direct request, including when the global FMP guard is enabled. Switching back preserves access to legacy caches.

The existing guarded five-symbol metadata rotation can prepare filing lists with `SEC_FILINGS_WARMING_ENABLED=1`, before public selection. It retains its shared collector lock, lease, pressure gate and bounded scope. Receipts show selection, preparation, source counts and unavailable lists; a partial scope makes the batch partial. There are no new trading events, canonical records, models or email deliveries.

## Validation

The final Python 3.14.2 run passes 29 filing/parser, public API/queue, cache and scheduler tests. One unrelated admin cache-debug price-point expectation fails identically against unchanged released source in the other checkout (expected one price point, observed zero); it is excluded only after that reproduction. No full-suite claim.

Twenty checksummed saved company submissions replay through the actual selected adapter with core fundamentals still FMP, global FMP disabled and all network transport forbidden. They retain 18,634 links in twenty independent shared caches. Clearing process caches and repeating leaves persistent state identical, with no further source reads. Manifest SHA-256: `052495c6b28b8f7b3837cdbafbadbe5e67ca51ef78c2d1576124c9ab3487acf8`; state SHA-256: `e2aebf002186278c99240826f9899225825ae9023648a3b7392a67fd97af6805`. Local receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/filing-list-independent-replay.json`. JPMorgan has 26,339 source rows and is explicitly capped at its latest 2,000; the adapter does not claim complete annual or older archive coverage.

Deployment, current scheduled preparation and public activation verification remain next. This release does not supply full transcripts, change market data or complete the FMP shutdown.
