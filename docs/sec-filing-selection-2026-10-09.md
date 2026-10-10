# Independently selected SEC company filing lists

October 9 Pacific / October 10 UTC, 2026. The ticker filing-list reader can select `SEC_FILINGS_PROVIDER=sec_edgar` independently of core financial ratios and prices. It uses the existing verified issuer submissions parser and normalized public contract, including accession identity, filing versus acceptance date, pagination, source hash and explicit recent-only coverage. The default remains FMP until activation.

Direct lists now occupy `sec_company_filings` shared caches, preserving legacy `sec_filings` rows for rollback. Public cache misses enqueue the existing filing job without synchronous source transport. The same job calls the selected adapter. FMP cannot satisfy a selected direct request, including when the global FMP guard is enabled. Switching back preserves access to legacy caches.

The existing guarded five-symbol metadata rotation can prepare filing lists with `SEC_FILINGS_WARMING_ENABLED=1`, before public selection. It retains its shared collector lock, lease, pressure gate and bounded scope. Receipts show selection, preparation, source counts and unavailable lists; a partial scope makes the batch partial. There are no new trading events, canonical records, models or email deliveries.

## Validation

The final Python 3.14.2 run passes 29 filing/parser, public API/queue, cache and scheduler tests. One unrelated admin cache-debug price-point expectation fails identically against unchanged released source in the other checkout (expected one price point, observed zero); it is excluded only after that reproduction. No full-suite claim.

Twenty checksummed saved company submissions replay through the actual selected adapter with core fundamentals still FMP, global FMP disabled and all network transport forbidden. They retain 18,634 links in twenty independent shared caches. Clearing process caches and repeating leaves persistent state identical, with no further source reads. Manifest SHA-256: `052495c6b28b8f7b3837cdbafbadbe5e67ca51ef78c2d1576124c9ab3487acf8`; state SHA-256: `e2aebf002186278c99240826f9899225825ae9023648a3b7392a67fd97af6805`. Local receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/filing-list-independent-replay.json`. JPMorgan has 26,339 source rows and is explicitly capped at its latest 2,000; the adapter does not claim complete annual or older archive coverage.

Deployment, current scheduled preparation and public activation verification remain next. This release does not supply full transcripts, change market data or complete the FMP shutdown.


## Actual scope preparation and official filename correction

PR519/4d0a1e41 is deployed with100matching hashes on four workers. Actual04:40UTC metadata cron prepares five filing lists. A separately labeled bounded manual rotation completes all54current symbols:49valid lists, three known SEC-directory absences(BNPQY,DRAM,QTUM), and two parser refusals(CX,LAMR). Public selection remains FMP.

Original SEC submissions prove both refusals concern literal consecutive periods within official filenames, such as `cemex.s.a.b..de.c.v..txt` and `lamar.advertising.co..cl.a.txt`. The parser now rejects dot traversal segments, rather than every adjacent period anywhere in a filename. Absolute, encoded or unsafe path characters remain rejected. Twenty-seven focused checks pass, including traversal rejection. Actual payload replay accepts1000CX/1001LAMRfilings with unchanged source hashes, dates and accession identities. Both still need refreshed production caches after release.

Source hashes: CX`3bd8261d0cec70695ef68c865c21c8193f511eab4aa587eb999dc218db9191c0`; LAMR`4fb3a06b89ac3ee110ebaa79de999a56bd64c3822d1e75a97dca0618d3ac469b`. Receipts: filing-scope-preparation-0444.json, filing-absence-probe.json and filing-filename-replay.json in the current ignored evidence folder. No canonical writes, emails or source activation.


## Verified empty and absent sources, October 9 Pacific

The live 54-symbol preparation found three SEC-directory absences in addition to 49 prepared lists and two official filename cases corrected by PR522. The isolated SEC filing cache now persists verified directory absence and verified empty submission lists for one hour, preserving unavailable versus empty status during pagination. Public requests return that coverage without repeatedly queuing work. Transient source failures are not persisted as directory absence, and an expired absence can recover to a valid list. The metadata warmer attempts explicit filing coverage for a verified directory absence but stops on transport refusal.

Forty focused checks pass on Python 3.14.2. The existing admin debug test still expects one price point but receives zero; the same failure was reproduced against unchanged source in the preceding filing release. There is no full-suite claim. Public filing selection remains FMP until deployment and live coverage verification. No canonical events, emails, purchases or price-provider changes.
