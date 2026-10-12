# House scanned descriptions and security evidence

October 12, 2026. Implemented locally; not deployed or eligible for publication.

The 31-page original House PTR 9116376 contains 459 rows. The completed visual review is preserved in `backend/config/house_ptr_descriptions.json`, bound to the exact original hash, discovery metadata, full page/row population and candidate descriptions. Its SHA256 is `811cf48fe16add61d4ba20e59e69733118cd9c2723541469e38edf39d14e5ade`; the original PDF hash is `9112dbf52d805f97500c314cbfacf58b114c9448d0f9a81091bafdff63da1a95`.

455 descriptions match after case, punctuation and wrapping normalization. Two corrections are supported by enlarged original images: p13:r6 Rollins suffix0795638 (distinct from p21:r14 suffix0796638), and p20:r9 LENNOX rather than LENHOX. Two remain explicitly unclear: final printed character in the Regeneron-like description p23:r9 and the Oracle bond maturity day p29:r7. Neither is silently guessed. Repeated offline review generation produces the same hash.

The review records seven trust headings and the MURA Holdings account heading. Four duplicate-looking pairs across pages30/31 remain eight distinct lots from different trusts. Owner codes are unchanged. Instrument indicators comprise410 equity candidates,9 depositary receipts,4 index-linked instruments,12 hybrids,4 options,1ETF,16 municipal debts and3 corporate debts. These are descriptive indicators, not approved canonical security mappings.

## Guarded preparation change

The reader applies457 reviewed descriptions, retains the original candidate alongside them, records trust/account headings, and leaves two uncertain candidates explicit. Exact-source, metadata, page population, candidate, review scope and identity guards reject incompatible reviews. Null tickers, unresolved canonical classification and mandatory publication hold remain enforced by the existing collector and publisher. Security mappings cannot be introduced through this registry. Existing economic fields and all459 source hashes remain unchanged. Retained originals are immutable and daily quarantine timing is unchanged.

## Independent security findings

Ten official SEC originals are retained as lossless gzip files with original and compressed hashes in [the evidence manifest](evidence/house-security-2026-10-12/manifest.json). [Identity findings](evidence/house-security-2026-10-12/identity-findings.json) establish useful relationships for six disclosed rows: VMRK, P, BLK (two rows), FDXF and PSKY. These are review evidence only, not an activated mapping registry. In particular, BlackRock Funding is the prior legal name of the current listed parent after its2024 reorganization; FedEx Freight has its own2026 spin-off ticker. Kimco OP units and the Vertiv LLC description remain separate unresolved identities. Dated reports are preserved; source retrieval today does not backdate Walnut availability.

## Validation and remaining work

137 focused tests pass on Python3.14.2 across description/preparation, existing review/quarantine, Congress publication, direct feeds and SEC collection. Every normalized row is compared against the preparation without description enrichment: only the two corrected name fields change; hashes, dates, owners, amount brackets, tickers, classifications and holds match. Malformed and injected mappings fail closed. Collector/publication checks retain zero canonical transactions, events and emails, a single immutable original across daily retries, and a hold even for erroneous parsed status. Exact release verification is pending. A first expanded command named a nonexistent test file and ran no tests; corrected invocation is recorded in the checkpoint.

The capture helper initially lacked bs4 and failed before network access, then used the standard-library HTML parser. The first capture bound rejected the7.18MB Kimco original; a bounded12MB local capture completed all10 originals, retaining21,957,985 uncompressed bytes as1,063,580 compressed bytes. No source response is treated as a ticker mapping merely because it contains a ticker.

Complete filing security review, mixed-instrument reconciliation and affected consumer proof remain prerequisites to publication. Three other scanned filings and ordinary production retention remain separate gates. Financial/institutional/transcript coverage, sustained capacity, all monitoring/watchlist/daily/weekly/ranking consumers, final Massive and zero-FMP/billing verification remain open. No new source selection, schedule, subscription, customer email or cancellation.
