# House scanned descriptions and security evidence

October 12, 2026. Deployed and runtime verified below; not eligible for publication.

The 31-page original House PTR 9116376 contains 459 rows. The completed visual review is preserved in `backend/config/house_ptr_descriptions.json`, bound to the exact original hash, discovery metadata, full page/row population and candidate descriptions. Its SHA256 is `811cf48fe16add61d4ba20e59e69733118cd9c2723541469e38edf39d14e5ade`; the original PDF hash is `9112dbf52d805f97500c314cbfacf58b114c9448d0f9a81091bafdff63da1a95`.

455 descriptions match after case, punctuation and wrapping normalization. Two corrections are supported by enlarged original images: p13:r6 Rollins suffix0795638 (distinct from p21:r14 suffix0796638), and p20:r9 LENNOX rather than LENHOX. Two remain explicitly unclear: final printed character in the Regeneron-like description p23:r9 and the Oracle bond maturity day p29:r7. Neither is silently guessed. Repeated offline review generation produces the same hash.

The review records seven trust headings and the MURA Holdings account heading. Four duplicate-looking pairs across pages30/31 remain eight distinct lots from different trusts. Owner codes are unchanged. Instrument indicators comprise410 equity candidates,9 depositary receipts,4 index-linked instruments,12 hybrids,4 options,1ETF,16 municipal debts and3 corporate debts. These are descriptive indicators, not approved canonical security mappings.

## Guarded preparation change

The reader applies457 reviewed descriptions, retains the original candidate alongside them, records trust/account headings, and leaves two uncertain candidates explicit. Exact-source, metadata, page population, candidate, review scope and identity guards reject incompatible reviews. Null tickers, unresolved canonical classification and mandatory publication hold remain enforced by the existing collector and publisher. Security mappings cannot be introduced through this registry. Existing economic fields and all459 source hashes remain unchanged. Retained originals are immutable and daily quarantine timing is unchanged.

## Independent security findings

Ten official SEC originals are retained as lossless gzip files with original and compressed hashes in [the evidence manifest](evidence/house-security-2026-10-12/manifest.json). [Identity findings](evidence/house-security-2026-10-12/identity-findings.json) establish useful relationships for six disclosed rows: VMRK, P, BLK (two rows), FDXF and PSKY. These are review evidence only, not an activated mapping registry. In particular, BlackRock Funding is the prior legal name of the current listed parent after its2024 reorganization; FedEx Freight has its own2026 spin-off ticker. Kimco OP units and the Vertiv LLC description remain separate unresolved identities. Dated reports are preserved; source retrieval today does not backdate Walnut availability.

## Validation and remaining work

137 focused tests pass on Python3.14.2 across description/preparation, existing review/quarantine, Congress publication, direct feeds and SEC collection. Every normalized row is compared against the preparation without description enrichment: only the two corrected name fields change; hashes, dates, owners, amount brackets, tickers, classifications and holds match. Malformed and injected mappings fail closed. Collector/publication checks retain zero canonical transactions, events and emails, a single immutable original across daily retries, and a hold even for erroneous parsed status. Exact release verification is recorded below. A first expanded command named a nonexistent test file and ran no tests; corrected invocation is recorded in the checkpoint.

The capture helper initially lacked bs4 and failed before network access, then used the standard-library HTML parser. The first capture bound rejected the7.18MB Kimco original; a bounded12MB local capture completed all10 originals, retaining21,957,985 uncompressed bytes as1,063,580 compressed bytes. No source response is treated as a ticker mapping merely because it contains a ticker.

Complete filing security review, mixed-instrument reconciliation and affected consumer proof remain prerequisites to publication. Three other scanned filings and ordinary production retention remain separate gates. Financial/institutional/transcript coverage, sustained capacity, all monitoring/watchlist/daily/weekly/ranking consumers, final Massive and zero-FMP/billing verification remain open. No new source selection, schedule, subscription, customer email or cancellation.

## Deployed verification, October12 at20:17 UTC

[PR623](https://github.com/moore11j/congress-tracker/pull/623), sourceed58915c, merged asdb1caf31746ce4f732acc40aa4e4342dace38eb4. Backend workflow38303042800/job114990358391 succeeds, including readiness and Premium privacy checks. Secret scan and Vercel pass. Unchanged dependency audits remain failures: Pillow10.4.0 has33 reported advisory entries; frontend production audit reports five vulnerable package groups. Manifests are unchanged; no full-suite/green-CI claim. A deployment-status read initially supplied the merge hash where the CI tool required the PR head; reloaded checks and read the same run successfully, without redispatch.

Python3.12.15 deployed replay validates the actual31-page fixture in isolated SQLite with network/reconciliation blocked. It verifies459 distinct rows,457 reviewed/two unclear descriptions, two corrections, distinct trust headings, null tickers/unresolved canonical classification, identical daily repeat and one immutable original. A two-hour retry makes no request; an erroneous parsed document status still cannot publish. Transactions/events/emails/production writes remain zero. All80 expected deployed hashes match. Source verifier and receipt: verify_house_description_release_oct12.py and house-description-release-1791836253.json in the primary artifact directory.

The API still uses shared2CPU/2048MB and database performance1CPU/2048MB. Machine receipt house-description-machines-oct12.json verifies the exact release image and passing checks. Public receipt issuer-staging-public-1791836247.json: ready0.337s, feeds0.448/0.545s, AAPL1.183s, NVDA1.263s, AZOpress0.331s, all200. These are immediate samples. Core remainsFMP and institutional publicationfalse.

Capacity receipt role-cpu-1791836183.json still shows two-hourAPI mean0.17503CPU above0.125shared allowance, balance1685 at20:15 and no throttle in that latest sample. Sustained recovery is not proved. Ordinary receipt ordinary-idle-cpu-1791836399.json records enrichment9.3861CPU/41.43wall seconds, prior collector0.2521CPU and idle reference0.2348CPU; due reference/research partial exits require their coverage receipts, not a crash assumption. Thirty-second process sample process-cpu-1791836338.json is quiet but misses exited processes, so cannot establish the total CPU budget.

Read-only house-scan-holds-1791836384.json at20:19:43 finds the four old failed scans unchanged, zero originals and52/52/52/39attempts. **Timing correction:** failed rows become eligible one hour after19:20, but `backend/crontab` runs Congress collection at minute17. Therefore20:20 is eligibility, not a scheduled run; the next eligible ordinary trigger is21:17UTC, subject to queue/budget. The earlier20:20 observation expectation was incorrect. No collector was forced or schedule changed. This gate remains open; runtime fixture success is not production retention proof.

All local handles are terminal; no local PostgreSQL was started. App attachment was attempted but rejected by the100-attachment limit; the ordinary PR link remains available. All remaining full-FMP cutover gates listed above remain active.
