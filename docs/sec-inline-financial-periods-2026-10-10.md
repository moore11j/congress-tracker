# SEC financial periods missing from companyfacts

October 10, 2026. Deployed as PR548/a29f81f6; ordinary scheduled preparation and public responses verified at 23:04 UTC.

## Problem and repair

NextEra's freshly retrieved companyfacts ends in March 2026 although its July 24 original 10-Q reports June 30. This leaves current ratios unavailable and historical panels behind the latest filed quarter. The original contains both NextEra and Florida Power & Light facts; some subsidiary contexts use the parent's identifier with a legal-entity dimension. CIK matching alone is insufficient.

The background preparer now checks official submissions against the latest available supported companyfacts period. A newer completed filing day permits at most one original 10-Q/10-K request. Only standard US GAAP, unsegmented target-entity, USD facts and explicitly supported inline transformations qualify. Conflicts, mismatched identity, unsafe directory paths, unsupported numeric formats and namespace redefinitions fail closed. Other taxonomy/currency and missing individual concepts are separate coverage gaps. This is not general HTML table scraping or a solution for every historical gap.

The original companyfacts hash stays unchanged. Added inputs identify the original filing URL, hash, accession, CIK, context and fact. Total regulated/unregulated operating revenue takes precedence over the smaller customer-contract component. Current core fundamentals remain selected from FMP; this release prepares free ratios and repairs the already-selected SEC historical financial reader. NEE alone invalidates its historical cache; ratios use v4 within existing ten-scope rotation. No new schedule, concurrency, public event, alert or email path.

## Source evidence

- [NEE June 2026 10-Q](https://www.sec.gov/Archives/edgar/data/753308/000075330826000060/nee-20260630.htm), 3,077,754 bytes, SHA256 `b77bdb6bb1815103f47126dbebadd164be5525c177f6f419a01b60340e4e686f`.
- Current companyfacts SHA256 `99e84510f602363147dc6c83321aa76859e185d0807019d1efc9e0da60491954`; submissions SHA256 `cadf20208603178b1da7eb23614a898acc798205507697e5d1472f4846bd53a7`.
- Parent June quarter revenue $7.534bn; current six months $14.235bn, prior six months $12.947bn. Reconstructed TTM revenue $28.700bn and operating cash flow $13.803bn. Unsupported capex/free cash flow remain missing.
- Numeric transformation validation follows the [XBRL transformation registry](https://www.xbrl.org/Specification/inlineXBRL-transformationRegistry/REC-2020-02-12/inlineXBRL-transformationRegistry-REC-2020-02-12.html), with supported versions explicitly bound to namespace URIs.

## Validation

Python 3.14.2 locally, production Python 3.12.15 remains to be verified for this release.

- 114 focused financial/source/cache/warmer checks pass. An initial invocation referenced a nonexistent test filename and ran no tests. First actual run had three exception-type regressions; preserving the existing SecFundamentalsError contract resolves all three.
- Captured-source 22-company background replay: six current NEE metrics recovered; other 21 companies' values unchanged. 49 captured source reads, identical cache repeat without further transport, zero canonical writes/emails. NVIDIA and Exxon recoveries remain unchanged.
- Actual PostgreSQL 16.15 isolated schema, one pooled connection: NEE 6 annual/12 quarterly rows, identical cached repeat, nine refused overlaps, zero checked-out pooled connections or idle transactions during source I/O. A simulated 429 preserves the existing cache and releases the advisory lock. Demand queue identity/attempts unchanged; second warmer run does not refetch. Initial harness asserted the first array item was newest; corrected to select June by date, then passed. Test schema removed and server stopped.
- Original fixtures, source manifests and replay receipts are retained locally under `artifacts/direct-feeds/free-replacements-2026-10-09/`; durable results are summarized here.

## Release and ordinary operation

[PR548](https://github.com/moore11j/congress-tracker/pull/548) merged as `a29f81f6cbcdb5681d0c3988fda7d4555c060e65`; backend workflow `38093520162` succeeds. All nine source/proof hashes match. Secret scan passes; unchanged frontend Next.js ecosystem and backend Pillow dependency audits fail. A read-only file receipt attempted before rollout completion failed and establishes no runtime evidence; the completed-release receipt succeeds.

The first public checks at 23:02 returned the truthful loading state and queued NEE with zero attempts. Ordinary `warm_sec_research` at23:04:01 prepares6annual/12quarterly rows, reports zero canonical writes/emails and selects SEC financial statements. The source and per-input hashes match the reviewed original. NEE's separate ratio cache still awaits ordinary rotation; public core remains default FMP. General enrichment identity remains queued/zero attempts because the dedicated warmer has supplied its cache.

Four backend/site responses after preparation all return200 and identical content:6annual/12quarterly, latest quarterJune30, TTM revenue28.700bn. Latencies0.464/0.620/0.362/0.564s; fingerprint `d052ba0806772427e12d586a9d67019415104c8bc094acd2d175b49dad246b53`. Source=`sec_edgar`, status=`partial` for unsupported fields. Receipts `inline-runtime-1791673459.json`, `inline-public-final-1791673471.json`.

Post-deployment two-service receipt `two-service-verify-1791673417.json` confirms release image, seven operational hashes, exactly two machines and empty scheduled queue;1544MB available. Feed0.40–0.54s/newtickercontext1.06–1.51s.

## Remaining gates

 This is not broad coverage, full-suite, rendering, sustained delivery or zero-FMP-traffic evidence. Institutions, transcripts, core coverage, other consumers and final Massive pricing remain separate migration gates.
