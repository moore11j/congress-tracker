# Accenture annual issuer financial preparation

October 12, 2026. Implemented and locally validated; release and ordinary scheduled observation pending. Public core fundamentals remain FMP.

## Gap and source

The 17:47 paired presence audit identifies ACN as the sole compared issuer whose prepared SEC period (May 31) trails its FMP period (August 31). The October 1 earnings exhibit provides annual GAAP statements before the annual periodic filing. This does not resolve the 255 other verified issuers without current complete SEC revenue periods.

The [reviewed Accenture SEC exhibit](https://www.sec.gov/Archives/edgar/data/1467373/000146737326000037/q4fy26earnings8-kexhibit.htm) was already retained by the ordinary press collector as document 5174. A read-only PostgreSQL export at 17:51:57 UTC captures the existing three-source bundle and cached FMP comparison, with zero source requests or production writes. Sources are public issuer financial evidence, not customer records.

| Original | SHA-256 |
|---|---|
| SEC company submissions | 9548f2074d11f3090c5a9005213e0a61576ec3dcc5b15a62b0c7318ca51e4014 |
| Filing index | bf625da2f8dc3037cc1facda5b03d7cc2849eb668ca8082a81d39bfd7996c44b |
| Complete submission | 4762f7d13771adf6a0bbd979f9ce922b6d0b270e175225d14455dfd5fa22ed9b |
| Extracted exhibit | da174b2f14b8e378fca891e3021b23496e47ee2190be5e57735999b5fea3d3d3 |

## Implementation

A lightweight reviewed-issuer registry generalizes the existing AutoZone path to ACN. It preserves AutoZone keys, cache versions and values. The existing bounded SEC press collector prepares the financial receipt from already-fetched originals; it adds no financial HTTP request, new schedule or larger batch. Only ACN's press/current-ratio preparation cache version changes. The financial warmer detects changed source receipts using the existing dependency mechanism.

The ACN parser accepts only this reviewed URL, issuer, publication day and annual period. It verifies statement captions and USD-thousands units, annual versus quarterly columns, parent versus consolidated net income, signed capex, accounting identities and cash/balance reconciliation. Missing, duplicate, changed or ambiguous tables fail closed. Future releases require review; this is not a general HTML scraper.

Ten prepared fields are gross/operating/net margin, revenue growth, basic EPS growth, operating-margin expansion, free cash flow, FCF margin/growth and current ratio. Derived gross profit and FCF retain formulas; balance sheet observations stay instantaneous rather than becoming annual flows. It excludes adjusted income, blended depreciation/other amounts as EBITDA, valuation, leverage and consensus fields. Before advancing the period, the reader rechecks the original source bundle and latest SEC earnings identity. Older fields cannot carry forward under the newer date.

## Actual source comparison

Both candidate and cached FMP periods are August 31, 2026. Agreement is field-specific; this is not wholesale provider equivalence.

| Field | Reviewed issuer calculation | Cached FMP |
|---|---:|---:|
| Revenue growth, % | 6.4737695936 | 6.4737695936 |
| Basic EPS growth, % | 11.1472742067 | 11.1472742067 |
| Operating margin, % | 15.3753441890 | 15.7503429505 |
| Operating-margin expansion, points | 0.6986869823 | 0.3749987615 |
| Gross margin, % | 32.0497356250 | 32.0101661496 |
| Parent net margin, % | 11.2788722605 | 11.3249054422 |
| Free cash flow, USD | 11,620,598,000 | absent |
| Current ratio | 1.4271254598 | not exported in this comparison |

The two growth fields agree within 1.1e-14. Other margins demonstrably differ; their underlying FMP construction is not established by this audit and must not be described as a formatting difference. Public core/score activation remains gated by methodology review and consumer rebaselining.

## Validation

Python 3.14.2: 245 focused tests pass across issuer parsing/staging/readers, SEC financial preparation/warming, earnings integrity, free/Finnhub combination and financial alert basis transitions. The ACN-specific scenario rebaselines a source change without an alert, emits one alert for a later genuine threshold crossing, deduplicates repeat evaluation and rebaselines rollback. No test sends email.

The actual saved three-source bundle passes a local isolated SQLite collector/staging/reader/free-combination replay at 17:56 UTC: three source documents/revisions, one financial snapshot, identical repeat payload and timestamp, identical consumer values, zero canonical financials, events or research documents. Six reads are of local originals; there are zero network requests, production writes or emails. CPU for this local two-pass scenario was 1.375 seconds, not production capacity proof. Tests also preserve AutoZone behavior and reject changed identity, columns, units, accounting, dates and source receipts. Initial test invocations had a wrong working directory and then a nonexistent optional test filename; corrected runs established the reported results.

Local artifacts: primary `artifacts/direct-feeds/free-replacements-2026-10-09/acn-financials-oct12/{manifest,replay}.json` and retained originals. The tracked test fixture contains factual table excerpts, not the complete captured source.

## Remaining release and migration gates

Release review, deployment/runtime hashes, actual deployed parser and ordinary collector-to-ratio propagation remain. API sustained capacity and PR615's ordinary exhausted-plan CPU path remain observational gates. At 17:54 the ordinary reference worker still has due work and retains two-source requests per run; no idle saving is claimed. Broader financial coverage/definition parity, institutional publication capacity/coverage, transcripts, all scheduled consumers, final Massive subscription/validation, global zero-FMP traffic and billing cancellation remain. No public provider switch, price plan, host, schedule, customer email, landing-page change or cancellation occurs in this task.
