# Foreign annual financial coverage, October 14, 2026

Before PR634, the selected SEC statement reader returned no annual rows for ASML, SAP and Novartis. Reviewed annual reports establish ASML as EUR US GAAP, SAP as EUR IFRS and Novartis as USD IFRS. This change supplies six annual periods per issuer, keeping reporting currencies and source evidence explicit. It does not fill quarterly results, current TTM metrics, ADR EPS, analyst estimates or canonical core fundamentals.

## Source review

| Issuer | Annual original | SHA256 |
| --- | --- | --- |
| ASML | [2025 20-F](https://www.sec.gov/Archives/edgar/data/937966/000162828026011378/asml-20251231.htm) | `7a3902f17f4ea364b45172a5b7da443b033e25d01404a42c9169547a4d9b72b7` |
| SAP | [2025 20-F](https://www.sec.gov/Archives/edgar/data/1000184/000110465926020058/sap-20251231x20f.htm) | `4628dc2d12f68bfbbe7c07a2099a6916db30d9547b8f3d1424b451272790fc15` |
| NVS | [2025 20-F](https://www.sec.gov/Archives/edgar/data/1114448/000111444826000004/nvs-20251231.htm) | `ee4e72876e1e8fe464286874b883191ffac5a9ba630aacf7023e59a66878eda0` |

The directory independently resolves all three ticker/CIK pairs. Fifty-four original table amounts, six metrics across 2023, 2024 and 2025 for each issuer, match the candidate source evidence exactly. ASML's 2025 revenue is EUR32,667.3 million; SAP's is EUR36,800 million; Novartis net sales are USD54,532 million.

SAP's capex tag covers the reported combined intangible and property/equipment purchase line. SAP and Novartis use explicitly tagged continuing-operations operating cash flow for cash generation; missing continuing-operations facts never fall back to consolidated cash flows. Net income retains consolidated profit, with historical discontinued operations disclosed. Novartis revenue is net sales, excluding other revenue; margin denominators are labelled accordingly. ASML arithmetic free cash flow excludes separately reported intangible purchases, differing from the issuer's adjusted measure. No missing amounts are assumed zero.

## Reader changes and validation

Issuer-specific tag overrides leave the shared IFRS concepts unchanged and return independent profile dictionaries. The Novartis original exposed an unrelated local xlink namespace alias and older 2015 transformations. The reader now permits additional namespace aliases while rejecting any rebinding of root financial or transform namespaces. Only the reviewed legacy dot-decimal and English full-month date forms are added; identity, dates, units, conflicting facts and numeric bounds remain enforced. The [XBRL Transformation Registry 3](https://www.xbrl.org/Specification/inlineXBRL-transformationRegistry/REC-2015-02-26/inlineXBRL-transformationRegistry-REC-2015-02-26.html) defines those formats. This is not implementation of every legacy transform.

Actual Windows Python3.14.2 replay with sockets forbidden verifies all54 amounts, whole-response repeat, real SQLite statement-cache repeat with no new requests, and zero canonical fundamentals/events. Independently parsed SAP and Novartis originals reproduce all18 facts each. ASML's24.86MB original exceeds the unchanged20MB inline recovery bound; its current companyfacts supplies the verified annual data. Future oversized-original recovery remains an explicit limitation, not an unlimited parser allowance. Local artifacts: `artifacts/foreign-batch-oct14/manifest.json`, originals, extracted table text and `replay.json`.

Initial tests and replay exposed fixture formatting and a local date-variable collision; both were corrected before release. Final Linux Python3.12.3 run passes187 affected tests (two deprecation warnings), and the same full three-source/cache replay passes. Production release9b5f8b87 and ordinary/public verification now pass as recorded below. No new schedule, provider selection, hosting cost, purchase or customer email.

## Other observed migration progress

Ordinary SEC collection480 at16:47 completes with no source errors, explicitly skips the reviewed October12 EDGAR closure and requests October7/8/9/13. It discovers666 13F and1,462 Form4 entries, unchanged from the previous run; existing1,629 unresolved documents remain held, with zero new public writes. Receipt `collector-calendar-1791996768.json` closes the calendar repair's ordinary-run verification, not the broader coverage gate.

Core financial coverage and definitions, institutional publication, transcripts, sustained capacity, all alert/ranking consumers, final Massive entitlement, global zero-FMP traffic and billing remain open. FMP stays enabled.

## Deployment and ordinary verification, 17:03 UTC

[PR634](https://github.com/moore11j/congress-tracker/pull/634), source `aea7089fe45d66a8584c8dfe48be905f15054076`, merged as `9b5f8b871517e504ce9dee1e9b7f71e9b380522a`. Backend workflow38602508077 completed successfully. Eighteen normalized runtime hashes and deployed pure profile/legacy-number checks match (`foreign-runtime-1791997141.json`). Secret scan passed; unchanged backend/frontend dependency audits failed and their logs were inspected through the required CI tool. No dependency manifests changed; no all-CI or full-suite success is claimed. PR attachment failed at the app's100-attachment limit; the normal PR link remains available.

The existing16:59 SEC research job refreshed all three issuers, verified independently in `foreign-receipt-1791997364.json`. Each public endpoint now contains six annual rows, where baseline16:56 reads had no rows. Full public payloads repeat identically between `foreign-public-1791997162.json` and `foreign-public-1791997322.json`; annual, quarterly, summary and reportingBasis sections exactly match the saved-source replay. Receipt `foreign-public-repeat-oct14.json` retains hashes. Repeat latency: ASML0.561s, SAP0.420s and NVS0.310s. Missing quarters/current TTM/EPS remain explicit.

At17:02, the live image is9b5f8b87, one API shared2CPU/2GB and one database performance1CPU/2GB; no extra host or cost. Scheduled metadata completed5/5 and news42/42 fresh caches in its current scope; partial SEC earnings/research and truncated institutional reference coverage remain explicit. Arm's separate core preparation is still old/unavailable awaiting ordinary rotation. `preload-receipts-1791997339.json` records aggregate receipts and machine sizes. Public Feed0.499s/Insights0.729s, AAPL2.542s/NVDA2.312s return200; Insights contains research without the unavailable fallback (`insights-incident-1791997325.json`). These samples do not prove sustained capacity. Initial sandbox-only network permission failures made no production requests, then authorized network reads succeeded.

The owner's screenshot concerns Codex remote execution. This turn and its tools work; account usage permits work with37% weekly consumed. That does not identify the historical429 cause or establish a permanent repair. No Codex configuration change was made. FMP remains enabled pending the broader acceptance gates.
