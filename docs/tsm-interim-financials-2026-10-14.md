# TSM interim financial ratios from SEC originals

October 14, 2026. Deployed155cc0b3; exact runtime and ordinary preparation verified. Public core remains FMP.

TSM's free financial preparation has no current SEC ratios because the standard core reader expects USD US GAAP facts. Its free Finnhub association supplies two classification inputs, leaving the fundamentals classification unavailable. The new reader calculates five dimensionless ratios from three reviewed consolidated statements in TWD, on the same Taiwan-adopted IFRS basis.

## Reviewed sources and accounting boundary

| Source | Filed | SHA256 |
| --- | --- | --- |
| [2026 first-half statement](https://www.sec.gov/Archives/edgar/data/1046179/000104617926000541/a2026q2consolidatedreport-.htm) | August 14, 2026 | `87a317c976eae5fdffa37421e16f1d0d4248abc15a2ea7e0c3883cd0510a48c4` |
| [2025 first-half statement](https://www.sec.gov/Archives/edgar/data/1046179/000104617925000097/tsmc2025q2consolidatedfina.htm) | August 14, 2025 | `8510c495d472da69ff9cdf3d8a5d64212c6c296d3514cd5cce8dde5ed9a274f1` |
| [2025 annual consolidated statement](https://www.sec.gov/Archives/edgar/data/1046179/000104617926000024/a2025q4consolidatedreport-.htm) | February 26, 2026 | `ba24f40bf0bccb828bd54627d294b9e49c84d288cb0afb49f7cffeb0f7734b01` |

The SEC covers independently identify the issuer and link these statements. The [annual cover](https://www.sec.gov/Archives/edgar/data/1046179/000104617926000024/tsm-fsx20260226x6k.htm) distinguishes consolidated statements from parent-company-only statements; only the former is used. The [interim cover](https://www.sec.gov/Archives/edgar/data/1046179/000104617925000097/tsm-fsx20250814x6k.htm) identifies the same Taiwan-IFRS basis. TSM's separate 20-F uses IASB IFRS and is not spliced into these calculations. Historical financial panels retain their existing separate annual reader.

All values are reported in thousands of New Taiwan dollars. Table captions, issuer identity, ordered period headers, row uniqueness, exact original hashes and four independent income reconciliations are enforced. Missing amounts are never zero. Repeated comparative observations must agree. New or amended financial reports, stale periods and changed originals hold the reader for review; this release does not promise automatic parsing of future unreviewed quarters.

## Calculations and consumer result

Current trailing amounts use FY2025 plus H1 2026 minus H1 2025; prior trailing amounts use FY2024 plus H1 2025 minus H1 2024. Revenue is TWD4,440,492,429,000 and operating income is TWD2,491,156,024,000 for the year ended June30,2026. The resulting prepared fields are:

| Field | SEC result |
| --- | ---: |
| Gross margin | 64.2301687618% |
| Operating margin | 56.1008956514% |
| Revenue growth | 30.5566836757% |
| Operating margin change | +7.3832605136 percentage points |
| Current ratio | 2.4576351395 |

These replace the current Finnhub values64.46%,56.38%,31.41% and2.4576 through the existing SEC-first combination policy; the difference is not presented as rounding alone. The isolated actual-cache consumer retains Finnhub's same-period39.86% ROE and gains a third scored input, changing unavailable to bullish. EV/EBITDA and net debt/EBITDA remain absent. This is a financial classification, not a full Confirmation Score, ranking, investment recommendation or production activation.

No TWD absolute amount, cash-flow amount, parent earnings or ADR EPS is inserted into a USD core field. The Massive integration now checks reporting currency before computing price/sales, P/E, earnings yield or FCF yield; separately verified USD market cap can remain available. USD behavior is preserved by regression tests. Only TSM's preparation version changes, using the existing serialized financial schedule and its unchanged batch limit.

## Verification

All19 unaffected complete original-source control results and repeats match deployed1752fe1d, including five existing coverage holds. Baseline source hash: fa13ae26d583522f7b641a1f7bb064aaee11ed228da8f0b7d05909e6591325e0.

Final affected Linux Python3.12.3 suite:146 passed. Condensed fixtures retain actual reported table cells and original hashes; tests cover mismatched identity/currency/units, altered amounts, ambiguous/missing columns, changed originals, repeated comparatives, source availability, stale periods, amendments and both USD/TWD market-cap branches.

The full-original Linux replay verifies exact trailing arithmetic, identical repeated results and immutable source inputs, with transport forbidden. Its measured calculation CPU is1.1393seconds on this local environment, not a production capacity claim. The actual preparation pipeline persists exactly five fields in isolated SQLite and repeats without transport. Current captured Finnhub values flow through the actual combination and summary readers. Zero production writes or validation emails. Local receipts and original manifests are under `artifacts/tsm-interim-oct14/`.

Initial source discovery correctly held a filename mismatch and the ambiguous consolidated/parent-only annual links. Saved covers were reused for resolution; no source denial was retried. An initial local consumer harness supplied a string where a database date was required; corrected date conversion passes. Linux Git could not resolve the Windows worktree pointer, so native Git exported the exact1752fe1d baseline for the control replay. These are diagnostic corrections, not source or product changes.

## Release and ordinary-operation verification

PR641 source `b72790a1381c7c954042765485b3d16a0ce37a61` merged as `155cc0b34650f3aaee1f04140f75789c27612539`. Backend workflow38625627459/job116063926756 completed successfully.15 runtime source hashes match on Python3.12.15, including the Finnhub identity guard. The bounded runtime fixture reproduces all five ratios and complete current/prior/instant/source calculations with unchanged inputs and identical repeats. It uses retained parsed original statements, the captured company directory projection and a minimal facts-identity fixture; it is not itself a live acquisition. Receipt: `artifacts/tsm-interim-oct14/runtime-1792007099.json`.

The existing ordinary schedule then prepares TSM at2026-10-14T19:46:03.035099+00:00. All five persisted fields and current/prior/instant/source provenance match the complete-original replay. The captured live Finnhub combination restores three classification inputs and repeats identically in the offline consumer. HCA's ordinary19:31 refresh separately matches all eight fields and its complete current/prior operating reconciliation. Receipt and offline verification: `artifacts/tsm-interim-oct14/ordinary-verification.json`. No manually forced refresh or validation email.

Postrelease public samples: Feed0.460s, Insights0.500s with research present, news0.310s, research0.455s, AAPL2.930s and NVDA2.056s, all200; readiness reports database okay. These samples do not establish sustained workload capacity. Receipt: `artifacts/direct-feeds/free-replacements-2026-10-09/insights-incident-1792007030.json`. API remains shared2CPU/2048MB; database remains performance1CPU/2048MB. FMP remains selected for public core and institutional publication remainsfalse.

Secret scan and Vercel checks pass; unchanged frontend and Pillow audits fail, so no full-CI claim. Job-log retrieval returned502; the dedicated run diagnostics confirmed successful terminal deployment. PR attachment failed at the existing100 attachment limit. The first runtime diagnostic omitted the Finnhub identity guard due to a filename mismatch; corrected fifteen-file verification passes. No source selector, schedule, host size, subscription, customer email, video, landing or cancellation changes.

## Remaining gates

Broader financial completeness, future-quarter maintenance, historical quarterly panels, parent income/cash flow/leverage, institutional publication, transcripts, scores/rankings/all alerts, sustained performance, final Massive activation and zero-FMP/billing verification remain open. No subscription, source selection, schedule, host size, video or landing change.
