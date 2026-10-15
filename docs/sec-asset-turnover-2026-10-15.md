# SEC revenue-to-assets coverage

October 15, 2026. Implemented and tested locally on main baseline `5a4a1506`; not deployed.

The selected SEC historical-financial panel can now derive TTM revenue divided by total assets at the same period end. Both inputs retain source evidence, and the closing revenue and balance-sheet facts must match accession, filing date, period and currency. Conflicting, missing, nonfinite or nonpositive assets remain unavailable; zero revenue remains valid. No average-assets substitution, assets/liabilities fallback, forecast or new provider request is introduced. This extends the historical panel only, not canonical fundamentals, scoring or alert methodology.

The denominator follows FMP's documented revenue/total-assets formula: [official formulas](https://site.financialmodelingprep.com/developer/docs/formula). This is an explicit definition, not a claim of exact numeric agreement with FMP. The payload records `ttm_revenue_to_ending_total_assets` and full evidence. The frontend labels known-basis values Revenue / Assets; old unversioned cached responses retain their old label. Normal scheduled cache rotation applies without a global invalidation or request spike.

The existing FMP normalizer incorrectly substituted assets/liabilities when reported asset turnover was missing. That fallback is removed. Reported zero is preserved, and a known reported turnover receives explicit `revenue_to_total_assets` basis. Other ratio formulas remain unchanged.

## Checks

- Linux Python 3.12.3: 33 focused calculation, source-conflict, zero, identity, cache, comparison and consumer cases pass. The new cache case persists one row, repeats public reads with identical results, and makes only the original two synthetic source calls with no extra fetch or queue request.
- Broader financial compatibility: 154 pass, one forward-PE expectation fails. The identical `None` versus `25.0` assertion reproduces against the unchanged main ticker-financials module. No full-suite pass is claimed.
- Six frontend currency/default-panel checks and TypeScript `--noEmit --incremental false` pass.
- Twenty SHA-verified saved public SEC source pairs, observed October 9, replay with identical complete pre-existing fields and identical repeats. Fourteen now have the ratio. BABA, CBRS, NEE, NVO, TSM, XOM remain outside this generic current-period projection. This is a bounded saved-source rehearsal, not live or universe coverage.
- No production records, provider traffic, writes or emails were used by these tests. Local receipts: `artifacts/sec-asset-turnover-replay-oct15.json`, `sec-asset-turnover-final-oct15.log`, `sec-asset-turnover-broad-oct15.log`, `sec-asset-turnover-baseline-oct15.log`, `sec-asset-turnover-tsc-oct15.log`.

## Release and remaining work

Exact release, runtime and ordinary preparation remain unverified for this change. Core/prices/transcripts remain FMP; institutional publication remains off. Full financial coverage and definitions, valuation/sector/transcript replacement, current institutional writer/consumer proof, all alerts/rankings, sustained capacity, final Massive and zero-FMP/billing verification remain gates. The separate valuation-model choice and CAT/institutional payload approvals remain pending; no rejected production read/export was retried.

Screener PR658 is separately deployed as `5a4a1506` through successful workflow38671297186. Five runtime hashes and four synthetic checks pass on deployed Python3.12.15, with no production database/cache/scheduler reads. See the screener report. No new provider selection, schedule, host, subscription, video, landing-page or customer-email change.
