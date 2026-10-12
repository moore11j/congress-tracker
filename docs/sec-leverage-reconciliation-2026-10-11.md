# SEC leverage reconciliation, October 11, 2026

## Scope and result

Continue the FMP replacement by reconciling the retained net-debt/EBITDA field before implementation and score/alert activation. This is completed source investigation, not a deployed replacement. Public core fundamentals remain FMP, institutional publication remains disabled, and Massive remains the final subscription step. No landing-page edits.

A guarded production read captured current FMP key metrics, one quarterly balance sheet and four quarterly income statements for AAPL, MSFT, NVDA and LIN: 12 logical requests through the existing provider client and shared collector guard. All four published net-debt/EBITDA ratios reproduce from the captured net debt divided by the sum of the four reported EBITDA values. Provider usage accounting may be written by that client; no canonical financial data, scores, events or customer deliveries were changed.

The offline audit verifies the saved SEC manifest and every source/company hash for its full 20-company cohort, then uses the deployed period-alignment rules with network access prohibited. Four companies have all four requested, aligned standard component series: BRK-B, JNJ, LIN and NVDA. This count is for the saved October 9 companyfacts cohort only. Later inline and XOM continuity supplements were not replayed, and unsupported source coverage is retained explicitly. Component presence is a diagnostic candidate, not proof that every issuer's complete EBITDA can be reconstructed by an identical formula.

## Reconciliations

All monetary amounts below are USD billions unless noted.

| Issuer | FMP net debt | FMP trailing EBITDA | FMP ratio | Reconciliation |
|---|---:|---:|---:|---|
| AAPL | 44.763 | 168.486 | 0.2656778604750543 | Current interest expense is absent from standard companyfacts. It cannot be inferred as zero. FMP total debt differs by $37 million from the sum of the saved current/noncurrent long-term debt and commercial paper. |
| MSFT | 107.8733 | 212.433 | 0.5077991649131727 | FMP lease total is 88.5143 versus 88.519 in the original filing. Its quarterly D&A sum is 43.448. The original combined cash-flow line is 38.534 and explicitly includes other items, while separately reported depreciation plus intangible amortization is 39.000. No one of these amounts is silently substituted for another. |
| NVDA | 16.417 | 233.894 | 0.07018991508974151 | Exact SEC reconstruction: debt current 1.000 + noncurrent long-term debt 32.366 + operating leases 5.494 - cash 22.443. Denominator is aligned net income 192.880 + tax 36.863 + nonoperating interest 0.464 + D&A 3.687. |
| LIN | 23.115 | 13.432 | 1.720890410958904 | Reported combined debt 28.013 and cash 4.898 match FMP. The aligned net-income-based SEC denominator is 13.406, a $26 million difference from FMP. This remains a definition/component reconciliation, not rounding-equivalent parity. |

NVIDIA's debt reconstruction demonstrates that the existing FMP ratio includes operating leases. Matching it does not prove that an unreported finance-lease concept is zero or establish a rule for other issuers. The saved FMP annual period on the canonical cache is also not the period of this independently supplied TTM ratio; field-level provenance must retain the July 26 balance/TTM end for NVIDIA rather than relabel it with January 25.

SEC guidance describes earnings in EBITDA as GAAP net income and requires performance-measure reconciliation to net income. This supports retaining a stated calculation basis rather than silently equating operating income plus D&A with the same measure. See [SEC Non-GAAP Financial Measures, questions 103.01 and 103.02](https://www.sec.gov/rules-regulations/staff-guidance/corporation-finance-interpretations/non-gaap-financial-measures). The existing free Finnhub annual growth remains explicitly provider-defined, not asserted FMP-equivalent.

## Original source verification

Two original filings were fetched with the identified, allowlisted SEC client and saved locally, then independently hash-checked. Fourteen relevant entity-wide inline fact occurrences retain their contexts, periods, units, scale and displayed values in `leverage-originals-oct11/reviewed-facts.json`.

- [Microsoft June 30 10-K](https://www.sec.gov/Archives/edgar/data/789019/000119312526323660/msft-20260630.htm), 8,585,612 bytes, SHA256 `cec8684979a7b9f8af4003dcab01ac6b85cbe760aaa8dfbc2d266fdcbf8ac67f`.
- [NVIDIA July 26 10-Q](https://www.sec.gov/Archives/edgar/data/1045810/000104581026000075/nvda-20260726.htm), 1,517,158 bytes, SHA256 `db4b16c5444165cdbf917e846dbdd6b0a1e63cea0c17edef103914a21b08ce80`.

The web reader could not load the large Microsoft original; the normal SEC source client succeeded. No credentials or private records were exported.

## Checks and next implementation requirements

Local Python 3.14.2 audit exits successfully, all four FMP ratios reproduce within 1e-12 relative tolerance, NVIDIA's SEC denominator and debt reconstruction match exactly, and source hashes/periods are verified. Existing SEC projections repeat with identical full-payload hashes; no application regression-suite pass or deployed metric is claimed. All local diagnostic handles are terminal; no database process was started.

Next implement and test source-specific debt/lease reconciliation and complete EBITDA component evidence, including issuer extensions where justified by the original filing. Do not invert rounded Finnhub debt/equity ratios using unrelated equity, treat missing concepts as zero, combine quarterly EBITDA with a TTM denominator, double-count lease debt, or silently change a score input's definition. Reconcile income attributable to parent versus consolidated income, separate finance/operating lease policies, reject nonpositive denominator and incompatible periods, and preserve per-field observation/filing dates. Then measure whole-cohort numerical/coverage differences and replay scores, rankings and all alert consumers before activation. Wider Senate, institutions, transcripts, scheduled consumers, final Massive and zero-FMP/billing gates remain open.

## Local receipts

Under `artifacts/direct-feeds/free-replacements-2026-10-09`:

- `read_raw_leverage_comparison_oct11.py` and `raw-leverage-comparison-1791762117.json` capture the bounded FMP baseline.
- `audit_sec_leverage_oct11.py` and `sec-leverage-audit-oct11.json` retain the full offline component audit and comparisons.
- `leverage-originals-oct11/sources.json`, original HTML files and `reviewed-facts.json` retain the original-source verification.

The receipts are local ignored evidence; the numerical conclusions and original source hashes above are the durable tracked report. No provider selection, deployment, recurring schedule, capacity, subscription, email, video or cancellation change.

Audit receipt SHA256: `efe1b71bb588dcd264bfa273db964500059a014765bf88496a9e11ba633db11c`. FMP capture SHA256: `3a319604dcd63dd0ee543e9d91c6e0b1bc6323f96b128cb8b61d6f31ac3a0f92`.
