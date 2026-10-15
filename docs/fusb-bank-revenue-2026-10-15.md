# FUSB revenue denominator correction

October 15, 2026. Implemented locally; release and ordinary production refresh remain pending.

The retained replacement preparation treated First US Bancshares' deposit service charges as total company revenue. Its trailing denominator was $1.134 million, producing 684.5679% net margin and 770.4586% free-cash-flow margin. Core financials still select FMP; this defect was found in the prepared replacement, not established as a public core metric.

## Original-source reconciliation

The original [June 2026 quarterly statement](https://www.sec.gov/Archives/edgar/data/717806/000119312526337583/fusb-20260630.htm), [2025 annual statement](https://www.sec.gov/Archives/edgar/data/717806/000119312526103635/fusb-20251231.htm) and [June 2025 quarterly statement](https://www.sec.gov/Archives/edgar/data/717806/000095017025104822/fusb-20250630.htm) distinguish deposit service charges from total non-interest income and net interest income before credit-loss provisions. All amounts below are USD thousands.

| Period | Net interest income | Non-interest income | Deposit charges, excluded as total revenue |
| --- | ---: | ---: | ---: |
| FY2025 | 37,458 | 3,579 | 1,140 |
| FY2024 | 36,149 | 3,583 | 1,232 |
| H1 2026 | 18,715 | 2,158 | 560 |
| H1 2025 | 18,373 | 1,724 | 566 |
| H1 2024 | 18,216 | 1,700 | 597 |

The existing banking basis, net interest income plus non-interest income, yields current trailing revenue of **$41,813,000** and prior trailing revenue of **$39,913,000**. With unchanged net income and cash flow, net margin becomes **18.5660%**, free-cash-flow margin **20.8954%**, and revenue growth **4.7604%**. Credit-loss provisions are not deducted from revenue. This does not create a missing bank operating-income proxy or complete its financial classification.

Five captured public files, including companyfacts and issuer metadata, have verified SHA256 hashes in `artifacts/fusb-revenue-oct15/manifest.json`. The replay independently reads the three original income-statement tables before checking projector output.

## Selection and cache policy

The reviewed policy requires FUSB's CIK, ticker and supported bank classification. It applies to periods beginning January 2024 onward, the source-reviewed comparison window. Earlier fee-only revenue is held rather than carried into historical totals. A standalone 2018 `Revenues` observation remains outside this reviewed series and is not spliced into it.

Every selected period requires the exact standard net-interest and non-interest components plus the reviewed deposit-charge concept with matching dates and filing. Deposit charges must be nonnegative and no larger than non-interest income. Missing components, conflicting observations, changed tags/currency or unmatched filings leave the period unavailable, including when the fee-only period is newer than the complete bank series. Explicit total revenue within the reviewed window takes precedence, including conflicts. Other banks retain their existing policies.

Only FUSB receives new ratio and statement preparation versions. No global cache flush, source selector, schedule, request budget, hosting size or market-data dependency changes. Future aligned periods use the same checked source basis; simulation is not proof of an ordinary future filing.

## Validation and limits

The first policy tests passed but actual-source replay exposed the old 2018 revenue observation blocking the correction. The reviewed-period guard now handles it explicitly and has regression coverage. The first comprehensive replay also used exact equality between two algebraically equivalent floating-point growth calculations; the independent expected-value assertion now uses relative tolerance 1e-12. Neither failed attempt is counted as a successful replay.

The final public-original replay runs on Linux Python 3.12.3, exactly preserves complete results or explicit errors for 23 control issuers, prepares the five FUSB ratios and historical statement panel in isolated SQLite, and repeats both caches without additional transport. No canonical production records are read or written. Receipt: `artifacts/fusb-revenue-replay-oct15.json`.

The actual corrected values pass through a synthetic watchlist with the same `free_direct` provider before and after the calculation change. The existing revenue-basis safeguard initializes the corrected baseline without an alert, repeated evaluation emits none, a later synthetic business threshold crossing emits exactly one trigger and one monitoring record, repeat emits none, and basis rollback reinitializes without an alert. No emails or network requests occur. This is not production delivery, ranking rebaseline or full scheduled-consumer verification.

Nucor remains held: its operating subtotal requires issuer-extension and nonoperating-item interpretation that is not yet sufficiently supported. No pretax-income proxy was introduced.

Core financials, prices and transcripts remain FMP; institutional publication remains disabled. Broader financial coverage, transcript publication, current institutional consumers, all scheduled alerts/rankings, sustained capacity, final Massive activation and zero-FMP/billing gates remain open. Pending production payload-export approvals and valuation-model choice are unchanged.

Final affected Linux Python3.12.3 suite: **160 passed** in38.16seconds, covering bank policies, fundamentals/preparation, statements/cache/comparison/continuity and financial consumer/alert basis checks. No full-suite claim. `git diff --check` passes.
