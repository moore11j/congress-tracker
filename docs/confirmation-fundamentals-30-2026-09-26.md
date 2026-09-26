# Fundamentals allocation — September 26, 2026

The owner requested 30 points for fundamentals, funded equally by all other source allocations. This replaces the allocations in `confirmation-weighted-coverage-2026-09-26.md` while preserving its fixed 100-point coverage model.

| Source | Maximum points (display rounding) |
|---|---:|
| Fundamentals | 30.00 |
| Institutions | 18.89 |
| Price / volume | 13.89 |
| Congress | 8.89 |
| Insider buying | 8.89 |
| Analysts | 6.89 |
| Signals | 3.89 |
| Options flow | 3.89 |
| Macro positioning | 3.89 |
| Government contracts | 0.89 |

Each of the nine other sources loses exactly 10/9 points. The calculation retains full precision so total capacity is exactly 100; the rounded display allocations sum to 100.01. Evidence still scales by the existing strength/quality/freshness blend. Routine insider-selling opposition remains capped at one point. Full confirmation requires every allocation to be fully confirmed; fractional rounding cannot manufacture a score of 100. No ownership/freshness split is introduced in this release.

Score version `confirmation_score_v7_fundamentals_30`, classification v8, canonical 30-day context v7, ticker cache v14, outcome methodology `confirmation-v7-fundamentals-30`, divergence v6. Existing methodology-change guards rebaseline alerts and exclude incompatible ranking acceleration baselines. Recorded historical scores and outcomes remain unchanged; the September 26 chart annotation describes the final allocation.

After deployment, refresh disposable current views with `python -m app.jobs.refresh_current_confirmation`, register the current methodology against the release commit, and verify API gating, canonical paid-tier consistency and email previews without sending email.

Validation covers equal allocation transfers, exact total capacity, full versus partial confirmation, opposition, redaction, ticker/screener consistency, monitoring and custom alert rebaselining, Top 10 access, email limits, and historical annotations. The saved candidate snapshot places TSM at 52, BRK-B at 41 and AZO at 7; live data may differ.

Final focused checks: 170 backend scoring/access/digest/history/monitoring tests, two ticker/screener integration checks and 19 frontend checks passed.
