# Confirmation coverage audit — September 26, 2026

The owner reported that the Top Stocks leaderboard showed 25 stocks at 100 and requested a Top 10 limit. The leaderboard API now caps Premium, Pro and admin stock rows, including every prepared filter, at 10. Guest #3–#5 and Free #1–#5 access remain. The separately requested Pro email limit remains 25; this change concerns the leaderboard.

## Why the current score saturates

`confirmation_evidence.net_confirmation` divides supporting minus opposing weight by supporting plus opposing weight. Missing, inactive, mixed and neutral sources are excluded from that denominator. With two or more eligible sources and no opposition, any total supporting weight produces 100. The one-source cap does not solve this two-source saturation. The current score measures agreement among active directional sources, not full-source confirmation.

## Concrete alternative, not yet the production formula

An audit of the 500 candidates in the canonical September 26 20:49 UTC snapshot used the existing source priorities and strength/quality/freshness adjustments, but normalized signed evidence against the complete 10-source maximum of 113 points instead of the active evidence denominator.

| Stock | Existing score | Aligned sources | Full-source prototype |
|---|---:|---:|---:|
| TSM | 100 | 6 of 10 | 46 |
| BRK-B | 100 | 6 of 10; price/volume mixed | 42 |
| AZO | 100 | 2 of 10; fundamentals and price/volume mixed | 11 |

These are reproductions of saved inputs, not performance forecasts. All 500 candidates fall below 60 under that strict prototype. Keeping the existing minimum-60 Top Stocks filter would therefore empty the leaderboard. A global scoring change must decide how absolute score bands and relative discovery eligibility should work together; increasing scores merely to fill the leaderboard would repeat the original problem.

## Recommendations

- Reserve 100 for complete, sufficiently strong/fresh alignment. Quiet and mixed sources earn no confirmation credit; opposing evidence subtracts. Absence of activity is not bearish activity.
- Show aligned-source coverage beside the score. Two aligned sources must be visibly different from six, even when neither setup has bearish evidence.
- Distinguish quiet sources from missing, stale or unavailable data. Paid access must never affect the canonical score. Do not automatically exclude a source just because it has no activity; any genuinely inapplicable source policy needs an explicit, stable definition.
- Separate relative Top 10 position from an absolute claim of strong confirmation. Do not label a moderate setup strong merely because it is the best available today.
- Check correlated inputs: a signal derived from an insider trade should not be presented as wholly independent corroboration of that same trade. Preserve separate source labels while documenting dependence.
- Version the next formula and preserve recorded scores, historical outcomes and their original methodology. Do not interpret a formula-driven score drop as fresh bearish activity or send monitoring alerts for it.
- Evaluate the new score distribution and forward outcomes before calling any band a calibrated probability or claim of predictive accuracy.

The Top 10 change passes 35 access/digest checks and 26 homepage checks. The score formula has not been changed by the leaderboard-cap commit. Audit fixtures and scripts are stored locally under `artifacts/top-ideas-validation/`.
