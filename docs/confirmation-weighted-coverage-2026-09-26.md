# Weighted full-source Confirmation Score — September 26, 2026

The owner requested full confirmation at 100, with greater weight on fundamentals, institutions and price; equal Congress/insider buying priority; and lower signal/contract weights. This supersedes the active-evidence ratio described in `confirmation-net-evidence-2026-09-16.md` and the initial prototype in `confirmation-coverage-audit-2026-09-26.md`.

| Source | Full-source points |
|---|---:|
| Fundamentals | 20 |
| Institutional activity | 20 |
| Price / volume | 15 |
| Congress | 10 |
| Insider buying | 10 |
| Analysts | 8 |
| Signals | 5 |
| Options flow | 5 |
| Macro positioning | 5 |
| Government contracts | 2 |
| Total | 100 |

Each eligible source earns its maximum multiplied by the existing strength/quality/freshness blend (50%/35%/15%). Opposing evidence subtracts directly. Routine insider selling retains the existing maximum of one point; selling alone is not treated as equivalent to conviction buying. Mixed, neutral, quiet, missing, stale and immaterial sources earn zero. The denominator is always the full 100-point capacity. Partial coverage and near-perfect evidence cannot round up to 100. Government-contract support alone still cannot establish a bullish direction. These are research weights, not empirically calibrated return probabilities.

The unchanged absolute bands are inactive 0–19, weak 20–39, moderate 40–59, strong 60–79 and exceptional 80–100. Top Stocks is now explicitly a relative discovery ranking: it requires bullish direction, at least 20 points and two eligible aligned sources, then returns up to 10 stocks. It does not fill with subthreshold stocks or describe sub-60 setups as strong. Guest and Free identity/evidence restrictions remain unchanged. Pro retains the previously requested separate email limit.

## Saved-input comparison

Rebuilding the 500-candidate September 26 20:49 UTC snapshot gives TSM **49** with six aligned sources, BRK-B **38** with six aligned sources and mixed price/volume, and AZO **8** with two aligned sources. All three were previously 100. The first ten qualifying scores are 49, 38, 37, 34, 32, 31, 31, 30, 29 and 29. These numbers describe saved inputs, not frozen production scores.

Leaderboard and paid emails show aligned-source counts out of ten. Paid tier redaction preserves the canonical score and rank while removing locked source details and calculation totals; a subscription change must not change the stock's score. Ticker explanatory text distinguishes visibly aligned sources from the full weighted model.

## Versioning and release

New score version `confirmation_score_v6_weighted_coverage`, classification `confirmation_direction_v7_weighted_coverage`, 30-day context v6, ticker cache v13, outcome methodology `confirmation-v6-weighted-coverage`, divergence v5. Recorded historical score snapshots/outcomes are not recalculated. Disposable current ticker caches, market tiles and Top Stocks snapshots must be refreshed after backend deployment using `python -m app.jobs.refresh_current_confirmation`.

Standard confirmation monitoring and custom confirmation rules establish a fresh baseline when the methodology changes. They must not generate weakening, crossing or direction-change alerts just because the formula changed. Subsequent genuine transitions remain eligible. Historical series retain their recorded observations and should be interpreted with their methodology version.

## Remaining product suggestions

Distinguish quiet activity from unavailable/stale coverage; preserve source-level freshness and dates. Review dependence between generated signals and the disclosures that created them before describing them as independent confirmations. Do not loosen score bands to make the list look more exciting; evaluate forward outcomes before making predictive claims.

## Validation

The combined scoring, monitoring, custom-alert, outcome, Top Ideas access/digest and ticker-summary suite passed 225 tests. Eight ticker-summary tests also fail on the unchanged pre-scoring commit `2b79926b` (verified using a separate archived checkout): existing public-activity score assumptions and cache/coalescing mocks. This release introduces no additional failures in that suite. Nineteen leaderboard/ticker/history frontend tests and 26 homepage tests passed; the production frontend build passed. Regression coverage includes all-source 100, partial coverage, weighted source priority, opposing evidence, paid redaction, true guest ranks, Top 10 response limits, alert rebaselining and historical chart annotations.
