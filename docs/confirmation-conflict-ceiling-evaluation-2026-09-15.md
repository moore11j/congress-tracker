# Confirmation consistency experiment

## Decision

Do not promote the proposed weighted-evidence score ceiling as an accuracy improvement. The fixed candidate reduced the primary 30-day bullish accuracy in the available historical cohort. The candidate is retained only in offline research scripts; the application score formula, directional classifier, and outcome methodology remain unchanged.

The button alignment and shared contract-date repairs are separate implementation fixes. Recent-contract queries now exclude future awards; an absent start date is no longer replaced with a performance end date. Defensive score adapters also reject future contract dates, and ticker/Top Stocks/market-pressure current caches are versioned to exclude prior inputs. These changes are local and have not been deployed. Deployment must refresh Top Stocks and market-pressure snapshots; old Top Stocks snapshots are intentionally withheld until refreshed. Historical outcome entries are not rewritten.

## Fixed candidate

`revised = min(original opening score, floor(100 * aligned material evidence weight / total directional evidence weight))`

Eligibility and weights follow the existing divergence-v2 interpretation: present bullish/bearish sources, no known age above 90 days, magnitude at least 2, native contribution when positive, otherwise `(0.5 * strength + 0.35 * quality) / 10`. Unknown age retains the existing interpretation. Future (negative-age) evidence is excluded. Original direction, entries, and measured outcome flags are preserved. This is a consistency ceiling, not a fitted return model or probability estimate.

Primary comparison: bullish calls at the existing strong-or-better threshold of 60, measured at 30 days. Thresholds 40 and 80, the existing research security partition, and one-event-per-security results are diagnostics; no threshold was selected or adjusted after inspecting outcomes.

| Horizon / cohort at score ≥60 | Original correct / signals | Original accuracy | Revised correct / retained | Revised accuracy | Retained |
|---|---:|---:|---:|---:|---:|
| 30D bullish — primary | 258 / 669 | 38.57% | 209 / 561 | 37.25% | 83.86% |
| 30D all directions | 410 / 920 | 44.57% | 297 / 708 | 41.95% | 76.96% |
| 7D bullish — secondary | 870 / 1,841 | 47.26% | 692 / 1,423 | 48.63% | 77.29% |

The 108 removed 30D bullish calls were 45.37% correct, so the rule disproportionately removed winners from this cohort. The prior research test-security partition also declined for 30D bullish calls: 43.09% on 123 calls to 41.00% on 100. The first-event-per-security check declined from 38.20% on 610 to 37.19% on 519. Unfiltered accuracy is unchanged because no direction is changed.

## Limits

All 1,176 mature 30D events are from the earlier methodology and August 5–12 entry dates. The 7D sample has 3,061 events spanning August 5–September 2, including 726 from the newer methodology. This narrow, previously studied sample does not establish future performance or the effect on today's exact production population. It is sufficient to reject a claim that this experiment demonstrated a 30D improvement. Filtering opportunities is not the same as correcting their predictions.

The evaluation preserves the product's stored accuracy definition (correct direction or SPY-relative outperformance). Plain price-direction accuracy for the primary cohort also declined, from 32.74% to 31.55%. Aggregated frozen inputs do not support a replay of the contract-date correction; no accuracy claim is made for that data repair.

## Reproduction

Run from the repository root:

```powershell
python backend/scripts/research/evaluate_conflict_ceiling.py frontend/test-results/confirmation-research/cohort.json backend/artifacts/conflict-ceiling-evaluation.json
```

Input SHA-256: `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`. The input is checked for byte-for-byte preservation. Results are local research artifacts; no database access, provider access, new predictions, or changes to the existing prospective study occur.

Validation: 74 focused backend checks, 2 offline experiment checks, 12 frontend checks, TypeScript checking, and the earlier browser alignment check passed. A broader initial government-ingest run also hit four failures outside the final focused suite (feed description/entitlement expectations, inactive-state expectation, and a provider call blocked by local networking). Those failures were not resolved or established against a clean baseline; the broader suite is not claimed as passing.
