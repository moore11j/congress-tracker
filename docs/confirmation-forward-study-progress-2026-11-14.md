# Frozen bullish forward study: November 14 progress

Checked November 15, 2026 at 00:52 UTC (November 14 in America/Los_Angeles). This is a maturity-phase progress report. Final assessment remains November 17.

## Result

Added 101 thirty-day measurements from the existing Massive account. All 28 requests across 14 date batches returned HTTP 200 at the unchanged minimum 30-second spacing. The provider supplied 208 of 225 requested symbol/date observations. No FMP requests, public writes, deployment, subscription changes, new predictions, or model tuning occurred.

Current coverage: 439 of 447 due 30D outcomes measured (98.21%), eight due price gaps, and 16 outcomes maturing November 16. Seven-day coverage remains 458 of 463, with five gaps. All 796 earlier measurements, 463 decisions, 21 captures, active plan, baseline and enrollment manifest passed integrity checks. There are now 897 immutable measurements. Operational code matches the previously tested 28-check version; the unchanged frozen evaluator completed successfully.

Overall 30D accuracy is 237/439 (53.99%) for the baseline and 236/437 (54.00%) for the challenger. Bullish accuracy is 70/144 (48.61%) versus 69/142 (48.59%). This provides no meaningful evidence of improved bullish accuracy or a 75% success rate. Bearish results are identical between policies: 167/295 (56.61%).

The challenger has now excluded two measured bullish decisions: one loss and one win under the frozen definition. Its known-input retained subgroup is 3/3, spanning only two tickers; its descriptive 95% interval is 43.85% to 100%, and this is not validation. One known-input decision remains immature.

## Measured comparisons

Accuracy uses the frozen positive directional return OR positive two-decimal SPY excess rule. Raw success uses only positive directional return. Returns are direction-signed raw-price returns, not a portfolio simulation. Retained coverage is relative to measured baseline decisions in the same subgroup.

| Sample | Horizon | Policy | Correct / measured | Accuracy | Raw success | Retained coverage | Mean return | Mean SPY excess | Descriptive 95% interval |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| all | 7D | baseline | 260/458 | 56.77% | 53.93% | 100.00% | +0.81% | +0.98 pp | 52.19% to 61.23% |
| all | 7D | challenger | 259/456 | 56.80% | 53.95% | 99.56% | +0.88% | +1.06 pp | 52.21% to 61.27% |
| all | 30D | baseline | 237/439 | 53.99% | 52.39% | 100.00% | -2.09% | -2.44 pp | 49.31% to 58.59% |
| all | 30D | challenger | 236/437 | 54.00% | 52.40% | 99.54% | -1.98% | -2.35 pp | 49.32% to 58.62% |
| bullish | 7D | baseline | 75/151 | 49.67% | 49.67% | 100.00% | -0.52% | -1.03 pp | 41.80% to 57.55% |
| bullish | 7D | challenger | 74/149 | 49.66% | 49.66% | 98.68% | -0.31% | -0.84 pp | 41.75% to 57.60% |
| bullish | 30D | baseline | 70/144 | 48.61% | 45.83% | 100.00% | -2.01% | -0.96 pp | 40.59% to 56.70% |
| bullish | 30D | challenger | 69/142 | 48.59% | 45.77% | 98.61% | -1.66% | -0.65 pp | 40.52% to 56.74% |
| bearish | 7D | baseline | 185/307 | 60.26% | 56.03% | 100.00% | +1.46% | +1.97 pp | 54.69% to 65.58% |
| bearish | 7D | challenger | 185/307 | 60.26% | 56.03% | 100.00% | +1.46% | +1.97 pp | 54.69% to 65.58% |
| bearish | 30D | baseline | 167/295 | 56.61% | 55.59% | 100.00% | -2.14% | -3.16 pp | 50.91% to 62.14% |
| bearish | 30D | challenger | 167/295 | 56.61% | 55.59% | 100.00% | -2.14% | -3.16 pp | 50.91% to 62.14% |
| first_per_ticker | 7D | baseline | 252/443 | 56.88% | 53.95% | 100.00% | +0.79% | +0.98 pp | 52.23% to 61.42% |
| first_per_ticker | 7D | challenger | 251/441 | 56.92% | 53.97% | 99.55% | +0.87% | +1.05 pp | 52.25% to 61.46% |
| first_per_ticker | 30D | baseline | 232/432 | 53.70% | 52.08% | 100.00% | -2.10% | -2.45 pp | 48.99% to 58.35% |
| first_per_ticker | 30D | challenger | 231/430 | 53.72% | 52.09% | 99.54% | -1.99% | -2.36 pp | 49.00% to 58.38% |
| known_bullish | 7D | baseline | 4/6 | 66.67% | 66.67% | 100.00% | -3.86% | -3.55 pp | 30.00% to 90.32% |
| known_bullish | 7D | challenger | 3/4 | 75.00% | 75.00% | 66.67% | +2.16% | +2.36 pp | 30.06% to 95.44% |
| known_bullish | 30D | baseline | 4/5 | 80.00% | 80.00% | 100.00% | -2.30% | +0.11 pp | 37.55% to 96.38% |
| known_bullish | 30D | challenger | 3/3 | 100.00% | 100.00% | 60.00% | +14.01% | +15.41 pp | 43.85% to 100.00% |

## Missing prices and limits

The eight unmeasured 30D events are listed below. Every missing Massive observation was requested in this run; successful responses omitted those observations. No cause such as delisting or merger is inferred. Same-provider stock and SPY entry and target prices are required, so a price from a different source cannot fill just one leg.

| Ticker | Entry | Target | Missing Massive observations |
|---|---|---|---|
| LBRDA | 2026-09-28 | 2026-10-28 | LBRDA 2026-09-28, LBRDA 2026-10-28 |
| GREE | 2026-09-28 | 2026-10-28 | GREE 2026-09-28, GREE 2026-10-28 |
| DOMO | 2026-09-28 | 2026-10-28 | DOMO 2026-09-28, DOMO 2026-10-28 |
| SGRP | 2026-09-28 | 2026-10-28 | SGRP 2026-09-28 |
| GBTG | 2026-10-05 | 2026-11-04 | GBTG 2026-10-05, GBTG 2026-11-04 |
| DBRG | 2026-10-06 | 2026-11-05 | DBRG 2026-10-06, DBRG 2026-11-05 |
| FMBM | 2026-10-14 | 2026-11-13 | FMBM 2026-10-14 |
| LXP | 2026-10-14 | 2026-11-13 | LXP 2026-11-13 |

The five 7D gaps remain LBRDA, GREE, SGRP, GBTG and DBRG. Missing measurements are neither wins nor losses. Sixteen 30D targets remain November 16; those are immature, not missing-price failures.

The sample is concentrated: 199 of 439 measured 30D events entered September 28; 432 distinct tickers are represented. Date/ticker dependence makes the Wilson intervals descriptive, not independent-trial proof. Of 153 enrolled bullish decisions, 147 used unknown-input fallback, four qualified and two were filtered. Limited usable insider evidence is a major constraint.

Missed enrollment capture dates: 2026-09-18, 2026-09-21, 2026-09-22, 2026-09-23, 2026-09-24, 2026-09-30, 2026-10-01, 2026-10-12. These were not reconstructed or backdated. Later price collection changes measurement coverage only.

Frozen raw entry/target conventions are preserved, including potential corporate-action distortions. Mean returns are sensitive to extreme observations; no magnitude exclusions or retroactive price-basis changes were applied. Costs, slippage, borrow availability and liquidity are not modeled. The September research holdout was already consumed. A direct supervised 30-day return model remains proposed, not trained or validated.

## Evidence and next step

- Complete integrity and comparison receipt: `frontend/test-results/confirmation-research/forward-bullish-2026-09-15/operations/post-run-20261115T005253Z.json`.
- Current maturity counts: `frontend/test-results/confirmation-research/forward-bullish-2026-09-15/price-coverage/evaluations/20261115T005128521787Z.json`.
- Frozen measured summary: `frontend/test-results/confirmation-research/forward-bullish-2026-09-15/evaluations/20261115T005130Z.json`.
- Use the current coverage receipt for due/missing/immature counts; the frozen summary uses the last October 15 capture for unmeasured statuses.
- Continue the existing schedule through final November 16 maturity, then produce the November 17 local assessment, including any remaining coverage gaps.
- The October 16 reminder was already delivered. No repeated reminder, email or remote draft. No public scoring promotion is supported by this interim evidence.
