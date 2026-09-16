# Expanded free options activity study — September 14, 2026

Local research using the existing Massive account. No subscription upgrade, production write, live scoring change, or modification to historical public outcomes.

**Assessment: do not add this options model to live confirmation scoring.** The broader study did not find a >75% model. The prespecified ranking policy performed worse after adding options, while the secondary probability gate achieved only 50.5% 30D accuracy. Options slightly improved probability-error metrics relative to the price model, but did not establish useful 30D selection skill. This result concerns the sampled free aggregate inputs and tested models; it does not settle whether richer options information could help.

This assessment and the price-coverage explanation below were written after the frozen analysis. The original predictions, labels, model selection, and results have not been revised.

## Scope and coverage

The frozen sample contains 576 planned weekly observations across 12 surviving liquid stocks and six anchor windows. Collection used 216 cached API requests, 144 distinct contracts, and 12,420 daily bars. Each anchor selects one matched call/put pair, held fixed within that window; this is sampled unsigned options activity, not full-chain or buyer-initiated flow.

| Feature availability | Planned observations |
|---|---:|
| insufficient_contract_history | 18 |
| ok | 486 |
| outside_30_90_dte | 72 |

| Split | Measured 30D rows with features |
|---|---:|
| train | 238 |
| validation | 84 |
| test | 102 |

All comparisons use identical feature-covered observations in both model families. Availability exclusions are determined from pre-entry inputs, not future returns. Missing features are not fabricated.

Outcome availability across all planned observations: `{"30": {"immature": 24, "measured": 502, "missing_consistent_prices": 50}, "7": {"measured": 552, "missing_consistent_prices": 24}}`.

The 50 unmeasured 30D cases are all in the July anchor window. A post-analysis audit found 48 asset measurements whose available entry and target prices came from different providers, plus seven asset measurements missing an endpoint; some observations had both a stock and benchmark issue. The frozen protocol requires a consistent provider within each asset, so these observations remain unmeasured. No price sources were combined or rules relaxed after seeing the result. This leaves 84 covered 30D test observations from May and only 18 from July, materially limiting the second-period check. These counts concern the local research panel. Details are in `price-coverage-audit.json`.

## Primary result

On later dates, the frozen top-half policy achieved **34.6% 30D accuracy with options** (18/52), versus **46.0% without options** (23/50): **-11.4 percentage points**. This compares a historical price/market model with the same inputs plus options. It is not the live confirmation model’s accuracy.

The unfiltered bullish baseline is essential context: strong market periods can produce high headline accuracy without predictive options information.

| Model / policy | Horizon | Correct / retained | Ledger-style accuracy | Retained coverage | Raw-positive accuracy | Mean directional return | Mean excess vs SPY |
|---|---:|---:|---:|---:|---:|---:|---:|
| All covered observations, bullish | 30D | 51/102 | 50.0% | 100.0% | 48.0% | -0.45% | -0.86% |
| All covered observations, bullish | 7D | 80/141 | 56.7% | 100.0% | 51.1% | -0.17% | -0.20% |
| Price + market; top half per date | 30D | 23/50 | 46.0% | 49.0% | 44.0% | -1.09% | -1.47% |
| Price + market; top half per date | 7D | 40/72 | 55.6% | 51.1% | 54.2% | -0.25% | -0.26% |
| Price + market; probability ≥ 0.50 | 30D | 45/93 | 48.4% | 91.2% | 46.2% | -0.72% | -0.99% |
| Price + market; probability ≥ 0.50 | 7D | 75/127 | 59.1% | 90.1% | 52.8% | 0.03% | -0.06% |
| Price + market + options; top half per date | 30D | 18/52 | 34.6% | 51.0% | 32.7% | -2.82% | -3.29% |
| Price + market + options; top half per date | 7D | 40/72 | 55.6% | 51.1% | 52.8% | -0.06% | -0.07% |
| Price + market + options; probability ≥ 0.50 | 30D | 50/99 | 50.5% | 97.1% | 48.5% | -0.45% | -0.90% |
| Price + market + options; probability ≥ 0.50 | 7D | 74/131 | 56.5% | 92.9% | 51.1% | -0.21% | -0.28% |

The primary policy ranks every feature-eligible observation before excluding unmatured or unpriced outcomes from measurement. With an odd number of eligible stocks on a date, it keeps the rounded-up half. The 0.50 gate is a prespecified secondary result.

## Period checks and uncertainty

| Test anchor | Model, top-half policy | Horizon | Correct / retained | Accuracy |
|---|---|---:|---:|---:|
| 2026-05-01 | Price + market | 30D | 18/42 | 42.9% |
| 2026-05-01 | Price + market | 7D | 22/42 | 52.4% |
| 2026-07-01 | Price + market | 30D | 5/8 | 62.5% |
| 2026-07-01 | Price + market | 7D | 18/30 | 60.0% |
| 2026-05-01 | Price + market + options | 30D | 13/42 | 31.0% |
| 2026-05-01 | Price + market + options | 7D | 24/42 | 57.1% |
| 2026-07-01 | Price + market + options | 30D | 5/10 | 50.0% |
| 2026-07-01 | Price + market + options | 7D | 16/30 | 53.3% |

Paired 95% bootstrap intervals for the options-minus-baseline 30D accuracy difference, resampling whole groups:

- entry_date: -22.2 to +1.0 percentage points (12 groups, 3,000 draws).
- ticker: -25.3 to -1.3 percentage points (12 groups, 3,000 draws).

These are fixed-prediction, one-cluster-dimension-at-a-time sensitivity intervals. They do not account for the full history of model searches, retraining uncertainty, or simultaneous date/ticker dependence. Weekly 30D outcomes overlap.

## Selection and probability quality

Training used the three 2025 windows. The January 2026 window selected regularization by log loss from C = 0.01, 0.1, 1. Both models retained their training-only coefficients and preprocessing for the May/July tests. No threshold was tuned on those test outcomes.

| Model | Selected C | Validation log loss | Test log loss | Test Brier score |
|---|---:|---:|---:|---:|
| price_market | 1.0 | 0.6656 | 1.2447 | 0.4025 |
| price_market_options | 0.1 | 0.6844 | 1.0892 | 0.3757 |

Lower log loss/Brier is better. Accuracy alone can improve through retaining fewer or easier calls.

## Transfer to the immutable public ledger

Of 13 original events in the 12-stock universe, 12 had eligible historical options features. The frozen 0.50 probability gate hypothetically filters bullish events; bearish and unknown-feature events retain their original decisions. This is an overlapping, already-inspected ledger transfer check, not an independent validation set.

Feature coverage: `{"ok": 12, "outside_30_90_dte": 1}`.

### Full ledger, original fallback for unavailable inputs

| Model / policy | Horizon | Correct / retained | Ledger-style accuracy | Retained coverage | Raw-positive accuracy | Mean directional return | Mean excess vs SPY |
|---|---:|---:|---:|---:|---:|---:|---:|
| original | 30D | 553/1176 | 47.0% | 100.0% | 43.2% | -2.11% | -1.70% |
| price_market | 30D | 549/1170 | 46.9% | 99.5% | 43.1% | -2.12% | -1.71% |
| price_market_options | 30D | 553/1174 | 47.1% | 99.8% | 43.3% | -2.11% | -1.70% |
| original | 7D | 1564/3061 | 51.1% | 100.0% | 47.0% | -2.53% | -2.49% |
| price_market | 7D | 1561/3055 | 51.1% | 99.8% | 47.0% | -2.53% | -2.49% |
| price_market_options | 7D | 1563/3059 | 51.1% | 99.9% | 47.0% | -2.53% | -2.49% |

### Identical events with options features

| Model / policy | Horizon | Correct / retained | Ledger-style accuracy | Retained coverage | Raw-positive accuracy | Mean directional return | Mean excess vs SPY |
|---|---:|---:|---:|---:|---:|---:|---:|
| original | 30D | 6/12 | 50.0% | 100.0% | 50.0% | -1.80% | -1.01% |
| price_market | 30D | 2/6 | 33.3% | 50.0% | 33.3% | -2.93% | -1.93% |
| price_market_options | 30D | 6/10 | 60.0% | 83.3% | 60.0% | -1.45% | -0.60% |
| original | 7D | 5/12 | 41.7% | 100.0% | 41.7% | -0.82% | -1.09% |
| price_market | 7D | 2/6 | 33.3% | 50.0% | 33.3% | -0.49% | -0.28% |
| price_market_options | 7D | 4/10 | 40.0% | 83.3% | 40.0% | -0.91% | -1.03% |

No public event was removed or rescored. These tables are separate research copies.

The options gate would retain the original TSM event. It would filter CAT and JPM, both losing 30D calls in the frozen ledger. This moves the full-ledger hypothetical result from 553/1,176 (47.02%) to 553/1,174 (47.10%), a gain of only 0.08 percentage points. The 12-event covered subset and this already-inspected ledger cannot outweigh the negative broader ranking test or justify changing production scoring.

## Interpretation limits

- The broad study lacks historic snapshots of every confirmation input. It cannot establish how adding options would have changed the complete live model.
- Daily volume and VWAP do not identify whether a call/put was bought or sold, opened or closed. Treating call premium as automatic bullish buying would be unsupported.
- Twelve surviving, liquid stocks and two test windows do not represent the full ledger or all market regimes. Contract aging and incomplete historical activity limit coverage.
- Historical prices and some ledger results were examined in earlier research. The frozen chronological test does not erase that prior exposure or multiple-model-search risk.
- The existing ledger definition counts a bullish call correct when its raw return is positive OR its SPY excess is positive. Raw-positive accuracy is also shown, without changing public grading.
- Neither a high historical percentage nor a small positive difference establishes future 75% accuracy or supports a production promotion on its own.

## Verification and artifacts

The input audit passed 576 decision replays: removing every post-cutoff option bar left the model inputs unchanged. It checked 72 historical selections against the cached reference responses and verified all frozen input hashes.

Immutable ledger SHA-256: `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`.

Protocol: [options-activity-expanded-protocol-2026-09-14.md](options-activity-expanded-protocol-2026-09-14.md).

Local machine-readable artifacts are under `frontend/test-results/confirmation-research/options-activity-expanded/`: `cohort.json`, `analysis-plan.json`, `input-audit.json`, `features.json`, `panel.json`, `panel-audit.json`, `model-selection.json`, `test-predictions.json`, `ledger-predictions.json`, and `results.json`. Research scripts are under `backend/scripts/research/`.
