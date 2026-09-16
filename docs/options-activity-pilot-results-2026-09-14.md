# Free options-activity pilot results

The free Massive options research pipeline is implemented and its first eight-stock historical run is complete. This is an engineering pilot with fixed rules, not a trained or validated model. No paid plan, production scoring change, historical outcome modification, or scheduled production job was introduced.

**The fixed options rules did not improve this pilot.** The options gate dropped three correct 30D calls (TSM, NVDA, XOM). The results support keeping this rule out of live scoring; they do not establish that all options information is unhelpful.

## Same-event comparison

The baseline uses the original stored confirmation and outcome. The candidate gates can retain or abstain from an original bullish call; they do not change its score, direction, entry, or measurement. Missing feature coverage keeps the original decision. Accuracy is the existing directional return OR SPY-relative grading definition.

| Policy | 30D correct / retained | 30D retained coverage | 7D correct / retained |
|---|---:|---:|---:|
| Original confirmations | 5/8 (62.5%) | 100.0% | 5/8 (62.5%) |
| Stock/SPY trend control | 4/6 (66.7%) | 75.0% | 4/6 (66.7%) |
| Fixed options-activity gate | 2/5 (40.0%) | 62.5% | 2/5 (40.0%) |
| Options gate + stock/SPY trend | 1/4 (25.0%) | 50.0% | 2/4 (50.0%) |

A higher percentage on fewer retained observations is not evidence of a general improvement. If a rule retains no calls, its accuracy is undefined, not zero and not 100%. No thresholds were selected or changed using these outcomes.

| Policy | Average signed 30D return | Average signed 30D excess vs SPY | Raw directional hits |
|---|---:|---:|---:|
| Original | -0.1% | 0.7% | 5/8 |
| Stock/SPY trend control | 0.6% | 1.4% | 4/6 |
| Options gate | -2.8% | -2.1% | 2/5 |
| Combined gate | -3.7% | -2.9% | 1/4 |

For the six tickers with sufficient options history, the comparison is:

| Policy, same six covered tickers | 30D correct / retained | 7D correct / retained |
|---|---:|---:|
| Original | 5/6 (83.3%) | 3/6 (50.0%) |
| Stock/SPY trend control | 4/5 (80.0%) | 3/5 (60.0%) |
| Options gate | 2/3 (66.7%) | 0/3 (0.0%) |
| Combined gate | 1/2 (50.0%) | 0/2 (0.0%) |

JPM and WMT lack the required long-expiry baseline and retain their original decisions in the all-pilot table. Removing these missing-data cases does not turn the options gate into an improvement over the same covered baseline.

## Data coverage

| Expiration bucket | Feature status counts |
|---|---|
| 1–7 calendar days | insufficient_contract_history: 7; ok: 1 |
| 8–29 calendar days | insufficient_contract_history: 8 |
| 30–90 calendar days | ok: 6; insufficient_contract_history: 2 |

Each bucket samples one matched call/put pair at the same strike and expiration. These are not full-chain volume totals or observed buyer-initiated flows. The 30–90-day gate requires a five-session premium rate at least twice the preceding 20-session daily rate, call premium share of at least 60%, and a share increase of at least 10 percentage points. The combined gate also requires the stock and SPY above their 20-session moving averages.

| Ticker | Data cutoff | Long-expiry baseline | Activity ratio | Call premium share change | Options gate | Stored 30D correct |
|---|---|---|---:|---:|---|---|
| TSM | 2026-08-04 | ok | 1.17x | +5.6 pp | Abstain | Yes |
| AAPL | 2026-08-04 | ok | 3.78x | +11.2 pp | Retain | Yes |
| NVDA | 2026-08-04 | ok | 1.46x | -7.0 pp | Abstain | Yes |
| MSFT | 2026-08-04 | ok | 32.53x | +41.8 pp | Retain | Yes |
| AMZN | 2026-08-04 | ok | 6.98x | +21.0 pp | Retain | No |
| JPM | 2026-08-04 | insufficient_contract_history | Unknown | Unknown | Unknown: original retained | No |
| XOM | 2026-08-10 | ok | 0.86x | -14.0 pp | Abstain | Yes |
| WMT | 2026-08-07 | insufficient_contract_history | Unknown | Unknown | Unknown: original retained | No |

## Interpretation and next evidence

The eight liquid names were chosen before collecting options observations: TSM, AAPL, NVDA, MSFT, AMZN, JPM, XOM, WMT. Their original confirmations are concentrated in August 2026. This selection and short period cannot establish held-out accuracy, a 75% expected hit rate, or benefits for 90D/180D/365D horizons. No classifier was fit to this sample.

Short-dated contracts often do not have the 25 prior benchmark sessions required by the baseline. Treat that as insufficient history, not zero volume or negative evidence. The long-expiry comparison still has limitations: gross volume is unsigned; premium changes reflect prices and volatility; a fixed strike changes moneyness as the stock moves; proximity to expiration can change activity. Historical contract metadata and prices are not a complete original-vintage audit.

Keep live scoring unchanged. The next efficacy study needs a larger, date-correct options panel, comparison on identical eligible events against price/market controls, and an independent later period. Any learning must use labels that matured before the validation period, with a 30D maturity gap. Do not lower the activity threshold merely to obtain more favorable pilot results.

## Implementation and verification

- Collector: `backend/scripts/research/options_activity_pilot.py`.
- Feature calculation and immutable-outcome comparison: `backend/scripts/research/analyze_options_activity_pilot.py`.
- Report renderer: `backend/scripts/research/report_options_activity_pilot.py`.
- Protocol: `docs/options-activity-pilot-protocol-2026-09-14.md`.
- Local data: `frontend/test-results/confirmation-research/options-activity-pilot/`.

The completed collection reports 64 requests in its final invocation. Requests are serialized at least 13 seconds apart, restricted to free reference/aggregate endpoints, and resumed from local cache. The request cap stops a run without treating incomplete data as success. A local process lock prevents concurrent collectors. Credentials and credential-bearing pagination URLs are not persisted.

Run from the repository root:

```powershell
C:/Python314/python.exe backend/scripts/research/options_activity_pilot.py --collect --max-requests 80
C:/Python314/python.exe backend/scripts/research/analyze_options_activity_pilot.py
C:/Python314/python.exe backend/scripts/research/report_options_activity_pilot.py
```

Completed historical requests are cached; rerunning this frozen experiment does not repeatedly download the same data. This is a local research pipeline, not an installed nightly production collector. Customer pages do not call Massive through this research code.

The 11 options-pipeline tests passed, along with the 13 existing conviction/disclosure research tests. They cover New York EOD cutoffs, future-data exclusion, matched standard contracts, baseline availability, missing VWAP, daily-rate normalization, sanitized pagination, cache reuse, request budgets, and duplicate bars. The frozen public ledger SHA-256 remains `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`. The evaluation manifest hashes its frozen cohort, source price files, collected events, features, protocol, and code.
