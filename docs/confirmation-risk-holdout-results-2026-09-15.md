# Reserved-issuer risk confirmation

**Result: PASS for the prespecified risk-only objective.** On previously unused
issuers, the frozen expected-loss filter preserved accuracy, reduced mean downside
and the frequency of large losses, and reduced downside in all five quarters.
It did not establish higher expected returns. The earlier higher-return study
remains failed; this separately approved test does not rewrite that result.

No live scoring, public outcomes, caches, trades, reminders or existing prospective
study were changed. A risk-only research pass is not deployment approval.

## What was frozen before the holdout was opened

The [protocol](confirmation-risk-holdout-protocol-2026-09-15.md) selected only the
expected-loss filter. Five quarterly baseline/risk model pairs were fitted on the
original development companies using only appropriately matured earlier outcomes.
They reproduced all 10,421 prior saved forecasts to absolute tolerance 1e-12.
The model binaries, source files, protocol, training data and dependency versions
were hashed at **2026-09-16 06:05:33 UTC** (September 15 Pacific time), before the
reserved price export or SEC acquisition. Evaluation completed at 06:11:26 UTC.

The baseline selects the top quarter by the fundamentals classifier. The risk
filter selects the same count with the lowest estimated loss, restricted to the
baseline's top half. A simple low-volatility filter is retained as a control.
There is one candidate and one evaluation, with no holdout fitting or retuning.

## New companies, same historical periods

The final hash block contained 163 stocks. Its universe size and all three earlier
256-entry blocks exactly matched the frozen research samples. Four stocks lacked
SEC identity mappings and were excluded without replacement. All 159 mapped
reserved issuers were distinct from issuers in the earlier 768-stock prefix.

SEC acquisition returned 140 usable US-GAAP responses, 13 without that taxonomy,
and six HTTP 404s. After prior-feature, financial-freshness and outcome coverage
requirements, the evaluation contains **8,202 opportunities across 133 distinct
issuers and 65 weekly dates**, April 2025–June 2026. Each policy selects exactly
**2,082 opportunities** (25.38% coverage). Approximately 75% of baseline selections
are retained by the risk filter.

This is a new-issuer confirmation in previously inspected market periods. It is
not a prospective trial or an independent future market regime. The reserved
sample is now consumed and must never be described as untouched in later research.

## Main results

Metrics below average each entry date equally. They are gross, overlapping
30-calendar-day stock price-return observations, not portfolio returns.

| Metric | Original fundamentals selection | Frozen expected-loss filter | Low-volatility control |
|---|---:|---:|---:|
| Positive-return rate | 58.70% | **61.15%** | 58.90% |
| Mean downside loss* | 2.94% | **2.12%** | 2.28% |
| Frequency of endpoint losses ≥10% | 10.22% | **5.77%** | 6.26% |
| Mean 30-day price return | 2.24% | 2.35% | 1.93% |
| Average losing return, pooled | −7.19% | −5.50% | −5.58% |
| Worst-decile mean return, pooled | −17.30% | −12.29% | −12.87% |

*Mean downside assigns the magnitude of a negative endpoint return to losers and
zero to nonnegative returns, then averages all selections. It is not the average
loss conditional on losing. The two pooled tail statistics are descriptive.

Compared with the original selection, mean downside is about **28% lower**, and
the frequency of losses of at least 10% is about **43% lower** on point estimates.

## Prespecified statistical checks

Paired intervals resample eight consecutive weekly date clusters, using 2,000
draws. Positive improvement means better performance for the stated metric.

| Required check | Measured result | Decision |
|---|---|---|
| At least 50 issuers, 50 dates, 1,000 selections | 133 issuers, 65 dates, 2,082 selections | Pass |
| Preserve hit rate within 2 percentage points | +2.45 pp; 95% interval **−0.04 to +4.89 pp** | Pass for noninferiority |
| Lower mean downside | Reduction 0.82 pp; interval **+0.53 to +1.12 pp** | Pass |
| Fewer losses ≥10% | Reduction 4.44 pp; interval **+3.09 to +5.85 pp** | Pass |
| Lower mean downside in at least four quarters | Lower in **all five quarters** | Pass |

The hit-rate interval includes zero. The supported claim is preservation within
the frozen tolerance, not a conclusive accuracy increase. Downside reductions
are supported by the paired intervals on this reserved-issuer sample.

| Evaluation quarter | Baseline mean downside | Filter mean downside |
|---|---:|---:|
| 2025 Q2 | 1.60% | 1.48% |
| 2025 Q3 | 3.56% | 2.74% |
| 2025 Q4 | 2.45% | 1.55% |
| 2026 Q1 | 4.54% | 3.48% |
| 2026 Q2 | 2.56% | 1.35% |

## Return tradeoffs and control comparison

Mean return improves by only **0.11 percentage points**, with an interval of
**−0.53 to +0.61**. Higher returns are not established. The risk filter improves
mean returns in two of five quarters and lowers them in three. Its mean
SPY-excess price return is −0.31%, versus −0.42% for the baseline.

The eligible universe has a 3.22% mean raw price return, compared with 2.35% for
the filter, but substantially more downside and large-loss exposure. The filter's
return difference versus the universe is −0.88 points with a broad interval
(−3.77 to +1.44). This is a risk-control finding, not proof of market-beating returns.

Against the low-volatility control, the learned filter improves hit rate by 2.25
points (interval +0.77 to +3.88), mean return by 0.42 points (+0.15 to +0.79), and
mean downside by 0.16 points (+0.04 to +0.31). The further reduction in large-loss
frequency versus that control is uncertain. These are secondary comparisons;
the single primary comparison was fixed against the original fundamentals model.

## Verification, coverage and limits

- Frozen models/source hashes remain unchanged after evaluation. Training excludes
  all reserved issuers. Earlier-issuer membership was independently rechecked.
- All **160,351 filing references** in the prepared financial features precede
  their decision date. Later amendments cannot enter earlier predictions.
- All 8,202 evaluation observations are unique, and every policy has the same
  2,082 selection count. The saved forecasts precede aggregate evaluation.
- Five focused tests pass for issuer separation, unchanged population, strict
  success boundaries, minimum sample/quarter rules, and the distinct risk-only
  objective. The prior selection and timing tests remain applicable.

The broader prepared range contains 14,636 usable financial rows. Before that,
702 prior-feature rows lack downloaded company facts, 2,080 lack current usable
financials, and 128 have missing outcomes. The price builder separately reports
10,207 missing prior histories, 597 invalid prior inputs, three missing entries,
155 missing targets and 66 dates below the minimum prior-feature universe size.
These are different stages/ranges and must not be added as if disjoint evaluation
exclusions. Missing outcomes are not wins; evaluating only measurable cases can
introduce censoring bias.

There are 60 evaluation outcomes with absolute raw returns over 100%, retained
rather than filtered for their results. Corporate actions, current coverage and
SEC mapping/survival bias, historical extraction revisions and ticker-history
changes remain limitations. Eight-week blocks capture much overlapping 30-day
dependence but cannot supply more independent market regimes.

Dividends, fees, slippage, borrowing and capital allocation are excluded. This test
does not measure intraperiod drawdown or stop-loss execution. It does not calibrate
the app's bullish evidence score as a probability or demonstrate future performance.

## Artifacts and reuse

Runner: `backend/scripts/research/risk_reserved_confirmation.py`.
Read-only exporter: `export_risk_reserved.py`. Tests:
`test_risk_reserved_confirmation.py` in the same directory.

Artifacts in `frontend/test-results/confirmation-research/risk-reserved-confirmation-2026-09-15/`
include `freeze.json`, `models.joblib`, raw and issuer-filtered prices, frozen
issuer selection, SEC responses/fetch audit, prepared features/provenance,
predictions, full date-level results and a final manifest. The runner verifies
the frozen hashes and refuses a second completed evaluation. Inspect the saved
result instead of rerunning or repurposing this sample as a new holdout.

The evidence supports retaining this exact procedure as a candidate risk filter
for a separately authorized prospective shadow test. No deployment or new
monitoring automation has been performed.
