# Downside-aware selection results

**The risk hypothesis has encouraging historical evidence: expected-loss and
large-loss estimates reduced losses while maintaining the earlier hit rate. They
did not establish higher expected returns, so neither advances under the fixed
criteria and the final 163-symbol sample stays reserved.** No live score,
published outcome, trade, existing prospective study, or reminder was changed.

## Same coverage, fewer large losses

The comparison uses the SEC pilot's unchanged validation sample: 5,175 measured
30-day opportunities across 84 issuers and 64 weekly entry dates, April 2025–June
2026. Each policy selects exactly 1,331 opportunities, approximately the top
quarter each date. Every risk overlay selects only from the original fundamentals
model's top half. It cannot improve by making fewer calls or excluding future
losers. The earlier 60.03% baseline result reproduces exactly.

| Fixed policy | Positive-return rate | Mean 30-day price return | Mean downside loss* | Frequency of losses ≥10% |
|---|---:|---:|---:|---:|
| Original fundamentals selection | 60.03% | 2.36% | 2.79% | 8.28% |
| Simple low-volatility filter | 58.85% | 1.72% | 2.33% | 6.26% |
| **Estimated expected loss** | **61.32%** | **2.39%** | **1.92%** | **4.99%** |
| Estimated probability of ≥10% loss | 61.17% | 2.42% | 1.96% | 4.99% |
| Estimated lower-tail return | 59.86% | 2.20% | 2.26% | 6.26% |
| Estimated expected payoff | 56.82% | 3.23% | 4.13% | 14.94% |

*Mean downside loss is the magnitude of a negative endpoint return, with zero
assigned to a nonnegative return, averaged over all selected opportunities.
The table averages each entry date equally. These are overlapping gross price
returns, not a portfolio equity curve. Dividends, fees, slippage, financing and
capital allocation are excluded. Losses refer to the 30-day endpoint, not a
drawdown during the holding period or a modeled stop-loss execution.

## Expected-loss filter: what is supported

Compared with the original selection, the expected-loss policy has:

- A 1.29-point hit-rate increase; paired 95% interval **−0.60 to +3.15 points**.
  It passes the prespecified 2-point noninferiority margin, but does not establish
  a statistically positive accuracy gain.
- A 0.87-point reduction in mean downside loss; interval **+0.51 to +1.38 points**.
  The point estimate is about 31% lower downside loss.
- A 3.30-point reduction in the frequency of losses of at least 10%; interval
  **+2.17 to +4.99 points**. The point estimate is about 40% fewer such losses.
- Only a 0.03-point increase in mean raw return; interval **−0.59 to +0.84 points**.
  This is not demonstrated return improvement.

The pooled average losing return improves from −7.01% to −4.99%, and the pooled
worst-decile mean return improves from −17.19% to −10.80%. These pooled tail
statistics are descriptive; the formal paired comparisons use date-level metrics.
Approximately 74% of the original selections are retained.

The independently fitted large-loss classifier produces similar reductions. Its
return gain is only 0.06 points, with an interval of −0.62 to +0.86. Both learned
filters have better point estimates for returns and downside than the simple
low-volatility control, but neither proves higher raw returns over the baseline
or the eligible universe. The universe's equal-date mean price return is 2.56%.

The development stocks show the same risk direction: the expected-loss filter
raises hit rate from 59.30% to 60.81% and reduces mean downside from 2.71% to
2.21%, but mean return falls from 2.03% to 1.77%. These later development-stock
predictions use only earlier matured outcomes, but their issuers are also present
in training and they are not an independent company holdout.

## Why no candidate advances

The [protocol](confirmation-downside-protocol-2026-09-15.md) required preserved
accuracy, lower downside and higher returns, with uncertainty excluding no
improvement, plus consistency across quarters. Expected-loss and large-loss
policies pass the downside and hit-rate-preservation tests but fail the return
and four-positive-quarter requirements. Their apparent risk reduction is a
research lead, not a reason to rewrite the precommitted advancement rule.

The expected-payoff model has the highest observed return, 3.23%, but lowers hit
rate to 56.82% and nearly doubles large-loss frequency to 14.94%. Its return
improvement is also uncertain. It fails the approved objective of preserving
accuracy while reducing large losses.

This supports separating a risk estimate from a bullish-evidence score in future
research. It does not make the current evidence score a win probability, prove
profitable trading, or authorize adding a new model to live ticker pages.

## Fixed implementation and verification

Risk fitting uses only development-company outcomes matured before each quarter
start minus six months, matching the frozen fundamentals baseline. Inputs are
the existing filing-dated financial features, prior-close stock ranks and
20/60-session volatility. Median imputation fits only on training data, and
training dates receive equal weight.

The learned policies estimate expected negative return magnitude, probability of
return ≤−10%, the 10th-percentile return, or probability-weighted conditional gain
minus conditional loss. The latter caps positive training magnitudes at their
training-only 99th percentile; evaluation returns remain untouched. There is no
parameter sweep or post-result threshold change. Quantile regression is used as
a ranking input, not advertised as a calibrated prediction interval; see the
[scikit-learn quantile-regression example](https://scikit-learn.org/stable/auto_examples/ensemble/plot_gradient_boosting_quantile.html).

Six focused tests pass: coverage and candidate restrictions, independence from
future returns, score direction/tie handling, inclusive loss thresholds and zero
returns, payoff arithmetic, paired improvement signs, and the advancement gate
(some tests cover more than one invariant). The original feature and forecast
hashes are unchanged. All 10,421 development/validation opportunities are unique
and preserved, and all five training-label cutoffs are verified.

Eight-week moving-block intervals preserve common-date exposure and much of the
overlapping 30-day dependence. They remain exploratory: these historical stocks
and dates have been inspected across several model searches. Survival, missing
outcomes, corporate-action bases and current SEC extraction remain limitations.
No new independent future market regime is tested here.

Artifacts: `frontend/test-results/confirmation-research/downside-selection-2026-09-15/`
contains frozen predictions, complete date-level results, explicit failed checks,
null selection and a SHA-256 manifest. New scripts are
`backend/scripts/research/downside_selection.py` and `test_downside_selection.py`.

```powershell
$env:PYTHONPATH='frontend/test-results/confirmation-research/direct-return-deps;backend/scripts/research'
$env:OMP_NUM_THREADS='4'
python -m unittest backend/scripts/research/test_downside_selection.py
python backend/scripts/research/downside_selection.py frontend/test-results/confirmation-research/downside-selection-reproduction
```

The existing final stock sample and prospective insider study remain unchanged.
A future risk-only confirmation would need its own explicitly frozen objective
and independent evaluation; it should not be presented as passing this study's
higher-return requirement.
