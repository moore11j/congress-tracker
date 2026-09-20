# Direct 30-day return study — September 15, 2026

**Decision: no production scoring change.** Historical research can proceed now,
without waiting for new 30-day outcomes. This experiment found modest directional
signal, but no model that reliably improves both prediction accuracy and forecast
quality enough to replace the current score. The current score must not be described
as a calibrated probability of making money.

## What was actually tested

The [protocol](confirmation-direct-return-protocol-2026-09-15.md) was written before
the additional export and new-target evaluation. Four fixed model families were fit
separately to stock price returns and stock returns minus SPY, with no parameter or
threshold search. A losing stock that declines less than SPY is unsuccessful for the
first target and successful for the second. The earlier absolute-OR-relative label
was not used, so these percentages cannot be directly compared with older ledger
accuracy figures.

The read-only export added 249,750 cached price observations for the next 256 symbols
in the original fixed hash ordering, plus SPY/QQQ. The eligible universe remained 931
symbols. There was no stock overlap with the original sample; SPY/QQQ were shared
benchmarks. No provider hydration, database updates, outcome edits, live scoring
changes, or deployments occurred.

Predictions cover six quarters, January 2025 through June 2026, and 78 weekly entry
dates. Each quarter trains only on original-symbol outcomes fully matured before
that quarter starts. The added symbols never enter training. There are 18,956
original-symbol and 18,973 additional-symbol evaluation opportunities: 37,929 per
candidate/target. These are overlapping opportunities, not independent trials.

This is a price-feature reconstruction, not historical public scores. We cannot
reconstruct past fundamentals as known then from statement-period dates alone.
The periods have been examined in earlier research, and current-cache coverage
introduces selection/survival bias. Added symbols provide a cross-security check,
not a fresh independent market-period holdout.

## Results on the 256 added stocks

All methods below make a direction prediction for every covered opportunity.
Accuracy counts zero returns as nonpositive. The trend rule predicts positive
when the stock is above its trailing 200-session average.

| Fixed method | Stock up/down accuracy | Above/below SPY accuracy |
|---|---:|---:|
| Always predict positive | 49.58% | 42.76% |
| Always predict nonpositive | 50.42% | 57.24% |
| Training-prevalence majority | 43.53% | 57.24% |
| Stock trend rule | 51.13% | 52.26% |
| Market trend rule | 46.86% | 46.44% |
| Logistic classifier | 52.62% | 57.68% |
| Shallow boosted classifier | 52.15% | 56.68% |
| Ridge return regression | 53.81% | 56.91% |
| Shallow boosted return regression | 51.43% | 56.73% |

The strongest raw direction result, ridge, exceeds the stock trend rule by 2.69
percentage points. An eight-week moving-block paired bootstrap gives a 95%
interval of **−3.26 to +9.11 points**. This does not establish a reliable edge.
Its quarterly accuracy ranges from 43.48% to 66.29%. It predicts positive for
41.10% of opportunities; those positive predictions succeed 54.12% of the time.
These stronger-baseline uncertainty comparisons were added after viewing the main
results, without changing predictions, and are labeled diagnostic.

Gains against the training-majority baseline look much larger because that baseline
performs poorly through changing market conditions. They should not be used as a
headline claim of a 9–10 point improvement over useful simple alternatives.

For beating SPY, logistic improves by only 0.44 points over always predicting no
outperformance; the prespecified paired interval is **−0.09 to +0.74 points**.
It predicts outperformance for only 3.78% of opportunities. The other candidates
are less accurate than that baseline.

## A probability score needs more than direction accuracy

Brier error measures the squared difference between predicted probability and
the observed binary outcome; lower is better. The training-frequency forecast is
the baseline, estimated without the evaluation quarter's outcomes.

| Target | Frequency baseline | Logistic | Boosted classifier |
|---|---:|---:|---:|
| Positive stock return | 0.25159 | 0.28937 | 0.26072 |
| Stock outperforms SPY | 0.24488 | 0.27286 | 0.24452 |

Both raw-return classifiers have worse probability error than the simple baseline.
For relative returns, the boosted classifier's tiny probability-error improvement
has an interval spanning zero and comes with worse direction accuracy. Therefore
none passes the prespecified joint accuracy/probability criterion. Probability-bin
results are retained in the JSON; none is advertised as calibrated.

## Return estimates and ranking

The ridge model's raw-return RMSE is 25.68 percentage points versus 24.78 for the
training-mean baseline; its mean absolute error is also worse (14.45 versus 12.97).
The boosted raw-return regressor's RMSE is 24.84, also slightly worse. The boosted
excess-return regressor has only a 0.076% reduction in squared error versus its
baseline, with paired uncertainty spanning no improvement.

Equal-date mean rank correlations are small and positive (roughly 0.06–0.09).
For raw returns, mean top-quartile advantages over the same date's eligible stock
universe are −0.18 points for logistic, −0.40 for boosted classification, +0.26
for ridge, and +1.26 for boosted regression. These overlapping, gross opportunity
averages are not portfolio returns or validated trading profits. No execution,
financing, dividend, or turnover model is included. The last figure is an exploratory
ranking lead, not sufficient evidence to promote a score.

There are 137 original-sample and 106 added-sample outcomes with absolute raw price
returns over 100%. None was removed based on its outcome. In the added sample,
these 106 observations account for about 46–50% of regression squared errors.
Some may be legitimate extreme moves; corporate-action and symbol-history checks
are needed before economic-return claims. Regression training targets alone were
clipped at the training-only 1st/99th percentiles, as prespecified; evaluation
returns remained untouched.

The extension panel builder reports 131,867 incomplete-history/entry exclusions,
576 invalid/extreme trailing-input exclusions, 1,244 missing-target exclusions,
and five entry-basis discontinuities across its complete attempted date range.
Those counts cover panel construction, not just the six evaluation quarters.
Incomplete outcomes can bias the surviving sample; they are not scored as wins.

## What this means for Walnut

We can use historical evidence to reject weak methods now. This study does not
justify replacing the shared score or claiming a 70%, 80%, or 100% success chance.
It also does not test all current evidence-source weights: that requires trustworthy
historical snapshots of the evidence actually available when each score was issued.

The next useful inputs are point-in-time source snapshots across more market
conditions and independently checked price/corporate-action histories. More tuning
against these same dates would produce increasingly optimistic research estimates.
Keep the existing prospective study running; its fixed insider challenger is a
different hypothesis and was not changed by this study. The October 16 interim
reminder and November 17 final review remain as previously scheduled.

## Artifacts and verification

Scripts: `backend/scripts/research/analyze_direct_returns.py`,
`export_direct_return_extension.py`, `audit_direct_return_results.py`, and
`test_direct_returns.py`. Four focused invariants pass: separate target semantics,
strict label maturity, paired date blocks, and zero-return/empty-bullish handling.
All six real folds have their latest training target strictly before quarter start.
Predictions were saved before aggregate evaluation. Frozen outputs and input hashes
are in `frontend/test-results/confirmation-research/direct-return-2026-09-15/`:
`extension.json`, `predictions.json`, `results.json`, `diagnostics.json`, and
`manifest.json`. Large data remains in the ignored local research-artifact directory.

Reproduce from the repository root with Python 3.14, NumPy 2.4.3 and scikit-learn
1.8.0 (the local research dependency directory also contains SciPy):

```powershell
$env:PYTHONPATH='frontend/test-results/confirmation-research/direct-return-deps;backend/scripts/research'
$env:OMP_NUM_THREADS='4'
python -m unittest backend/scripts/research/test_direct_returns.py
python backend/scripts/research/analyze_direct_returns.py frontend/test-results/confirmation-research/historical-results.panel.json frontend/test-results/confirmation-research/direct-return-2026-09-15/extension.json frontend/test-results/confirmation-research/direct-return-reproduction
python backend/scripts/research/audit_direct_return_results.py frontend/test-results/confirmation-research/direct-return-reproduction
```

Methodological references: [scikit-learn time-series evaluation](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html)
and [probability calibration and scoring](https://scikit-learn.org/stable/modules/calibration.html).
The study uses explicit calendar-date maturity purges rather than a random split.
