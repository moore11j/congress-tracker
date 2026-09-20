# Cross-sectional ranking and probability calibration research

Protocol fixed before new-sample export or outcome evaluation. Research only.
Prior studies and 2025–2026 market periods have been inspected; this is iterative
research, not a fresh temporal holdout or proof of live performance.

## Hypotheses

1. Within-date stock feature ranks reduce unstable feature scales and market-level
   extrapolation that harmed prior probability forecasts.
2. Probability calibration on later, already-matured development outcomes can
   reduce overconfidence without using the evaluation quarter.
3. Predicting a stock's within-date forward return rank may support useful stock
   selection even when predicting its return magnitude is unreliable.

These are new hypotheses motivated by the failed direct-return study. The economic
motivation for momentum, volatility and liquidity inputs is consistent with
[Gu, Kelly and Xiu](https://www.nber.org/papers/w25398); their reported results are
not evidence that this implementation works.

## Data and chronology

Development: the original two fixed hash blocks (first 512 eligible entries,
excluding SPY/QQQ). Validation: entries 512:768 from the same coverage-qualified
2023 universe. Reserve entries 768 onward; do not export/evaluate them unless a
candidate passes. Current cache coverage/survival and historical revisions remain
limitations. No API purchases, production writes, model deployment, outcome edits,
or changes to the existing prospective study.

Use prior-close price/volume features and next-session open entries, with separate
positive stock-price-return and positive stock-minus-SPY targets at 30 calendar
days. Zero is nonpositive. FMP-attributed consistent basis only. Features use
253 prior benchmark sessions. Missing/invalid future prices remain unmeasured;
they do not decide feature-rank membership. Within-date ranks are computed over
all securities with usable prior-close features, independently in development
and validation groups, before measuring outcomes. Require at least 50 such names
for a date. No price, sector, return or ticker selections after outcomes.

Evaluate January 2025–June 2026 in six quarterly folds. For quarter start T,
base-model fitting uses development outcomes matured strictly before T minus
six calendar months; calibration entries start at that six-month boundary and
their outcomes must mature strictly before T. This creates a natural 30-day
purge. Do not refit the base model on calibration examples. At least 12 fit dates
and 8 calibration dates are required; otherwise record the fold as unavailable.
Weight each fitting/calibration date equally, so changing stock coverage does not
give one market week extra weight.

## Fixed candidates and baselines

Use bounded within-date ranks of momentum, trend, reversal, volatility, drawdown,
volume ratio, and dollar-volume features; exact feature names are stored in outputs.
No market-level predictor or identity feature enters the learned models.

- Rank logistic classification: C=0.1, standardized inputs, max_iter=2000.
- Rank histogram boosting classification: 4 leaves, 100 iterations, learning
  rate .05, min_samples_leaf=100, L2=10, no early stopping.
- Rank extra-trees classification: 200 trees, max_depth=5, min_samples_leaf=100,
  max_features=0.7, fixed seed.
- Rank histogram boosting regression: same tree settings, predicts a stock's
  forward return percentile among development stocks on the same entry date.
  Calibration converts its bounded rank forecast to each binary target separately.

Report each candidate before and after Platt calibration. Fit a sigmoid of the
logit prediction on calibration observations only, with nonnegative slope bounded
at 2 and fixed weak shrinkage toward slope 1/intercept 0. This restriction prevents
post-hoc reversal of a model's ranking. Calibration date weights sum to one per
date before normalization. No hyperparameter or threshold search.

Baselines: always-positive, always-nonpositive, calibration-window prevalence,
stock SMA200 direction, and fixed rankings by 12-minus-1-month momentum,
five-session reversal, low 60-session volatility, and their equal-weight average.
All stock-selection comparisons use the same eligible sample and entry dates.

Primary policy predicts direction at probability 0.5. Secondary policies are
fixed top quartile by forecast per date, and bullish qualification at probability
>=0.60 (coverage must be reported). Neither threshold may be tuned. Return averages
are overlapping opportunity returns, not portfolio performance; dividends, costs,
slippage and financing are not included.

## Evaluation and continuation rule

Report raw and SPY-relative accuracy, Brier loss, probability bins, bullish coverage
and precision, same-date top-quartile hit-rate and return advantage over the eligible
universe, rank correlation, and quarterly consistency. Eight-week moving-block
bootstrap (2,000 draws) preserves date clusters and much of 30-day overlap.

A directional candidate may advance only if, on validation, it improves Brier
loss versus calibration prevalence and accuracy versus both stock trend and
always-nonpositive, with 95% paired intervals excluding zero, and has positive
Brier improvement in at least four quarters. The raw-return target must pass;
relative-return performance is separate and cannot rescue a failed raw target.

A stock-selection candidate may advance separately if its fixed top quartile
improves both raw hit rate and raw return over the same-date universe with 95%
intervals excluding zero, positive raw return advantage in at least four quarters,
and positive hit-rate and return advantages versus all four fixed ranking baselines.
At least 50 evaluation dates and 1,000 selected examples are required. This would
be a selection model, not a validated full-coverage probability score.

Select at most one advancing candidate: first prefer the directional route;
within it choose lowest validation Brier loss, otherwise choose largest lower
confidence bound for top-quartile raw return advantage. Freeze the selected
candidate and evaluate it once on the reserved symbols. Since multiple candidates
are examined, passing validation alone is not a discovery claim. If no candidate
passes, preserve the reserve and report failures; do not keep tuning until chance
produces a winner. Further work should add independent information or periods.
