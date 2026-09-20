# Direct 30-day return research protocol

Written before exporting the additional symbol sample or evaluating the new targets.
Research only: no live score changes, outcome edits, provider hydration, or deployment.

## Question and data

Can fixed models predict (a) positive stock price returns and (b) positive stock
returns minus SPY returns, separately, over 30 calendar days? The legacy
absolute-OR-relative correctness label is not used. Zero returns are nonpositive.
These are price returns, excluding dividends, execution costs and financing.

Reuse the frozen 256-symbol historical price export and its established prior-close
features. Export the next 256 names in the same deterministic hash ordering of the
2023 coverage-qualified universe. Train only on the original symbols; reserve the
additional names for cross-security evaluation. Benchmarks are excluded as stocks.
Current cache coverage and survival bias remain. This is a reconstructed opportunity
panel, not a reconstruction of scores actually published at those dates.

The previously inspected 2025–2026 time period is exploratory, including when
examining new names. Do not call this an independent final temporal holdout.
Unavailable vintage fundamentals and retrospectively edited disclosures are excluded.

## Fixed procedure

Use the existing 61 price/volume/market features, ending at the prior close;
entry is the next session open. Label uses the first available benchmark session
on or after entry + 30 calendar days. Use the existing provider-attributed,
corporate-action-aware price basis and input integrity screens. Report missing
outcomes and future extremes; never remove rows because the outcome is unfavorable.

Evaluate six quarterly windows from January 2025 through June 2026. For each
quarter, fit only original-symbol examples with target dates strictly before
the quarter starts. Evaluate both original and additional symbols in that quarter.
Do not train on additional symbols or tune parameters using either test group.

For each target, fit these four fixed candidates, without parameter search:

- Logistic regression: median imputation, standardization, C=0.1, max_iter=2000.
- Shallow histogram gradient boosting classifier: 4 leaves, 100 iterations,
  learning rate .05, minimum 100 examples/leaf, L2=10, no early stopping.
- Ridge return regression: same preprocessing, alpha=100.
- Shallow histogram gradient boosting return regression: same tree settings,
  squared-error loss.

Regressions clip training targets to the training-only 1st/99th percentiles;
test outcomes stay untouched. Report prediction error against the same clipped
training-mean baseline and disclose sensitivity to extreme outcomes. No learned
probability calibration is claimed: evaluate Brier error and probability bins.

Baselines: always positive, always nonpositive, training-prevalence majority,
stock above/below 200-session average, and market above/below that average.
Classifier threshold is fixed at .5; regression threshold is zero. Report full
coverage direction accuracy, bullish precision and coverage, return errors,
cross-sectional rank correlation, and equal-date top-quartile return advantage
over the same day's eligible universe. Returns are overlapping opportunities,
not a portfolio equity curve. No thresholds selected after seeing results.

Report quarterly consistency and paired accuracy/Brier differences against the
training-only baseline using moving blocks of eight weekly entry dates (2,000
bootstrap draws). This preserves within-date market dependence and much of the
30-day overlap; 18 months still offers limited independent market regimes.
Top-quartile selection is evaluated by fixed model rankings, not hindsight returns.

## Decision

Historical improvement alone does not authorize promotion. A promising classifier
must beat training-prevalence baseline in both direction accuracy and Brier error,
with paired uncertainty excluding zero, on the additional symbols, without relying
on one quarter. Regression evidence must improve baseline error and ranking.
Otherwise reject or mark inconclusive. Existing prospective study and reminders
remain unchanged; a distinct future model would need its own frozen prospective test.
