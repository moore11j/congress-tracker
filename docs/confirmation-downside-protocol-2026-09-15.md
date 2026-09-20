# Downside-aware stock selection protocol

Fixed before training/evaluating new risk predictions. This follows the user's
approval to test whether downside estimates can preserve the fundamentals model's
hit rate while reducing large losses. Research only; no production score, outcome,
trade, reminder or existing prospective-study change.

## Frozen baseline and data

Reuse the SEC pilot's prepared features and immutable `fundamentals_trees` raw
probability predictions. Its fixed top quartile achieved an equal-date 60.03%
positive-price-return rate, but trailed the eligible universe in mean return.
Do not refit, edit or select a different baseline after examining this experiment.
Reuse the same 5,175 validation opportunities and 84 issuers; this is explicitly
iterative research on already-inspected dates and stocks, not a fresh holdout.
The final 163-symbol sample remains reserved unless the criteria below pass.

Use the same five quarterly folds April 2025–June 2026. Fit risk models only on
development-company outcomes matured strictly before quarter start minus six
calendar months, matching the base model's training information. The later window
is not used to tune risk policies. Inputs: the existing filing-dated financial
features, prior-close price ranks, and prior-close 20/60-session volatility levels.
Median imputation fits on training rows only; training dates receive equal weight.
Current revisions/survival, corporate actions, missing outcomes, and the small
number of market regimes remain limitations.

Primary outcome is 30-calendar-day stock price return. SPY-excess results are
secondary and cannot rescue a failed raw-return policy. Zero is nonpositive.
"Downside" means loss at the 30-day endpoint; this experiment cannot establish
intraperiod drawdown or stop-loss execution. No future-price loss filters are used.

## Fixed policies, all selecting the same number each date

Let k=ceil(N/4) for the measured same-date universe. The original selects the k
highest baseline probabilities. Each overlay may select k names only from the
highest ceil(N/2) baseline probabilities. Ties use ticker order. This preserves
approximately 25% coverage and prevents a claimed gain from simply abstaining.

1. Low-volatility control: select the lowest prior 60-session volatility.
2. Expected-loss model: select the smallest estimated E[max(-return,0)].
3. Large-loss classifier: select the smallest estimated P(return <= -10%).
4. Lower-tail model: select the largest estimated 10th-percentile return.
5. Expected-payoff model: select the largest p*E[return|return>0] minus
   (1-p)*E[-return|return<=0], using the frozen baseline p and two conditional
   magnitude regressors. Training positive-return targets alone are capped at the
   training-only 99th percentile; evaluation returns are never clipped.

Tree regressors/classifier: ExtraTrees, 200 trees, depth 5, minimum 100 per leaf,
max_features=.7, fixed seed. Lower-tail regression: histogram boosting quantile
loss at .10, 4 leaves, 100 iterations, rate .05, min_samples_leaf=100, L2=10,
no early stopping. No settings/threshold search. The low-volatility control is a
comparison, not an advancing learned candidate.

## Metrics and advancement

For every policy report equal-date hit rate, mean raw and excess return, mean
downside loss (zero for positive returns), incidence of <=-10% losses, average
return conditional on a loss, and descriptive pooled worst-decile mean return.
Report selection overlap, quarterly results and coverage. These are gross,
overlapping opportunities, not net portfolio returns; dividends, trading costs,
financing and capital allocation are excluded.

Use paired moving blocks of eight entry dates, 2,000 bootstrap draws. A learned
policy may advance only if: at least 50 dates/1,000 selections; raw hit-rate
noninferiority to baseline (95% lower bound above -2 percentage points); positive
raw return improvement and lower mean downside loss versus baseline, both with
95% lower bounds above zero; positive raw return improvement over the eligible
universe with 95% lower bound above zero; lower point-estimate large-loss incidence;
and positive return improvement over baseline in at least four of five quarters.
Also require positive return and lower downside point estimates versus the
low-volatility control, so learning must add something beyond volatility screening.

If multiple policies pass, select the largest lower confidence bound for return
improvement versus baseline. Freeze that single candidate for a separately
documented one-time check on reserved issuers. A validation pass alone is not proof
of discovery given the repeated experiments. If none passes, preserve the reserve
and report the negative result; do not adjust thresholds after seeing outcomes.
