# Frozen risk-only confirmation on reserved issuers

The user approved an independent test of accuracy preservation and loss reduction.
This is a new, explicitly narrower objective than the earlier return-improvement
study, which remains failed. Its results and thresholds are not rewritten.
Protocol and fitted models must be frozen before acquiring reserved-sample prices
or SEC financial data. No production changes, trades or new automation.

## One candidate

Freeze only the expected-loss overlay from `downside_selection.py`. Baseline:
the SEC fundamentals-only extra-trees classifier, seed 91626, 200 trees, depth 5,
minimum leaf 100, max_features=.7, median imputation. Risk estimator: extra-trees
regression of max(-30-day raw return,0), same tree settings, seed 91726, inputs
the frozen financial features, prior-close price ranks and 20/60-session volatility.
All fits use the original development-company data, with equal date weights and
labels matured strictly before each evaluation quarter minus six months. Do not
add old validation stocks to training. Serialize the five quarterly base/risk pairs
and verify their predictions reproduce the prior saved forecasts before new export.

For each date the baseline selects ceil(N/4) highest bullish probabilities. The
candidate selects the same count with lowest predicted loss, restricted to the
baseline's top ceil(N/2). Ties use ticker order. Retain simple low-volatility
selection within the same top half as a control, not another competing candidate.
No threshold, feature, model or trading-policy tuning on reserved observations.

## Reserved population

Use the remaining original coverage-qualified universe: SHA256 ordering
`expanded-research-v1|TICKER`, indices 768 onward (previously recorded as 163
eligible entries). Verify the full earlier 768-name prefix matches the three
frozen earlier price-export selections, excluding shared SPY/QQQ benchmarks, and
that eligibility remains 931 names. If these checks fail, stop; do not silently
substitute a different holdout. Remove SPY/QQQ from stock evaluation.

Use the already-cached SEC ticker/CIK mapping, so mapping does not change with
results. Exclude any reserved issuer whose CIK is present among earlier sampled
stocks, and retain one alphabetical ticker per CIK within the reserve. Unmapped,
unsupported, missing and excluded cases are documented without replacements.
Freeze this issuer selection before downloading SEC facts or evaluating outcomes.
Acquire all remaining mapped issuers; no outcome-based subsampling.

Price and filing-date feature reconstruction stays unchanged, including prior-close
cross-sectional ranks computed before checking future outcome availability. Use
only FMP-attributed consistent price bases; no paid provider calls or cache writes.
Keep a raw unfiltered export for audit and a derived issuer-filtered price panel.
SEC facts must have filing dates strictly before decision dates, match required
periods/currencies, and meet the previous freshness rules. Missing future prices
remain unmeasured. Fix April 2025–June 2026 evaluation quarters as before.

The sample is new across stocks/issuers, not a fresh market-period or prospective
test. Earlier studies inspected these dates. Current coverage and CIK mapping
introduce survival bias; source revisions and corporate-action bases remain risks.

## Prespecified risk-only decision

Primary comparison is candidate versus the original fundamentals selection,
at identical coverage and on identical measurable opportunities. Equal-date metrics
and paired moving blocks of eight dates, 2,000 draws, as in the previous experiment.
Require all of the following to call the risk-only check a pass:

1. At least 50 measured weekly dates, 50 distinct eligible issuers and 1,000 selected
   opportunities. Otherwise report insufficient evidence, not a pass.
2. Hit-rate preservation: 95% lower bound for candidate-minus-baseline hit rate
   strictly above -2 percentage points. This is noninferiority, not proof of higher
   accuracy.
3. Lower mean endpoint downside: 95% lower bound for baseline-minus-candidate
   E[max(-return,0)] strictly above zero.
4. Fewer large endpoint losses: 95% lower bound for reduction in P(return<=-10%)
   strictly above zero.
5. Lower mean downside in at least four of five quarters.

Report the low-volatility control, mean raw/excess returns with paired intervals,
average losing return, worst-decile mean return and selection overlap regardless
of success. Higher return is not a pass criterion, but deterioration must be
reported plainly. No portfolio profits, intraperiod drawdown or stop-loss claims;
dividends, transaction costs, financing and capital allocation are excluded.

Evaluate once. Preserve the result whether positive, negative or inconclusive.
A pass supports a separate risk-control hypothesis across new issuers; it does not
validate the app's bullish score as a probability, prove higher returns, establish
future performance, or authorize deployment. Any later prospective monitoring is
a separate action; the existing insider study and reminders remain unchanged.
