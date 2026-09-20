# Integrated confirmation score: no deduction advanced

**Result: the four nonzero deduction weights failed the development screen.**
The prespecified selector retained weight zero. No application score, direction,
cache, public outcome, UI, reminder or deployment was changed by this experiment.
The product requirement remains one confirmation score incorporating risk; this
particular formula has not earned adoption.

## Actual application scores, not the earlier proxy

The [protocol](confirmation-integrated-risk-protocol-2026-09-15.md) tested:

    adjusted bullish score = round_half_up(max(0, original score - weight × predicted loss))

Predicted loss is the estimated mean of max(-30-day raw return, 0), expressed in
percentage points. The candidate weights were fixed at 0, 1, 2, 4 and 8. Bearish
scores were unchanged; unavailable estimates had an explicit original-score
fallback. The score was not interpreted as a return probability.

The risk estimator used the same ExtraTrees recipe as the prior research, fitted
for the July 2026 fold on **6,918 rows from the original 83 development issuers**.
Its latest training outcome was December 31, 2025. The model, protocol, code and
source data were hashed at **2026-09-16 06:29:14 UTC**, before computing these
comparisons. No August application returns trained the risk estimator.

The actual application cohort contains 1,176 matured events: 808 bullish and 368
bearish, over just six entry dates, August 5–12, 2026. These are original
methodology-1 snapshots with their original returns and entries. This is not a
reconstruction of today's shared scoring code or its source-date repairs.

## Development result

Usable risk estimates covered 269 bullish events. The fixed issuer split assigned
215 events from 211 issuers to weight development, spanning six dates. Each
candidate selected the same 55 events by count: the top quarter within each date,
with ceiling rounding. Metrics below average dates equally.

| Weight | Positive 30-day raw return | Mean downside loss | Endpoint loss ≥10% | Mean raw return |
|---|---:|---:|---:|---:|
| 0: original score | 35.08% | 6.00% | 25.88% | −3.02% |
| 1 | 33.83% | 6.20% | 28.37% | −4.20% |
| 2 | 29.66% | 6.37% | 28.37% | −4.70% |
| 4 | 26.56% | 6.69% | 30.42% | −5.67% |
| 8 | 28.00% | 6.25% | 28.91% | −5.16% |

Mean downside assigns zero to nonnegative returns and the loss magnitude to
negative returns. It is not average loss conditional on losing. None of the four
deductions lowered downside, and all increased large-loss frequency and reduced
mean returns. All also lowered positive-return frequency. Even the smallest
deduction reduced mean return by 1.18 percentage points. These are development
results from a concentrated historical period, not app-wide expected accuracy.

The development sample met the protocol's minimum for exploratory calibration,
but no nonzero weight met its performance requirements. **Weight zero was frozen
at 06:34:11 UTC before the evaluation comparison.** No adjustment was rescued by
tuning on evaluation results.

## Evaluation and full-cohort behavior

The separate calibration-evaluation issuers contained 54 covered bullish events
from 54 issuers, over five dates; 15 were selected. Because development selected
zero, baseline and candidate selections are exactly identical. Their equal-date
positive raw-return frequency is 45.71%, mean downside 4.35%, large-loss frequency
13.33%, and mean raw return −2.26%. This is an unchanged-control result, not a
successful evaluation of an active risk deduction. The split excludes shared
issuers between weight development and evaluation; it does not imply those
issuers were absent from all earlier risk-model training or research.

The full bullish set selected 203 events at matched coverage, with 31.87%
equal-date raw positive-return frequency, 5.28% downside and −3.09% raw return.
Those metrics are also unchanged. The legacy raw-or-SPY-excess correctness rate
is separately recorded as 38.51%; it must not be substituted for the raw-return
rate. Existing 40/60/80 threshold diagnostics and first-per-issuer results are
saved without any threshold changes. All 368 bearish scores are unchanged.

## Coverage and provider limitations

The complete audit contains 333 covered events, including 269 bullish and 64
bearish. The remaining 843 have insufficient prior prices (818), unavailable
company facts (8), no current usable financials (14), or no issuer mapping (3).
Thus only **33.29% of the bullish cohort** had a usable estimate. Unknown inputs
are explicit fallbacks, not zero-risk observations.

The point-in-time price-ranking universe contained 598 eligible cached stocks at
each of seven relevant decision dates, determined before financial/outcome
eligibility. Public SEC acquisition checked 323 issuers: 316 had US-GAAP facts,
five lacked that taxonomy and two returned 404. Existing caches were reused;
missing public facts were acquired without production database writes.

Every covered 253-session history required between four and ten explicitly
identified Massive bars to supplement the preferred FMP cache. **No FMP-only
covered evaluation exists**, so the planned provider sensitivity is empty. That
limits transfer claims from the earlier FMP-trained historical risk model.
Original ledger outcome prices also retain their existing provider provenance.
Current SEC archives/mappings and filing-date reconstruction do not prove vintage
ingestion availability, eliminate revisions or remove survival/coverage bias.

Only six nearby entry dates supply the overall matured cohort. No meaningful
independent market-period confidence interval or prospective-accuracy claim is
made. The data and earlier research periods have already been inspected. Returns
are gross, overlapping price returns, excluding dividends, costs and portfolio
capital constraints. They do not measure intraperiod drawdown.

## Verification and artifacts

- Eight focused tests passed: score bounds/monotonicity, directional handling,
  unknown/stale inputs, invalid values, rounding, outcome-independent tie order,
  issuer identity, decision-time limits, matched coverage and zero-weight fallback.
- All 1,176 event IDs are unique and match the immutable source snapshots.
- All 3,978 financial provenance references precede their decision date.
- Calibration development/evaluation issuer sets are disjoint.
- All frozen source, data, model and selection hashes verified after evaluation.
- The selected policy preserved all 1,176 numeric scores and every baseline
  selection in the reported comparisons.

Implementation: `backend/scripts/research/integrated_risk_score.py`,
`evaluate_integrated_risk.py`, and `test_integrated_risk_score.py`.
Local artifacts: `frontend/test-results/confirmation-research/integrated-risk-2026-09-15/`,
including `freeze.json`, `model.joblib`, cached SEC facts, `predictions.json`,
`preparation.json`, `selection.json`, `score-assignments.json`, `results.json`
and `manifest.json`. The runner refuses to overwrite completed stages.

The earlier reserved-issuer result remains a pass for its exact fundamentals-based
selection policy. This experiment shows that its benefit cannot be assumed to
transfer to subtracting predicted loss from the application score. A different
joint score or calibration would be a new hypothesis requiring development and
evaluation, not a justified production change from these results.
