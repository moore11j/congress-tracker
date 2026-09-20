# Confirmation research status

Latest approved product change:
[source priorities with forward-only application](confirmation-source-priorities-2026-09-16.md).
New calculations use the approved weights; recorded historical scores and outcomes
remain unchanged. Implemented locally, not deployed. This is an evidence-weighting
decision, not a validated improvement in return prediction.

Product correction after these experiments:
[shared evidence-consistency ceiling](confirmation-evidence-consistency-2026-09-16.md).
The user explicitly requested that conflicting material evidence prevent a
100/100 score. That logical constraint is implemented locally across the shared
calculation, with new cache/methodology versions. It is not deployed and is not
claimed to improve predictive accuracy. The failed research findings below stand.

Latest completed work: [integrated-score deduction calibration](confirmation-integrated-risk-results-2026-09-15.md).
All four nonzero deductions failed the actual-score development screen; the
frozen selector kept weight zero. No integrated formula advanced or live score
changed. Only six mature entry dates and limited input coverage were available.

Earlier completed work: [reserved-issuer risk-only confirmation](confirmation-risk-holdout-results-2026-09-15.md).
The frozen expected-loss filter **passed the prespecified risk-only check** across
133 new issuers. This supports historical loss reduction with preserved hit rate;
it does not establish higher returns, future performance or deployment readiness.
No live scoring change results from these experiments.

Latest product decision: [incorporate risk into the existing confirmation score](confirmation-integrated-risk-design.md),
with one public score across the app. A separate public risk layer is not wanted.
The combined score mapping still needs development and validation; the historical
risk-filter pass does not validate a deduction from the current application score.

## Reserved sample is consumed

The original 931-name coverage-qualified universe was divided by the fixed
`expanded-research-v1|TICKER` SHA256 ordering. The remaining indices 768 onward
were acquired and evaluated after the risk-only model freeze. **Do not reuse
these 163 entries as an untouched holdout or describe them as still reserved.**
All earlier documents describing that sample as reserved reflect their earlier
study state. The latest result supersedes only its availability status, not their
results or criteria. Historical 2025–2026 market periods have been inspected
throughout the research program and are not fresh temporal validation.

## Recent studies

| Study | Outcome |
|---|---|
| [Direct 30-day returns](confirmation-direct-return-results-2026-09-15.md) | No validated replacement score |
| [Price ranks and calibration](confirmation-rank-calibration-results-2026-09-15.md) | No candidate passed |
| [Filing-dated SEC fundamentals](confirmation-sec-fundamentals-results-2026-09-15.md) | Top-quartile hit-rate lead; no proven return improvement |
| [Downside overlays](confirmation-downside-results-2026-09-15.md) | Reduced losses; failed stricter higher-return requirement |
| [Frozen reserved-issuer risk test](confirmation-risk-holdout-results-2026-09-15.md) | Passed separately approved risk-only objective; no higher-return claim |
| [Risk deduction within actual confirmation scores](confirmation-integrated-risk-results-2026-09-15.md) | All nonzero weights failed development; original scores retained |

The original prospective insider challenger and its reminders remain separate
and unchanged. No new prospective risk-filter automation or deployment has been
created. Any next prospective risk test should retain this procedure, freeze its
own future capture/entry rules, and avoid backfilling predictions after outcomes.
