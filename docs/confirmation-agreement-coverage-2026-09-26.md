# Agreement, quality and coverage — approved September 26, 2026

The owner approved the prototype in which broad, consistent evidence can reach approximately 80 without requiring every source to be active. This supersedes the linear score in `confirmation-fundamentals-30-2026-09-26.md`; it retains that document's source allocations, including fundamentals at 30 and total capacity 100.

For the canonical classified direction, let A be aligned evidence weight, O opposing weight, and C the sum of maximum allocations of eligible aligned sources divided by 100. Each evidence weight retains the existing 50% strength / 35% quality / 15% freshness blend.

- Agreement = max(0, (A − O) / (A + O)); zero without directional evidence.
- Evidence quality = A / maximum aligned source capacity; zero without aligned evidence.
- Score = round((80 × agreement + 20 × evidence quality) × sqrt(C)).
- A single aligned source is capped at 39. Missing, mixed, quiet, stale and immaterial sources contribute no aligned coverage. Opposing evidence reduces agreement.
- Only all ten sources confirming at full allocation may display 100; incomplete coverage and near-perfect evidence are capped at 99 even if rounding would otherwise produce 100.

No ticker-specific coefficients or public secondary score are introduced. The same calculation yields TSM 80, BRK-B 74 and AZO 30 on the saved approved examples. These are descriptive research scores, not estimated return probabilities. The model has not been validated for predictive investment performance.

Source contribution fields retain signed evidence-weight units; they do not sum to the nonlinear final score. API calculation metadata exposes agreement, evidence quality, weighted coverage and its square-root multiplier for entitled views. Locked-source calculations remain redacted. Ticker labels and leaderboard explanations distinguish evidence weights from the final score.

## Release and validation

Scoring version `confirmation_score_v8_agreement_coverage`, classification v9, canonical 30-day context v8, ticker cache v15 and outcome methodology `confirmation-v8-agreement-coverage`. Standard monitoring and custom rules rebaseline when this version changes. Ranking acceleration does not compare incompatible methodology versions. Recorded historical observations are retained; the September 26 chart annotation describes the recalibration.

Refresh disposable current ticker views, market tiles and Top Stocks after deployment with `python -m app.jobs.refresh_current_confirmation`. Register the deployed methodology and verify scores, consistent Premium/Pro rankings, Top 10 web limits, private guest ranks, and email previews without sending emails.

Regression cases cover the three approved examples and symbol independence, no evidence, mixed direction, missing/stale sources, stronger opposition, increasing quality, full versus near-full confirmation, single-source caps, tier redaction and alert rebaselining.

Final focused checks passed: 181 backend scoring/access/history/digest/monitoring/decision-layer tests, two ticker/screener integration checks, and 19 frontend tests.
