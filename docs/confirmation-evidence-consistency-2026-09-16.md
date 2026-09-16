# Confirmation scores must reflect opposing evidence

The later [approved source-priority change](confirmation-source-priorities-2026-09-16.md)
supersedes the weights and fixture result described below. This document records
the initial consistency implementation; historical scores are not rewritten.

The user explicitly requested a scoring change because a 100/100 directional
confirmation with material opposing evidence is misleading. This change enforces
that evidence-consistency requirement throughout the shared score calculation.
It does not claim improved return prediction and does not deploy the learned risk
deduction that failed the previous research screen.

## Calculation

Keep the existing source components, direction classifier and existing caps.
After all bonuses and caps, compute a final directional ceiling:

    floor(100 × aligned material evidence weight / total material directional weight)
    final score = min(previously calculated score, ceiling)

When material opposition exists, the ceiling is always below 100. With no material
opposition this rule adds no cap. Mixed scores retain their existing maximum of
59. The same ceiling applies to bullish and bearish confirmation, to every ticker,
and before score bands, rankings and output explanations are derived.

The score and divergence display import the same weights and eligibility logic
from `confirmation_evidence.py`: use an explicit nonzero absolute source
contribution where available, otherwise `(0.50 × strength + 0.35 × quality) / 10`,
with strength and quality bounded to 0–100. Require a weight of at least two and
a present bullish/bearish source. Exclude known future-dated or older-than-90-day
evidence. Unknown freshness retains the existing inclusion behavior; it is not
proof of freshness. Government-contract support participates in the agreement
ceiling just as it participates in the displayed divergence weights.

This remains a confirmation measure, not a probability of a positive return.
For example, 70% supporting evidence weight means at most 70/100 confirmation.
Additive bonuses cannot raise the score above that ceiling. Existing lower caps
still apply. The score may therefore be below the supporting-evidence percentage.

## Regression example

A synthetic Boeing-style fixture contains four bullish sources (analysts,
fundamentals, contracts, institutional activity) and three bearish sources
(Congress, price/volume, macro positioning). Contributions total 50 bullish and
30 bearish. The previous formula saturates at 100. The corrected result is
**62/100, Strong Bullish**, with Moderate Divergence and an explanation that
opposing evidence limits confirmation. This is a regression fixture, not a claim
that Boeing's current live score has been measured as 62.

## Shared consumers and history

Single-ticker, batch, normalized payload, source-context and entitlement-redacted
bundles all pass through `_score_bundle`. Updated source merges also recompute
derived score fields, preventing disagreement between displayed sources and a
previously computed score. Slim feed/screener outputs and the ticker decision
layer consume the corrected result. The decision summary explains an applied cap.

Ticker server/client cache generation is 10. The top-stocks context stamp and
market-pressure scoring stamp advance, rejecting old prepared scores. Outcomes
cache keys advance as well. New outcome captures record methodology
`confirmation-v3-evidence-consistency`, the scoring version and the cap inputs.
Existing historical scores and outcomes are not recalculated or rewritten.
The direction-classification version stays unchanged because that classifier's
rules did not change.

## Research distinction

The earlier offline conflict-ceiling study reported worse 30-day retained-cohort
accuracy; that finding remains valid and is not rewritten. The learned-loss
deduction experiment also failed and remains rejected. The present change is
authorized to correct what a confirmation score communicates, not because either
experiment proved better returns. Historical accuracy across the methodology
change must retain version attribution.

## Verification and release state

Focused tests cover a saturated conflicting fixture, unchanged aligned evidence,
symmetric bearish handling, stronger opposition reducing the ceiling, stale/future
and immaterial exclusions, matching divergence weights, redaction, source merges,
slim outputs, labels, cache stamps and immutable snapshot metadata. The final
focused run passes **115 tests**. TypeScript checking passes. The broader
ticker/screener suite has eight failures reproduced
with the prior scoring/source-update functions and prior ticker cache version:
two cache/coalescing fixtures, two old directional expectations and four screener
entitlement expectations. They are not fixed as part of this scoring change.

Changes are local and have not been deployed. Deploying the backend and frontend
and refreshing the prepared ranking/market-pressure snapshots is needed before
the website displays current scores under this methodology.
