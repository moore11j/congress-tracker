# Confirmation: subtract opposing evidence directly

The user approved replacing the activity-bonus formula after the BA explanation
showed that bearish Congress selling still added preliminary score points.
This supersedes the additive calculation in the deployed source-priority release.

For an established bullish or bearish direction:

    score = round(100 × (supporting weight − opposing weight)
                        / (supporting weight + opposing weight))

Clamp to 0–100. Any material opposition prevents rounding to 100. One eligible
source remains capped at 39. Neutral or mixed direction has zero directional
confirmation. Source priorities, strength/quality/freshness weighting and the
existing materiality threshold remain; non-directional, missing, future-dated and
older-than-90-day evidence contributes zero. The classifier now uses the same
eligibility exclusions as the score. Contract support alone cannot choose a
direction. The classifier's existing direction thresholds and horizon modifiers
remain.

There are no separate activity, breadth, agreement, quality, freshness or support
bonuses. Quality and freshness affect evidence weights once. For bearish
confirmation, bearish evidence supports and bullish evidence subtracts. Thus,
for a fixed direction, increasing opposing weight cannot increase the score.

Approved source priorities remain relative evidence weights, not fixed shares of
the final normalized score. `confirmation_evidence_weight` records each eligible
weight. `confirmation_contribution` records signed normalized percentage points,
which sum to the raw net score before rounding and the single-source cap. The
immutable outcome snapshot also retains the complete `score_calculation`.

## BA reproduction

Using the production inputs saved at 2026-09-16 17:40:49 UTC:

- Supporting weight: fundamentals 12.59, institutions 14.11, analysts 4.81 = 31.51.
- Opposing weight: Congress 8.24, macro positioning 5.24 = 13.48.
- Price/volume is mixed and contributes zero. Other sources are inactive.
- Net weight = 18.03; total directional weight = 44.99.
- `round(100 × 18.03 / 44.99)` = **40/100, Moderate Bullish**.

This is a reproduction from saved inputs, not a claim that live data is frozen.

## Meaning and prospective application

The number measures net directional agreement, not a forecast probability or
absolute data coverage. Two or more eligible sources with sufficient evidence to
establish direction and no opposition can reach 100; missing/mixed evidence earns
no points and is excluded from the directional denominator. More coverage by
itself earns no bonus. Return-prediction accuracy has not been validated for this
formula.

New scoring version: `confirmation_score_v5_net_evidence`; classification:
`confirmation_direction_v6_net_evidence`; methodology:
`confirmation-v5-net-evidence`; ticker cache version 12. Historical values,
methodology associations and outcomes remain untouched. New captures store the
signed contributions; older displays continue reading their recorded values.

The screener uses the same signed contributions, and the browser no longer
recreates an alternate score for legacy unprojected payloads. Such payloads show
a refreshing state until the server supplies its entitlement projection.
