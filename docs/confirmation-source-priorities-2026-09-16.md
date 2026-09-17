# Approved confirmation priorities: prospective application

Deployed on September 16, 2026 after the user's explicit deployment approval.
The user approved these source priorities and requested forward-only application.

| Source | Previous maximum points | Approved maximum points |
| --- | ---: | ---: |
| Fundamentals | 16 | 20 |
| Institutional activity | 20 | 16 |
| Insider buys | 7 | 12 |
| Congress | 5 | 10 |
| Analysts | 8 | 8 |
| Government contracts | 20 | 5 |
| Insider sells | 2 | 1 |

These are maximum source components, not percentages that sum to 100. Existing
breadth, agreement, quality, freshness, price confirmation and lower score caps
remain. Existing bearish/mixed component ratios remain, except for the approved
insider-selling maximum. Contract activity's native 0–20 input is scaled to 0–5.

The direction classifier, cross-source divergence and final conflict ceiling use
the same approved source priorities. Evidence weight is the source maximum times
`(0.50 × strength + 0.35 × quality + 0.15 × freshness) / 100`.
The classifier retains its existing horizon modifiers; contract support still
does not independently select the direction. Materiality is checked before
applying priority, so credible insider selling remains weak opposing evidence.

The final directional score cannot exceed the floored percentage of material
evidence weight supporting that direction. Material opposition therefore prevents
100/100. A synthetic Boeing-style regression fixture produces 60/100 with Moderate
Divergence under these priorities; this is not a measurement of Boeing's live score.
The previous fixture result and weighting description in the
[initial consistency change](confirmation-evidence-consistency-2026-09-16.md)
describe the earlier implementation.

## Forward-only behavior

- New calculations use `confirmation_score_v4_source_priorities`, direction
  classification `confirmation_direction_v5_source_priorities`, divergence
  `divergence-v4-source-priorities`, and outcome methodology
  `confirmation-v4-source-priorities`.
- Current-context caches are versioned so new calculations use the new formula.
- Recorded scores, source contributions, methodology identities and realized
  outcomes are preserved. No production backfill or historical rewrite was run.
- Historical imports preserve recorded scores and use a separate, non-current
  `confirmation-historical-recorded-v1` identity when their original methodology
  is unknown. They do not acquire today's scoring weights or methodology label.
  Same-day import deduplication works across methodology versions.
- New source payloads store `confirmation_contribution` separately from the
  native `score_contribution` input. Current displays use the actual new points;
  historical displays use stored values with a legacy-field fallback, without
  applying today's weights to older records.

## Verification

Focused backend checks cover source maxima, weak insider-selling opposition,
contract limits, conflicting evidence, canonical projections, screener output,
rankings, monitoring and outcome preservation. Across the focused runs, all 162
selected tests passed after correcting the new historical-import regression test.
The existing screener CSV entitlement test was excluded because its expected
Premium requirement differs from the existing Pro requirement.

TypeScript checking passed, as did all nine ticker decision-layer and outcome-chart
frontend tests. The history regression activates the new methodology, repeats an
import, and verifies the old snapshot ID, score, hash, source values and methodology
remain unchanged.

## Production release verification

- Commit `6daec12212c2ee81ae9bad23213251aa0de04df3` pushed to main. Vercel reported
  success and the public app-version endpoint returned this exact commit.
- Fly image `confirmation-priorities-6daec122`, digest
  `sha256:a885d771aff273d0380204ef91c5dd75aa283484f0d33938de5541b87f4fd1a6`,
  passed rolling deployment checks on both API machines and the cron machine.
  `/ready` reported service and database OK; the BA page returned HTTP 200.
- Refreshed canonical BA and NVDA caches under the new scoring version. At
  verification, full-evidence BA was 70/100 Strong Bullish (81 before the conflict
  ceiling), and NVDA was 81/100 Exceptional Bullish. The logged-out BA projection
  was 64/100, reflecting the existing viewer-source entitlement policy. These are
  observations at release time, not permanently fixed scores.
- The current methodology is `confirmation-v4-source-priorities`. Historical
  backfilling remains disabled. Top Stocks was refreshed with the new methodology
  and returned ten entries at `2026-09-16T17:40:49.935823Z`.
- Before/after read-only checksums matched for all 34,694 existing score snapshots,
  4,130 outcome entries and 5,339 horizon observations. No historical values changed.
- Expanded release checks: 180 passed, four failed and one deselected. All four
  government-contract failures reproduced on an isolated unchanged baseline
  (two legacy feed payload assertions, an inactive-state assertion and a provider
  request blocked in the test environment). No new failing test was introduced.

Local verification artifacts are under `backend/.local/confirmation-*release*`
and `backend/.local/confirmation-release-warm.log`.

These are approved rules for evidence weighting and consistency. They do not
establish improved 30-day return prediction, and the previous research findings
remain unchanged.
