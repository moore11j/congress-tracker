# Confirmation: subtract opposing evidence directly

The user approved replacing the activity-bonus formula after the BA explanation
showed that bearish Congress selling still added preliminary score points.
This supersedes the additive calculation in the deployed source-priority release.

For an established bullish or bearish direction:

    score = round(100 Ã— (supporting weight âˆ’ opposing weight)
                        / (supporting weight + opposing weight))

Clamp to 0â€“100. Any material opposition prevents rounding to 100. One eligible
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
- `round(100 Ã— 18.03 / 44.99)` = **40/100, Moderate Bullish**.

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

## Validation

184 selected backend tests passed across focused runs, including the new signed
evidence properties, BA reproduction, source-availability rebuild, tier projection,
ranking, monitoring and immutable-history regressions. The previously identified
screener CSV entitlement assertion was excluded. Two time-dependent ledger
fixtures now use a fixed market afternoon so UTC/New York midnight does not
invalidate their unrelated assertions. TypeScript and nine frontend tests passed.

## Production verification

Backend release `55873665` deployed to both API machines and the cron machine as
`confirmation-net-55873665`, digest
`sha256:5a153102a5d99afe925ca491b8998b55d6ff125737d5d05b870890e99fdc7591`.
All Fly checks passed, and `/ready` returned service/database OK. Vercel reported
success and the app-version endpoint verified the matching frontend commit. A
frontend-only label follow-up is `fffd4345`; Vercel reported success and the live
app-version endpoint returned `fffd43453d3f200e0978d0a4baf9a2b906ee54e5`.

Current BA inputs differ from the earlier saved fixture: price/volume is now
bearish (weight 9.74), and Congress weight is 7.87 after freshness aging. Supporting
weight remains 31.51; opposing weight is 22.85. The live score is therefore
`round(100 Ã— (31.51 âˆ’ 22.85) / 54.36)` = **16/100**. Its direction is bullish,
with an inactive confirmation band under the retained 0â€“19 threshold. NVDA's
full-evidence score is **67/100, Strong Bullish**. These readings are time-specific.

Top Stocks was refreshed under the new method and returned ten entries at
`2026-09-17T00:06:54.486916Z`. Historical backfill remains disabled. Before/after
read-only checksums matched for 34,700 score snapshots, 4,130 outcome entries and
5,411 horizon observations; no pre-existing records were changed.

Local evidence is saved in `backend/.local/confirmation-net-release-warm.log` and
`backend/.local/confirmation-net-history-{before,after}.json`.


## Weak directional labels and current-view refresh

Scores below 20 with a bullish or bearish direction display “Weak bullish lean”
or “Weak bearish lean” across ticker, trend, screener, leaderboard, and market-map
views. Neutral/no-evidence and conflicted states retain their own labels. This is
a display change; score thresholds and historical recorded bands are unchanged.

Market-map live and scheduled calculations now use the same cached 30-day card
inputs as ticker pages and Top Stocks. The current-view refresh job recalculates
current-version canonical ticker caches, every saved market tile, and Top Stocks.
It preserves recorded score history and price/cache freshness timestamps, performs
no provider fetches, and sends no monitoring alerts. Old cache versions remain
ineligible for current responses. Feed, watchlist, and screener scores continue
using the shared net-evidence formula when requested.

Validation: 52 focused backend tests passed, including preservation of historical
score points and quote freshness during a current-view refresh.
