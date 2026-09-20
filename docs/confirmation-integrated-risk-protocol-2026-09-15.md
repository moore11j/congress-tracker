# Integrated confirmation score: exploratory calibration

The user authorized development and testing of risk inside the existing score.
This experiment uses actual frozen application scores, not the fundamentals-only
classifier as a substitute. No production writes or deployment are part of this
experiment. All available historical samples have previously been inspected.

## Fixed candidate family

For a bullish event with usable inputs:

    candidate = round_half_up(max(0, original_score - lambda * predicted_loss))

`predicted_loss` estimates max(-30-day raw return, 0), in percentage points.
Choose lambda from **0, 1, 2, 4, 8** on development issuers only. Zero is the
unchanged-score control. These are candidate weights, not established financial
constants. Scores remain on 0–100 and never increase because of this adjustment.
Bearish, mixed and inactive scores stay unchanged. Missing, stale or unsupported
inputs retain the original score with an explicit unknown-input fallback; they
are never interpreted as zero estimated loss. No direction changes or new public
risk display. Integer score ties use ticker and event ID, never future outcomes.

Fit the existing expected-loss ExtraTrees recipe on the original SEC development
rows only, with outcomes strictly before January 1, 2026 (the July 2026 fold's
six-month separation rule). Freeze the artifact before evaluating ledger outcomes.
Do not retrain the risk estimator on August ledger returns or earlier validation
issuers. Financial and price feature definitions remain those in the prior study.

## Actual-score cohort and inputs

Use the immutable `cohort.json` score snapshots. Primary evaluation uses bullish
events with recorded 30-day outcomes; preserve their original direction, score,
entry, returns, methodology and event identity. These are historical methodology-1
scores, not a reconstruction of today's source/date fixes. This comparison cannot
establish how a current-version reconstructed score would perform.

Choose the last cached SPY session close available at the calculation timestamp,
bounded by the snapshot's recorded market date. Require 253 consecutive benchmark
sessions, with the same prior-input integrity checks as the risk training data.
Use FMP-attributed prices where available; fill missing prior bars only from the
previously exported Massive rows explicitly marked split-adjusted price return,
with equal raw/adjusted close. Count mixed-provider histories and report FMP-only
sensitivity separately. Never replace a cached FMP row. This provider transfer
is a limitation, not proof that all corporate actions are reconciled.

Compute price ranks over all cached stocks with sufficient prior input history
at that date, before SEC eligibility or outcome checks. Exclude SPY and QQQ and
require at least 50 prior-eligible stocks. Universe and coverage are retrospective.
Use the frozen SEC ticker mapping. Acquire missing public company facts only for
ledger tickers with usable prior price inputs. Cache all responses. Apply the
previous strictly-before-decision filing rules and freshness/period requirements.
Do not select companies by observed returns. Missing inputs remain documented.

Issuer split: SHA256(`integrated-risk-v1|CIK`), first eight hexadecimal digits
modulo 100; below 80 is development and the rest evaluation. All events/share
classes of an issuer stay together. The split is for exploratory calibration;
these issuers and the six mature entry dates are not an untouched holdout.

## Calibration, comparison and decision

Within each entry date select ceil(N/4) bullish events, ordered by the original
or integrated integer score. Compare identical eligible opportunities and counts.
Average date metrics equally. Report positive raw-return rate, mean downside,
frequency of raw endpoint losses >=10%, mean raw/excess return, and the existing
public raw-or-excess correctness separately. No claims of portfolio performance.

On development only require at least 100 covered bullish events, 50 issuers and
four dates before selecting a nonzero lambda. Among candidates require hit rate
no more than two percentage points below the original selection, mean return no
more than 0.5 percentage points below it, and no higher large-loss frequency.
Require strictly lower mean downside. Choose lowest mean downside, then smallest
lambda. If none qualifies, keep lambda zero. Save calibration results and the
chosen formula/hash before opening evaluation returns for comparison.

Report the selected candidate on evaluation issuers once, whether favorable or
unfavorable, plus full-cohort fallback behavior, first-per-issuer sensitivity,
FMP-only coverage and unchanged bearish handling. Report existing thresholds
40/60/80 without retuning; reductions in coverage are not improved predictions.

Do not produce reassuring date-block confidence intervals from six adjacent entry
dates. At least 50 weekly date clusters, 50 issuers and 1,000 selected opportunities
would be needed for a later substantive validation using the prior risk-only
criteria. This run can yield an exploratory candidate or no useful candidate;
it cannot authorize promotion or establish improved future accuracy. No automated
future collection is created by this experiment. Existing insider monitoring is
unchanged. Preserve source/data/model hashes and do not overwrite completed runs.
