# Risk within the confirmation score

## Product decision

The user wants one 0–100 confirmation score with risk incorporated into its
calculation. Do not add a separate public risk score, card, or optional filter.
This replaces the earlier proposal to present a separate risk layer. The internal
calculation may use multiple models, but users receive one confirmation score
and an explanation of the factors that affected it.

Apply the final calculation through the shared scoring service and all its
consumers, including ticker pages, rankings, feeds, cached summaries and newly
recorded outcomes. Do not implement a Boeing-specific adjustment or a frontend
deduction. Preserve historical observations with their original methodology.

## What the research establishes

The frozen expected-loss selection policy passed the historical risk-only check
documented in [the reserved-issuer results](confirmation-risk-holdout-results-2026-09-15.md).
It selected the lowest estimated losses within the fundamentals classifier's
top half. Its baseline was the fundamentals classifier, **not the application's
current confirmation score**. It did not test subtracting risk points from that
score, establish a calibrated probability for each stock, or validate a new
0–100 mapping. The reserved sample has been consumed.

## Required score semantics

The current score measures confirmation of its associated direction; a high
bearish score is possible. The tested risk target is E[max(-30-day return, 0)],
which measures downside for someone owning the stock. Consequently:

- For bullish confirmation, greater estimated downside should reduce the
  integrated score when other inputs remain equal.
- Do not apply that same deduction to bearish confirmation: the tested model
  does not estimate the risk of a bearish prediction being wrong.
- Do not invent an equivalent bearish model by reversing the long-loss estimate.
  Any directional extension needs its own target and evaluation.
- Missing, stale or unsupported model inputs must not be treated as zero risk.
  A fallback needs explicit methodology metadata and a consistent explanation.
- A score of 80 must not be described as an 80% chance of a positive return
  without separate probability calibration evidence.

## Implementation and validation still required

Define and freeze a combined score mapping on development data, including its
directional scope and missing-input behavior. Evaluate the **combined score**
against the application's existing score at matched coverage, reporting hit
rate, downside, large-loss frequency and raw returns. Historical reuse must be
labeled exploratory; later captured forecasts provide prospective validation.
Do not select arbitrary deductions merely to make conflicting examples look
more plausible.

Production integration requires a reproducible current-model artifact, available
point-in-time inputs, shared inference, cache/methodology versioning and tests
that all scoring entry points agree. Risk must affect the numeric score before
its band, explanation, ranking and outcome snapshot are derived.

This document records the requested single-score design. It does not implement
or deploy a new score formula; the conversion from the tested selection policy
to an integrated confirmation score remains unvalidated.

First implementation and calibration result:
[the direct risk-deduction family failed its development screen](confirmation-integrated-risk-results-2026-09-15.md).
The offline implementation tested four nonzero weights against actual frozen
application scores and selected zero. The single-score product decision remains;
this tested mapping is not suitable for promotion on the available evidence.
