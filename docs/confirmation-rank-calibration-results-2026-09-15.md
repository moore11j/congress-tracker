# Rank and calibration follow-up

**No candidate passed the prewritten continuation criteria. The final 163-name
hash block remains reserved.** This is a completed additional experiment, not a
repeat of the earlier direct-return model run.

The study exported the next 256 coverage-qualified symbols without using outcomes.
Of these, 254 had measured evaluation opportunities. Four fixed model families
were evaluated before and after calibration, for raw-positive and SPY-relative
targets separately. Twenty within-date ranked price/volume inputs replace the
earlier unbounded price/market feature values. Rank membership includes usable
prior-close observations even when the later outcome is unavailable.

Training and calibration are separate chronological windows with 30-day outcome
purges. January–March 2025 was skipped under the prespecified minimum-history
rule: only eight fit dates were available, below the required 12. The remaining
five quarters span April 2025–June 2026 and 65 weekly dates: 31,717 development
opportunities across 511 stocks and 15,714 validation opportunities across 254.
The validation symbols never enter model training or probability calibration.

## Validation findings

| Fixed model | Raw direction accuracy | Raw Brier error | Top-quartile raw hit rate | Top-quartile return advantage over universe |
|---|---:|---:|---:|---:|
| Rank logistic | 53.51% | 0.24981 | 57.65% | −0.69 pp |
| Rank boosted classifier | 53.47% | 0.25031 | 56.41% | −0.87 pp |
| Rank extra-trees classifier | 53.81% | 0.24917 | 57.78% | −0.65 pp |
| Forward-return-rank regressor | 54.26%* | 0.24874* | 57.31% | −0.29 pp |

*The raw output of the return-rank regressor is a percentile forecast, not a
probability. Its threshold-0.5 direction/Brier diagnostics do not establish a
probability score and cannot qualify for the directional advancement route.

The always-positive baseline achieves 52.70% raw direction accuracy; always
nonpositive achieves 47.30%; the stock trend rule achieves 49.54%. The extra-trees
model beats the trend rule by 4.27 points (eight-week paired interval 1.80–5.62),
but its gain over always-positive is only 1.11 points (−3.35 to +7.40), and its
gain over always-nonpositive also has an interval including zero. The latter
fails the prewritten directional gate. This shows why a weak trend baseline
alone is insufficient proof of useful forecasting.

Uncalibrated rank classifiers improve Brier loss against the lagged calibration
frequency baseline (0.25927). However, the separate six-month calibration window
does not help: calibrated raw classifier accuracy falls to 49.78–50.39% and
Brier errors rise to 0.25739–0.25843. Regime changes affect the calibration sample
as well as the base model. No calibrated score is promoted.

The raw top-quartile universe hit rate is 53.56% on an equal-date basis. Several
models increase the frequency of positive returns but decrease average return.
For extra-trees, the top quartile wins on 57.78% of opportunities per date,
but its average return trails the same-date universe by 0.65 percentage points;
the paired return interval is −1.99 to +0.84. That is not a successful return
selection result.

## Simple ranking controls

| Fixed top-quartile ranking | Raw hit rate | Hit-rate advantage | Return advantage | Return-advantage block 95% interval |
|---|---:|---:|---:|---:|
| 12-minus-1-month momentum | 57.30% | +3.74 pp | +0.96 pp | −0.014 to +1.56 pp |
| Five-session reversal | 52.67% | −0.90 pp | +0.73 pp | −0.39 to +1.88 pp |
| Low 60-session volatility | 56.68% | +3.12 pp | −1.18 pp | −2.51 to +0.48 pp |
| Equal-weight composite | 58.80% | +5.23 pp | +0.01 pp | −1.37 to +1.21 pp |

The momentum control is a possible lead, but its return interval still spans
zero. The composite's higher hit rate does not translate to a meaningful return
advantage. None is relabeled as a confirmed winner after the fact, and the learned
models fail to outperform all fixed controls on both selection objectives.

SPY-relative classifiers reach roughly 57.0–57.4% direction accuracy; their
probability improvements versus prevalence remain uncertain. All learned
SPY-relative top-quartile return-advantage intervals include zero. The full raw
and relative results, including unsuccessful candidates, are retained.

## Limits and artifacts

This is exploratory work on previously inspected market periods, despite new
validation securities. There are 78 validation outcomes with absolute raw returns
above 100%, retained rather than removed for their results. Coverage/survival bias,
provider adjustments, serially overlapping 30-day windows and uncertain vintage
data remain. Return averages are gross overlapping opportunities, not a tradable
portfolio's net performance.

The next experiment changes the information set using filing-dated SEC financial
facts. It does not keep tuning this failed price-only procedure or consume the
reserved final sample. No production score, ledger outcome, reminder or prospective
study was changed.

Protocol: `docs/confirmation-rank-calibration-protocol-2026-09-15.md`.
Scripts: `rank_calibration_model.py`, `evaluate_rank_calibration.py`,
`export_rank_validation.py`, and `test_rank_calibration.py` under
`backend/scripts/research/`. Eight focused tests pass, covering temporal purges,
outcome-independent feature ranks, equal date weighting, monotonic calibration,
future-return ranking, fixed selection and the advancement rule.

Immutable local inputs, prepared features, predictions, fit metadata, results,
and the null selection are in
`frontend/test-results/confirmation-research/rank-calibration-2026-09-15/`.
Run with the existing `direct-return-deps` Python dependency path. The model script
saves predictions before the separate evaluator runs. No hyperparameter sweep
was performed.
