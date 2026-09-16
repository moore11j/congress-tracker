# Confirmation model research: 30D priority

**Decision: no tested candidate establishes >75% 30D accuracy on held-out events. Do not change the live model on the strength of this study.** Some alternatives improve the numerical hit rate, but much of that improvement comes from suppressing bullish calls or making almost everything bearish. None demonstrates a reliable, broad improvement in bullish forecasting.

This was read-only research. Past events, opening scores and directions, entries, measured outcomes, grading rules, production caches, and the live confirmation model were not changed. Counterfactual predictions exist only in local research outputs. No deployment was performed.

## The actual cohort

The export contains all 3,075 verified public continuous events, using the same original-anchor projection as the ledger. It includes completed measurements on open and closed theses. It does not double-count internal score updates or open separate events at scoring-version transitions.

| Horizon | Completed | Correct | Overall accuracy | Bullish | Bearish |
|---|---:|---:|---:|---|---|
| 30D | 1,176 | 553 | 47.02% | 323/808 = **39.98%** | 230/368 = **62.50%** |
| 7D | 3,061 | 1,564 | 51.09% | 1,002/2,088 = **47.99%** | 562/973 = **57.76%** |
| 90D / 180D / 365D | 0 each | — | Not yet measurable | — | — |

The page's directional filters work on its 500-event preview, which explains why those percentages can differ from this full-ledger breakdown. No claim is made that the original opening score is a calibrated probability: a score of 80 does not imply an 80% chance of a correct outcome.

The existing accuracy definition is preserved exactly: bullish wins on a positive ticker return **or** outperformance of SPY; bearish wins on a negative ticker return **or** underperformance of SPY. Depending on the stock and benchmark returns, both counterfactual directions can satisfy that rule. The analysis does not replace it with a different target to manufacture higher accuracy. Plain up/down accuracy is also reported: **43.20% for 30D** and **47.04% for 7D** under the original calls.

All 1,176 completed 30D events have actual frozen source payloads and evidence-provenance records. None uses placeholder component inputs. The older August report, based on only 36 measurements, is not an adequate calibration reference for this cohort.

## Why this is not yet a temporal backtest

The 30D cohort covers only six opening sessions:

| Opened | Events |
|---|---:|
| August 5 | 141 |
| August 6 | 479 |
| August 7 | 193 |
| August 10 | 268 |
| August 11 | 23 |
| August 12 | 72 |

The earliest target close was September 4, after the last opening session in this sample. Therefore **zero 30D labels would have been available to train a model before any of these events opened**. Splitting August 5–7 into training and August 10–12 into testing would still use future outcomes to train the model. It would not be a deployable historical replay.

Instead, this study uses a fixed **security-separated, same-period holdout**: 709 training events, 248 validation events, and 219 locked test events. All events for the same security remain in one partition, even if the ticker or direction changes. Model/threshold selection uses validation results, then the test partition is evaluated without changing the selection. This tests transfer to other securities in the same market episode; it does **not** establish transfer to later market conditions.

Reported Wilson intervals and the supplemental security-cluster bootstrap quantify sampling variability within this episode. They do not capture market-regime uncertainty. The distinction follows the [scikit-learn guidance on leakage](https://scikit-learn.org/stable/common_pitfalls.html) and [time-dependent validation](https://scikit-learn.org/stable/modules/cross_validation.html).

## Models tested

Inputs are 102 features constructed only from the opening snapshot: original score, source count, each source's presence/direction/strength/quality/contribution/freshness, and aggregate agreement, durable-source counts, and directional evidence. Identity, entry prices, future returns, close status, latest confirmations, and later-enriched regime features are excluded. No 30D event has the newer frozen regime/score-delta feature block, so historical SPY/sector-relative-trend gating cannot be evaluated from those original inputs.

The comparison includes 52 simple qualification rules and ten learned model configurations: three regularized logistic regressions, three shallow decision trees, two random forests, and two shallow boosted models. Learned configurations are tested as bullish-only qualification, replacement of weak bullish calls with bearish calls, unrestricted directional classification, and selective directional classification. Thresholds are selected on validation data with minimum coverage requirements; retained bullish rules require at least 30 bullish validation calls and 20% of that partition's bullish opportunities. The analysis is a bounded comparison, not a proof that no conceivable model could do better.

**All accuracies below use the same 219-event test partition.** Coverage is the fraction of those events retained by a candidate. The original 51.60% test baseline differs from the 47.02% full-cohort baseline because this is a smaller, distinct subset.

| Candidate | Test correct / retained | 30D test accuracy | Test coverage | Bullish test accuracy | Interpretation |
|---|---:|---:|---:|---:|---|
| Original calls | 113/219 | **51.60%** | 100% | 64/144 = 44.44% | Reference for this test set |
| Keep all original bearish calls; bullish only when institutional activity is absent | 63/98 | **64.29%** | 44.75% | 14/23 = 60.87% | Best simple rule by validation selection; bullish sample is tiny and unstable |
| Learned bullish gate; original bearish calls unchanged | 73/127 | **57.48%** | 57.99% | 24/52 = 46.15% | Forest depth 8, probability threshold 0.45; modest retained-call improvement |
| Same learned model, flip rejected bullish calls to bearish | 130/219 | **59.36%** | 100% | 24/52 = 46.15% | More bearish exposure; weaker than always bearish |
| Regularized directional classifier | 138/219 | **63.01%** | 100% | 2/5 = 40.00% | Logistic C=0.01, threshold 0.55; 214 of 219 calls become bearish |
| Selective directional classifier | 55/84 | **65.48%** | 38.36% | No bullish calls | Forest depth 4, confidence threshold 0.65; every retained call is bearish |
| Always bearish diagnostic | 139/219 | **63.47%** | 100% | No bullish calls | Necessary regime benchmark, not a deployment recommendation |

Filtering a call does not make its historical prediction correct. It changes which opening opportunities a hypothetical future policy would admit. The original event and result remain in the ledger. These simulations use the existing entry/measurement endpoints; they do not replay later re-qualification, new opening dates, altered reversal paths, a complete stock universe, or a trade execution strategy.

The simple institutional-absence rule is not a justified instruction to remove institutional data. Its retained bullish accuracy is **32.43% on validation (12/37)**, **60.87% on test (14/23)**, and **44.77% across the full historical cohort (77/172)**. The apparent total improvement comes partly from retaining every bearish event while dropping most bullish events. That instability is not evidence of a robust 60% bullish edge.

## Can anything show greater than 75% retrospectively?

Yes, but the examples fail the relevant checks:

* The selected directional forest scores **80.08% on 472 historical events** when its training events are included in the evaluation. It retains only 40.14% of the cohort, issues **no bullish calls**, falls to **73.08% on validation**, and reaches only **65.48% on the locked test**. Its test Wilson interval is 54.83–74.76%. The 80% figure is not an honest generalization estimate.
* Keeping only existing bearish events with opening scores at least 75 produces **77.36% on 53 events**, just **4.51% of all 30D events**. Its full-cohort interval is 64.47–86.55%; only seven such events fall in the test partition, with five correct (71.43%). This says little about broad coverage and nothing about fixing bullish calls.

Thus there are ways to display a historical percentage over 75%, but **no tested model demonstrates the requested improvement at credible coverage on unseen events**, much less in an unseen market period.

## What the bullish evidence says

Higher opening scores are not monotonically associated with better 30D bullish performance:

| Minimum bullish score | Events | 30D accuracy |
|---|---:|---:|
| 40 | 808 | 39.98% |
| 60 | 669 | 38.57% |
| 70 | 436 | 39.68% |
| 80 | 162 | 38.89% |
| 90 | 28 | 57.14% |

The final row is far too small to establish a reliable exception. Stronger-looking source stacks also fail to establish a bullish edge: bullish institutional activity is present in 631 bullish calls with 38.67% accuracy; bullish fundamentals in 187 with 35.83%; bullish price/volume in 466 with 38.63%. Fresh durable agreement gives 41.56% on 77 bullish events. A bearish macro source inside a bullish event is a possible conflict warning: 18/69 = 26.09% correct. These are overlapping, retrospective associations, not causal estimates or independently validated thresholds.

The current code increases bullish direction evidence for durable sources and assigns larger bullish than bearish contributions to fundamentals and institutional activity. The data does not justify increasing those bullish weights again simply because the sources sound durable. A separate, empirically calibrated 30D eligibility/confidence layer is a better research direction than treating source breadth or a high composite score as a high probability of success.

All six SPY measurement windows in this cohort were negative, approximately -0.55% to -2.16%. Across the same event universe, always bullish yields **40.31%**, while always bearish yields **64.46%**. Existing bearish calls score **62.50%**, slightly below that unconditional bearish benchmark. The stronger bearish accuracy therefore cannot yet be attributed entirely to model skill; market conditions explain a substantial part of the gap. The counterfactual baseline does not alter any original bullish event.

## Current versus original model and the other horizons

Production metadata identifies the current methodology as introduced **August 21**. Every completed 30D event in this export opened under the preceding methodology. The current model's 30D entry cohort has not matured. Continuous events still belong to their immutable original opening forecasts; subsequent model updates do not rewrite those forecasts.

Early 7D results by original entry methodology:

| Entry methodology | 7D events | Overall | Bullish | Bearish |
|---|---:|---:|---:|---:|
| Earlier entries | 2,335 | 54.00% | 810/1,528 = 53.01% | 451/807 = 55.89% |
| Current methodology entries | 726 | 41.74% | 192/560 = **34.29%** | 111/166 = **66.87%** |

These cohorts opened in different periods and have different security mixes. This is a warning to investigate bullish qualification, not a controlled estimate of the effect of the model change.

Improving 30D does not automatically improve 7D. On the same 219 held-out early events, the original calls have **57.08% 7D accuracy**. The learned bullish gate has **55.12%** at 58% coverage; the mostly bearish logistic model has **52.97%** at full coverage; the selective forest has **48.81%** at 38% coverage. The simple institutional-absence gate has 61.22% at 45% coverage, but its small bullish sample and validation instability remain. These are secondary diagnostics on the selected 30D models, not independently validated 7D optimizations. No 90D, 180D, or 365D improvement can be measured yet.

## Recommended next decision

1. **Leave historical outcomes and the current model unchanged for now.** None of the tested candidates warrants a 75% claim or immediate promotion.
2. Evaluate a prospective bullish qualification layer separately from bearish logic. Use the newly frozen regime, relative-strength, source-age, conflict, and score-change features as their 30D cohorts mature. Treat fresh relative-price confirmation and opposition from macro/other independent sources as hypotheses to test, not as proven new weights.
3. Require later, non-overlapping outcome cohorts for promotion: training labels must be available before validation/test openings. Preserve security grouping and exclude later-confirmation features. Compare against original calls and unconditional directional benchmarks, with bullish/bearish counts, coverage, raw and SPY-relative results, and return distributions.
4. Set a coverage floor before tuning, so reaching a target by suppressing nearly every bullish event cannot pass. A useful initial research gate would retain at least half of bullish opportunities and all current bearish opportunities, with multiple later cohorts and meaningful bullish sample counts. A >75% total target is an acceptance criterion to test, not something the data currently supports.
5. Keep any proposed model's predictions separate from published history until reviewed. Continue reporting each horizon independently. No shadow deployment, collection change, retraining schedule, or automatic promotion was created by this task.

## Integrity, limits, and reproducibility

The database export enforced a read-only transaction and selected existing records only. It performed no price hydration, recomputation into application tables, cache updates, or methodology registration. All exported original correctness flags exactly match the research implementation of the current grading definition. There were zero snapshot creations after entry dates and zero late evidence-provenance timestamps in the export; these checks do not independently re-audit every upstream source's history.

Stored outliers remain unchanged, including GOSS's previously identified split-basis problem. There are three 30D returns beyond +100% (TJGC, LGHL, PTIX); this study does not assume all are errors or remove them. Even changing the correctness of all three would move the full-cohort baseline by at most 3/1,176 = 0.26 percentage points, though return averages and fitted models can be much more sensitive. No return average here is a simulated portfolio return or includes execution costs. Return magnitudes and model-return comparisons should not be trusted as clean portfolio evidence pending a separately authorized price audit.

Reproducible scripts are in `backend/scripts/research/`: `export_confirmation_cohort.py`, `analyze_confirmation_models.py`, and `test_confirmation_research.py`. The six invariant tests pass, covering immutable inputs, preserved grading (including the OR case), absence of future/identity features, same-security grouping, abstention coverage, unmodified extreme returns, and interval/empty-cohort handling.

The local, Git-ignored export and machine-readable results are under `frontend/test-results/confirmation-research/`. The frozen cohort SHA-256 is `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`; it was identical after analysis. The main output is `model-results.json`; `supplement.json` contains version cohorts, the 7D baseline, and security-cluster bootstrap diagnostics. Analysis uses scikit-learn 1.7.2 in an isolated local research dependency directory. Application dependencies were not changed.
