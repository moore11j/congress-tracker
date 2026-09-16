# Macro and market-condition experiment

**Result: stronger bearish macro evidence helps only slightly on 30D. No tested rule supports a live scoring change or a >75% claim.** A combined macro/market qualification rule improves the historical 7D hit rate more noticeably, with substantially lower coverage. No production scores, past outcomes, or caches were changed.

## Fixed hypotheses and results

These are ten fixed exploratory policies, not a search for an optimal weight. Every policy retains all original bearish calls. A blocked bullish opportunity is represented only as an abstention in the local simulation; its actual public event and result remain untouched. The same 1,176 completed 30D and 3,061 completed 7D events from the prior study are used, including open and closed theses and all stored outliers. Accuracy uses the unchanged raw-or-SPY definition.

| Policy | 30D accuracy | 30D retained | 7D accuracy | 7D retained |
|---|---:|---:|---:|---:|
| Original calls | **47.02%** | 1,176 (100%) | **51.09%** | 3,061 (100%) |
| Original bullish calls must remain bullish under today's direction formula | 46.97% | 1,173 | 51.08% | 3,056 |
| Treat moderate bearish macro strength/quality symmetrically with moderate bullish macro | 47.08% | 1,166 | 51.17% | 3,031 |
| Multiply bearish macro evidence by 1.5 | 47.12% | 1,165 | 51.17% | 3,025 |
| Multiply bearish macro evidence by 2 | 47.43% | 1,147 (97.53%) | 51.31% | 2,988 (97.62%) |
| Withhold bullish calls against fresh bearish macro | **48.33%** | 1,107 (94.13%) | 52.07% | 2,871 (93.79%) |
| Fresh bearish macro requires additional stock confirmation | 47.78% | 1,128 | 51.65% | 2,935 |
| Weak 7D market trend requires additional stock confirmation | 47.32% | 1,139 (96.85%) | 54.39% | 2,291 (74.84%) |
| Negative 7D and 30D market trends require additional stock confirmation | 47.02% | 1,176 | 51.09% | 3,061 |
| Fresh bearish macro **or** weak market trend requires additional stock confirmation | **47.95%** | 1,099 (93.45%) | **54.86%** | 2,220 (72.53%) |

The best 30D macro veto raises bullish accuracy from 39.98% to **41.27%**, retaining 739 of the original 808 bullish opportunities. It omits 69 bullish calls: 51 were wrong and 18 were correct. The overall lift is 1.31 percentage points, not a transformation of the model.

The combined rule raises 7D bullish accuracy from 47.99% to **52.61%**, retaining 1,247 of 2,088 bullish opportunities (59.72%). It omits 841 bullish calls, including 346 that were correct. Its overall coverage is higher, 72.53%, because all 973 bearish calls remain. Thus the 54.86% headline requires a meaningful reduction in bullish opportunities; it is not an improvement at constant coverage.

## Exact rule definitions

* **Fresh bearish macro:** original snapshot has an active bearish macro source with recorded freshness of no more than ten days. Unknown age is not treated as fresh.
* **Weak market trend:** both SPY and QQQ have negative trailing seven-calendar-day price returns as reconstructed before the original score timestamp. Requiring negative trailing 30D returns as well is the stricter alternative. Missing history is marked unknown, not bullish, and does not trigger a market veto.
* **Additional stock confirmation:** the original price/volume source is active, bullish, and at most seven days old; at least two other bullish sources are active and no more than 30 days old. Macro, price/volume, and government contracts do not count toward those two additional sources. This tests the existing frozen price/volume evidence; it does not add an independently reconstructed sector-relative signal.
* **Macro strength normalization:** original moderate bearish values of strength 50 / quality 61 become strength 70 / quality 77 inside the research calculation, and the macro directional multiplier becomes 1.20 instead of 1.08. Moderate bullish values are already 70 / 77. These are counterfactual values, never written to a snapshot. The aggregate macro cache collapses strong and moderate headwinds into rating 2, so the old component snapshot cannot recover the original headwind magnitude; this is a moderate-strength normalization hypothesis, not an exact reconstruction of a symmetric model.
* **Weight tests:** today's direction classifier is run with bearish macro evidence multiplied by 1.5 or 2. A historical bullish opportunity is retained only if it remains bullish. No new bearish prediction is manufactured. The current-direction replay is included as a comparator so effects of reclassification can be separated from the incremental macro change.

## The market was not clearly bearish at the original score times

Most completed 30D events were scored before the later price decline. The reconstructed trailing returns at their score cutoffs were:

| Last available close | Events using this cutoff | SPY trailing 7D | SPY trailing 30D | QQQ trailing 7D | QQQ trailing 30D |
|---|---:|---:|---:|---:|---:|
| Aug 4 | 167 | +4.11% | +3.56% | +7.16% | +1.58% |
| Aug 5 | 601 | +5.53% | +2.46% | +8.40% | -0.76% |
| Aug 6 | 191 | +3.62% | +2.79% | +4.55% | +0.74% |
| Aug 7 | 123 | +3.51% | +3.74% | +5.09% | +1.63% |
| Aug 10 | 30 | +2.03% | +2.39% | +2.97% | -0.64% |
| Aug 11 | 64 | -0.10% | +2.07% | -0.75% | -0.97% |

Cutoff dates are when data was available to the score, not entry-session dates, so these counts differ from the earlier entry-date table. Only 64 of the 1,176 completed 30D events had both benchmark seven-day returns negative at scoring. None had both benchmarks negative at both tested horizons. Applying the market decline observed during the outcome window to an earlier score would create look-ahead bias.

The market rule triggers more often for the later 7D cohort: 1,233 of 3,061 events have both seven-day benchmark returns negative. This explains why its effect differs across horizons. It does not establish that the rule will improve 30D as those later entries mature.

## COT and the confirmed implementation asymmetry

The application's Macro Positioning component is based on CFTC positioning mapped into ticker-specific headwinds/tailwinds. COT is a positioning measure, rather than a contemporaneous price-trend indicator. Any future test using raw historical COT changes or percentiles must respect the actual release time, not treat Tuesday position dates as Tuesday availability. The [CFTC explains its usual Friday release of Tuesday positions](https://www.cftc.gov/es/node/128971).

The ticker-level macro interpretation assigns a rating of 2 to aggregates at or below -0.35, rating 4 at or above +0.35, and rating 5 at or above +1.25. Both confirmation input paths convert rating into `strength = 30 + 10 * rating` and `quality = 45 + 8 * rating`. This gives bearish macro strength/quality of 50/61 while bullish can receive 70/77 or 80/85. The separate asset-level COT rating uses absolute magnitude; it is the ticker-summary conversion that has this asymmetry.

The asymmetry is real, but the experiment shows that fixing it alone hardly moves these historical accuracy figures. It should not be presented as the explanation for the entire bullish underperformance. Direction, magnitude, data quality, and freshness should ultimately have separate semantics; a bearish direction should not automatically mean low-quality evidence.

## Validation and limitations

* All results are exploratory and retrospective. The earlier security holdout has already been examined. Its repeated diagnostic results are in `macro-results.json`, but are not described as a fresh test. The six-session 30D cohort still cannot support a non-overlapping temporal validation.
* Original macro and stock evidence comes from immutable opening snapshots. SPY/QQQ context is separately reconstructed from 97 existing historical cache rows read with a production read-only transaction. Nothing was backfilled into the event records.
* The benchmark calculation selects only attributed, positive-price rows dated before the score's last eligible completed close. It uses raw closes consistently, prior available closes for calendar offsets, a maximum four-day staleness tolerance, and 20:00 UTC regular closes for this July–September window. It does not invent weekend or missing sessions. This narrow implementation is a research calculation, not a general exchange-calendar implementation for deployment.
* The historical cache was updated after many original scores. This is not a vintage-provider-data audit: later revisions or historical price-basis differences may affect the reconstructed signals. Raw COT report history, market breadth, volatility, and independent ticker/sector relative strength were not added in this experiment. They remain untested features, not silently approximated by the outcome returns.
* No model was fitted and no parameter was retuned after viewing this experiment's results. The ten policies were fixed in the script before execution. Selecting the highest number now is still post-hoc selection; later validation is required.
* Historical prices, scores, directions, grading, event continuity, and measured returns remain immutable. Simulation abstentions affect only research denominators. Future opening dates, requalification and reversal paths are not replayed.

## Recommendation

Do not promote these policies into live confirmation scoring yet. The fresh-macro veto gives only a small 30D lift. The combined rule is a candidate for a separately approved prospective shadow comparison because it improves 7D at meaningful, explicitly reduced bullish coverage; its 30D advantage remains unproven. Preserve bearish logic initially, evaluate the candidates against later maturing 30D events, and retain both accuracy and coverage as promotion criteria. Do not increase COT weight broadly on the assumption that it identifies every weakening market in advance.

Research scripts: `export_market_context.py`, `analyze_macro_hypothesis.py`, and `test_macro_research.py` under `backend/scripts/research/`. The original research scripts are unchanged except for these additions. Ten invariant tests pass across the research suite, including exclusion of future closes, unknown market data, stale macro handling, and immutable source inputs. The original cohort hash remains `de0885ffc1c1b026541b0cb364efd7c0b5dcf9feb62108d7d08473a49303613b`. Additional input/source hashes and all per-event reconstructed contexts are preserved in the Git-ignored local `frontend/test-results/confirmation-research/macro-results.json`.
