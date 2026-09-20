# Filing-dated fundamentals: continued model search

**No candidate passes the prewritten improvement criteria.** The strongest new
hit-rate lead is a fundamentals-only tree model: its fixed top quartile has a
60.03% positive-return rate, versus 52.64% for the eligible universe, averaged
equally across entry dates. However, that selection's mean 30-day price return is
2.36%, versus 2.56% for the universe. More winning observations did not produce a
demonstrated return advantage. This is not a validated replacement score.

This pilot follows the completed [rank/calibration study](confirmation-rank-calibration-results-2026-09-15.md),
which also rejected its candidates. The final 163-symbol hash block remains
reserved. No live score, historical outcome, production cache, existing prospective
study or reminder was changed.

## New information, rather than more price-model tuning

The prior cached fundamentals lacked reliable filing availability dates. This
experiment downloaded public SEC company-facts records and reconstructed features
using their filing dates and accession numbers. The APIs and their limitations
are documented by the [SEC](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).

The fixed sample selected 96 current-SEC-mapped development companies and 96
validation companies by deterministic hash, before downloading financial data.
There are no shared SEC issuer IDs between those groups. Acquisition produced
181 responses with US-GAAP facts, nine without that taxonomy, and two HTTP 404s.
Missing names were not replaced. Requests used the existing repository research
User-Agent and a delay greater than half a second; no paid data or credentials.

Only 10-K/10-Q facts and amendments filed strictly before each prior-close
decision date are usable. Quarterly flows must span 75–110 days; YTD and annual
values cannot become fake quarterly revenue. Balance-sheet items use instant
facts. Margins require matching periods and currency. Later restatements are
unavailable to earlier predictions, and current inputs older than 200 days become
missing. Missing Q4 values are not inferred from annual-minus-YTD in this pilot.

The new features include revenue growth and its change, margins and margin change,
income growth, quarterly income/assets, liabilities/assets, cash/assets, current
ratio, filing age and availability indicators. Values retain filing provenance.
All **206,636 recorded filing references** in the prepared features predate their
decision date. Six focused reconstruction tests pass.

These are reconstructed public filing facts, not proof the app ingested them on
the filing date. Current SEC extraction, mapping/survival bias, company changes,
tag coverage, and possible archive revisions still limit a vintage backtest.

## Fixed test

Two classifiers—regularized logistic and shallow extra-trees—compare price-only,
fundamentals-only, and combined inputs. Every comparison uses identical observations
with prior-date financial coverage. Model settings were frozen before evaluating
the new inputs. Both raw-positive stock returns and positive stock-minus-SPY
returns are evaluated separately, with zero counted as nonpositive.

Training uses development-stock outcomes matured before the quarter start minus
six months. The later window supplies the frequency baseline only; the failed
calibration procedure from the preceding experiment is not reused. Validation
stocks never enter model fitting or imputation. Date weights prevent better-covered
weeks from dominating fitting.

Five evaluation quarters span April 2025–June 2026. There are 5,246 development
opportunities across 83 companies and **5,175 validation opportunities across 84
companies**, on 64 entry dates. One date has no measured company in this smaller
sample. The prepared wider date range has 9,318 development and 9,215 validation
rows; 1,452/1,283 prior-price observations lack a current usable revenue or asset
fact, and 83/84 otherwise eligible observations lack a measured outcome.

## Raw-return results on validation companies

| Fixed input/model | Full direction accuracy | Brier error | Top-quartile positive-return rate | Top-quartile return advantage over universe |
|---|---:|---:|---:|---:|
| Price logistic | 49.45% | 0.25434 | 53.92% | −1.55 pp |
| Price extra-trees | 49.93% | 0.25295 | 55.12% | −1.02 pp |
| Fundamentals logistic | 53.57% | 0.25330 | 56.46% | +0.20 pp |
| Fundamentals extra-trees | 51.86% | 0.24910 | **60.03%** | **−0.20 pp** |
| Combined logistic | 50.51% | 0.25833 | 52.51% | −0.87 pp |
| Combined extra-trees | 50.16% | 0.25133 | 56.00% | −0.45 pp |

Direction accuracy is observation-weighted; top-quartile results are equal-date
averages. Top-quartile coverage is approximately 25%, with 1,331 selected
overlapping opportunities per candidate. These quantities should not be mixed or
advertised as full-coverage model accuracy.

The always-positive baseline reaches 52.56% direction accuracy, compared with
53.57% for the fundamentals logistic model. Its 4.12-point improvement over its
matching price-only model has a paired block interval of 1.93–6.20, but the price
model itself performs poorly. The small Brier improvement has an interval spanning
zero (−0.00227 to +0.00391), failing the joint improvement rule.

The fundamentals tree model improves Brier loss versus price-only by 0.00384
(interval +0.00032 to +0.00663), but its direction-accuracy improvement of 1.93
points is uncertain (−1.69 to +5.67). Combining the financial and price inputs
does not solve this; combined logistic significantly worsens probability error.

## Why the 60% lead is not called a working return model

The fundamentals tree selection improves raw hit rate over its same-date universe
by 7.39 points, with an eight-week block interval of +1.66 to +10.93. But its
return advantage is −0.20 points, with an interval of −1.14 to +1.09. Return
advantage is positive in only three of five quarters, below the required four.
Its selection return improvement over the price-only tree is also uncertain.

The fundamentals logistic selection has a small positive average return advantage
(+0.20 points), but its interval is −0.82 to +1.97 and it improves returns in only
two quarters. Neither selection passes. A higher win rate with smaller winners or
larger losses does not establish useful expected-return improvement.

For SPY-relative direction, the strongest combined model reaches 57.16%, versus
56.93% for always predicting no outperformance. No financial-input model improves
both paired probability error and direction accuracy over its price control with
uncertainty excluding zero. Relative success cannot rescue a failed raw-return
objective under this protocol.

All intervals are exploratory: multiple models and previous hypotheses have been
examined, the historical market periods are reused, and overlapping 30-day
observations remain dependent. The eight-week bootstrap accounts for common-date
exposure and much serial overlap but does not create new independent regimes.
Reported returns are gross price returns; dividends, trading costs, borrowing,
turnover, and capital allocation are excluded. No portfolio profit claim is made.

## What the search has added

The work produced a reproducible filing-date financial reconstruction and identified
a narrow positive-return-frequency lead. It did not produce a robust general
30-day forecast or justify turning today's evidence score into a probability.
The important unresolved question is whether a model can improve the distribution
of returns, including loss size, rather than selecting more small winners.

Further work should introduce an independently justified hypothesis or new evidence,
such as historical earnings surprises with release timestamps, rather than tune
these already-inspected thresholds. The existing prospective insider study remains
separate and unchanged. No new future run or reminder was created by this research.

## Reproduction

Protocol: `docs/confirmation-sec-fundamentals-protocol-2026-09-15.md`.
Scripts under `backend/scripts/research/`: `fetch_sec_fundamentals_pilot.py`,
`sec_filing_features.py`, `sec_filing_model.py`, `test_sec_filing_features.py`.
Local artifacts under `frontend/test-results/confirmation-research/sec-filing-pilot-2026-09-15/`
retain the frozen symbol selection, SEC response cache, fetch audit, prepared
features/provenance, predictions, complete results, and file-hash manifest.

```powershell
$env:PYTHONPATH='frontend/test-results/confirmation-research/direct-return-deps;backend/scripts/research'
$env:OMP_NUM_THREADS='4'
python -m unittest backend/scripts/research/test_sec_filing_features.py
python backend/scripts/research/sec_filing_model.py frontend/test-results/confirmation-research/sec-filing-pilot-2026-09-15
```

Model reruns use the cached SEC files and never connect to production. Preserve
the existing artifact directory when making a modified future experiment.
