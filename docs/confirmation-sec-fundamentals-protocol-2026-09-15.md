# Filing-dated SEC fundamentals pilot

Written after the rank/calibration study failed its continuation criteria and
before downloading/evaluating the new financial inputs. No final reserve symbols
are consumed by this pilot. All work remains local research.

Hypothesis: filing-dated financial growth, profitability and balance-sheet ratios
add information absent from the failed price-only models. Existing reconstructed
fundamental snapshots using period-end plus 45 days are not used.

Use SEC company-facts data with accession number, period dates, and filing date.
The SEC describes these public, unauthenticated APIs in its
[official documentation](https://www.sec.gov/search-filings/edgar-application-programming-interfaces).
Use the repository's existing research User-Agent, at most two requests/second,
cache raw responses locally, and stop on access/rate-limit failures. No paid data.

From each already-defined development and validation symbol group, select the
first 96 current-SEC-mapped symbols under fixed SHA256 ordering `sec-filing-v1|TICKER`.
Selection happens before financial downloads, without looking at returns. Current
mapping and historical price survival remain selection biases. Failed/missing
downloads are reported, with no replacement selected based on outcomes.

As of a prior-close decision date, use only 10-K/10-Q facts (including amendments)
whose `filed` date is strictly earlier than that decision date, and whose period
end is no later than that date. Date-only filings are thus unavailable on the same
day. Later amendments/restatements cannot be used earlier. Current SEC extraction
is not an archived vintage snapshot; retain that qualification.

Quarterly facts must span 75–110 days; do not treat YTD/annual amounts as quarters.
Do not derive missing fourth quarters in this pilot. For YoY comparisons, find
an available quarter ending within 35 days of one calendar year before the latest
quarter. Use only USD values, and do not mix taxonomies or currencies. If multiple
tags cover the same period, use a fixed documented priority. Require revenue and
income periods to match before computing margins. Quarterly data older than
200 days and balance-sheet data older than 200 days are missing.

Features: quarterly revenue growth, revenue-growth change, net/operating margins,
margin change, net-income change scaled by prior absolute income, quarterly return
on assets, liabilities/assets, cash/assets, current ratio, filing freshness, and
explicit availability indicators. Missing values remain missing; model imputation
is trained only on fitting rows. SEC tag choices and raw facts are retained.

Reuse the existing rank-study panel and chronology, including outcome-independent
price-feature ranks. Fit original development stocks only; validate separate
stocks. Retain five evaluable quarters April 2025–June 2026. As previously fixed,
base training labels mature before T minus six months; calibration examples are
later and mature before T. For this pilot, evaluate uncalibrated forecasts only:
the previous calibration procedure did not improve them. This choice is explicit
and made before financial results, not a new claim of untouched research.

Two fixed classifiers: rank-study logistic (C=.1) and extra-trees (200 trees,
depth 5, minimum 100 per leaf, max_features=.7). Test price-only, fundamentals-only,
and combined features on identical available observations. Date-balanced training
weights; separate raw-positive and positive-SPY-excess labels. No parameter search.
Report the subset with a current usable revenue or balance-sheet observation;
the same eligibility applies to all three input families and is based on prior data.

Evaluate fixed .5 direction and per-date top-quartile selection. Report Brier loss,
direction accuracy, paired differences versus price-only and simple baselines,
same-date selected hit rate and return, missing coverage, and quarterly consistency.
Use eight-week date-block intervals. Financial improvement must beat its matching
price-only model on both Brier error and accuracy with paired 95% intervals above
zero before it can be a lead. A selection lead additionally needs positive hit and
return advantages versus its price-only selection and the same-date universe,
with paired 95% intervals above zero, at least 50 dates/1,000 selected cases, and
positive return advantage in four quarters. A small pilot success still requires
a separately frozen broader check; it does not permit deployment.
