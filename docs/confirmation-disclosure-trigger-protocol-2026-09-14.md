# Fixed unusual-purchase disclosure experiment

Written before computing disclosure-triggered returns on September 14, 2026. This is offline research, with no application imports, live scoring changes, or changes to the frozen public ledger.

## Hypothesis and cohort

Test the existing hypothesis without tuning its threshold: an insider's disclosed purchase is at least twice the median size of their previous purchases in the same ticker. Require at least three earlier known-dollar purchase filing groups within 1,095 days. Require known, positive net insider buying for the ticker in the preceding 90 days, including the trigger filing. Reuse the previous security exclusions, duplicate handling, and suspect monetary-value quarantine. Do not substitute guessed dollar values.

Collapse qualifying actors into one ticker/filing-date signal. A filing is available the following calendar day because intraday acceptance timestamps are unavailable. Entry is the first observed SPY session strictly after filing, at the stock's official open on a consistent price basis. The 90-day net window is evaluated on filing date plus one day, so subsequent disclosures cannot enter it.

Primary sampling uses a 90-calendar-day cooldown per ticker, beginning with each accepted qualifying disclosure date. Apply the cooldown before inspecting prices, maturity, or returns; a missing-price event still consumes its cooldown. Report all distinct ticker/filing signals and only the first qualifying signal per ticker as fixed sensitivity checks. These are not additional threshold searches.

## Comparison and measurements

Use the same timing and sampling for an ordinary-purchase comparison: at least three prior known purchases, positive known net buying, and no triggering buyer reaching twice their usual size on that filing date. This is an observational comparison, not a randomized or causally matched control. Show calendar-year breakdowns and ticker counts so different exposure periods are visible.

Primary horizon: 30 calendar days after entry, closing on the first SPY session on or after the target. Secondary: 7 calendar days. Preserve the existing bullish correctness definition: a positive stock return OR positive two-decimal SPY-relative return. Also show positive-stock-return frequency separately. A new research entry date does not replace a public event's entry or outcome.

Use only already-exported attributed historical prices. Require stock and SPY entry and target rows from the same provider for each measurement; prefer FMP, then Massive, with no cross-provider endpoint splicing. Undo the cache's adjusted-open factor before comparison to the same provider's raw-close basis. Do not filter on future return magnitude. Report missing prices and immature events separately. Reject calendar coverage gaps exceeding four days at entry/target rather than silently moving the target across a missing-data interval.

## Evidence limits

The rule was selected after earlier ledger and historical-panel research. Reusing those tickers or periods is exploratory. Before looking at returns, classify securities outside the union of the previously inspected public ledger and historical price panel as previously unexamined securities. Report how many can actually be measured; this is still not an untouched future-period test. Exported filing dates do not prove that the application had ingested those records contemporaneously. Sparse buyer history, survivor coverage, revisions, and reporting-date accuracy remain limitations.

Report descriptive Wilson intervals and ticker-cluster bootstrap intervals, with explicit small-sample limitations. Freeze all cohorts before outcome calculation and hash inputs, cohort membership, and code. Verify the original ledger hash at completion. Do not claim 75% expected accuracy or authorize production promotion from a small favorable subset.

## Coverage supplement, declared after the first run

The original exports left 11 of 40 due unusual-purchase 30D signals and 47 of 103 due ordinary-purchase 30D signals without consistent prices. Preserve the first run as `disclosure-trigger-results.initial.json`. Make one read-only export of existing attributed price-cache rows for **every ticker in both frozen all-filings cohorts**, plus SPY, from 2023 through September 11, 2026. This is a coverage supplement, with no refitting, rule adjustment, fresh price hydration, application mutation, or selective fetching of favorable cases. Report before/after counts. Newly measurable observations were not used to change the rule but remain exploratory rather than prospective validation.
