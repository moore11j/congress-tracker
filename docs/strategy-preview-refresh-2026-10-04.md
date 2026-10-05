# Strategy directory preview refresh — October 4, 2026

The directory's **Performance vs SPY** preview still read the archived `equityCurve`, although the detail API already supplied `modelChart`. This left July/August preview dates after the earlier daily-chart release.

The directory and detail components now use the same `strategyChartView` selector. Active models use daily points and matching return/CAGR/drawdown values, cash, dates and coverage notices. Missing daily data never falls back to a stale backtest. Newly activated models keep the first-session notice; historical-only models and an explicitly selected Historical research detail view retain their archived data. Date-only labels use UTC and display the day/year.

Directory previews read the existing daily model cache through `getStrategy`, which uses `cache: no-store`; the page is forced dynamic. The deployed weekday chart-refresh job continues to update that same cache after each market close. No extra scheduler, provider hydration, strategy evaluation or email job is introduced. October 4 is Sunday, so current priced curves end October 2; Blake Moore awaits its first session.

The directory rankings remain archived research rankings and are now explicitly labeled as such, distinguishing their multiyear metrics from the daily preview. This change does not recalculate or combine historical research and live model results.

## Validation and release

- Ten focused frontend checks pass, including rendered directory/detail regressions with intentionally conflicting old and new curves, matching metrics/cash, missing-cache behavior, partial coverage, first-session state and explicit historical selection.
- Isolated candidate production build passes, including type checking and all 67 static pages. Live deployment verification is pending.
- No backend change, production data write, email or preference change. Concurrent ticker work and shared documents are preserved.
