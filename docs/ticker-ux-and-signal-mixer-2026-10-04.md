# Ticker clarity and Signal Mixer — October 4, 2026

## Scope and delivery

The owner authorized implementing the competitive recommendations and simplifying the ticker page: chart first, research tabs below, source cards alongside, ranges inside the chart, clearer catalysts/risks/watch items, and removal of redundant table/source and buy/sell controls. Existing research tabs, activity tables, chart markers and entitlements remain available.

Implemented against the October 4 source; validation baseline archived from `98e65467`. The owner approved commit and deployment after preview review. Release `bfb7602d63fbc18a1a00aa5ad47e51f9d1f2e2a6` was pushed and deployed October 4. The separate editorial release `fdafb86f` remains preserved.

Production verification: [Vercel deployment](https://vercel.com/moore11js-projects/congress-tracker/7d5G8MrU6fJHVxxr8hSMkPL9d7Sz) and [Fly workflow 37242718354](https://github.com/moore11j/congress-tracker/actions/runs/37242718354) succeeded. Both public `/api/app-version` endpoints returned the release SHA; all four API/cron/video machines run its image. `/ready` returned status/database `ok`; anonymous Signal Mixer POST returned 401. Signed-in AAPL showed chart-first layout, sidebar, retained marker controls, and Congress staying disabled after a range change. The workflow had a non-fatal Git cleanup annotation; deployment and verification steps succeeded. A later documentation receipt may advance the frontend version without application changes.

Live data limitations: refreshed AAPL context includes `free_cash_flow: null`; the existing public projection strips provider identity, so the snapshot currently displays unavailable cash/source context. Broad mixer studies returned explicit source-record/company-limit errors. These are open data/UX limitations, not a claim of comprehensive historical coverage. Saved rules and live alerts remain unimplemented.

A bounded signed-in production study completed: September 1–October 4, 2026, Congress purchases following recorded contracts within seven days, zero fees and 10 bps slippage. From 236 trigger rows, 155 lacked prior confirmation and 79 were duplicate symbol/day rows; two setups matched. Both remain pending at 30/90/365-day horizons, with no shortened or invented outcomes. This verifies the deployed study/diagnostics path, not mature historical return coverage.

## Ticker changes

- [Ticker page](../frontend/app/ticker/[symbol]/page.tsx): chart precedes Overview/Financials/Ownership and other tabs in the main column; existing evidence cards remain in the right column. The outer Activity view and Trade side panels are removed; legacy source/side query parameters no longer hide tables or restrict the main page to buys/sells. Table pagination and access boundaries are retained.
- [Chart loader](../frontend/components/ticker/TickerChartLoader.tsx): 1D, 5D, 1M, 3M, 6M, 1Y controls appear inside the chart in populated, loading, unavailable and stale states. Ranges update the chart without navigating the page or hiding tables; marker/indicator selections persist across successful range changes. Initial URL range remains supported. Daily-price resolution is labeled; no intraday feed is introduced. The chart starts promptly when foreground data is ready, using the existing hydration/request controls.
- [Decision panels](../frontend/components/ticker/TickerDecisionPanels.tsx): emerald catalysts, rose risks, sky changes and amber watch items use icons, distinct headings and subtle bordered backgrounds. Two items per category are initially shown; additional items remain available through native keyboard-accessible disclosures. Source links, dates, full descriptions and Premium gating remain.
- [Fundamentals snapshot](../frontend/components/ticker/TickerFundamentalsSnapshot.tsx): growth, free cash flow, net debt/EBITDA and EV/EBITDA appear near the score, with period/provider/retrieval/stale/missing context and a link to Financials. The existing detailed sidebar card remains. [Cache summary](../backend/app/services/fundamentals_cache.py) adds descriptive cash/provider context outside the scoring calculation. No Confirmation Score weights, historical forward-P/E average or new safety rating is introduced.

## Signal Mixer v1

[UI](../frontend/components/backtesting/SignalMixerWorkbench.tsx) is an expandable workspace at the top of Backtesting, with a ticker link to `/backtesting?strategy=mixer#signal-mixer`. [API](../backend/app/routers/backtests.py) exposes `POST /backtests/signal-mixer` under the existing authenticated Premium backtesting feature and rate limit. No database migration is required.

[Service](../backend/app/services/backtesting/signal_mixer.py) provides a bounded historical event study:

- Trigger: insider or Congress purchase with an explicit filing/report date. Optional distinct-buyer minimum within the selected window.
- Earlier confirmation: the other purchase source, a positive-value government contract, or a published analyst upgrade. An upgrade is not an earnings-estimate revision.
- Window: 1–90 preceding calendar days, excluding same-day confirmations because date-only records cannot establish order. Optional price-above-SMA50 condition uses only information through the trigger date.
- Entry: next common stock/SPY daily close after disclosure, within seven calendar days. Horizons: 30/90/365 calendar days from entry, with next common close within seven days. Incomplete horizons remain pending.
- Adjustable fees and slippage on both entry and exit, applied equally to stock and benchmark. Per-horizon output includes sample size, positive-return rate, beat-SPY rate, median net/excess return, worst return, loss count and exclusion counts. Up to 50 auditable outcome examples include trigger/confirmation IDs and dates.
- One setup per symbol/day; repeated overlapping holdings are excluded separately for each horizon. Maximum five-year request window, 20,000 rows per source and 500 trigger symbols; oversized requests fail explicitly rather than silently truncating.

Contracts use the later of award date and Walnut's first recorded observation. That is a conservative data-availability proxy, not verified original public announcement timing. Purchases missing explicit filing dates are excluded. Analyst history uses provider publication dates; later corrections and incomplete history remain limitations.

This is not a capital-constrained portfolio, a survivorship-free dataset, or a validated predictive strategy. Stored close-price/corporate-action treatment is reused; dividends, taxes, liquidity and market impact are not modeled. Small samples and repeated filter searches do not prove an edge. Saved mixer rules and continuous alerts are not implemented. The existing general portfolio engine still excludes execution costs; the new mixer does not complete roadmap R6.

TradingView Advanced Charts migration and historical forward-P/E comparisons remain deferred pending demonstrated need, permitted access and comparable historical estimates. No pricing changes.

## Validation

- Python **3.14.2**, using the existing research-ops environment and isolated SQLite: **66 passed** across `test_signal_mixer.py`, `test_backtesting.py`, and `test_fundamentals_cache.py`. Cases cover sequence ordering, missing filing dates, observation-date contracts, distinct buyers, SMA look-ahead prevention, next-close execution, costs, overlap, pending horizons, missing prices, request bounds, 401/403 gates, database-backed outcomes and unchanged fundamental scoring.
- Frontend focused regression selection: **99 passed / 5 failed**. All five failures reproduce on unchanged `98e65467`: one trade-table expectation, one analyst-tab expectation, and three Signals/institutional hydration expectations. The two obsolete outer-control/range assertions were updated for the requested behavior. No full-suite pass is claimed.
- TypeScript and the production build passed. Build API bases explicitly targeted the local fixture service; this does not establish live data coverage.
- Real components rendered locally with explicitly synthetic data and a local fixture API. Desktop and effective 390px mobile inspection covered chart controls, persistent marker selection after range changes, full-text expansion, Financials navigation, mixer submission/results and changed-input notices. Mobile document width was 375px within a 390px viewport, with no page-level horizontal overflow in the fixture.
- Local-only screenshots, mock API, preview source and test/build logs are in ignored `artifacts/ticker-ux-20261004/`. The temporary preview route was removed before the build. This is populated component QA, not a production-authenticated ticker/data-coverage audit.
- A read-only public cached AAPL context request returned `public_context_cache_miss`; no production data, account settings, subscriptions or delivery configuration were changed. No paid generation or external messages.

Release verification above supersedes the original local-only delivery boundary. Broader source coverage, fundamentals source wording/data availability, practical mixer defaults and the known suite failures remain follow-up work.
