# Walnut native chart audit — 2026-08-22

## Decision

The frontend has one external charting runtime: `lightweight-charts@5.1.0`. It is used only by `PremiumTickerChart`, Walnut's price/candlestick terminal. Every other data visualization below is a custom SVG (or CSS distribution bar). No Recharts, Chart.js, ECharts, Nivo, Victory, Visx, or D3 dependency is installed.

The native SVG work should therefore standardize on the shared Walnut chart layer rather than add a large dependency. `components/charts/WalnutLineChart.tsx` is the first primitive: it has RAF-batched nearest-point lookup, pointer/touch scrubbing, keyboard inspection, a bounded multi-series tooltip, first-mount reveal, and reduced-motion support. `WalnutChartContainer.tsx` provides fixed-size skeleton, isolated error, and a 320px near-viewport lazy boundary. `chartFormatters.ts` and `chartPerformanceUtils.ts` are the shared formatting/performance utilities.

## Inventory

Counts are the frontend bounds visible in implementation; API values can be lower. “Parent” means SSR/client parent data; “client” means a component fetch.

| Route / surface | Component / file | Type and library | Typical points / source | Loading, interaction, status |
| --- | --- | --- | --- | --- |
| `/ticker/[symbol]`, ticker activity, insider issuer chart | `PremiumTickerChart.tsx` | Line/area/candles/histogram; TradingView Lightweight Charts | Daily price bundle, indicators, volume and event markers; client-fetched by `TickerChartLoader` or passed by insider | Skeleton and detailed crosshair/pinned readout; desktop/touch support delegated to Lightweight Charts; **A: leave internals unchanged** |
| `/backtesting` | `BacktestChart.tsx` | Two-series SVG line; **Walnut shared line chart** | Backtest timeline, passed from parent; usually daily run history | First-load left-to-right reveal, multi-series tooltip/crosshair, touch scrub, keyboard, fixed height; **B: migrated** |
| `/member/[slug]` | `PerformanceChart.tsx` | SVG performance/alpha line with event markers | Member + benchmark performance parent data; potentially hundreds of daily points | Existing rich tooltip/pinning, pointer lookup, marker details; **B: next migration** |
| `/member/[slug]` | `MemberAnalyticsClient.tsx` | SVG buy/sell mini bars and donut | Client trade/activity payload; 24 buckets / category rows | Basic hover, no shared loading boundary; **C: migrate chart primitives** |
| `/insider/[slug]` | `InsiderAnalyticsClient.tsx` | SVG buy/sell mini bars and donut, plus ticker terminal above | Client trade data; bounded trend buckets | Basic hover; ticker instance is **A**, analytics SVG is **C** |
| Institution profile | `HoldingsAllocationChart.tsx` | SVG donut | Parent 13F holdings; top 10 plus Other | Mouse/focus tooltip and semantic title; no touch/pinned state; **B: migrate interaction/skeleton** |
| Profile landing dashboards | `EnhancedProfileDashboards.tsx` | Donuts, compact SVG bars/lines, sparklines | Overview endpoints; charts bounded at 6–24 or 8–12 periods | Page skeleton exists; chart-local interaction/loading varies; **C: split into shared primitives** |
| Government department/profile | `EnhancedGovernmentDashboard.tsx`, `app/departments/[slug]/page.tsx` | SVG line/area, bars, donuts | Department profile/overview data; bounded series | No shared hover/crosshair; **C: migrate** |
| Outcomes ledger | `OutcomeLedgerClient.tsx` | SVG score-band bars and scatter plot | Client/filter-derived snapshot data; may grow with filters | Own state/tooltip behavior; dense scatter needs sampling review; **B: priority migration** |
| Compare | `app/compare/[left]/[right]/page.tsx`, `CompareAnalytics.tsx` | Comparison data and cards; no separate chart engine detected | Parent/API comparison series | Reuse shared multi-series line primitive when chart surface is enabled; **B** |
| Strategies | `StrategyDetail.tsx` and strategy cards | Data-rich cards; no standalone SVG chart implementation detected | Parent/API strategy data | Keep current product surface; introduce sparklines only where temporal context is supplied; **D candidate** |
| Insights / macro | `InsightsMacroPositioningPanel.tsx`, `MarketSnapshot.tsx` | SVG gauges/macro distributions | Parent/API market snapshot data | Static/low-density visualizations; **C: apply shared container only where async** |
| Watchlists / monitoring | `WatchlistDetailContent.tsx`, `WatchlistRecentActivity.tsx`, `MonitoringDashboard.tsx` | SVG micro charts/distributions | Client/parent watchlist data | Small bounded visuals; **D: optimize separately** |
| Market pressure | `MarketPressureMapClient.tsx` | SVG heat map/export chart | Client map data | Specialized export/interaction; preserve separate renderer; **C: evaluate independently** |
| Ticker subpanels | `TickerAnalystConsensusTab.tsx`, `TickerFinancialsPanel.tsx`, `TickerOwnershipPanel.tsx`, `TickerValuationTab.tsx`, `TickerKpiNavigation.tsx`, `TickerSignalsSourceCardClient.tsx`, `TickerInstitutionalSourceCardClient.tsx`, `DecisionTrendChart.tsx` | SVG mini charts / CSS bars | Ticker API data, parent/client depending on tab | Small or secondary graphics; ticker price chart itself remains **A**; rest **D/C** |

Files such as `WalnutBrandMark`, search icons, landing decoration, and other non-data SVGs are intentionally excluded from the chart inventory.

## TradingView boundary

`PremiumTickerChart.tsx` is the sole `createChart()` call and sole import consumer of TradingView Lightweight Charts. It powers the main ticker chart, ticker activity alias, and the insider issuer stock-chart embed. Installed version: `5.1.0`.

It already uses native series, markers, crosshair subscriptions, a skeleton, and a wrapper readout. It must not be reimplemented as SVG or have its data pipeline changed. A supported container opacity transition is safe; no internal DOM/path manipulation is allowed. The documented API exposes `LastPriceAnimationMode`, which is an updating-last-price behavior, not a supported initial left-to-right series-reveal API. No TradingView initial line-draw animation is enabled.

## Shared visual specification

- Line: 2.8px primary; 2px dashed benchmark; rounded joins/caps.
- Grid: `rgba(148,163,184,.12)`; axis labels 11px tabular numerals.
- Padding: 18 / 84 / 34 / 64 for shared SVG time series.
- Tooltip: fixed 224px, edge-clamped, dark opaque panel; one row per active series.
- Crosshair: one subtle vertical line plus per-series marks.
- Motion: 560ms ease-out reveal, 240ms skeleton-to-chart fade; instant render under reduced motion.
- Touch: `pan-y` keeps vertical page scrolling available; pointer capture and nearest-index snap provide horizontal scrubbing.
- Status: chart-local skeleton/error states reserve final chart height. Below-fold consumers can opt into `lazy` (320px root margin).

## Staged rollout

1. Completed: inventory, shared utilities/container/line chart, Backtesting migration, regression tests.
2. In progress: profile performance and institutional allocation. Congress performance now uses the shared binary nearest-point lookup, RAF hover scheduling, reveal clipping, keyboard support, and touch scrubbing; the allocation donut now supports touch selection and reduced-motion-safe entry. Generic profile and government line trends, dashboard mix pie charts, and Congress/insider/institution/government activity charts now use shared interactive primitives. Profile-card sparklines now reveal and scrub visually; profile landing sector rows animate and expose source-backed current/prior activity on demand.
3. In progress: Outcomes score-band and event scatter charts now support focus, touch, keyboard inspection, and reveal motion. Compare has no historical chart data in its current API response, so no comparison chart is rendered until a source-backed series is available. Dense outcome datasets remain measured before any display-only downsampling.
4. Finally: strategies, insights, and selected sparklines.

No data transformations, backend calculations, cache headers, entitlements, or TradingView data flows changed in phase 1. No downsampling is currently applied.

## Measurement plan

Capture warm/cold route navigations for congress, insider, institution, department, activities, backtesting, strategies, outcomes, and compare before each phase. Record TTFB, FCP/LCP, primary chart availability, chart fetch duration, JS request/transfer size, and pointer update frequency. This repository has no checked-in Playwright configuration, so screenshots and recording require a running authenticated app/environment; phase 1 does not claim before/after production metrics.
