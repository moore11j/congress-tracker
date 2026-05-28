import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import {
  DEFAULT_PORTFOLIO_LOOKBACK_DAYS,
  PORTFOLIO_LOOKBACK_OPTIONS,
  PORTFOLIO_MODE,
  normalizeMemberPortfolioChartData,
  normalizeMemberPortfolioEventMarkers,
} from "../lib/portfolioPerformance.mjs";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const memberPage = read("app/member/[slug]/page.tsx");
const insiderPage = read("app/insider/[slug]/page.tsx");
const api = read("lib/api.ts");

function persistedPortfolioFixture() {
  return {
    status: "ok",
    persisted_only: true,
    run_id: 3,
    entity_type: "congress_member",
    entity_id: "J000310",
    lookback_days: 1095,
    mode: PORTFOLIO_MODE,
    benchmark_symbol: "^GSPC",
    requested_start_date: "2023-05-20",
    effective_start_date: "2024-05-20",
    effective_end_date: "2026-05-20",
    effective_window_days: 731,
    effective_window_reason: "first_active_holding",
    no_active_holdings: false,
    curve_quality_status: "warning",
    longest_flat_segment_days: 7,
    pct_days_with_price_gaps: 3.2,
    data_coverage_notes: ["Some holdings used stale prices."],
    summary: {
      starting_value: 100000,
      ending_value: 131356.529,
      benchmark_ending_value: 177286.333,
      total_return_pct: 31.356529,
      benchmark_return_pct: 77.286333,
      alpha_pct: -45.929804,
      cagr_pct: 9.533521,
      max_drawdown_pct: -12.1,
      volatility_pct: 10.2,
      sharpe_ratio: 1.16994,
      win_rate_pct: 55.5,
      average_exposure_pct: 80,
      ending_cash_pct: 20,
      points_count: 3,
      positions_count: 158,
      skipped_events_count: 3,
      skip_diagnostics: {
        non_equity_asset: 2,
        sale_without_position: 1,
      },
    },
    opening_positions_count: 14,
    warmup_diagnostics: {
      warmup_start_date: "2021-05-20",
      visible_start_date: "2023-05-20",
      warmup_days: 1095,
      opening_positions_count: 14,
      sale_without_position_before_warmup: 4,
      sale_without_position_after_warmup: 1,
      opening_position_estimated: false,
      estimated_opening_positions_count: 1,
      estimated_opening_positions_symbols: ["AWK"],
      estimated_opening_positions_value: 8000,
      sale_without_position_before_estimation: 2,
      sale_without_position_after_estimation: 1,
    },
    points: [
      {
        asof_date: "2023-05-20",
        strategy_value: 100000,
        benchmark_value: 100000,
        strategy_return_pct: 999,
        benchmark_return_pct: 999,
        alpha_pct: 0,
        daily_return_pct: 0,
        active_positions: 0,
        exposure_pct: 0,
        cash_pct: 100,
      },
      {
        asof_date: "2024-05-20",
        strategy_value: 115000,
        benchmark_value: 130000,
        strategy_return_pct: 999,
        benchmark_return_pct: 999,
        alpha_pct: -15,
        daily_return_pct: 1,
        active_positions: 40,
        exposure_pct: 80,
        cash_pct: 20,
      },
      {
        asof_date: "2026-05-20",
        strategy_value: 131356.529,
        benchmark_value: 177286.333,
        strategy_return_pct: 999,
        benchmark_return_pct: 999,
        alpha_pct: -45.929804,
        daily_return_pct: 1,
        active_positions: 158,
        exposure_pct: 80,
        cash_pct: 20,
      },
    ],
    positions: [
      {
        source_event_id: 101,
        symbol: "AAPL",
        side: "buy",
        entry_date: "2024-05-20",
        exit_date: null,
        entry_price: 185.25,
        exit_price: null,
        shares: 80.97,
        market_value: 15000,
        return_pct: 12.5,
        status: "open",
        skip_reason: null,
      },
      {
        source_event_id: 102,
        symbol: "MSFT",
        side: "purchase",
        entry_date: "2024-05-20",
        exit_date: "2025-05-20",
        trade_date: "2024-05-15",
        report_date: "2024-05-20",
        entry_price: 410,
        exit_price: 430,
        shares: 19.51,
        market_value: 8000,
        return_pct: -3.2,
        status: "closed",
        skip_reason: null,
      },
      {
        source_event_id: 104,
        symbol: "AWK",
        side: "estimated_opening_position",
        entry_date: "2023-05-20",
        exit_date: "2025-07-01",
        trade_date: "2025-06-25",
        report_date: "2025-07-01",
        entry_price: 100,
        exit_price: 110,
        shares: 80,
        market_value: 8800,
        amount_min: 1000,
        amount_max: 15000,
        return_pct: 10,
        status: "closed",
        skip_reason: null,
        source_type: "estimated_opening_position",
        source_reason: "prior_acquisition_not_found_in_available_disclosures",
        confidence: "estimated",
      },
      {
        source_event_id: 103,
        symbol: "TSLA",
        side: "sale",
        entry_date: null,
        exit_date: null,
        trade_date: "2025-06-01",
        report_date: "2025-06-15",
        entry_price: null,
        exit_price: null,
        shares: null,
        market_value: null,
        amount_min: 1001,
        amount_max: 15000,
        return_pct: null,
        status: "skipped",
        skip_reason: "unmatched_sell",
        skip_category: "sale_without_position",
      },
    ],
  };
}

test("member page renders persisted Portfolio Mode chart and summary metrics", () => {
  assert.match(memberPage, /DeferredMemberPortfolioSection/);
  assert.match(memberPage, /getMemberPortfolioPerformance\(canonicalMemberId/);
  assert.match(memberPage, /lookback_days: portfolioLookbackDays/);
  assert.match(memberPage, /mode: PORTFOLIO_MODE/);
  assert.match(memberPage, /Disclosure-lag realistic portfolio/);
  assert.match(memberPage, /Trades are simulated after public disclosure, not transaction date\. Open positions are carried forward through the selected window\./);
  assert.match(memberPage, /<PerformanceChart[\s\S]*subjectLabel="Portfolio"[\s\S]*chartLabel="Portfolio Return"/);
  assert.match(memberPage, /normalizeMemberPortfolioEventMarkers\(portfolio\)/);
  assert.match(memberPage, /events=\{portfolioEvents\}/);

  for (const label of [
    "Total Return",
    "CAGR",
    "Alpha",
    "S&P Return",
    "Max Drawdown",
    "Sharpe",
    "Win Rate",
    "Simulated Trades",
    "Active Tickers",
  ]) {
    assert.match(memberPage, new RegExp(label.replace("&", "&")));
  }
  assert.doesNotMatch(memberPage, /Simulated Trades \/ Active Positions/);
  assert.match(memberPage, /distinctActiveTickerPositions/);
  assert.match(memberPage, /portfolioTradeCountPromise/);
  assert.match(memberPage, /Methodology details/);
  assert.match(memberPage, /Opening holdings/);
  assert.match(memberPage, /Estimated openings/);
  assert.match(memberPage, /Active tickers at end/);
  assert.match(memberPage, /Excluded/);
  assert.match(memberPage, /Non-simulatable assets/);
  assert.match(memberPage, /Unmatched sales/);
  assert.doesNotMatch(memberPage, /Options, bonds, and other non-equity assets are excluded from the equity portfolio simulation\./);
  assert.match(memberPage, /conservatively estimated and scaled to the simulated portfolio size/);
});

test("member portfolio chart includes ticker-terminal-style hover readout labels", () => {
  const chart = read("components/member/PerformanceChart.tsx");

  assert.match(chart, /Pinned readout/);
  assert.match(chart, /Crosshair readout/);
  assert.match(chart, /Portfolio value/);
  assert.match(chart, /Portfolio return/);
  assert.match(chart, /benchmarkLabel\} value/);
  assert.match(chart, /Relative vs benchmark/);
  assert.match(chart, /Events on this marker/);
  assert.match(chart, /Estimated opening holding: basis is marked at the start of the selected window\./);
  assert.match(chart, /No trades on this date\./);
  assert.match(chart, /onClick=\{handleClick\}/);
  assert.match(chart, /cursorX: number/);
  assert.match(chart, /cursorY: number/);
  assert.match(chart, /readoutHorizontalStyle\(activeReadout\.cursorX\)/);
  assert.match(chart, /readoutVerticalStyle\(activeReadout\.cursorY\)/);
  assert.match(chart, /pointer-events-auto/);
  assert.match(chart, /pointer-events-none/);
  assert.match(chart, /READOUT_EDGE_OFFSET = 56/);
  assert.match(chart, /READOUT_SCROLLBAR_WIDTH = 18/);
  assert.match(chart, /handleReadoutClick/);
  assert.match(chart, /clickedScrollbar/);
  assert.match(chart, /x > WIDTH \/ 2/);
  assert.match(chart, /clamp\(12px, \$\{preferredLeft\}, \$\{maxLeft\}\)/);
  assert.match(chart, /maxHeight: `calc\(100% - \$\{bottom\} - 12px\)`/);
  assert.match(chart, /maxHeight: `calc\(100% - \$\{top\} - 12px\)`/);
  assert.match(chart, /overflowY: "auto"/);
});

test("member portfolio chart uses directional buy and sell arrow markers", () => {
  const chart = read("components/member/PerformanceChart.tsx");

  assert.match(chart, /Buy marker up arrow/);
  assert.match(chart, /Sell marker down arrow/);
  assert.match(chart, /#34d399/);
  assert.match(chart, /#fb7185/);
  assert.match(chart, /M0 -7 L6 0 H3 V7 H-3 V0 H-6 Z/);
  assert.match(chart, /M0 7 L6 0 H3 V-7 H-3 V0 H-6 Z/);
  assert.doesNotMatch(chart, /<circle cx=\{marker\.x\} cy=\{marker\.y\}/);
});

test("member portfolio chart renders one event marker per resolved chart date", () => {
  const chart = read("components/member/PerformanceChart.tsx");

  assert.match(chart, /resolveEventChartPoint\(event\.date, points\)/);
  assert.match(chart, /const key = dateKey\(chartPoint\?\.point\.asof_date\)/);
  assert.match(chart, /eventGroups\.set\(key, list\)/);
  assert.match(chart, /markerTypeForEvents\(groupedEvents\)/);
  assert.match(chart, /markerPath\(marker\.markerType\)/);
  assert.doesNotMatch(chart, /marker\.events\.map\(\(\{ event, x, y \}\)/);
});

test("portfolio event markers expose simulated buys, sells, and skipped disclosures", () => {
  const markers = normalizeMemberPortfolioEventMarkers(persistedPortfolioFixture());

  assert.equal(markers.length, 6);
  assert.deepEqual(
    markers.map((marker) => [
      marker.date,
      marker.symbol,
      marker.side,
      Math.round(marker.value),
      marker.price,
      marker.return_pct,
      marker.simulation_status,
      marker.skip_category,
      marker.source_type,
    ]),
    [
      ["2023-05-20", "AWK", "Estimated Opening", 8000, 100, 10, "simulated", null, "estimated_opening_position"],
      ["2024-05-20", "AAPL", "Buy", 15000, 185.25, 12.5, "simulated", null, null],
      ["2024-05-20", "MSFT", "Buy", 7999, 410, -3.2, "simulated", null, null],
      ["2025-05-20", "MSFT", "Sell", 8389, 430, -3.2, "simulated", null, null],
      ["2025-06-15", "TSLA", "Sell", 8001, null, null, "skipped", "sale_without_position", undefined],
      ["2025-07-01", "AWK", "Sell", 8000, 110, 10, "simulated", null, "estimated_opening_position"],
    ],
  );
});

test("portfolio lookback controls are capped at 3Y and omit All", () => {
  assert.equal(DEFAULT_PORTFOLIO_LOOKBACK_DAYS, 1095);
  assert.deepEqual(
    PORTFOLIO_LOOKBACK_OPTIONS.map((option) => option.label),
    ["30D", "90D", "180D", "1Y", "3Y"],
  );
  assert.deepEqual(
    PORTFOLIO_LOOKBACK_OPTIONS.map((option) => option.value),
    [30, 90, 180, 365, 1095],
  );
  assert.doesNotMatch(JSON.stringify(PORTFOLIO_LOOKBACK_OPTIONS), /\bAll\b/i);
  assert.doesNotMatch(memberPage, />All</);
});

test("changing portfolio lookback wires the selected days into the read endpoint", () => {
  assert.match(memberPage, /getPortfolioLookbackParam\(sp\)/);
  assert.match(memberPage, /query\.set\("portfolio_lb", String\(portfolioLookbackDays\)\)/);
  assert.match(memberPage, /lookback_days: portfolioLookbackDays/);
  assert.match(api, /\/api\/members\/\$\{bioguideId\}\/portfolio-performance/);
  assert.match(api, /lookback_days: params\?\.lookback_days/);
  assert.match(api, /mode: params\?\.mode/);
});

test("portfolio chart normalizes plotted returns from value fields", () => {
  const portfolio = persistedPortfolioFixture();
  const { memberSeries, benchmarkSeries } = normalizeMemberPortfolioChartData(portfolio);
  const finalPortfolioPoint = memberSeries.at(-1);
  const finalBenchmarkPoint = benchmarkSeries.at(-1);

  assert.ok(finalPortfolioPoint);
  assert.ok(finalBenchmarkPoint);
  assert.equal(memberSeries[0].strategy_return_pct, 0);
  assert.equal(benchmarkSeries[0].cumulative_return_pct, 0);
  assert.ok(Math.abs(finalPortfolioPoint.strategy_return_pct - portfolio.summary.total_return_pct) < 0.0001);
  assert.ok(Math.abs(finalBenchmarkPoint.cumulative_return_pct - portfolio.summary.benchmark_return_pct) < 0.0001);
});

test("portfolio chart does not render a flat line when summary return is non-zero", () => {
  const { memberSeries } = normalizeMemberPortfolioChartData(persistedPortfolioFixture());
  const plottedValues = memberSeries.map((point) => point.strategy_return_pct);
  assert.ok(Math.max(...plottedValues) - Math.min(...plottedValues) > 1);
  assert.equal(plottedValues.at(-1) > 30, true);
});

test("missing or failed portfolio responses stay compact and graceful", () => {
  assert.match(memberPage, /\.catch\(\(\) => null\)/);
  assert.match(memberPage, /Portfolio simulation is not available for this lookback yet\./);
  assert.match(memberPage, /Portfolio simulation could not be loaded\./);
  assert.match(memberPage, /Portfolio simulation is temporarily unavailable while this run is revalidated\./);
  assert.match(memberPage, /portfolio\?\.persisted_only === true/);
  assert.match(memberPage, /portfolio\.status === "ok"/);
});

test("member URLs canonicalize bioguide links back to readable slugs", () => {
  const memberSlug = read("lib/memberSlug.ts");

  assert.match(memberSlug, /function isBioguideId/);
  assert.match(memberSlug, /!\s*isBioguideId\(cleanSlug\)/);
  assert.match(memberPage, /if \(slug !== canonicalSlug\)/);
  assert.match(memberPage, /redirect\(`\/member\/\$\{canonicalSlug\}/);
});

test("portfolio quality notes render for zero holdings and limited price coverage", () => {
  assert.match(memberPage, /No simulated holdings were active in this window\./);
  assert.match(memberPage, /Simulation starts on/);
  assert.match(memberPage, /when this member first had active holdings in the selected window\./);
  assert.match(memberPage, /Some holdings have limited price history, so parts of the simulated curve may use stale or incomplete pricing\./);
  assert.match(memberPage, /curveQualityStatus === "warning" \|\| curveQualityStatus === "poor"/);
  assert.match(api, /curve_quality_status\?/);
  assert.match(api, /data_coverage_notes\?/);
  assert.match(api, /effective_start_date\?/);
  assert.match(api, /no_active_holdings\?/);
});

test("member page has one primary performance chart and compact secondary analytics", () => {
  assert.equal((memberPage.match(/<PerformanceChart/g) ?? []).length, 1);
  assert.doesNotMatch(memberPage, /Member Alpha Analytics/);
  assert.doesNotMatch(memberPage, /Performance Curve/);
  assert.match(memberPage, /Trade Outcome Analytics/);
  assert.match(memberPage, /DeferredMemberAnalyticsStats/);
  assert.match(memberPage, /Compact metrics from individually scored disclosures\./);
});

test("insider Portfolio Mode UI remains unwired", () => {
  assert.doesNotMatch(insiderPage, /getMemberPortfolioPerformance/);
  assert.doesNotMatch(insiderPage, /portfolio-performance/);
  assert.doesNotMatch(insiderPage, /Disclosure-lag realistic portfolio/);
});
