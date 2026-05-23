import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import {
  DEFAULT_PORTFOLIO_LOOKBACK_DAYS,
  PORTFOLIO_LOOKBACK_OPTIONS,
  PORTFOLIO_MODE,
  normalizeMemberPortfolioChartData,
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
      skipped_events_count: 0,
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

  for (const label of [
    "Total Return",
    "CAGR",
    "Alpha",
    "S&P Return",
    "Max Drawdown",
    "Sharpe",
    "Win Rate",
    "Positions",
    "Skipped",
  ]) {
    assert.match(memberPage, new RegExp(label.replace("&", "&")));
  }
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
  assert.match(memberPage, /portfolio\?\.persisted_only === true/);
  assert.match(memberPage, /portfolio\.status === "ok"/);
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
