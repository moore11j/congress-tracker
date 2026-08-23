import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const read = (path) => readFileSync(join(process.cwd(), path), "utf8");
const lineChart = read("components/charts/WalnutLineChart.tsx");
const container = read("components/charts/WalnutChartContainer.tsx");
const performance = read("components/charts/chartPerformanceUtils.ts");
const backtest = read("components/backtesting/BacktestChart.tsx");
const performanceChart = read("components/member/PerformanceChart.tsx");
const allocationChart = read("components/institution/HoldingsAllocationChart.tsx");
const dashboard = read("components/profiles/EnhancedProfileDashboards.tsx");
const governmentDashboard = read("components/profiles/EnhancedGovernmentDashboard.tsx");
const donutChart = read("components/charts/WalnutDonutChart.tsx");
const activityBarChart = read("components/charts/WalnutActivityBarChart.tsx");
const memberAnalytics = read("components/member/MemberAnalyticsClient.tsx");
const insiderAnalytics = read("components/insider/InsiderAnalyticsClient.tsx");
const outcomes = read("components/outcomes/OutcomeLedgerClient.tsx");
const profileSparkline = read("components/charts/WalnutProfileSparkline.tsx");
const sectorMovementBars = read("components/profiles/WalnutSectorMovementBars.tsx");
const congressInteractiveCharts = read("components/profiles/CongressInteractiveCharts.tsx");
const departmentSpendingMix = read("components/profiles/WalnutDepartmentSpendingMix.tsx");

test("shared line chart batches pointer work and supports touch, keyboard, and reduced motion", () => {
  assert.match(lineChart, /requestAnimationFrame/);
  assert.match(lineChart, /touchAction: "pan-y"/);
  assert.match(lineChart, /prefers-reduced-motion/);
  assert.match(lineChart, /ArrowLeft/);
  assert.match(lineChart, /clip-path 560ms/);
  assert.match(lineChart, /minValue/);
  assert.match(lineChart, /\* width/);
});

test("shared chart container reserves space and lazy-renders near the viewport", () => {
  assert.match(container, /rootMargin: "320px 0px"/);
  assert.match(container, /WalnutChartSkeleton/);
  assert.match(container, /WalnutChartErrorState/);
});

test("nearest lookup is logarithmic and backtesting adopts the shared chart layer", () => {
  assert.match(performance, /while \(low < high\)/);
  assert.match(backtest, /WalnutLineChart/);
  assert.match(backtest, /WalnutChartContainer/);
  assert.match(backtest, /Active tickers/);
  assert.match(backtest, /areaGradient: \{ top: "rgba\(74,222,128,0\.34\)", bottom: "rgba\(74,222,128,0\)" \}/);
  assert.match(lineChart, /linearGradient/);
  assert.match(lineChart, /areaGradient/);
});

test("profile charts use the shared interaction performance rules without dropping profile-specific data", () => {
  assert.match(performanceChart, /nearestChartIndex/);
  assert.match(performanceChart, /requestAnimationFrame/);
  assert.match(performanceChart, /touchAction: "pan-y"/);
  assert.match(performanceChart, /chart\.eventMarkers/);
  assert.match(performanceChart, /profileArea/);
  assert.match(performanceChart, /linearGradient/);
  assert.match(allocationChart, /onPointerDown/);
  assert.match(allocationChart, /touchAction: "manipulation"/);
  assert.match(allocationChart, /walnut-chart-entry/);
});

test("profile and government snapshot trends use the shared interactive area chart", () => {
  assert.match(lineChart, /areaColor/);
  assert.match(lineChart, /valueFormat/);
  assert.match(dashboard, /WalnutLineChart/);
  assert.match(dashboard, /Monthly activity by profile type/);
  assert.match(governmentDashboard, /WalnutLineChart/);
  assert.match(governmentDashboard, /if \(!bars\)/);
});

test("dashboard pie charts share an accessible interactive donut primitive", () => {
  assert.match(donutChart, /requestAnimationFrame/);
  assert.match(donutChart, /prefers-reduced-motion/);
  assert.match(donutChart, /touchAction: "manipulation"/);
  assert.match(donutChart, /ArrowLeft/);
  assert.match(donutChart, /setPointerCapture/);
  assert.match(donutChart, /size = 144/);
  assert.match(dashboard, /WalnutDonutChart/);
  assert.match(governmentDashboard, /WalnutDonutChart/);
});

test("activity charts share pointer, keyboard, animation, and reduced-motion behavior", () => {
  assert.match(activityBarChart, /requestAnimationFrame/);
  assert.match(activityBarChart, /prefers-reduced-motion/);
  assert.match(activityBarChart, /touchAction: "pan-y"/);
  assert.match(activityBarChart, /ArrowLeft/);
  assert.match(activityBarChart, /setPointerCapture/);
  assert.match(dashboard, /WalnutActivityBarChart/);
  assert.match(governmentDashboard, /WalnutActivityBarChart/);
  assert.match(memberAnalytics, /WalnutActivityBarChart/);
  assert.match(insiderAnalytics, /WalnutActivityBarChart/);
});

test("outcome charts support keyboard, touch, and focus inspection", () => {
  assert.match(outcomes, /aria-pressed/);
  assert.match(outcomes, /requestAnimationFrame/);
  assert.match(outcomes, /touchAction: "pan-y"/);
  assert.match(outcomes, /ArrowLeft/);
  assert.match(outcomes, /Event outcomes by date and return/);
});

test("profile-card sparklines and moving-sector bars reveal and inspect on demand", () => {
  assert.match(profileSparkline, /requestAnimationFrame/);
  assert.match(profileSparkline, /onPointerMove/);
  assert.match(profileSparkline, /X · \{xAxisLabel\}/);
  assert.match(profileSparkline, /Y · \{metricLabel\}/);
  assert.match(profileSparkline, /z-\[60\]/);
  assert.match(sectorMovementBars, /requestAnimationFrame/);
  assert.match(sectorMovementBars, /aria-pressed/);
  assert.match(sectorMovementBars, /onPointerEnter/);
  assert.match(sectorMovementBars, /Top mover:/);
  assert.match(sectorMovementBars, /min-h-9/);
  assert.match(dashboard, /WalnutProfileSparkline/);
  assert.match(dashboard, /WalnutSectorMovementBars/);
  assert.match(dashboard, /card\.trend\?\.points/);
  assert.match(dashboard, /overflow-visible/);
});

test("Congress dashboard snapshot, metric, exposure, and sector charts are interactive", () => {
  assert.match(congressInteractiveCharts, /WalnutLineChart/);
  assert.match(congressInteractiveCharts, /minValue=\{0\}/);
  assert.match(congressInteractiveCharts, /height=\{expanded \? 220 : 190\} width=\{expanded \? 420 : 360\} axisFontSize=\{expanded \? 16 : 12\}/);
  assert.match(congressInteractiveCharts, /requestAnimationFrame/);
  assert.match(congressInteractiveCharts, /aria-pressed/);
  assert.match(congressInteractiveCharts, /onPointerEnter/);
  assert.match(dashboard, /CongressSnapshotChart/);
  assert.match(dashboard, /CongressMetricTrend/);
  assert.match(dashboard, /CongressSectorExposure/);
  assert.match(dashboard, /CongressNetSectorBars/);
  assert.doesNotMatch(congressInteractiveCharts, /Net activity \{money\(active\.current_value\)\}/);
  assert.match(dashboard, /size=\{176\}/);
  assert.match(dashboard, /size=\{160\}/);
});

test("insider dashboard reuses the interactive snapshot, metric, and sector chart primitives", () => {
  assert.match(congressInteractiveCharts, /InsiderSnapshotTrend/);
  assert.match(dashboard, /InsiderSnapshotTrend/);
  assert.match(dashboard, /CongressMetricTrend/);
  assert.match(dashboard, /CongressSectorExposure/);
  assert.match(dashboard, /CongressNetSectorBars rows={rows}/);
  assert.match(dashboard, /flavor === "institutions" \|\| flavor === "insiders"/);
  assert.match(dashboard, /expandedChart \? "text-\[11px\]" : "text-xs"/);
});

test("institutional dashboard reuses animated metric, exposure, and net-position primitives", () => {
  assert.match(dashboard, /institutionalMetricSeries/);
  assert.match(dashboard, /expandedChart = flavor === "institutions"/);
  assert.match(dashboard, /expanded=\{expandedChart\}/);
  assert.match(dashboard, /CongressSectorExposure rows={rows}/);
  assert.match(dashboard, /CongressNetSectorBars rows={movements.map/);
});

test("department spending mix reveals and supports period inspection", () => {
  assert.match(departmentSpendingMix, /requestAnimationFrame/);
  assert.match(departmentSpendingMix, /prefers-reduced-motion/);
  assert.match(departmentSpendingMix, /onPointerEnter/);
  assert.match(departmentSpendingMix, /aria-pressed/);
  assert.match(governmentDashboard, /WalnutDepartmentSpendingMix rows={rows}/);
});
