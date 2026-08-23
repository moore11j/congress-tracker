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

test("shared line chart batches pointer work and supports touch, keyboard, and reduced motion", () => {
  assert.match(lineChart, /requestAnimationFrame/);
  assert.match(lineChart, /touchAction: "pan-y"/);
  assert.match(lineChart, /prefers-reduced-motion/);
  assert.match(lineChart, /ArrowLeft/);
  assert.match(lineChart, /clip-path 560ms/);
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
});

test("profile charts use the shared interaction performance rules without dropping profile-specific data", () => {
  assert.match(performanceChart, /nearestChartIndex/);
  assert.match(performanceChart, /requestAnimationFrame/);
  assert.match(performanceChart, /touchAction: "pan-y"/);
  assert.match(performanceChart, /chart\.eventMarkers/);
  assert.match(allocationChart, /onPointerDown/);
  assert.match(allocationChart, /touchAction: "manipulation"/);
  assert.match(allocationChart, /walnut-chart-entry/);
});
