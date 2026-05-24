import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

const insiderPage = read("app/insider/[slug]/page.tsx");
const api = read("lib/api.ts");
const tickerChart = read("components/ticker/PremiumTickerChart.tsx");

test("insider page renders chart toggle and defaults to performance curve", () => {
  assert.match(insiderPage, /type ChartMode = "performance" \| "stock"/);
  assert.match(insiderPage, /chartModeFromParams/);
  assert.match(insiderPage, /return one\(sp, "chart"\) === "stock" \? "stock" : "performance"/);
  assert.match(insiderPage, /Performance Curve/);
  assert.match(insiderPage, /Company Stock/);
});

test("company stock mode requests insider-scoped stock chart data", () => {
  assert.match(api, /getInsiderStockChart/);
  assert.match(api, /\/api\/insiders\/\$\{encodeURIComponent\(reportingCik\)\}\/stock-chart/);
  assert.match(api, /lookback_days: params\.lookback_days/);
  assert.match(api, /symbol: params\.symbol/);
  assert.match(insiderPage, /getInsiderStockChart\(reportingCik/);
  assert.match(insiderPage, /stockChartPromise/);
});

test("insider stock chart hides ticker-page overlay controls and only allows insider markers", () => {
  assert.match(insiderPage, /allowedMarkerKinds=\{\["insider"\]\}/);
  assert.match(insiderPage, /showMarkerControls=\{false\}/);
  assert.match(insiderPage, /Showing this insider's disclosed buys and sells only\./);
  assert.match(tickerChart, /allowedMarkerKinds/);
  assert.match(tickerChart, /visibleMarkerKinds\.includes\(marker\.kind\)/);
});

test("company stock chart has buy sell marker details and empty state", () => {
  assert.match(tickerChart, /event\.kind === "insider" && event\.meta/);
  assert.match(tickerChart, /filing_date/);
  assert.match(tickerChart, /signal_score/);
  assert.match(insiderPage, /No company stock chart is available for this insider yet\./);
  assert.match(insiderPage, /PremiumTickerChartSkeleton/);
});

