import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

const insiderPage = read("app/insider/[slug]/page.tsx");
const insiderErrorBoundary = read("app/insider/[slug]/error.tsx");
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

test("insider page offers expanded lookback windows", () => {
  assert.match(insiderPage, /type Lookback = "30" \| "90" \| "180" \| "365" \| "1095"/);
  assert.match(insiderPage, /\{ label: "30D", value: "30" \}/);
  assert.match(insiderPage, /\{ label: "90D", value: "90" \}/);
  assert.match(insiderPage, /\{ label: "180D", value: "180" \}/);
  assert.match(insiderPage, /\{ label: "1Y", value: "365" \}/);
  assert.match(insiderPage, /\{ label: "3Y", value: "1095" \}/);
  assert.match(insiderPage, /LOOKBACK_OPTIONS\.some\(\(option\) => option\.value === v\) \? \(v as Lookback\) : "90"/);
  assert.match(insiderPage, /LOOKBACK_OPTIONS\.map\(\(option\) =>/);
  assert.match(insiderPage, /lookback === option\.value/);
  assert.match(insiderPage, /\{option\.label\}/);
});

test("insider lookback links preserve stock chart and issuer params", () => {
  assert.match(insiderPage, /query\.set\("lookback", lookback\)/);
  assert.match(insiderPage, /query\.set\("chart", chartMode\)/);
  assert.match(insiderPage, /if \(issuer\) query\.set\("issuer", issuer\)/);
  assert.match(insiderPage, /href=\{hrefWithParams\(insiderName, reportingCik, option\.value, chartMetric, issuer \|\| undefined, chartMode\)\}/);
  assert.match(insiderPage, /href=\{hrefWithParams\(insiderName, reportingCik, lookback, chartMetric, issuer \|\| undefined, "stock"\)\}/);
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

test("insider profile optional sections fall back instead of throwing the route", () => {
  assert.match(insiderPage, /async function loadInsiderSection/);
  assert.match(insiderPage, /fallbackInsiderSummary/);
  assert.match(insiderPage, /fallbackInsiderAlphaSummary/);
  assert.match(insiderPage, /fallbackInsiderTrades/);
  assert.match(insiderPage, /fallbackInsiderTopTickers/);
  assert.match(insiderPage, /section: "alpha-summary"/);
  assert.match(insiderPage, /section: "trades"/);
  assert.match(insiderPage, /section: "stock-chart"/);
  assert.match(insiderPage, /Trade outcomes unavailable/);
  assert.match(insiderPage, /No recent activity found/);
});

test("insider route has a branded recovery boundary", () => {
  assert.match(insiderErrorBoundary, /"use client"/);
  assert.match(insiderErrorBoundary, /This insider profile could not fully load\./);
  assert.match(insiderErrorBoundary, /Back to landing/);
  assert.match(insiderErrorBoundary, /reset/);
});
