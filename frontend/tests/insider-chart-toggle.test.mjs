import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

const insiderPage = read("app/insider/[slug]/page.tsx");
const insiderErrorBoundary = read("app/insider/[slug]/error.tsx");
const api = read("lib/api.ts");
const tradeDisplay = read("lib/tradeDisplay.ts");
const addTickerToWatchlist = read("components/watchlists/AddTickerToWatchlist.tsx");
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
  assert.match(insiderPage, /function DeferredCompanyStockChart/);
  assert.doesNotMatch(insiderPage, /const stockChartPromise/);
});

test("secondary insider analytics are requested inside deferred sections", () => {
  assert.match(insiderPage, /function DeferredTopTickers/);
  assert.match(insiderPage, /getInsiderTopTickers\(reportingCik, lookbackDays, 10, issuer/);
  assert.match(insiderPage, /Analytics temporarily unavailable\. Try again shortly\./);
  assert.doesNotMatch(insiderPage, /const topTickersPromise/);
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
  assert.match(insiderPage, /if \(chartMode === "stock" && chartSymbol\) query\.set\("symbol", chartSymbol\)/);
  assert.match(insiderPage, /href=\{hrefWithParams\(insiderName, reportingCik, option\.value, chartMetric, issuer \|\| undefined, chartMode, stockSymbol\)\}/);
  assert.match(insiderPage, /href=\{hrefWithParams\(insiderName, reportingCik, lookback, chartMetric, issuer \|\| undefined, "stock", stockSymbol\)\}/);
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
  assert.match(insiderPage, /Recent trades unavailable\./);
});

test("insider recent trades are public paginated rows with truthful empty and error states", () => {
  assert.match(api, /getInsiderTrades\(/);
  assert.match(api, /page: options\?\.page/);
  assert.match(insiderPage, /RECENT_TRADES_PAGE_SIZE = 20/);
  assert.match(insiderPage, /recentTradesPage = clampPage\(one\(sp, "recent_trades_page"\)\)/);
  assert.match(insiderPage, /page: recentTradesPage/);
  assert.match(insiderPage, /recentTradesTotal === 0/);
  assert.match(insiderPage, /No recent activity found\./);
  assert.match(insiderPage, /Recent trades unavailable\./);
  assert.match(insiderPage, /pageParam="recent_trades_page"/);
  assert.match(insiderPage, /sectionId="recent-trades"/);
  assert.match(insiderPage, /TickerActivityPaginationFooter/);
});

test("insider recent trades expose watchlist add and pnl source badge", () => {
  assert.match(insiderPage, /AddTickerToWatchlist/);
  assert.match(insiderPage, /<AddTickerToWatchlist symbol=\{display\.displaySymbol\} variant="compact" align="left" \/>/);
  assert.match(addTickerToWatchlist, /setAuthGateOpen\(true\)/);
  assert.match(addTickerToWatchlist, /Create a free account/);
  assert.match(tradeDisplay, /pnlSource = firstNestedText\(record, "pnl_source", "pnlSource"\)/);
  assert.match(insiderPage, /pnlSourceBadgeLabel\(display\.pnlSource\)/);
  assert.match(insiderPage, /if \(source === "eod"\) return "EOD"/);
  assert.match(insiderPage, /\{pnlSourceLabel\}/);
});

test("insider route has a branded recovery boundary", () => {
  assert.match(insiderErrorBoundary, /"use client"/);
  assert.match(insiderErrorBoundary, /This insider profile could not fully load\./);
  assert.match(insiderErrorBoundary, /Back to landing/);
  assert.match(insiderErrorBoundary, /reset/);
});
