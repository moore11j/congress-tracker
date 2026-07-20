import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

const insiderPage = read("app/insider/[slug]/page.tsx");
const insiderAnalyticsClient = read("components/insider/InsiderAnalyticsClient.tsx");
const insiderErrorBoundary = read("app/insider/[slug]/error.tsx");
const api = read("lib/api.ts");
const tradeDisplay = read("lib/tradeDisplay.ts");
const addTickerToWatchlist = read("components/watchlists/AddTickerToWatchlist.tsx");
const tickerChart = read("components/ticker/PremiumTickerChart.tsx");

test("insider page renders company stock chart only", () => {
  assert.doesNotMatch(insiderPage, /type ChartMode/);
  assert.doesNotMatch(insiderPage, /chartModeFromParams/);
  assert.doesNotMatch(insiderAnalyticsClient, /Performance Curve/);
  assert.doesNotMatch(insiderAnalyticsClient, /<PerformanceChart/);
  assert.match(insiderAnalyticsClient, /Company Stock Chart/);
  assert.match(insiderAnalyticsClient, /query\.set\("chart", "stock"\)/);
});

test("company stock mode requests insider-scoped stock chart data", () => {
  assert.match(api, /getInsiderStockChart/);
  assert.match(api, /\/api\/insiders\/\$\{encodeURIComponent\(reportingCik\)\}\/stock-chart/);
  assert.match(api, /lookback_days: params\.lookback_days/);
  assert.match(api, /symbol: params\.symbol/);
  assert.doesNotMatch(insiderPage, /getInsiderStockChart\(reportingCik/);
  assert.match(insiderAnalyticsClient, /getInsiderStockChart\(reportingCik/);
  assert.match(insiderAnalyticsClient, /PremiumTickerChartSkeleton/);
  assert.doesNotMatch(insiderPage, /const stockChartPromise/);
});

test("secondary insider analytics are SSR-seeded client sections without top tickers", () => {
  assert.match(insiderPage, /<InsiderAnalyticsClient/);
  assert.match(insiderPage, /getInsiderAlphaSummary\(reportingCik/);
  assert.match(insiderPage, /initialAlphaSummary=\{initialAlphaSummary\}/);
  assert.match(insiderPage, /initialTrades=\{initialTrades\}/);
  assert.match(insiderPage, /getInsiderTrades\(reportingCik, lookbackDays, 5, normalizedIssuer/);
  assert.doesNotMatch(insiderPage, /getInsiderTopTickers/);
  assert.doesNotMatch(insiderPage, /initialTopTickers/);
  assert.match(insiderAnalyticsClient, /"use client"/);
  assert.match(insiderAnalyticsClient, /getInsiderAlphaSummary\(reportingCik/);
  assert.match(insiderAnalyticsClient, /getInsiderTrades\(reportingCik/);
  assert.doesNotMatch(insiderAnalyticsClient, /getInsiderTopTickers/);
  assert.doesNotMatch(insiderAnalyticsClient, /TopTickersPanel/);
  assert.doesNotMatch(insiderAnalyticsClient, /Top tickers/);
  assert.doesNotMatch(insiderAnalyticsClient, /initialTopTickers/);
  assert.doesNotMatch(insiderAnalyticsClient, /Analytics temporarily unavailable\. Try again shortly\./);
  assert.doesNotMatch(insiderPage, /const topTickersPromise/);
});

test("insider profile header falls back to recent trade identity", () => {
  assert.match(insiderPage, /needsHeaderFallback = !summary\.primary_company_name \|\| !summary\.primary_role/);
  assert.match(insiderPage, /section: "header-trades"/);
  assert.match(insiderPage, /source: "InsiderHeaderTrades"/);
  assert.match(insiderPage, /firstText\(summary\.primary_role, headerTrade\?\.role\)/);
  assert.match(insiderPage, /firstText\(summary\.primary_company_name, headerTrade\?\.company_name, headerTrade\?\.companyName/);
});

test("insider page offers expanded lookback windows", () => {
  assert.match(insiderPage, /type Lookback = "30" \| "90" \| "180" \| "365" \| "1095"/);
  assert.match(insiderPage, /\{ label: "30D", value: "30" \}/);
  assert.match(insiderPage, /\{ label: "90D", value: "90" \}/);
  assert.match(insiderPage, /\{ label: "180D", value: "180" \}/);
  assert.match(insiderPage, /\{ label: "1Y", value: "365" \}/);
  assert.match(insiderPage, /\{ label: "3Y", value: "1095" \}/);
  assert.match(insiderPage, /LOOKBACK_OPTIONS\.some\(\(option\) => option\.value === v\) \? \(v as Lookback\) : "90"/);
  assert.match(insiderAnalyticsClient, /LOOKBACK_OPTIONS\.map\(\(option\) =>/);
  assert.match(insiderAnalyticsClient, /lookback === option\.value/);
  assert.match(insiderAnalyticsClient, /\{option\.label\}/);
});

test("insider lookback links preserve stock chart and issuer params", () => {
  assert.match(insiderPage, /query\.set\("lookback", lookback\)/);
  assert.match(insiderPage, /query\.set\("chart", "stock"\)/);
  assert.match(insiderPage, /if \(issuer\) query\.set\("issuer", issuer\)/);
  assert.match(insiderPage, /if \(chartSymbol\) query\.set\("symbol", chartSymbol\)/);
  assert.match(insiderAnalyticsClient, /href=\{hrefWithParams\(insiderName, reportingCik, option\.value, issuer, stockSymbol\)\}/);
  assert.doesNotMatch(insiderAnalyticsClient, /chartMetric/);
  assert.doesNotMatch(insiderAnalyticsClient, /chartMode/);
});

test("insider stock chart hides ticker-page overlay controls and only allows insider markers", () => {
  assert.match(insiderAnalyticsClient, /allowedMarkerKinds=\{\["insider"\]\}/);
  assert.match(insiderAnalyticsClient, /showMarkerControls=\{false\}/);
  assert.match(insiderAnalyticsClient, /Showing this insider's disclosed buys and sells only\./);
  assert.match(tickerChart, /allowedMarkerKinds/);
  assert.match(tickerChart, /visibleMarkerKinds\.includes\(marker\.kind\)/);
});

test("company stock chart has buy sell marker details and empty state", () => {
  assert.match(tickerChart, /event\.kind === "insider" && event\.meta/);
  assert.match(tickerChart, /filing_date/);
  assert.match(tickerChart, /signal_score/);
  assert.match(insiderAnalyticsClient, /No company stock chart is available for this insider yet\./);
  assert.match(insiderAnalyticsClient, /PremiumTickerChartSkeleton/);
});

test("insider profile optional sections fall back instead of throwing the route", () => {
  assert.match(insiderPage, /async function loadInsiderSection/);
  assert.match(insiderPage, /fallbackInsiderSummary/);
  assert.match(insiderAnalyticsClient, /fallbackInsiderAlphaSummary/);
  assert.match(insiderAnalyticsClient, /fallbackInsiderTrades/);
  assert.match(insiderAnalyticsClient, /setAlphaUnavailable\(true\)/);
  assert.match(insiderAnalyticsClient, /setTradesUnavailable\(true\)/);
  assert.match(insiderAnalyticsClient, /setStockChartUnavailable\(true\)/);
  assert.match(insiderAnalyticsClient, /Refreshing the latest analytics from disclosed activity\./);
  assert.doesNotMatch(insiderAnalyticsClient, /Analytics temporarily unavailable\. Try again shortly\./);
});

test("insider recent trades are public paginated rows with truthful empty and error states", () => {
  assert.match(api, /getInsiderTrades\(/);
  assert.match(api, /page: options\?\.page/);
  assert.match(insiderAnalyticsClient, /RECENT_TRADES_PAGE_SIZE = 20/);
  assert.match(insiderPage, /recentTradesPage = clampPage\(one\(sp, "recent_trades_page"\)\)/);
  assert.match(insiderAnalyticsClient, /page: recentTradesPage/);
  assert.match(insiderAnalyticsClient, /recentTradesTotal === 0/);
  assert.match(insiderAnalyticsClient, /No recent activity found\./);
  assert.match(insiderAnalyticsClient, /tradesUnavailable/);
  assert.match(insiderAnalyticsClient, /pageParam="recent_trades_page"/);
  assert.match(insiderAnalyticsClient, /sectionId="recent-trades"/);
  assert.match(insiderAnalyticsClient, /TickerActivityPaginationFooter/);
});

test("insider recent trades expose watchlist add and pnl source badge", () => {
  assert.match(insiderAnalyticsClient, /AddTickerToWatchlist/);
  assert.match(insiderAnalyticsClient, /<AddTickerToWatchlist symbol=\{display\.displaySymbol\} variant="compact" align="left" \/>/);
  assert.match(addTickerToWatchlist, /setAuthGateOpen\(true\)/);
  assert.match(addTickerToWatchlist, /Create a free account/);
  assert.match(tradeDisplay, /pnlSource = firstNestedText\(record, "pnl_source", "pnlSource"\)/);
  assert.match(insiderAnalyticsClient, /pnlSourceBadgeLabel\(display\.pnlSource\)/);
  assert.match(insiderAnalyticsClient, /if \(source === "eod"\) return "EOD"/);
  assert.match(insiderAnalyticsClient, /\{pnlSourceLabel\}/);
});

test("insider route has a branded recovery boundary", () => {
  assert.match(insiderErrorBoundary, /"use client"/);
  assert.match(insiderErrorBoundary, /This insider profile could not fully load\./);
  assert.match(insiderErrorBoundary, /Back to landing/);
  assert.match(insiderErrorBoundary, /reset/);
});
