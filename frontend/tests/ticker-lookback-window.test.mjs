import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const tickerPage = read("app/ticker/[symbol]/page.tsx");
const chartLoader = read("components/ticker/TickerChartLoader.tsx");
const tickerContextCard = read("components/ticker/TickerContextCard.tsx");
const tickerSignalActivityClient = read("components/ticker/TickerSignalActivityClient.tsx");
const api = read("lib/api.ts");

test("ticker page keeps confirmation on 30D while chart uses selected URL range", () => {
  assert.match(tickerPage, /const lookback = clampLookback\(one\(sp, "lookback"\)\)/);
  assert.match(tickerPage, /type Lookback = "1" \| "5" \| "30" \| "90" \| "180" \| "365"/);
  assert.match(tickerPage, /v === "1" \|\| v === "5" \|\| v === "30"/);
  assert.match(tickerPage, /\(\["1", "5", "30", "90", "180", "365"\] as const\)\.map/);
  assert.match(tickerPage, /href=\{hrefWithFilters\(normalizedSymbol, value, source, side\)\}[\s\S]*?scroll=\{false\}/);
  assert.match(tickerPage, /const SIGNAL_WINDOW_DAYS = 30/);
  assert.match(tickerPage, /const lookbackDays = Number\(lookback\)/);
  assert.match(tickerPage, /recent_days: lookbackDays/);
  assert.match(tickerPage, /getTickerSignalsSummary\(normalizedSymbol,[\s\S]*?lookback_days: lookbackDays/);
  assert.match(tickerPage, /lookbackDays=\{selectedLookbackDays\}/);
  assert.doesNotMatch(tickerSignalActivityClient, /lookbackStartKey/);
  assert.match(api, /congress_recent_days: params\.congress_recent_days/);
  assert.match(api, /insider_recent_days: params\.insider_recent_days/);
  assert.match(tickerPage, /effectiveWindowDays \?\? SIGNAL_WINDOW_DAYS/);
  assert.match(tickerPage, /activityConfirmationScoreBundle \?\? confirmationScoreBundle/);
  assert.match(tickerPage, /const selectedLookbackDays = Number\(lookback\)/);
  assert.match(tickerPage, /normalizeOptionsFlowSummary\(optionsFlowSummary, normalizedSymbol, effectiveLookbackDays\)/);
  assert.match(tickerPage, /optionsFlow = \{ \.\.\.optionsFlow, lookback_days: effectiveLookbackDays \}/);
  assert.match(tickerPage, /<TickerChartLoader symbol=\{normalizedSymbol\} days=\{selectedLookbackDays\} \/>/);
  assert.doesNotMatch(tickerPage, /<TickerChartLoader symbol=\{normalizedSymbol\} days=\{lookbackDays\} \/>/);
  assert.doesNotMatch(tickerPage, /getTickerSignalsSummary\(normalizedSymbol,[\s\S]*?lookback_days: SIGNAL_WINDOW_DAYS/);
});

test("ticker activity filters keep small controls compact", () => {
  assert.match(tickerPage, /lg:grid-cols-\[minmax\(28rem,1fr\)_max-content_max-content\]/);
  assert.match(tickerPage, /lg:w-\[25rem\]/);
  assert.match(tickerPage, /lg:w-\[17rem\]/);
});

test("ticker chart helper forwards selected days to chart-bundle", () => {
  assert.match(chartLoader, /getTickerChartBundle\(symbol, days,/);
  assert.match(chartLoader, /\}, \[attempt, days, shouldLoad, symbol\]\)/);
  assert.match(api, /buildApiUrl\(`\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/chart-bundle`, \{ days \}\)/);
});

test("ticker activity requests visible trade prices while heavy tab disclosure rows stay base-only", () => {
  assert.match(tickerPage, /enrich_prices: 0/);
  assert.match(tickerContextCard, /enrich_prices: 0/);
  assert.doesNotMatch(tickerPage, /source: "TickerEvents"/);
  assert.doesNotMatch(tickerPage, /limit: 100/);
  assert.match(tickerPage, /enrich_prices: 1,[\s\S]*?source: "TickerCongressActivity"/);
  assert.match(tickerPage, /enrich_prices: 1,[\s\S]*?source: "TickerInsiderActivity"/);
  assert.match(tickerPage, /ACTIVITY_FETCH_SIZE = ACTIVITY_PAGE_SIZE \+ 1/);
  assert.match(tickerPage, /const boundedEvents = \[/);
});
