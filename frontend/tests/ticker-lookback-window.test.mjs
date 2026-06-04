import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const tickerPage = read("app/ticker/[symbol]/page.tsx");
const chartLoader = read("components/ticker/TickerChartLoader.tsx");
const api = read("lib/api.ts");

test("ticker page uses selected URL lookback for chart and activity windows", () => {
  assert.match(tickerPage, /const lookback = clampLookback\(one\(sp, "lookback"\)\)/);
  assert.match(tickerPage, /const lookbackDays = Number\(lookback\)/);
  assert.match(tickerPage, /recent_days: lookbackDays/);
  assert.match(tickerPage, /lookback_days: lookbackDays/);
  assert.match(tickerPage, /const selectedLookbackDays = Number\(lookback\)/);
  assert.match(tickerPage, /<TickerChartLoader symbol=\{normalizedSymbol\} days=\{selectedLookbackDays\} \/>/);
  assert.match(tickerPage, /lookbackStartKey=\{lookbackStartDateKey\(selectedLookbackDays\)\}/);
  assert.doesNotMatch(tickerPage, /<TickerChartLoader symbol=\{normalizedSymbol\} days=\{lookbackDays\} \/>/);
});

test("ticker chart helper forwards selected days to chart-bundle", () => {
  assert.match(chartLoader, /getTickerChartBundle\(symbol, days,/);
  assert.match(chartLoader, /\}, \[days, symbol\]\)/);
  assert.match(api, /buildApiUrl\(`\/api\/tickers\/\$\{symbol\}\/chart-bundle`, \{ days \}\)/);
});
