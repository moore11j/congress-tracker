import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";
import { createRequire } from "node:module";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const exports = {};
const source = fs.readFileSync("lib/tickerChartQuote.ts", "utf8");
vm.runInNewContext(ts.transpileModule(source, {
  compilerOptions: { module: ts.ModuleKind.CommonJS },
}).outputText, { exports });
const labels = exports.chartQuoteLabels;

test("after-hours prices keep delay and a separate comparison label", () => {
  const result = labels({ price_kind: "minute_close", is_delayed: true,
    market_session: "after_hours", regular_session_date: "2026-11-25", previous_close_date: "2026-11-24" });
  assert.equal(result.price, "After-hours Price (delayed)");
  assert.equal(result.extended, "After-hours");
  assert.match(result.extendedComparison, /with that session's regular close/);
  assert.match(result.comparison, /2026-11-25 regular session versus the 2026-11-24 regular-session close/);
});

test("regular and daily prices do not get after-hours labels", () => {
  assert.equal(labels({ price_kind: "minute_close", is_delayed: true, market_session: "regular" }).price, "Delayed Price");
  assert.equal(labels({ price_kind: "daily_close" }).price, "Daily Close");
  assert.equal(labels({ market_session: "regular" }).extended, null);
  assert.equal(labels(null).comparison, null);
});

test("premarket return remains distinct from Day percent", () => {
  const result = labels({ price_kind: "minute_close", market_session: "pre_market", is_delayed: true });
  assert.equal(result.price, "Pre-market Price (delayed)");
  assert.equal(result.extended, "Pre-market");
  assert.match(result.extendedComparison, /with the previous regular-session close/);
});

for (const days of [1, 5]) test(`${days}D rendered chart separates daily and after-hours returns`, () => {
  const chartExports = {};
  const require = createRequire(import.meta.url);
  const code = ts.transpileModule(fs.readFileSync("components/ticker/PremiumTickerChart.tsx", "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  vm.runInNewContext(code, { exports: chartExports, Intl, Date, process: { env: { NODE_ENV: "test" } },
    require: name => name === "@/lib/tickerChartQuote" ? exports
      : name === "@/lib/api" || name === "lightweight-charts" ? {}
      : name === "@/lib/tickerHeaderPrice" ? { formatPriceSession: value => value }
      : require(name),
  });
  const bundle = { symbol: "NBIS", days, resolution: "1min", markers: [],
    prices: [{ date: "2026-11-26T00:59:00Z", close: 85.06 }], benchmark: { points: [], symbol: "SPY" },
    quote: { current_price: 85.06, day_change: -2.32, day_change_pct: -2.657198,
      regular_session_price: 84.99, extended_hours_change: 0.07, extended_hours_change_pct: 0.08236,
      price_kind: "minute_close", market_session: "after_hours", is_delayed: true, delay_minutes: 15,
      asof: "2026-11-26T00:59:00Z", regular_session_date: "2026-11-25", previous_close_date: "2026-11-24" } };
  const html = renderToStaticMarkup(React.createElement(chartExports.PremiumTickerChart, { bundle }));
  assert.match(html, /Day %<\/p><p[^>]*>-2\.66%/);
  assert.match(html, /After-hours %<\/p><p[^>]*>\+0\.08%/);
  assert.match(html, /Regular Close<\/p><p[^>]*>\$84\.99/);
  assert.match(html, /After-hours Price \(delayed\)/);
  assert.match(html, /Quotes delayed 15 minutes/);
  assert.match(html, /Price observed Nov 26, 12:59 AM UTC/);
  assert.doesNotMatch(html, /Daily Close<\/p>/);
});
