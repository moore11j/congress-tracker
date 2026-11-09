import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);
const source = fs.readFileSync("components/ticker/TickerFundamentalsSnapshot.tsx", "utf8");
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
const exports = {};
vm.runInNewContext(code, { exports, Intl, require: (name) => name === "@/lib/format" ? { formatDateShort: (value) => value } : require(name) });

for (const label of ["FMP", "SEC EDGAR", "SEC EDGAR + Finnhub"]) {
  test(`public projection displays ${label} without an internal provider field`, () => {
    const html = renderToStaticMarkup(exports.TickerFundamentalsSnapshot({ summary: {
      context: { source_label: label }, as_of: "2026-09-30", updated_at: "2026-11-01", data_state: "stale",
      metrics: { revenue_growth: { display: "6.4%", state: "bullish" } },
    } }));
    assert.ok(html.includes(`Source: ${label}`));
    assert.ok(!html.includes("Source unavailable"));
    for (const value of ["6.4%", "2026-09-30", "2026-11-01", "Stale data", "Missing values shown as —"]) assert.ok(html.includes(value));
  });
}

test("legacy provider fallback remains supported", () => {
  const html = renderToStaticMarkup(exports.TickerFundamentalsSnapshot({ summary: { context: { provider: "fmp", source_label: " " } } }));
  assert.ok(html.includes("Source: FMP"));
});

test("missing source remains explicit without guessing a provider", () => {
  for (const summary of [null, {}, { context: {} }, { context: { source_label: " " } }]) {
    const html = renderToStaticMarkup(exports.TickerFundamentalsSnapshot({ summary }));
    assert.ok(html.includes("Source unavailable"));
    assert.ok(html.includes("Reporting period unavailable"));
  }
});

for (const [currency, amount, expected] of [["CAD", 1668000000, "1.7B"], ["DKK", 77859000000, "77.9B"], ["EUR", 0, "0"], ["CHF", -123000000, "123M"]]) {
  test(`native cash is displayed as ${currency} with its own basis`, () => {
    const html = renderToStaticMarkup(exports.TickerFundamentalsSnapshot({ summary: {
      as_of: "2026-06-30", context: { source_label: "SEC EDGAR", free_cash_flow: null,
        free_cash_flow_display: { value: amount, currency, period_date: "2026-06-30", period_basis: "TTM", basis: "TTM reviewed cash calculation" } },
    } }));
    assert.ok(html.includes(currency));
    // ICU versions may retain a trailing zero for an integral compact amount.
    assert.ok(expected === "123M" ? /123(?:\.0+)?M/.test(html) : html.includes(expected));
    assert.ok(html.includes("TTM reviewed cash calculation"));
    assert.ok(html.includes(`Reported in ${currency}; no currency conversion.`));
    assert.ok(!html.includes("$"));
  });
}

test("invalid display data stays missing and legacy USD values still render", () => {
  const valid = { value: 123000000, currency: "CAD", period_date: "2026-06-30", period_basis: "TTM", basis: "TTM cash" };
  for (const change of [{ currency: "bad currency" }, { value: NaN }, { period_date: "2025-06-30" }, { period_basis: "quarterly" }]) {
    const html = renderToStaticMarkup(exports.TickerFundamentalsSnapshot({ summary: {
      as_of: "2026-06-30", context: { free_cash_flow_display: { ...valid, ...change } },
    } }));
    assert.ok(!html.includes("123M"));
    assert.ok(!html.includes("no currency conversion"));
  }
  const html = renderToStaticMarkup(exports.TickerFundamentalsSnapshot({ summary: { context: { free_cash_flow: 123000000 } } }));
  assert.match(html, /\$123(?:\.0+)?M/);
});
