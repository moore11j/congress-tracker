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
