import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import ts from "typescript";
import vm from "node:vm";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";

const source = fs.readFileSync("lib/researchMarketPrices.ts", "utf8");
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ES2022 } }).outputText;
const { researchMarketAssets, researchQuoteStatus } = await import(`data:text/javascript;base64,${Buffer.from(code).toString("base64")}`);

test("BMNR external price assumption finds Ethereum and preserves its target", () => {
  const results = researchMarketAssets([{ subject: "Ethereum", metric: "ETHUSD", expected_direction: "increase", expected_magnitude: "to $5,000", expected_timeframe: "6–12 months", monitoring_mode: "manual" }]);
  assert.equal(results.length, 1);
  assert.equal(results[0].symbol, "ETHUSD");
  assert.equal(results[0].expectations[0], "increase · to $5,000 · 6–12 months");
});

test("multiple commodity and crypto assumptions yield distinct relevant cards", () => {
  const results = researchMarketAssets([
    { subject: "Gold", metric: "price" },
    { subject: "Company", metric: "HGUSD price" },
    { subject: "Company", metric: "XAG/USD" },
    { subject: "ETH", metric: "ETH/USD price" },
    { subject: "Ethereum", metric: "ETHUSD_price" },
  ]);
  assert.deepEqual(results.map((asset) => asset.symbol).sort(), ["ETHUSD", "GCUSD", "HGUSD", "SILUSD"]);
});

test("company-only and partial-word claims do not get unrelated quotes", () => {
  assert.deepEqual(researchMarketAssets([{ subject: "BMNR", metric: "revenue" }, { subject: "Golden opportunity", metric: "margin" }, { subject: "Resolution", metric: "earnings" }]), []);
});

test("stale, missing, future, and invalid prices never appear as current", () => {
  const now = Date.parse("2026-09-27T04:00:00Z");
  const quote = { status: "ok", price: 2500, as_of: "2026-09-27T03:55:00Z" };
  assert.equal(researchQuoteStatus(quote, now), "available");
  assert.equal(researchQuoteStatus({ ...quote, as_of: "2026-09-25T03:55:00Z" }, now), "stale");
  for (const bad of [undefined, { ...quote, price: null }, { ...quote, price: NaN }, { ...quote, price: Infinity }, { ...quote, price: 0 }, { ...quote, status: "unavailable" }, { ...quote, as_of: null }, { ...quote, as_of: "invalid" }, { ...quote, as_of: "2026-10-01" }]) {
    assert.equal(researchQuoteStatus(bad, now), "unavailable");
  }
});

function renderPriceCards(quotes) {
  const exports = {};
  const js = ts.transpileModule(fs.readFileSync("components/research-memory/ResearchMarketPrices.tsx", "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
  const modules = {
    react: { ...React, useEffect() {}, useState: (initial) => [initial === null ? quotes : initial, () => {}] },
    "react/jsx-runtime": jsxRuntime,
    "next/link": ({ children, ...props }) => React.createElement("a", props, children),
    "@/lib/api": { getInsightsOverview() { throw new Error("Rendering must not reach a provider"); } },
    "@/lib/researchMarketPrices": { researchQuoteStatus },
    "@/lib/researchEvidence": { researchCheckTime: (value) => value },
  };
  vm.runInNewContext(js, { exports, require: (name) => modules[name], Intl });
  return renderToStaticMarkup(React.createElement(exports.ResearchMarketPrices, {
    assets: [{ symbol: "ETHUSD", name: "Ethereum", group: "crypto", expectations: ["increase · to $5,000"] }],
  }));
}

test("rendered card includes price, change, target and Insights link", () => {
  const html = renderPriceCards([{ symbol: "ETHUSD", group: "crypto", status: "ok", price: 2500, change_percent: 2.5, as_of: new Date(Date.now() - 60000).toISOString() }]);
  for (const text of ["Ethereum price", "$2,500.00", "+2.50%", "24h", "to $5,000", 'href="/insights/crypto"', "Trend assessment requires manual review"]) assert.ok(html.includes(text), text);
});

test("rendered stale and absent quotes clearly distinguish last price from current price", () => {
  const stale = renderPriceCards([{ symbol: "ETHUSD", group: "crypto", status: "ok", price: 2500, change_percent: 2.5, as_of: "2000-01-01T00:00:00Z" }]);
  assert.match(stale, /Update overdue/);
  assert.match(stale, /Last available/);
  assert.doesNotMatch(stale, /\+2.50%/);
  const unavailable = renderPriceCards([]);
  assert.match(unavailable, /Price unavailable/);
  assert.match(unavailable, /Retry prices/);
  assert.doesNotMatch(unavailable, /\$0\.00/);
  assert.match(renderPriceCards(null), /Loading/);
});
