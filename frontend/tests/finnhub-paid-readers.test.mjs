import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createRequire } from "node:module";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";
import * as financialCurrency from "../lib/financialCurrency.mjs";

// Render the actual panels. Stub only navigation/network integrations that are
// outside this source-label/access contract; hooks use the real React server renderer.
const require = createRequire(import.meta.url);
const root = resolve(import.meta.dirname, "..");
const loaded = new Map();
function load(relative) {
  const path = resolve(root, relative);
  if (loaded.has(path)) return loaded.get(path);
  const source = ts.transpileModule(readFileSync(path, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, target: ts.ScriptTarget.ES2020 },
    fileName: path,
  }).outputText;
  const module = { exports: {} };
  const localRequire = (id) => {
    if (id === "@/components/billing/UpgradeLink") return {
      UpgradeLink: ({ children }) => React.createElement("a", { href: "/pricing" }, children),
      UpgradeImpression: ({ children }) => React.createElement(React.Fragment, null, children),
    };
    if (id === "@/lib/api") return { getTickerAnalystConsensusEvents: () => { throw new Error("Unexpected network request"); } };
    if (id === "@/lib/financialCurrency.mjs") return financialCurrency;
    if (id === "@/lib/format") return load("lib/format.ts");
    if (id === "@/lib/styles") return load("lib/styles.ts");
    if (id === "@/components/ui/LoadingSkeleton") return load("components/ui/LoadingSkeleton.tsx");
    return require(id);
  };
  new Function("require", "module", "exports", source)(localRequire, module, module.exports);
  loaded.set(path, module.exports);
  return module.exports;
}

const AnalystPanel = load("components/ticker/TickerAnalystConsensusTab.tsx").TickerAnalystConsensusTab;
const FinancialPanel = load("components/ticker/TickerFinancialsPanel.tsx").TickerFinancialsPanel;
const render = (component, data) => renderToStaticMarkup(React.createElement(component, { data, symbol: "AAPL" }));

test("paid target-only panel renders independently dated USD evidence without a recommendation date", () => {
  const html = render(AnalystPanel, {
    access: { detailsLocked: false }, availability: { status: "partial" },
    currentSnapshot: { source: "finnhub", symbol: "AAPL", snapshotDate: null,
      targetSourceUpdatedAt: "2026-10-14T12:00:00Z", targetHorizon: "unverified",
      priceTargetRange: { low: 100, high: 200, median: 145, consensus: 150, analystCount: 7 } },
  });
  assert.match(html, /Finnhub targets updated/);
  assert.match(html, /USD per listed share/);
  assert.match(html, /Target horizon unavailable/);
  assert.match(html, /\$150/);
  assert.doesNotMatch(html, /Finnhub recommendation period/);
});

test("free panel does not render a target range even if a caller supplies one", () => {
  const html = render(AnalystPanel, {
    access: { detailsLocked: true }, availability: { status: "partial" },
    currentSnapshot: { recommendationLabel: "Bullish", priceTargetRange: { consensus: 98765 } },
  });
  assert.doesNotMatch(html, /98,765|98765|Finnhub targets updated/);
  assert.match(html, /Premium/);
});

test("financial panel labels proprietary averages and renders zero EPS forecast", () => {
  const html = render(FinancialPanel, {
    status: "partial", annual: [], quarterly: [], earnings: [], summary: {},
    forecastBasis: "Finnhub averages include analyst and proprietary estimates.",
    forecasts: { nextQuarter: { period: "Q1 FY 2027", date: "2026-12-31", source: "finnhub", revenueEstimate: 200, epsEstimate: 0 } },
    subsections: { analyst_estimates: { status: "limited" } },
  });
  assert.match(html, /Finnhub averages include analyst and proprietary estimates/);
  assert.match(html, /Q1 FY 2027/);
  assert.match(html, /Forecast/);
  assert.doesNotMatch(html, /Analyst estimates are not available for this ticker/);
});
