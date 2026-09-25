import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import vm from "node:vm";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);
function load(relativePath, imports = {}, suffix = "") {
  const source = readFileSync(new URL(relativePath, import.meta.url), "utf8") + suffix;
  const script = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  const exports = {};
  vm.runInNewContext(script, { exports, require: (name) => name in imports ? imports[name] : require(name) });
  return exports;
}
const chart = load("../lib/outcome-chart.ts");
const { ContextualUpgrade } = load("../components/billing/ContextualUpgrade.tsx", {
  "next/link": { default: ({ href, children, prefetch, ...props }) => React.createElement("a", { href, ...props }, children) },
  "@/components/analytics/VisibleEvent": { VisibleEvent: ({ children, className }) => React.createElement("div", { className }, children) },
  "@/lib/productAnalytics": { trackEvent() {} },
});
const { OutcomeChartPremiumGate } = load("../components/outcomes/OutcomeChartPremiumGate.tsx", {
  "@/components/billing/ContextualUpgrade": { ContextualUpgrade },
});
const { ScatterPanel, canViewPremiumOutcomes } = load("../components/outcomes/OutcomeLedgerClient.tsx", {
  "next/form": {},
  "@/components/feed/FeedSymbolAutosuggestEnhancer": {},
  "@/components/outcomes/OutcomeChartPremiumGate": { OutcomeChartPremiumGate },
  "@/lib/api": {},
  "@/lib/entitlements": {},
  "@/lib/outcome-chart": chart,
}, "\nexport { ScatterPanel, canViewPremiumOutcomes };\n");

test("locked outcome charts blur and disable the preview, not the Premium CTA", () => {
  const html = renderToStaticMarkup(React.createElement(OutcomeChartPremiumGate, {
    unlocked: false, title: "Event Outcomes", body: "Explore event returns with Premium.", feature: "outcomes_event_chart",
  }, React.createElement("button", null, "Chart interaction")));
  assert.match(html, /aria-label="Event Outcomes"/);
  assert.match(html, /aria-hidden="true" inert=""/);
  assert.match(html, /blur-\[5px\]/);
  assert.match(html, /\[grid-area:1\/1\]/); // CTA contributes height on narrow screens.
  assert.match(html, /<button>Chart interaction<\/button><\/div><div/);
  assert.match(html, /href="\/pricing"/);
  assert.match(html, /Unlock with Premium/);
  assert.match(html, /border-emerald-300\/25/);
});

test("Premium, Pro, and admin retain unmodified chart interactions", () => {
  assert.equal(canViewPremiumOutcomes("free"), false);
  for (const tier of ["premium", "pro", "admin"]) {
    const html = renderToStaticMarkup(React.createElement(OutcomeChartPremiumGate, {
      unlocked: canViewPremiumOutcomes(tier), title: "Event Outcomes", body: "Premium", feature: "outcomes_event_chart",
    }, React.createElement("button", null, "Chart interaction")));
    assert.equal(html, "<button>Chart interaction</button>");
  }
});

test("both chart gates await verified entitlements and leave the table preview intact", () => {
  const source = readFileSync(new URL("../components/outcomes/OutcomeLedgerClient.tsx", import.meta.url), "utf8");
  assert.match(source, /\[chartsUnlocked, setChartsUnlocked\] = useState\(false\)/);
  assert.match(source, /setChartsUnlocked\(entitlements.status !== "temporarily_unavailable" && canViewPremiumOutcomes/);
  assert.equal((source.match(/<OutcomeChartPremiumGate unlocked=\{chartsUnlocked\}/g) ?? []).length, 2);
  assert.match(source, /title="Performance by Score Band"[\s\S]*?<BarChartPanel/);
  assert.match(source, /title="Event Outcomes"[\s\S]*?<ScatterPanel/);
  assert.match(source, /const effectivePageSize = hasPremiumTable \? pageSize : 10/);
  assert.match(source, /nextPageSize > 10 && !gatePremiumTable\(\)/);
  assert.match(source, /<EventsTable\s+snapshots=\{publicPreviewSnapshots\}/);
});

test("chart selection excludes extreme returns symmetrically without mutating ledger values", () => {
  const values = [-8051.16, -196.06, -100, -7.2, 0, 45, 100, 196.06, NaN, Infinity];
  const ledger = Object.freeze(values.map((returnValue, id) => Object.freeze({ id, returnValue })));
  const result = chart.selectOutcomeChartPoints(ledger);
  assert.deepEqual(Array.from(result.points, (point) => point.returnValue), [-100, -7.2, 0, 45, 100]);
  assert.equal(result.omittedCount, 5);
  assert.deepEqual(ledger.map((point) => point.returnValue), values);
  assert.equal(result.points[0], ledger[2]);
  assert.equal(chart.selectOutcomeChartPoints([]).omittedCount, 0);
});

function snapshot(id, ticker, value, provisional = false) {
  return {
    id, ticker, entry_session_date: "2026-08-28", lifecycle_status: "open",
    data_integrity_status: "verified",
    outcomes: { "7D": { status: provisional ? "pending" : "matured", directional_return_pct: value, return_pct: -value } },
    live_mark: { directional_return_pct: value, return_pct: -value, price_date: "2026-09-04" },
  };
}

test("rendered scatter scales to visible points and retains measured and provisional points", () => {
  const snapshots = [snapshot(1, "GOSS", -8051.16), snapshot(2, "TJGC", -196.06), snapshot(3, "AAPL", -2.9), snapshot(4, "TSM", 7.2, true)];
  const html = renderToStaticMarkup(React.createElement(ScatterPanel, { snapshots, horizon: "7D" }));
  assert.equal((html.match(/<circle /g) ?? []).length, 2);
  assert.match(html, /AAPL opened/);
  assert.match(html, /TSM opened.*current provisional return/);
  assert.doesNotMatch(html, /GOSS opened|TJGC opened|8051|196\.1/);
  assert.match(html, /10\.0%/);
  assert.doesNotMatch(html, /100\.0%/);
  assert.match(html, /2 returns outside ±100% omitted from this chart/);
  assert.match(html, /Events remain in the table and summary metrics/);
});

test("a ticker with only extreme returns gets an explicit chart message, not a fake zero point", () => {
  const html = renderToStaticMarkup(React.createElement(ScatterPanel, { snapshots: [snapshot(1, "GOSS", -8051.16)], horizon: "7D" }));
  assert.doesNotMatch(html, /<circle /);
  assert.match(html, /All matching returns are outside the chart range/);
  assert.doesNotMatch(html, /Outcome measurements pending/);
  assert.match(html, /1 return outside ±100%/);
});
