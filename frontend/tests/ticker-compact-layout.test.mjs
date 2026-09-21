import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);
let provider = { data: null, disabled: false, failed: false, retry() {} };
const cache = new Map();
function load(relative) {
  if (cache.has(relative)) return cache.get(relative);
  const code = ts.transpileModule(fs.readFileSync(relative, "utf8"), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
  const module = { exports: {} };
  new Function("require", "module", "exports", code)((name) => {
    if (name.endsWith("TickerOperationalIntelligenceProvider")) return { useTickerOperationalIntelligence: () => provider };
    if (name === "next/link") return ({ href, children, ...props }) => React.createElement("a", { href, ...props }, children);
    if (name.startsWith("@/") || name.startsWith(".")) {
      const base = name.startsWith("@/") ? name.slice(2) : path.join(path.dirname(relative), name);
      return load([`${base}.ts`, `${base}.tsx`].find(fs.existsSync));
    }
    return require(name);
  }, module, module.exports);
  cache.set(relative, module.exports);
  return module.exports;
}
const { collectResearchFindings, selectResearchFindings } = load("lib/tickerResearchFindings.ts");
const { TickerSourceAlignment } = load("components/ticker/TickerSourceAlignment.tsx");
const { TickerOperationalIntelligenceCard, ResearchFindingRow } = load("components/ticker/TickerOperationalIntelligenceCard.tsx");
const { ResearchCoverage } = load("components/research-memory/ResearchCoverage.tsx");
const render = (Component, props = {}) => renderToStaticMarkup(React.createElement(Component, props));
const event = (id, extra = {}) => ({ id, title: "Management expects a launch, subject to approval.", summary: "Full interpretation remains available.", source_type: "press_release", event_type: "product_launch", source_url: "https://example.test/release", published_at: "2026-09-20", materiality: "high", confidence: "medium", evidence_excerpt: "Exact source passage.", ...extra });
const data = (extra = {}) => ({ symbol: "MU", status: "ok", catalysts: [], risks: [], opportunities: [], watch_next: [], coverage: [], lookback_days: 120, ...extra });

test("research combines shared identity but preserves every category interpretation", () => {
  const source = data({ catalysts: [event("one")], opportunities: [event("one")], watch_next: [event("one", { title: "Calendar 2028", summary: "An explicit future milestone." })], risks: [event("two")] });
  const before = structuredClone(source);
  const findings = collectResearchFindings(source);
  assert.equal(findings.length, 2);
  assert.deepEqual(findings[0].categories, ["catalysts", "opportunities", "watch_next"]);
  assert.equal(findings[0].variants.length, 3);
  assert.equal(findings[0].variants[2].item.title, "Calendar 2028");
  assert.equal(findings[0].item.title, source.catalysts[0].title);
  assert.deepEqual(source, before);
  for (const category of ["catalysts", "opportunities", "watch_next"]) assert.equal(selectResearchFindings(findings, category, "latest")[0].id, "one");
});

test("identical headlines on different evidence IDs remain separate; no new display cap", () => {
  const findings = collectResearchFindings(data({ catalysts: Array.from({ length: 40 }, (_, i) => event(String(i))) }));
  assert.equal(findings.length, 40);
  assert.equal(selectResearchFindings(findings, "all", "latest").length, 40);
  assert.deepEqual(collectResearchFindings(null), []);
});

test("sorting handles missing dates and materiality without mutating the collection", () => {
  const findings = collectResearchFindings(data({ risks: [event("old", { published_at: "2026-01-01" }), event("recent", { materiality: "low" }), event("undated", { published_at: null, materiality: "medium" })] }));
  const before = structuredClone(findings);
  assert.deepEqual(selectResearchFindings(findings, "risks", "latest").map(x => x.id), ["recent", "old", "undated"]);
  assert.deepEqual(selectResearchFindings(findings, "risks", "material").map(x => x.id), ["old", "undated", "recent"]);
  assert.deepEqual(findings, before);
});

test("expanded research row retains full text, watch interpretation and safe provenance", () => {
  const finding = collectResearchFindings(data({ catalysts: [event("one")], watch_next: [event("one", { title: "Calendar 2028", summary: "An explicit future milestone." })] }))[0];
  const html = render(ResearchFindingRow, { finding });
  for (const value of ["subject to approval", "Full interpretation remains available", "Calendar 2028", "An explicit future milestone", "Exact source passage", "medium extraction confidence", "high materiality", "https://example.test/release", "Evidence &amp; interpretation", "Evidence excerpt"]) assert.ok(html.includes(value), value);
  assert.match(html, /<details/);
  assert.doesNotMatch(html, /line-clamp|truncate/);
  const unsafe = collectResearchFindings(data({ risks: [event("bad", { source_url: "javascript:alert(1)" })] }))[0];
  assert.doesNotMatch(render(ResearchFindingRow, { finding: unsafe }), /href=/);
});

test("compact coverage retains distinct statuses, counts and both check timestamps", () => {
  const items = ["ready", "partial", "disabled", "not_checked", "unavailable", "empty", "stale"].map((status, i) => ({ source_type: `source_${i}`, status, documents_seen: i, last_checked_at: "2026-09-21T02:32:00Z", last_success_at: i ? null : "2026-09-20T01:15:00Z" }));
  const html = render(ResearchCoverage, { items, compact: true });
  for (const value of ["Checked", "Partially analyzed", "Not enabled", "Awaiting coverage", "Temporarily unavailable", "No documents returned", "Refresh overdue", "Last attempted", "Last successful check", "No completed source check yet", "Coverage details", "1 document returned", "6 documents returned"]) assert.ok(html.includes(value), value);
  assert.equal((html.match(/>✓</g) ?? []).length, 1);
  assert.match(render(ResearchCoverage, { items }), /sm:grid-cols-3/); // unchanged default presentation
});

const divergence = { state: "moderate_divergence", label: "Moderate divergence", bullish_source_count: 2, bearish_source_count: 1, active_source_count: 4, source_breakdown_available: true, bullish_sources: [{ key: "a", label: "Analysts" }], bearish_sources: [{ key: "p", label: "Price / Volume" }], neutral_sources: [{ key: "n", label: "Neutral source" }], explanation: "Sources disagree on direction.", methodology_version: "test-methodology" };
test("alignment keeps opposing and neutral sources plus complete methodology", () => {
  const html = render(TickerSourceAlignment, { divergence });
  for (const text of ["2 bullish", "1 bearish", "Analysts", "Price / Volume", "Neutral source", "Sources disagree on direction", "test-methodology", "4 active sources evaluated", "weighted disagreement", "not their weight"]) assert.ok(html.includes(text), text);
  assert.match(html, /<details/);
  assert.doesNotMatch(html, /grid-cols-2/);
});

test("unavailable and zero-count alignment cannot look like full bearish evidence", () => {
  assert.match(render(TickerSourceAlignment), /unavailable/);
  const html = render(TickerSourceAlignment, { divergence: { ...divergence, state: "unavailable", label: "Unavailable", bullish_source_count: 0, bearish_source_count: 0, source_breakdown_available: false } });
  assert.equal((html.match(/width:0%/g) ?? []).length, 2);
  assert.match(html, /Source breakdown unavailable/);
  assert.doesNotMatch(html, /width:100%|bg-emerald-300\/10/);
});

test("research renders accessible filters, safe empty/loading/error and disabled states", () => {
  provider = { ...provider, data: data({ risks: [event("risk")] }) };
  const html = render(TickerOperationalIntelligenceCard, { symbol: "MU" });
  assert.match(html, /aria-pressed="true"/);
  assert.match(html, /aria-controls=/);
  assert.match(html, /Sort company developments/);
  assert.match(html, /Material first/);
  provider = { ...provider, data: data() };
  assert.match(render(TickerOperationalIntelligenceCard, { symbol: "MU" }), /No material operating-source signals/);
  provider = { ...provider, data: null };
  assert.match(render(TickerOperationalIntelligenceCard, { symbol: "MU" }), /Loading company developments/);
  provider = { ...provider, failed: true };
  assert.match(render(TickerOperationalIntelligenceCard, { symbol: "MU" }), /role="alert"/);
  assert.match(render(TickerOperationalIntelligenceCard, { symbol: "MU" }), /Try again/);
  provider = { ...provider, disabled: true };
  assert.equal(render(TickerOperationalIntelligenceCard, { symbol: "MU" }), "");
  provider = { data: null, disabled: false, failed: false, retry() {} };
});

test("compact Overview preserves entitlement checks and calibration inspection", () => {
  const page = fs.readFileSync("app/ticker/[symbol]/page.tsx", "utf8");
  assert.match(page, /Confirmation and source alignment/);
  assert.match(page, /divergenceLocked \?/);
  assert.match(page, /confirmationLocked && confirmationGate/);
  assert.match(page, /similarHistoricalSetupsLocked \?/);
  assert.match(page, /inert=\{confirmationLocked\}/);
  assert.match(page, /Confirmation interpretation/);
  assert.match(page, /Comparison details/);
  const trend = fs.readFileSync("components/ticker/DecisionTrendChart.tsx", "utf8");
  assert.match(trend, /<details/);
  assert.match(trend, /event.description/);
  assert.match(trend, /onKeyDown=\{inspectWithKeyboard\}/);
});
