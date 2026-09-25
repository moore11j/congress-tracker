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
let researchAccess = { status: "allowed", retry() {} };
const cache = new Map();
function load(relative) {
  if (cache.has(relative)) return cache.get(relative);
  const code = ts.transpileModule(fs.readFileSync(relative, "utf8"), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } }).outputText;
  const module = { exports: {} };
  new Function("require", "module", "exports", code)((name) => {
    if (name.endsWith("ResearchMemoryAccess")) return { useResearchMemoryAccess: () => researchAccess, ResearchMemoryAccessNotice: load("components/research-memory/ResearchMemoryAccess.tsx").ResearchMemoryAccessNotice };
    if (name === "@/lib/api") return { ApiError: class extends Error {}, getEntitlements() { throw new Error("Unexpected render-time request"); } };
    if (name === "@/lib/productAnalytics") return { trackEvent() {} };
    if (name.endsWith("VisibleEvent")) return { VisibleEvent: ({ children, className }) => React.createElement("div", { className }, children) };
    if (name.endsWith("TickerOperationalIntelligenceProvider")) return { useTickerOperationalIntelligence: () => provider };
    if (name === "next/link") return ({ href, children, prefetch, ...props }) => React.createElement("a", { href, ...props }, children);
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

test("Research Memory capabilities require Premium even with a stale Free payload", () => {
  const { hasEntitlement, defaultEntitlements, premiumEntitlements, proEntitlements } = load("lib/entitlements.ts");
  for (const key of ["view_research_memory", "create_research_memory", "use_custom_thesis_ai", "monitor_research_memory", "receive_thesis_alerts"]) {
    assert.equal(hasEntitlement(defaultEntitlements, key), false);
    assert.equal(hasEntitlement({ ...defaultEntitlements, features: [key] }, key), false);
    assert.equal(hasEntitlement(premiumEntitlements, key), true);
    assert.equal(hasEntitlement(proEntitlements, key), true);
    assert.equal(hasEntitlement({ ...defaultEntitlements, is_admin: true }, key), true);
  }
});

test("Company developments lock renders the common CTA without premium findings", () => {
  researchAccess = { status: "locked", retry() {} };
  provider = { data: data({ risks: [event("private", { title: "PREMIUM_FINDING" })] }), disabled: true, failed: false, retry() {} };
  const html = render(TickerOperationalIntelligenceCard, { symbol: "MU" });
  assert.match(html, /Company developments/);
  assert.match(html, /Unlock with Premium/);
  assert.match(html, /bg-emerald-300/);
  assert.doesNotMatch(html, /PREMIUM_FINDING|Evidence &amp; interpretation|Build a thesis/);
  researchAccess = { status: "allowed", retry() {} };
  provider = { data: null, disabled: false, failed: false, retry() {} };
});

test("Your Research has a Premium CTA and preserves disabled-feature behavior", () => {
  const { TickerResearchMemoryCard } = load("components/ticker/TickerResearchMemoryCard.tsx");
  researchAccess = { status: "locked", retry() {} };
  assert.match(render(TickerResearchMemoryCard, { symbol: "MU" }), /Your Research/);
  assert.match(render(TickerResearchMemoryCard, { symbol: "MU" }), /Unlock with Premium/);
  const previous = process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED;
  process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED = "false";
  assert.equal(render(TickerResearchMemoryCard, { symbol: "MU" }), "");
  if (previous === undefined) delete process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED;
  else process.env.NEXT_PUBLIC_RESEARCH_MEMORY_ENABLED = previous;
  researchAccess = { status: "allowed", retry() {} };
  assert.doesNotMatch(render(TickerResearchMemoryCard, { symbol: "MU" }), /Unlock with Premium/);
});

test("access uncertainty shows a retry or loading state, not paid contents", () => {
  const { ResearchMemoryAccessNotice } = load("components/research-memory/ResearchMemoryAccess.tsx");
  assert.match(render(ResearchMemoryAccessNotice, { status: "error", retry() {} }), /role="alert"/);
  assert.match(render(ResearchMemoryAccessNotice, { status: "error", retry() {} }), /Try again/);
  assert.match(render(ResearchMemoryAccessNotice, { status: "loading", retry() {} }), /role="status"/);
  assert.equal(render(ResearchMemoryAccessNotice, { status: "allowed", retry() {} }), "");
});

test("every blurred decision section has an accessible common upgrade CTA", () => {
  const { TickerDecisionPanels } = load("components/ticker/TickerDecisionPanels.tsx");
  const layer = { symbol: "MU", catalysts: [], risks: [], what_changed: [], watch_items: [] };
  const html = render(TickerDecisionPanels, { layer, locked: true });
  assert.equal((html.match(/Unlock with Premium/g) ?? []).length, 4);
  assert.equal((html.match(/inert=""/g) ?? []).length, 4);
  assert.match(html, /Understand the catalysts/);
  assert.match(html, /Understand the risks/);
  assert.match(html, /See what changed/);
  assert.doesNotMatch(render(TickerDecisionPanels, { layer, locked: false }), /Unlock with Premium/);
  assert.match(fs.readFileSync("app/ticker/[symbol]/page.tsx", "utf8"), /divergenceLocked \? <ContextualUpgrade/);
});

test("Premium fetches wait for access and direct Research routes include the gate", () => {
  const source = fs.readFileSync("components/ticker/TickerOperationalIntelligenceProvider.tsx", "utf8");
  assert.ok(source.indexOf('access !== "allowed") return') < source.indexOf("getTickerOperationalIntelligence(symbol)"));
  assert.match(source, /access !== "allowed" \? \{ data: null/);
  for (const file of ["app/monitoring/research/page.tsx", "app/monitoring/research/[id]/page.tsx"]) assert.match(fs.readFileSync(file, "utf8"), /<ResearchMemoryAccessBoundary>/);
  const context = fs.readFileSync("components/ticker/TickerContextCard.tsx", "utf8");
  assert.ok(context.indexOf("researchItems.map") < context.indexOf("<TickerOperationalIntelligenceCard"));
});
