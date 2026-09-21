import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import ts from "typescript";

const code = ts.transpileModule(fs.readFileSync("lib/tickerOperationalOverview.ts", "utf8"), { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { mergeOperationalOverview } = await import(`data:text/javascript;base64,${Buffer.from(code).toString("base64")}`);
const now = Date.parse("2026-09-21T00:00:00Z");
const base = { symbol: "MU", confirmation: { score: 74 }, catalysts: [{ category: "fundamentals", title: "Fundamentals", description: "Reported growth" }], risks: [], what_changed: [], watch_items: [{ category: "price_volume", title: "Tape", description: "Watch tape" }] };
const event = (id, extra = {}) => ({ id, title: "Management expects a product launch, subject to approval.", summary: "Longer detail", event_type: "product_launch", source_type: "press_release", source_url: "https://example.test/release", published_at: "2026-09-20T00:00:00Z", materiality: "high", confidence: "medium", ...extra });
const data = (extra = {}) => ({ symbol: "MU", catalysts: [], risks: [], opportunities: [], watch_next: [], ...extra });

test("AI findings flow into existing Overview buckets without altering confirmation", () => {
  const positive = event("positive");
  const result = mergeOperationalOverview(base, data({ catalysts: [positive], opportunities: [positive], risks: [event("risk", { title: "Management lowered guidance." })], watch_next: [event("watch", { title: "Approval decision in Q4." })] }), now);
  assert.equal(result.catalysts.length, 2); // one operating item, one existing input
  assert.equal(result.catalysts[0].description, positive.title); // preserve uncertainty verbatim
  assert.equal(result.catalysts[0].sourceUrl, positive.source_url);
  assert.equal(result.risks[0].evidenceId, "risk");
  assert.equal(result.watch_items[0].description, "Approval decision in Q4.");
  assert.deepEqual(result.confirmation, base.confirmation);
  assert.equal(base.catalysts.length, 1); // no mutation of server data
});

test("What Changed includes only dated findings in the last 30 days", () => {
  const result = mergeOperationalOverview(base, data({ catalysts: [event("recent"), event("old", { title: "Older release", published_at: "2026-07-01" }), event("missing", { title: "Undated", published_at: null }), event("future", { title: "Future date", published_at: "2026-12-01" })] }), now);
  assert.deepEqual(result.what_changed.map(x => x.evidenceId), ["recent"]);
});

test("opportunities deduplicate into catalysts, neutral facts do not invent watch items", () => {
  const positive = event("launch");
  const result = mergeOperationalOverview(base, data({ opportunities: [positive, { ...positive, id: "syndicated" }] }), now);
  assert.equal(result.catalysts.filter(x => x.evidenceId).length, 1);
  assert.deepEqual(result.watch_items, base.watch_items);
});

test("unavailable and cross-ticker data retain existing confirmation inputs", () => {
  assert.equal(mergeOperationalOverview(base, null, now), base);
  assert.equal(mergeOperationalOverview(base, data({ symbol: "AAPL", catalysts: [event("wrong")] }), now), base);
});

test("operational rows are bounded and leave room for existing inputs", () => {
  const result = mergeOperationalOverview(base, data({ catalysts: Array.from({ length: 10 }, (_, i) => event(String(i), { title: `Development ${i}` })) }), now);
  assert.equal(result.catalysts.filter(x => x.evidenceId).length, 3);
  assert.equal(result.catalysts.at(-1).category, "fundamentals");
});

test("Research houses details beneath briefs and shares one fetch with Overview", () => {
  const context = fs.readFileSync("components/ticker/TickerContextCard.tsx", "utf8");
  const research = context.slice(context.indexOf('{activeTab === "research" ? ('), context.indexOf('{activeTab === "ownership" ? ('));
  assert.ok(research.indexOf("researchItems.map") < research.indexOf("<TickerOperationalIntelligenceCard"));
  assert.equal((context.match(/<TickerOperationalIntelligenceCard/g) ?? []).length, 1);
  assert.match(context, /<TickerOperationalIntelligenceProvider/);
  for (const file of ["TickerOperationalIntelligenceCard", "TickerDecisionPanels"]) {
    const source = fs.readFileSync(`components/ticker/${file}.tsx`, "utf8");
    assert.match(source, /useTickerOperationalIntelligence/);
    assert.doesNotMatch(source, /getTickerOperationalIntelligence\(/);
  }
  const panel = fs.readFileSync("components/ticker/TickerDecisionPanels.tsx", "utf8");
  assert.match(panel, /researchSourceHref\(item.sourceUrl\)/);
  assert.match(panel, /href="#research"/);
  assert.match(panel, /failed \?/);
});
