import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const page = read("app/ticker/[symbol]/page.tsx");
const card = read("components/ticker/TickerContextCard.tsx");
const api = read("lib/api.ts");

test("ticker overview renders the approved decision layer structure", () => {
  assert.match(api, /export type TickerDecisionLayer/);
  assert.match(api, /decision_layer\?: TickerDecisionLayer \| null/);
  assert.match(page, /decisionLayer=\{contextBundle\?\.decision_layer \?\? null\}/);
  assert.match(page, /30-DAY CONFIRMATION/);
  assert.match(page, /WHAT CHANGED \(30D\)/);
  assert.match(page, /CATALYSTS/);
  assert.match(page, /RISKS/);
  assert.match(page, /WHAT TO WATCH NEXT/);
  assert.match(page, /Score history unavailable/);
});

test("ticker overview keeps one confirmation score and removes old watch sentence", () => {
  assert.match(page, /<span className="text-2xl text-slate-500">\/ 100<\/span>/);
  assert.doesNotMatch(page, /Signal conviction is reinforcing this move\./);
});

test("macro positioning is a real lazy ticker tab", () => {
  assert.match(card, /type ContextTab = "overview" \| "news" \| "financials" \| "ownership" \| "events" \| "macro"/);
  assert.match(card, /onClick=\{\(\) => setActiveTab\("macro"\)\}/);
  assert.match(card, /getTickerMacroPositioning\(symbol/);
  assert.match(card, /activeTab === "macro"/);
});
