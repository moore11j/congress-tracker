import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const componentSource = fs.readFileSync(path.join(root, "components/research/ResearchActions.tsx"), "utf8");
const tickerPage = fs.readFileSync(path.join(root, "app/ticker/[symbol]/page.tsx"), "utf8");
const comparePage = fs.readFileSync(path.join(root, "app/compare/[left]/[right]/page.tsx"), "utf8");

test("ticker and compare pages expose Create Research actions", () => {
  assert.match(tickerPage, /<ResearchActions/);
  assert.match(tickerPage, /kind: "ticker"/);
  assert.match(comparePage, /<ResearchActions subject=\{\{ kind: "compare", data \}\}/);
});

test("research actions include the required Phase 4 outputs", () => {
  for (const label of [
    "Copy Walnut Take",
    "Copy Data Bullets",
    "Copy Comparison Conclusion",
    "Create X Card",
    "Create Reddit DD Outline",
    "Export Research Brief",
    "Share Research URL",
  ]) {
    assert.match(componentSource, new RegExp(label));
  }
  assert.match(componentSource, /buildSimplePdf/);
  assert.match(componentSource, /socialCardSvg/);
});

test("research actions are deterministic and do not call an LLM endpoint", () => {
  assert.doesNotMatch(componentSource, /openai|chat\/completions|generateText|ai_marketing/i);
  assert.match(componentSource, /buildResearchOutputs/);
});
