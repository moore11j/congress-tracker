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
  assert.match(tickerPage, /canCreateResearch=\{canViewProContext\}/);
  assert.match(comparePage, /canCreateResearch/);
  assert.match(comparePage, /<ResearchActions canCreateResearch=\{canCreateResearch\} subject=\{\{ kind: "compare", data \}\}/);
});

test("Create Research is gated behind Pro entitlement checks", () => {
  assert.match(componentSource, /canCreateResearch: boolean/);
  assert.match(componentSource, /if \(!canCreateResearch\) return null/);
  assert.match(tickerPage, /\{canViewProContext \? \(/);
  assert.match(comparePage, /getEntitlements\(authState\.token/);
  assert.match(comparePage, /hasEntitlement\(entitlements, "institutional_feed"\)/);
});

test("research actions include the required Phase 4 outputs", () => {
  for (const label of [
    "Copy Walnut Take",
    "Copy Data Bullets",
    "Create X Card",
    "Create Reddit DD Outline",
    "Export Research Brief",
    "shareResearchUrl",
  ]) {
    assert.match(componentSource, new RegExp(label));
  }
  assert.match(componentSource, /buildSimplePdf/);
  assert.match(componentSource, /drawPageBase/);
  assert.match(componentSource, /socialCardSvg/);
  assert.match(componentSource, /subject\.kind === "compare"/);
  assert.match(componentSource, /<button type="button" onClick=\{\(\) => run\(\(\) => shareResearchUrl\(\)\)\}/);
  assert.doesNotMatch(componentSource, /label="Copy Comparison Conclusion"/);
  assert.doesNotMatch(componentSource, /label="Share Research URL"/);
});

test("research actions are deterministic and do not call an LLM endpoint", () => {
  assert.doesNotMatch(componentSource, /openai|chat\/completions|generateText|ai_marketing/i);
  assert.match(componentSource, /buildResearchOutputs/);
});
