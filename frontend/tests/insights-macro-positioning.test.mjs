import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const insightsPage = read("app/insights/page.tsx");
const component = read("components/insights/InsightsMacroPositioningClient.tsx");
const tickerFlyout = read("components/ticker/ConfirmationSourcesFlyout.tsx");
const api = read("lib/api.ts");
const landing = read("app/landing/page.tsx");

test("insights page renders macro positioning section with stable anchor", () => {
  assert.match(insightsPage, /<InsightsMarketSnapshotClient \/>[\s\S]*<InsightsMacroPositioningClient \/>[\s\S]*<InsightsNewsClient/);
  assert.match(component, /id="macro-positioning"/);
  assert.match(component, /Institutional futures positioning across major markets\./);
  assert.match(api, /\/api\/insights\/macro-positioning/);
});

test("macro positioning section has pro lock, cards, and flyout", () => {
  assert.match(component, /Included with Walnut Pro\./);
  assert.match(component, /Upgrade to Pro/);
  assert.match(component, /function MacroCard/);
  assert.match(component, /function MacroFlyout/);
  assert.match(component, /role="dialog"/);
  assert.match(component, /Overall Bias/);
  assert.match(component, /Weekly Trend/);
  assert.match(component, /Interpretation/);
});

test("macro positioning user-facing code avoids provider terminology", () => {
  for (const source of [component, insightsPage]) {
    assert.doesNotMatch(source, /\bCOT\b|Commitment of Traders|CFTC|FMP|provider|endpoint/i);
  }
});

test("landing does not point to a standalone macro page", () => {
  assert.doesNotMatch(landing, /\/macro-positioning/);
  assert.match(landing, /\/insights#macro-positioning/);
});

test("ticker macro positioning avoids irrelevant locked chips and neutral missing-data copy", () => {
  assert.match(tickerFlyout, /macroSource\?\.present \|\| macroSource\?\.locked/);
  assert.doesNotMatch(tickerFlyout, /macroLocked\) && !active\.includes\("macro_positioning"\)/);
  assert.doesNotMatch(tickerFlyout, /currently neutral for this investment thesis/);
  assert.match(tickerFlyout, /Macro Positioning is not available for this ticker yet\./);
});

test("macro positioning does not present missing trend as stable", () => {
  assert.match(component, /return "Unavailable";/);
  assert.match(component, /Institutional positioning is available for the latest weekly report\./);
  assert.match(component, /Latest interpretation is unavailable\./);
});
