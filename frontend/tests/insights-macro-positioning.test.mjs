import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const insightsPage = read("app/insights/page.tsx");
const component = read("components/insights/InsightsMacroPositioningPanel.tsx");
const tickerFlyout = read("components/ticker/ConfirmationSourcesFlyout.tsx");
const api = read("lib/api.ts");
const landing = read("app/landing/page.tsx");

test("insights page renders approved dashboard hierarchy", () => {
  assert.match(insightsPage, /<InsightsMarketSnapshotClient \/>[\s\S]*<InsightsNewsClient[\s\S]*<InsightsMacroPositioningPanel \/>[\s\S]*<ResearchBriefsSection \/>/);
  assert.match(component, /id="macro-positioning"/);
  assert.match(component, /View full Macro Positioning feed/);
  assert.match(api, /\/api\/insights\/macro-positioning/);
});

test("macro positioning panel has pro lock, compact rows, and feed links", () => {
  assert.match(component, /Upgrade to Pro/);
  assert.match(component, /function findMarket/);
  assert.match(component, /No recent positioning update\./);
  assert.doesNotMatch(component, /getMacroPositioningFeed/);
  assert.match(component, /MACRO_POSITIONING_HREF = "\/feed\/macro-positioning"/);
  assert.match(component, /ChartPulseIcon/);
  assert.match(component, /RatesIcon/);
  assert.match(component, /RiskIcon/);
  for (const heading of ["Equity Positioning", "Rates", "US Dollar", "Gold", "Oil", "Bitcoin", "Risk-On / Risk-Off"]) {
    assert.match(component, new RegExp(heading));
  }
  assert.match(component, /\/feed\/macro-positioning/);
});

test("macro positioning user-facing code avoids provider terminology", () => {
  for (const source of [component, insightsPage]) {
    assert.doesNotMatch(source, /Commitment of Traders|CFTC|FMP|provider|endpoint/i);
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
  assert.match(tickerFlyout, /href="\/pricing"/);
  assert.match(tickerFlyout, /Upgrade to Pro/);
});

test("macro positioning does not present missing trend as stable", () => {
  assert.match(component, /return "Insufficient data";/);
  assert.match(component, /No recent positioning update\./);
  assert.doesNotMatch(component, /missing.*stable/i);
});
