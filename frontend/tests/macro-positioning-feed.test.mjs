import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const page = read("app/feed/macro-positioning/page.tsx");
const component = read("components/insights/InsightsMacroPositioningClient.tsx");
const feedShell = read("components/feed/FeedPageClient.tsx");
const api = read("lib/api.ts");

test("macro positioning activity is routed through the insights widget", () => {
  assert.match(page, /redirect\("\/insights#macro-positioning"\)/);
  assert.match(api, /\/api\/feed\/macro-positioning/);
  assert.doesNotMatch(feedShell, /\/feed\/macro-positioning/);
  assert.doesNotMatch(feedShell, />\s*Macro Positioning\s*<\/a>/);
});

test("macro positioning widget renders required table hierarchy", () => {
  for (const heading of ["Report Date", "Market", "Positioning", "Weekly Change", "Historical Range", "Trend", "Insight"]) {
    assert.match(component, new RegExp(heading));
  }
  assert.match(component, /Overview/);
  assert.match(component, /Positioning Feed/);
  assert.match(component, /Significant Changes/);
  assert.match(component, /All Markets/);
  assert.match(component, /getMacroPositioningFeed/);
});

test("macro positioning feed pagination uses 25 50 100", () => {
  assert.match(component, /const pageSizeOptions = \[25, 50, 100\] as const/);
  assert.doesNotMatch(component, /5 \/ page/);
  assert.doesNotMatch(component, /10 \/ page/);
});

test("macro positioning all-markets view remains selectable in the widget", () => {
  assert.match(component, /useState<MacroFeedView>\("significant"\)/);
  assert.match(component, /<option value="all">All Markets<\/option>/);
  assert.match(component, /setView\(event\.target\.value as MacroFeedView\)/);
});

test("macro positioning locked view avoids real values and has upgrade action", () => {
  assert.match(component, /Macro positioning requires Pro\./);
  assert.match(component, /Upgrade to Pro/);
  assert.match(component, /data\?\.locked_copy/);
  assert.doesNotMatch(component, /lockedMacroPositioningFallback/);
});

test("macro positioning user-facing code avoids provider terminology", () => {
  assert.doesNotMatch(component, /\bCOT\b|Commitment of Traders|CFTC|FMP|provider|endpoint/i);
});
