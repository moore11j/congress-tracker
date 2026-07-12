import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const page = read("app/feed/macro-positioning/page.tsx");
const client = read("components/feed/MacroPositioningFeedClient.tsx");
const feedShell = read("components/feed/FeedPageClient.tsx");
const api = read("lib/api.ts");

test("macro positioning feed has direct route and dedicated endpoint", () => {
  assert.match(page, /<MacroPositioningFeedClient \/>/);
  assert.match(api, /\/api\/feed\/macro-positioning/);
  assert.match(feedShell, /\/feed\/macro-positioning/);
});

test("macro positioning feed renders required table hierarchy", () => {
  for (const heading of ["Report Date", "Market", "Positioning", "Weekly Change", "Historical Range", "Trend", "Insight"]) {
    assert.match(client, new RegExp(heading));
  }
  assert.match(client, /Significant Changes/);
  assert.match(client, /All Markets/);
  assert.match(client, /View Macro Overview/);
  assert.match(client, /\/insights#macro-positioning/);
});

test("macro positioning feed pagination uses 25 50 100", () => {
  assert.match(client, /const pageSizeOptions = \[25, 50, 100\] as const/);
  assert.doesNotMatch(client, /5 \/ page/);
  assert.doesNotMatch(client, /10 \/ page/);
});

test("macro positioning locked view avoids real values and has upgrade action", () => {
  assert.match(client, /Upgrade to Pro/);
  assert.match(client, /Pro feature/);
  assert.match(client, /Locked/);
});

test("macro positioning user-facing code avoids provider terminology", () => {
  assert.doesNotMatch(client, /\bCOT\b|Commitment of Traders|CFTC|FMP|provider|endpoint/i);
});
