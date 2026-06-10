import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const componentPath = path.join(process.cwd(), "components", "watchlists", "AddTickerToWatchlist.tsx");
const typesPath = path.join(process.cwd(), "lib", "types.ts");

const componentSource = fs.readFileSync(componentPath, "utf8");
const typesSource = fs.readFileSync(typesPath, "utf8");

test("add-to-watchlist modal distinguishes focused rows from saved membership", () => {
  assert.match(typesSource, /symbols\?: string\[\]/);
  assert.match(componentSource, /function watchlistHasSymbol/);
  assert.match(componentSource, /normalizedSymbolValue\(item\) === normalized/);
  assert.match(componentSource, /const isInWatchlist = watchlistHasSymbol\(watchlist, normalizedSymbol\)/);
  assert.match(componentSource, /onClick=\{\(\) => handleWatchlistRowClick\(watchlist\)\}/);
  assert.match(componentSource, /isInWatchlist \? "Added" : "Add"/);
  assert.doesNotMatch(componentSource, />Selected</);
  assert.doesNotMatch(componentSource, /Add to selected watchlist/);
});

test("add-to-watchlist trigger guards mobile feed taps from navigation and focus scroll", () => {
  assert.match(componentSource, /type="button"/);
  assert.match(componentSource, /event\.preventDefault\(\)/);
  assert.match(componentSource, /event\.stopPropagation\(\)/);
  assert.doesNotMatch(componentSource, /href="#/);
  assert.doesNotMatch(componentSource, /scrollIntoView/);
  assert.doesNotMatch(componentSource, /autoFocus=/);
});

test("add-to-watchlist UX gives clear toast feedback and no-watchlist CTA", () => {
  assert.match(componentSource, /No ticker symbol available for this disclosure\./);
  assert.match(componentSource, /No watchlist found\. Create one first\./);
  assert.match(componentSource, /Create watchlist/);
  assert.match(componentSource, /Added \$\{normalizedSymbol\} to/);
  assert.match(componentSource, /is already in/);
  assert.match(componentSource, /aria-live=\{toast\.tone === "error" \? "assertive" : "polite"\}/);
});
