import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const addTickerToWatchlist = read("components/watchlists/AddTickerToWatchlist.tsx");
const watchlistsDashboard = read("components/watchlists/WatchlistsDashboard.tsx");
const watchlistCreateForm = read("components/watchlists/WatchlistCreateForm.tsx");
const watchlistDetailContent = read("components/watchlists/WatchlistDetailContent.tsx");
const watchlistNamesSource = read("lib/watchlistNames.ts");

function loadWatchlistNameHelper() {
  const executableSource = watchlistNamesSource
    .replace(/^import type .*;\r?\n/m, "")
    .replace(
      "export function nextDefaultWatchlistName(watchlists: Pick<WatchlistSummary, \"name\">[])",
      "function nextDefaultWatchlistName(watchlists)",
    )
    .replace("new Set<number>()", "new Set()");
  return new Function(`${executableSource}; return nextDefaultWatchlistName;`)();
}

test("ticker add with zero watchlists carries create intent to watchlists", () => {
  assert.match(addTickerToWatchlist, /useRouter/);
  assert.match(addTickerToWatchlist, /normalizeTickerSymbol\(symbol\)/);
  assert.match(addTickerToWatchlist, /intent=addTicker/);
  assert.match(addTickerToWatchlist, /symbol=\$\{encodeURIComponent\(normalizedSymbol\)\}/);
  assert.match(addTickerToWatchlist, /returnTo=\$\{encodeURIComponent\(returnTo\)\}/);
  assert.match(addTickerToWatchlist, /createdAt=\$\{Date\.now\(\)\}/);
  assert.match(addTickerToWatchlist, /if \(items\.length === 0\) \{\s*router\.push\(createWatchlistHref\);\s*return;\s*\}/);
});

test("watchlist creation consumes pending ticker intent and lands on detail route", () => {
  assert.match(watchlistsDashboard, /pendingTickerIntentFromSearchParams/);
  assert.match(watchlistsDashboard, /searchParams\.get\("create"\) !== "1"/);
  assert.match(watchlistsDashboard, /pendingTickerIntentMaxAgeMs = 15 \* 60 \* 1000/);
  assert.match(watchlistsDashboard, /safeInternalReturnTo/);
  assert.match(watchlistsDashboard, /await addToWatchlist\(created\.id, pendingTickerIntent\.symbol\)/);
  assert.match(watchlistsDashboard, /router\.push\(`\/watchlists\/\$\{created\.id\}`\)/);
  assert.match(watchlistsDashboard, /rememberWatchlistToast\(`Watchlist created, but we couldn't add \$\{pendingTickerIntent\.symbol\}\. Please try again\.`\)/);
  assert.match(watchlistsDashboard, /if \(!pendingTickerIntent\) \{\s*await refreshWatchlists\(\);\s*return;\s*\}/);
});

test("pending ticker flow has a visible failure message without final query params", () => {
  assert.match(watchlistsDashboard, /window\.sessionStorage\.setItem\(pendingWatchlistToastKey, message\)/);
  assert.match(watchlistDetailContent, /window\.sessionStorage\.getItem\(pendingWatchlistToastKey\)/);
  assert.match(watchlistDetailContent, /window\.sessionStorage\.removeItem\(pendingWatchlistToastKey\)/);
  assert.match(watchlistDetailContent, /role="alert"/);
});

test("watchlist create form asks for a name and passes created id to callbacks", () => {
  assert.match(watchlistCreateForm, />Name your watchlist<\/h2>/);
  assert.match(watchlistCreateForm, /const created = await createWatchlist\(trimmed\)/);
  assert.match(watchlistCreateForm, /await onCreated\?\.\(created\)/);
  assert.match(watchlistCreateForm, /Creating this watchlist will add \{pendingTickerSymbol\}/);
  assert.doesNotMatch(watchlistCreateForm, />Create a watchlist<\/h2>/);
});

test("default watchlist numbering uses active names and lowest available number", () => {
  const nextDefaultWatchlistName = loadWatchlistNameHelper();

  assert.equal(nextDefaultWatchlistName([]), "Watchlist 1");
  assert.equal(nextDefaultWatchlistName([{ name: "Watchlist 1" }]), "Watchlist 2");
  assert.equal(nextDefaultWatchlistName([{ name: "Watchlist 1" }, { name: "Watchlist 3" }]), "Watchlist 2");
  assert.equal(nextDefaultWatchlistName([{ name: "AI Stocks" }]), "Watchlist 1");
});
