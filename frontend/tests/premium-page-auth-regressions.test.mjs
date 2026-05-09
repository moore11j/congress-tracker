import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const signalsPage = read("app/signals/page.tsx");
const screenerPage = read("app/screener/page.tsx");
const leaderboardPage = read("app/leaderboards/congress-traders/page.tsx");
const backtestingWorkbench = read("components/backtesting/BacktestingWorkbench.tsx");
const savedViewsBar = read("components/saved-views/SavedViewsBar.tsx");
const entitlements = read("lib/entitlements.ts");
const api = read("lib/api.ts");
const screenerClient = read("components/screener/ScreenerClientPage.tsx");
const signalsClient = read("components/signals/SignalsClientPage.tsx");
const leaderboardClient = read("components/leaderboards/CongressTraderLeaderboardClientPage.tsx");

test("protected premium pages render client bearer fallbacks when SSR only has ct_auth_hint", () => {
  assert.match(signalsPage, /SignalsClientPage/);
  assert.match(signalsPage, /if \(!authToken\)/);
  assert.match(screenerPage, /optionalPageAuthState/);
  assert.match(screenerPage, /authState\.hasAuthHint/);
  assert.match(screenerPage, /ScreenerClientPage/);
  assert.match(leaderboardPage, /CongressTraderLeaderboardClientPage/);
  assert.match(leaderboardPage, /if \(!authToken\)/);
});

test("client premium fallbacks use shared API functions with bearer-compatible fetches", () => {
  assert.match(api, /window\.localStorage\.getItem\(authTokenStorageKey\)/);
  assert.match(signalsClient, /getSignalsAll\(/);
  assert.match(screenerClient, /getScreener\(params\)/);
  assert.match(leaderboardClient, /getCongressTraderLeaderboard\(/);
  assert.match(backtestingWorkbench, /getBacktestPresets\(\)/);
  assert.match(backtestingWorkbench, /getEntitlements\(\)/);
});

test("admin users are treated as entitled by frontend helpers", () => {
  assert.match(entitlements, /entitlements\.tier === "admin" \|\| entitlements\.user\?\.is_admin/);
  assert.match(entitlements, /Math\.max\(entitlements\.limits\[feature\]/);
});

test("leaderboard and client fallbacks avoid raw HTTP debug text in user-facing 401 states", () => {
  assert.match(leaderboardPage, /cleanLeaderboardError/);
  assert.match(leaderboardPage, /Sign in required\./);
  assert.match(leaderboardPage, /Premium access required\./);
  assert.match(signalsClient, /cleanProtectedError/);
  assert.match(screenerClient, /cleanProtectedError/);
  assert.doesNotMatch(leaderboardClient, /URL:|Body:/);
});

test("saved view edited state uses canonical signatures instead of raw selected-view params", () => {
  assert.match(savedViewsBar, /function savedViewSignature/);
  assert.match(savedViewsBar, /savedViewSignature\(view\.params, paramKeys, defaultParams\) === currentSignature/);
  assert.match(savedViewsBar, /activeViewIsDirty = Boolean\(activeView && savedViewSignature/);
  assert.match(savedViewsBar, /savedViewSignature\(\{\}, paramKeys, defaultParams\)/);
});

test("screener monitoring premium badge reserves inline space to avoid overlap", () => {
  assert.match(screenerPage, /className="inline-flex rounded-lg pr-20"/);
  assert.match(screenerClient, /className="inline-flex rounded-lg pr-20"/);
});
