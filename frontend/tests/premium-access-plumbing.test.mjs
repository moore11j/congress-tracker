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
const entitlements = read("lib/entitlements.ts");
const api = read("lib/api.ts");
const serverAuth = read("lib/serverAuth.ts");

test("signals page preserves the full filter and saved-view surface", () => {
  assert.match(signalsPage, /Mode/);
  assert.match(signalsPage, /Side/);
  assert.match(signalsPage, /Sort/);
  assert.match(signalsPage, /Confirm/);
  assert.match(signalsPage, /Direction/);
  assert.match(signalsPage, /Sources/);
  assert.match(signalsPage, /Signals table/);
  assert.match(signalsPage, /SavedViewsBar/);
  assert.doesNotMatch(signalsPage, /SignalsClientPage/);
});

test("screener page preserves presets, filter sections, and workflow controls", () => {
  assert.match(screenerPage, /Starter presets/);
  assert.match(screenerPage, /Base filters/);
  assert.match(screenerPage, /Intelligence filters/);
  assert.match(screenerPage, /Backtest this screen/);
  assert.match(screenerPage, /ScreenerExportButton/);
  assert.match(screenerPage, /Monitoring/);
  assert.doesNotMatch(screenerPage, /ScreenerClientPage/);
});

test("leaderboard page preserves filters and source-mode tabs", () => {
  assert.match(leaderboardPage, /Lookback/);
  assert.match(leaderboardPage, /Chamber/);
  assert.match(leaderboardPage, /Sort/);
  assert.match(leaderboardPage, /Min Trades/);
  assert.match(leaderboardPage, /Limit/);
  assert.match(leaderboardPage, /Congress/);
  assert.match(leaderboardPage, /Insiders/);
  assert.doesNotMatch(leaderboardPage, /CongressTraderLeaderboardClientPage/);
});

test("backtesting workbench preserves full workflow controls", () => {
  assert.match(backtestingWorkbench, /Backtest Signals &amp; Portfolios/);
  assert.match(backtestingWorkbench, /Watchlist/);
  assert.match(backtestingWorkbench, /Screens/);
  assert.match(backtestingWorkbench, /Signals/);
  assert.match(backtestingWorkbench, /Congress Strategy/);
  assert.match(backtestingWorkbench, /Portfolio Settings/);
  assert.match(backtestingWorkbench, /Run Backtest/);
});

test("admin entitlement and cross-site tier hint plumbing stay non-secret", () => {
  assert.match(entitlements, /entitlements\.tier === "admin" \|\| entitlements\.user\?\.is_admin/);
  assert.match(api, /entitlementHintCookieName = "ct_entitlement_hint"/);
  assert.match(api, /window\.localStorage\.setItem\(entitlementTierStorageKey, tier\)/);
  assert.match(serverAuth, /entitlementHintCookieName/);
  assert.doesNotMatch(api, /document\.cookie = `\$\{backendSessionCookieName\}=\$\{token\}/);
  assert.doesNotMatch(api, /ct_session=\$\{token\}/);
});

test("leaderboard renders clean protected errors instead of raw ApiError bodies", () => {
  assert.match(leaderboardPage, /cleanLeaderboardError/);
  assert.match(leaderboardPage, /Sign in required\./);
  assert.match(leaderboardPage, /Premium access required\./);
  assert.doesNotMatch(leaderboardPage, /URL:|Body:/);
});
