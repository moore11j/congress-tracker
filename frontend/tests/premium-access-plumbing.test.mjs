import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const signalsPage = read("app/signals/page.tsx");
const screenerPage = read("app/screener/page.tsx");
const leaderboardPage = read("app/leaderboards/congress-traders/page.tsx");
const leaderboardTable = read("components/leaderboards/CongressTraderLeaderboardTable.tsx");
const backtestingWorkbench = read("components/backtesting/BacktestingWorkbench.tsx");
const signalsResultsClient = read("components/signals/SignalsResultsClient.tsx");
const screenerResultsClient = read("components/screener/ScreenerResultsClient.tsx");
const leaderboardResultsClient = read("components/leaderboards/CongressTraderLeaderboardClientResults.tsx");
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
  assert.match(leaderboardPage, /name="sort"/);
  assert.match(leaderboardPage, /Min Trades/);
  assert.match(leaderboardPage, /Limit/);
  assert.match(leaderboardPage, /Congress/);
  assert.match(leaderboardPage, /Insiders/);
  assert.match(leaderboardPage, /Universe/);
  assert.match(leaderboardPage, /Performance Model/);
  assert.match(leaderboardPage, /Trade Outcomes/);
  assert.match(leaderboardPage, /Portfolio Simulation/);
  assert.match(leaderboardPage, /Portfolio Window/);
  assert.doesNotMatch(leaderboardPage, /CongressTraderLeaderboardClientPage/);
});

test("leaderboard portfolio mode stays Congress-only and supports the public 1Y and 3Y endpoint contract", () => {
  assert.match(leaderboardPage, /parsePerformanceModel/);
  assert.match(leaderboardPage, /sourceMode !== "congress"/);
  assert.match(leaderboardPage, /PORTFOLIO_LOOKBACK_OPTIONS/);
  assert.match(leaderboardPage, /parsePortfolioLookback/);
  assert.match(leaderboardPage, /raw === "1095" \? 1095 : 365/);
  assert.match(leaderboardPage, /normalizePortfolioLookback/);
  assert.match(leaderboardPage, /parseLimit\(getParam\(sp, "limit"\), isPortfolioMode \? 100 : 10\)/);
  assert.match(leaderboardPage, /const targetLimit = option === "portfolio" && !active \? 100 : limit/);
  assert.match(leaderboardPage, /mode", "realistic_disclosure_lag"/);
  assert.match(leaderboardPage, /mode: performanceModel === "portfolio" \? "realistic_disclosure_lag"/);
  assert.match(leaderboardPage, /min_trades: performanceModel === "portfolio" \? undefined : minTrades/);
  assert.match(leaderboardResultsClient, /performance_model: performanceModel/);
  assert.match(leaderboardResultsClient, /chamber: performanceModel === "portfolio" \? undefined : chamber/);
  assert.match(leaderboardResultsClient, /mode: performanceModel === "portfolio" \? "realistic_disclosure_lag"/);
  assert.match(leaderboardResultsClient, /min_trades: performanceModel === "portfolio" \? undefined : minTrades/);
  assert.match(api, /performance_model: params\?\.performance_model === "outcomes" \? "trade_outcomes" : params\?\.performance_model/);
  assert.doesNotMatch(leaderboardPage, /include_poor_quality/);
  assert.doesNotMatch(leaderboardResultsClient, /include_poor_quality/);
  assert.doesNotMatch(api, /include_poor_quality/);
});

test("leaderboard portfolio quality display uses coverage language", () => {
  assert.match(leaderboardTable, /Benchmark Return/);
  assert.match(leaderboardTable, /CAGR/);
  assert.match(leaderboardTable, /Sharpe/);
  assert.match(leaderboardTable, /Max Drawdown/);
  assert.match(leaderboardTable, /Share of simulated portfolio positions/);
  assert.match(leaderboardTable, /public data-quality threshold/);
  assert.match(leaderboardTable, /Lower-coverage simulations are excluded from rankings/);
  assert.doesNotMatch(leaderboardTable, /Data Quality/);
  assert.doesNotMatch(leaderboardTable, /return "Warning"/);
});

test("leaderboard sort headers render an intentional direction label", () => {
  assert.match(leaderboardTable, /SortHeaderLabel/);
  assert.match(leaderboardTable, /sortDirectionLabel/);
  assert.match(leaderboardTable, /sortHrefs/);
  assert.doesNotMatch(leaderboardTable, /\? " v" : ""/);
});

test("transition data loaders use shared authenticated API helpers", () => {
  assert.match(signalsPage, /SignalsResultsClient/);
  assert.match(screenerPage, /ScreenerResultsClient/);
  assert.match(leaderboardPage, /CongressTraderLeaderboardClientResults/);
  assert.match(signalsResultsClient, /getSignalsAll/);
  assert.match(screenerResultsClient, /getScreener/);
  assert.match(leaderboardResultsClient, /getCongressTraderLeaderboard/);
  assert.match(api, /function requestInitWithEntitlements/);
  assert.match(api, /headers\.set\("Authorization", `Bearer \$\{token\}`\)/);
  assert.match(api, /credentials: init\?\.credentials \?\? "include"/);
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
  assert.match(leaderboardPage, /message\.startsWith\("Fetch failed for "\)/);
  assert.match(leaderboardResultsClient, /message\.startsWith\("Fetch failed for "\)/);
  assert.doesNotMatch(leaderboardPage, /URL:|Body:/);
});
