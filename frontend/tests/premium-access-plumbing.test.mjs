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
  assert.match(screenerPage, /Core Filters/);
  assert.match(screenerPage, /Intelligence Filters/);
  assert.match(screenerPage, /Technical Filters/);
  assert.match(screenerPage, /Fundamental Filters/);
  assert.match(screenerPage, /Backtest this screen/);
  assert.match(screenerPage, /ScreenerExportButton/);
  assert.match(screenerPage, /Monitoring/);
  assert.doesNotMatch(screenerPage, /ScreenerClientPage/);
});

test("leaderboard page uses compact segmented control groups", () => {
  assert.match(leaderboardPage, /Congress/);
  assert.match(leaderboardPage, /Insiders/);
  assert.match(leaderboardPage, /Universe/);
  assert.match(leaderboardPage, /Performance Model/);
  assert.match(leaderboardPage, /Trade Outcomes/);
  assert.match(leaderboardPage, /Portfolio Simulation/);
  assert.match(leaderboardPage, /Simulation Window/);
  assert.match(leaderboardPage, /Rows/);
  assert.doesNotMatch(leaderboardPage, /<select/);
  assert.doesNotMatch(leaderboardPage, /selectClassName/);
  assert.doesNotMatch(leaderboardPage, />\s*Apply\s*</);
  assert.doesNotMatch(leaderboardPage, /CongressTraderLeaderboardClientPage/);
});

test("leaderboard row limit defaults to 10 and changes immediately via URL links", () => {
  assert.match(leaderboardPage, /const LIMIT_OPTIONS = \[10, 25, 50, 100\] as const/);
  assert.match(leaderboardPage, /function parseLimit\(raw: string, fallback = 10\)/);
  assert.match(leaderboardPage, /return LIMIT_OPTIONS\.includes\(parsed as \(typeof LIMIT_OPTIONS\)\[number\]\) \? parsed : fallback/);
  assert.match(leaderboardPage, /const limit = parseLimit\(getParam\(sp, "limit"\)\)/);
  assert.match(leaderboardPage, /LIMIT_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardPage, /limit: option/);
  assert.match(leaderboardPage, /url\.searchParams\.set\("limit", String\(params\.limit\)\)/);
  assert.doesNotMatch(leaderboardPage, /isPortfolioMode \? 100 : 10/);
  assert.doesNotMatch(leaderboardPage, /limit: 100/);
});

test("leaderboard limit is applied across congress, portfolio, and insider data fetches", () => {
  assert.match(leaderboardPage, /source_mode: option/);
  assert.match(leaderboardPage, /performance_model: option/);
  assert.match(leaderboardPage, /performanceModel === "portfolio"/);
  assert.match(leaderboardPage, /const PERFORMANCE_MODEL_OPTIONS: CongressTraderLeaderboardPerformanceModel\[\] = \["outcomes", "portfolio"\]/);
  assert.match(leaderboardPage, /sourceMode === "insiders"/);
  assert.match(leaderboardPage, /limit,\s*authToken/s);
  assert.match(leaderboardResultsClient, /limit,/);
  assert.match(leaderboardResultsClient, /source_mode: sourceMode/);
  assert.match(leaderboardResultsClient, /performance_model: performanceModel/);
});

test("leaderboard defaults Congress to portfolio while forcing insiders to trade outcomes", () => {
  assert.match(leaderboardPage, /if \(sourceMode !== "congress"\) return "outcomes"/);
  assert.match(leaderboardPage, /if \(normalized === "outcomes" \|\| normalized === "trade_outcomes"\) return "outcomes"/);
  assert.match(leaderboardPage, /return "portfolio"/);
  assert.match(leaderboardPage, /params\.performance_model \?\? "portfolio"/);
  assert.match(leaderboardPage, /params\.source_mode === "congress" \? params\.performance_model \?\? "portfolio" : "outcomes"/);
  assert.match(leaderboardPage, /sourceMode === "insiders" && rawPerformanceModel\.trim\(\)\.toLowerCase\(\) === "portfolio"/);
  assert.match(leaderboardPage, /redirect\(buildUrl/);
  assert.match(leaderboardPage, /source_mode: "insiders"/);
  assert.match(leaderboardPage, /performance_model: "outcomes"/);
});

test("leaderboard keeps trade-outcome lookback links for insider mode", () => {
  assert.match(leaderboardPage, /TRADE_LOOKBACK_OPTIONS/);
  assert.match(leaderboardPage, /const LOOKBACK_OPTIONS = \[30, 90, 180, 365, 1095\] as const/);
  assert.match(leaderboardPage, /\{ label: "30D", days: 30 \}/);
  assert.match(leaderboardPage, /\{ label: "90D", days: 90 \}/);
  assert.match(leaderboardPage, /\{ label: "180D", days: 180 \}/);
  assert.match(leaderboardPage, /\{ label: "1Y", days: 365 \}/);
  assert.match(leaderboardPage, /\{ label: "3Y", days: 1095 \}/);
  assert.match(leaderboardPage, /LOOKBACK_OPTIONS\.includes\(parsed as \(typeof LOOKBACK_OPTIONS\)\[number\]\) \? parsed : 365/);
  assert.match(leaderboardPage, /Trade Outcomes Window/);
  assert.match(leaderboardPage, /TRADE_LOOKBACK_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardPage, /lookback_days: option\.days/);
  assert.match(leaderboardPage, /source_mode: sourceMode/);
  assert.match(leaderboardPage, /performance_model: "outcomes"/);
  assert.match(leaderboardPage, /params\.source_mode === "congress" \? params\.performance_model \?\? "portfolio" : "outcomes"/);
  assert.match(leaderboardPage, /url\.searchParams\.set\("source_mode", params\.source_mode\)/);
  assert.match(leaderboardPage, /url\.searchParams\.set\("performance_model", performanceModel\)/);
});

test("leaderboard disables portfolio simulation under insiders without a navigation link", () => {
  assert.match(leaderboardPage, /INSIDER_PORTFOLIO_DISABLED_TITLE = "Portfolio Simulation is currently available for Congress only\."/);
  assert.match(leaderboardPage, /if \(isInsiderMode && option === "portfolio"\) \{/);
  assert.match(leaderboardPage, /<button\s+key=\{option\}\s+type="button"\s+disabled\s+aria-disabled="true"\s+title=\{INSIDER_PORTFOLIO_DISABLED_TITLE\}/);
  assert.match(leaderboardPage, /className=\{disabledPillClassName\(\)\}/);
  assert.match(leaderboardPage, /if \(isInsiderMode && option === "portfolio"\) \{[\s\S]*?<button[\s\S]*?disabled[\s\S]*?<\/button>[\s\S]*?\}\s*const targetSort/);
  assert.match(leaderboardPage, /const targetSourceMode = option === "portfolio" \? "congress" : sourceMode/);
});

test("leaderboard portfolio mode stays Congress-only and supports all public window endpoint contracts", () => {
  assert.match(leaderboardPage, /parsePerformanceModel/);
  assert.match(leaderboardPage, /sourceMode !== "congress"/);
  assert.match(leaderboardPage, /PORTFOLIO_LOOKBACK_OPTIONS/);
  assert.match(leaderboardPage, /\{ label: "30D", days: 30 \}/);
  assert.match(leaderboardPage, /\{ label: "90D", days: 90 \}/);
  assert.match(leaderboardPage, /\{ label: "180D", days: 180 \}/);
  assert.match(leaderboardPage, /\{ label: "1Y", days: 365 \}/);
  assert.match(leaderboardPage, /\{ label: "3Y", days: 1095 \}/);
  assert.match(leaderboardPage, /parsePortfolioLookback/);
  assert.match(leaderboardPage, /PORTFOLIO_LOOKBACK_OPTIONS\.some\(\(option\) => option\.days === parsed\) \? parsed : 365/);
  assert.match(leaderboardPage, /normalizePortfolioLookback/);
  assert.match(leaderboardPage, /const targetLookbackDays = option === "portfolio" \? 365 : normalizeTradeLookback\(lookbackDays\)/);
  assert.match(leaderboardPage, /Simulation Window/);
  assert.match(leaderboardPage, /PORTFOLIO_LOOKBACK_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardPage, /parseLimit\(getParam\(sp, "limit"\)\)/);
  assert.match(leaderboardPage, /limit,/);
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

test("leaderboard portfolio keeps quality-filter notes but removes the data quality column", () => {
  assert.doesNotMatch(leaderboardTable, /Benchmark Return/);
  assert.match(leaderboardTable, /CAGR/);
  assert.match(leaderboardTable, /Alpha/);
  assert.match(leaderboardTable, /Sharpe/);
  assert.match(leaderboardTable, /Max Drawdown/);
  assert.match(leaderboardTable, /Position Win Rate/);
  assert.match(leaderboardTable, /Share of simulated portfolio positions/);
  assert.match(leaderboardTable, /public data-quality threshold/);
  assert.match(leaderboardTable, /Lower-coverage simulations are excluded from rankings/);
  assert.doesNotMatch(leaderboardTable, /Data Quality/);
  assert.doesNotMatch(leaderboardTable, /High coverage/);
  assert.doesNotMatch(leaderboardTable, /Sufficient coverage/);
  assert.doesNotMatch(leaderboardTable, /portfolioCoverageLabel/);
  assert.doesNotMatch(leaderboardTable, /return "Warning"/);
});

test("leaderboard sort headers render an intentional direction label", () => {
  assert.match(leaderboardTable, /SortHeaderLabel/);
  assert.match(leaderboardTable, /sortDirectionLabel/);
  assert.match(leaderboardTable, /"asc"/);
  assert.match(leaderboardTable, /"desc"/);
  assert.doesNotMatch(leaderboardTable, /low first/);
  assert.doesNotMatch(leaderboardTable, /high first/);
  assert.match(leaderboardTable, /sortHrefs/);
  assert.doesNotMatch(leaderboardTable, /\? " v" : ""/);
  assert.doesNotMatch(leaderboardTable, />V</);
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
  assert.match(api, /credentials: fetchInit\.credentials \?\? "include"/);
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
  assert.match(leaderboardPage, /Leaderboard is temporarily busy\. Please retry in a moment\./);
  assert.match(api, /error\.status !== 503/);
  assert.match(api, /setTimeout\(resolve, 1000\)/);
  assert.match(leaderboardResultsClient, /const controller = new AbortController\(\)/);
  assert.match(leaderboardResultsClient, /controller\.abort\(\)/);
  assert.match(leaderboardResultsClient, /actionLabel=\{errorMessage === "Leaderboard is temporarily busy\. Please retry in a moment\." \? "Retry" : undefined\}/);
  assert.match(leaderboardPage, /message\.startsWith\("Fetch failed for "\)/);
  assert.match(leaderboardResultsClient, /message\.startsWith\("Fetch failed for "\)/);
  assert.doesNotMatch(leaderboardPage, /URL:|Body:/);
});
