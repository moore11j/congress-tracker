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
const leaderboardFiltersClient = read("components/leaderboards/CongressTraderLeaderboardFiltersClient.tsx");
const backtestingWorkbench = read("components/backtesting/BacktestingWorkbench.tsx");
const congressMemberAutosuggest = read("components/backtesting/CongressMemberAutosuggest.tsx");
const signalsResultsClient = read("components/signals/SignalsResultsClient.tsx");
const signalsFiltersClient = read("components/signals/SignalsFiltersClient.tsx");
const screenerResultsClient = read("components/screener/ScreenerResultsClient.tsx");
const leaderboardResultsClient = read("components/leaderboards/CongressTraderLeaderboardClientResults.tsx");
const entitlements = read("lib/entitlements.ts");
const api = read("lib/api.ts");
const serverAuth = read("lib/serverAuth.ts");
const entitlementHintRefresh = read("components/auth/EntitlementHintRefresh.tsx");
const tickerPage = read("app/ticker/[symbol]/page.tsx");
const defaultPlanConfig = read("lib/defaultPlanConfig.ts");
const tickerInstitutionalSourceCard = read("components/ticker/TickerInstitutionalSourceCardClient.tsx");

test("signals page preserves the full filter and saved-view surface", () => {
  assert.match(signalsFiltersClient, /Mode/);
  assert.match(signalsFiltersClient, /Side/);
  assert.match(signalsFiltersClient, /Sort/);
  assert.match(signalsFiltersClient, /Confirm/);
  assert.match(signalsFiltersClient, /Direction/);
  assert.match(signalsFiltersClient, /Sources/);
  assert.match(signalsFiltersClient, /INSTITUTIONAL/);
  assert.match(signalsPage, /Signals table/);
  assert.match(signalsPage, /SignalsFiltersClient/);
  assert.match(signalsFiltersClient, /SavedViewsBar/);
  assert.doesNotMatch(signalsPage, /SignalsClientPage/);
});

test("institutional user-facing copy avoids source plumbing language", () => {
  const institutionalBlocks = Array.from(
    defaultPlanConfig.matchAll(/\{[\s\S]*?feature_key: "institutional_[\s\S]*?\n  \},/g),
    (match) => match[0],
  );
  assert.ok(institutionalBlocks.length >= 2);
  for (const block of institutionalBlocks) {
    assert.doesNotMatch(block, /\b(?:FMP|provider|vendor|cache|data provider|source provider)\b/i);
  }
});

test("ticker institutional card uses the product state label contract", () => {
  for (const label of ["Locked", "Unavailable", "Quiet", "Active"]) {
    assert.match(tickerInstitutionalSourceCard, new RegExp(`"${label}"`));
  }
  assert.doesNotMatch(tickerInstitutionalSourceCard, /BULLISH SUPPORT/);
  assert.doesNotMatch(tickerInstitutionalSourceCard, /return "INACTIVE"/);
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
  assert.match(leaderboardFiltersClient, /Congress/);
  assert.match(leaderboardFiltersClient, /Insiders/);
  assert.match(leaderboardFiltersClient, /Universe/);
  assert.match(leaderboardFiltersClient, /Performance Model/);
  assert.match(leaderboardFiltersClient, /Trade Outcomes/);
  assert.match(leaderboardFiltersClient, /Portfolio Simulation/);
  assert.match(leaderboardFiltersClient, /Simulation Window/);
  assert.match(leaderboardFiltersClient, /Rows/);
  assert.match(leaderboardFiltersClient, /Apply filters/);
  assert.match(leaderboardPage, /CongressTraderLeaderboardFiltersClient/);
  assert.doesNotMatch(leaderboardFiltersClient, /<select/);
  assert.doesNotMatch(leaderboardFiltersClient, /selectClassName/);
  assert.doesNotMatch(leaderboardPage, /CongressTraderLeaderboardClientPage/);
});

test("leaderboard row limit defaults to 10 and applies through the filter button", () => {
  assert.match(leaderboardPage, /const LIMIT_OPTIONS = \[10, 25, 50, 100\] as const/);
  assert.match(leaderboardFiltersClient, /const LIMIT_OPTIONS = \[10, 25, 50, 100\] as const/);
  assert.match(leaderboardPage, /function parseLimit\(raw: string, fallback = 10\)/);
  assert.match(leaderboardPage, /return LIMIT_OPTIONS\.includes\(parsed as \(typeof LIMIT_OPTIONS\)\[number\]\) \? parsed : fallback/);
  assert.match(leaderboardPage, /const limit = parseLimit\(getParam\(sp, "limit"\)\)/);
  assert.match(leaderboardFiltersClient, /LIMIT_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardFiltersClient, /onClick=\{\(\) => updateDraftFilters\(\{ limit: option \}\)\}/);
  assert.match(leaderboardFiltersClient, /router\.push\(buildLeaderboardHref\(pathname, searchParamsString, nextFilters\), \{ scroll: false \}\)/);
  assert.match(leaderboardPage, /url\.searchParams\.set\("limit", String\(params\.limit\)\)/);
  assert.match(leaderboardFiltersClient, /params\.set\("limit", String\(nextFilters\.limit\)\)/);
  assert.doesNotMatch(leaderboardPage, /isPortfolioMode \? 100 : 10/);
  assert.doesNotMatch(leaderboardPage, /limit: 100/);
});

test("leaderboard limit is applied across congress, portfolio, and insider data fetches", () => {
  assert.match(leaderboardFiltersClient, /sourceMode: option/);
  assert.match(leaderboardFiltersClient, /performanceModel: option/);
  assert.match(leaderboardPage, /performanceModel === "portfolio"/);
  assert.match(leaderboardFiltersClient, /const PERFORMANCE_MODEL_OPTIONS: CongressTraderLeaderboardPerformanceModel\[\] = \["outcomes", "portfolio"\]/);
  assert.match(leaderboardFiltersClient, /sourceMode === "insiders"/);
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
  assert.match(leaderboardFiltersClient, /filters\.sourceMode === "congress" \? filters\.performanceModel : "outcomes"/);
  assert.match(leaderboardPage, /sourceMode === "insiders" && rawPerformanceModel\.trim\(\)\.toLowerCase\(\) === "portfolio"/);
  assert.match(leaderboardPage, /redirect\(buildUrl/);
  assert.match(leaderboardPage, /source_mode: "insiders"/);
  assert.match(leaderboardPage, /performance_model: "outcomes"/);
});

test("leaderboard keeps trade-outcome lookback links for insider mode", () => {
  assert.match(leaderboardFiltersClient, /TRADE_LOOKBACK_OPTIONS/);
  assert.match(leaderboardPage, /const LOOKBACK_OPTIONS = \[30, 90, 180, 365, 1095\] as const/);
  assert.match(leaderboardFiltersClient, /\{ label: "30D", days: 30 \}/);
  assert.match(leaderboardFiltersClient, /\{ label: "90D", days: 90 \}/);
  assert.match(leaderboardFiltersClient, /\{ label: "180D", days: 180 \}/);
  assert.match(leaderboardFiltersClient, /\{ label: "1Y", days: 365 \}/);
  assert.match(leaderboardFiltersClient, /\{ label: "3Y", days: 1095 \}/);
  assert.match(leaderboardPage, /LOOKBACK_OPTIONS\.includes\(parsed as \(typeof LOOKBACK_OPTIONS\)\[number\]\) \? parsed : 365/);
  assert.match(leaderboardFiltersClient, /Trade Outcomes Window/);
  assert.match(leaderboardFiltersClient, /TRADE_LOOKBACK_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardFiltersClient, /lookbackDays: option\.days/);
  assert.match(leaderboardResultsClient, /source_mode: sourceMode/);
  assert.match(leaderboardFiltersClient, /performanceModel: "outcomes"/);
  assert.match(leaderboardPage, /params\.source_mode === "congress" \? params\.performance_model \?\? "portfolio" : "outcomes"/);
  assert.match(leaderboardPage, /url\.searchParams\.set\("source_mode", params\.source_mode\)/);
  assert.match(leaderboardPage, /url\.searchParams\.set\("performance_model", performanceModel\)/);
});

test("leaderboard disables portfolio simulation under insiders without a navigation link", () => {
  assert.match(leaderboardFiltersClient, /INSIDER_PORTFOLIO_DISABLED_TITLE = "Portfolio Simulation is currently available for Congress only\."/);
  assert.match(leaderboardFiltersClient, /if \(draftIsInsiderMode && option === "portfolio"\) \{/);
  assert.match(leaderboardFiltersClient, /<button\s+key=\{option\}\s+type="button"\s+disabled\s+aria-disabled="true"\s+title=\{INSIDER_PORTFOLIO_DISABLED_TITLE\}/);
  assert.match(leaderboardFiltersClient, /className=\{disabledPillClassName\(\)\}/);
  assert.match(leaderboardFiltersClient, /if \(draftIsInsiderMode && option === "portfolio"\) \{[\s\S]*?<button[\s\S]*?disabled[\s\S]*?<\/button>[\s\S]*?\}\s*const targetSort/);
  assert.match(leaderboardFiltersClient, /const targetSourceMode = option === "portfolio" \? "congress" : draftFilters\.sourceMode/);
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
  assert.match(leaderboardFiltersClient, /const targetLookbackDays = option === "portfolio" \? 365 : normalizeTradeLookback\(draftFilters\.lookbackDays\)/);
  assert.match(leaderboardFiltersClient, /Simulation Window/);
  assert.match(leaderboardFiltersClient, /PORTFOLIO_LOOKBACK_OPTIONS\.map\(\(option\) =>/);
  assert.match(leaderboardPage, /parseLimit\(getParam\(sp, "limit"\)\)/);
  assert.match(leaderboardPage, /limit,/);
  assert.match(leaderboardPage, /mode", "realistic_disclosure_lag"/);
  assert.match(leaderboardFiltersClient, /mode", "realistic_disclosure_lag"/);
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
  assert.match(api, /function authHeaders\(sessionToken\?: string \| null\)/);
  assert.match(api, /return \{ Cookie: `\$\{backendSessionCookieName\}=\$\{sessionToken\}` \}/);
  assert.doesNotMatch(api, /headers\.set\("Authorization"/);
  assert.doesNotMatch(api, /Bearer \$\{/);
  assert.match(api, /credentials: fetchInit\.credentials \?\? "include"/);
});

test("backtesting workbench preserves full workflow controls", () => {
  assert.match(backtestingWorkbench, /Backtest Signals &amp; Portfolios/);
  assert.match(backtestingWorkbench, /Watchlist/);
  assert.match(backtestingWorkbench, /Screens/);
  assert.match(backtestingWorkbench, /Signals/);
  assert.match(backtestingWorkbench, /Congress Strategy/);
  assert.match(backtestingWorkbench, /Include exempt acquisitions/);
  assert.match(backtestingWorkbench, /Buy and hold/);
  assert.match(congressMemberAutosuggest, /Search members by name/);
  assert.match(congressMemberAutosuggest, /Search by insider name or CIK/);
  assert.match(backtestingWorkbench, /For event-driven modes, this overrides the hold period/);
  assert.match(backtestingWorkbench, /include_exempt_acquisitions: view === "insider" \? includeExemptAcquisitions : false/);
  assert.match(backtestingWorkbench, /buy_and_hold: buyAndHold/);
  assert.match(backtestingWorkbench, /Portfolio Settings/);
  assert.match(backtestingWorkbench, /Run Backtest/);
});

test("admin entitlement and cross-site tier hint plumbing stay non-secret", () => {
  assert.match(entitlements, /entitlements\.tier === "admin"/);
  assert.match(entitlements, /entitlements\.user\?\.is_admin/);
  assert.match(entitlements, /effective_tier\?: EntitlementTier/);
  assert.match(entitlements, /entitlements\.effective_tier === "admin"/);
  assert.match(entitlements, /entitlements\.is_admin/);
  assert.match(api, /entitlementHintCookieName = "ct_entitlement_hint"/);
  assert.match(api, /window\.localStorage\.setItem\(entitlementTierStorageKey, tier\)/);
  assert.match(serverAuth, /entitlementHintCookieName/);
  assert.doesNotMatch(api, /document\.cookie = `\$\{backendSessionCookieName\}=\$\{token\}/);
  assert.doesNotMatch(api, /ct_session=\$\{token\}/);
});

test("admin cross-site auth hint refresh repairs stale free SSR paint", () => {
  assert.match(entitlementHintRefresh, /getMe\(\{ force: true, source: "EntitlementHintRefresh" \}\)/);
  assert.match(entitlementHintRefresh, /response\.user\.is_admin && renderedTier !== "admin"/);
  assert.match(entitlementHintRefresh, /window\.location\.reload\(\)/);
  assert.match(screenerPage, /<EntitlementHintRefresh enabled=\{!authToken && authState\.entitlementHint != null\} renderedTier=\{entitlements\.tier\} \/>/);
  assert.match(tickerPage, /<EntitlementHintRefresh enabled=\{!authToken && authState\.hasAuthHint\} renderedTier=\{entitlements\?\.tier \?\? null\} \/>/);
  assert.match(tickerPage, /const hasAuthForEntitlementDisplay = Boolean\(authToken \|\| authState\.hasAuthHint\)/);
  assert.match(tickerPage, /allowAuthHintEntitlementOverride=\{authState\.hasAuthHint\}/);
  assert.match(tickerPage, /displaySourceEntitlementsForTickerContext\(/);
  assert.match(tickerPage, /activityMeta\?\.locked && fallbackMeta\?\.locked === false/);
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
