import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("ticker chart checks hydration status before requesting chart bundle", () => {
  const chart = read("components/ticker/TickerChartLoader.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export async function getTickerHydrationStatus/);
  assert.match(api, /export async function requestTickerHydration/);
  assert.match(chart, /getTickerHydrationStatus\(symbol,/);
  assert.match(chart, /shouldRequestHydration\(status\)/);
  assert.match(chart, /requestedHydrationSymbols/);
  assert.match(chart, /requestTickerHydration\(symbol,/);
  assert.match(chart, /reason: "ticker_page_view"/);
  assert.match(chart, /getTickerChartBundle\(symbol, days,/);
  assert.match(chart, /CHART_HYDRATION_DELAY_MS/);
});

test("ticker chart renders stale freshness as an updating state", () => {
  const chart = read("components/ticker/PremiumTickerChart.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export type TickerChartFreshness/);
  assert.match(api, /freshness\?: TickerChartFreshness/);
  assert.match(api, /status === "stale"/);
  assert.match(chart, /freshnessBlocksChart/);
  assert.match(chart, /Price chart updating/);
  assert.match(chart, /Latest market data is temporarily unavailable\./);
  assert.match(chart, /Updated through/);
  assert.doesNotMatch(chart, /provider|cache|FMP|Polygon|Massive/i);
});

test("ticker context does not eagerly request heavy tab data on overview mount", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /if \(activeTab !== "news"\)/);
  assert.match(card, /if \(activeTab !== "financials"\)/);
  assert.match(card, /if \(activeTab !== "events"\)/);
  assert.match(card, /getTickerNews\(symbol,/);
  assert.match(card, /getTickerFinancials\(symbol,/);
  assert.match(card, /getTickerSecFilings\(symbol,/);
  assert.doesNotMatch(card, /requestTickerHydration\(symbol,/);
});

test("ticker signal activity uses ticker-specific summary instead of broad signals endpoint", () => {
  const client = read("components/ticker/TickerSignalActivityClient.tsx");
  const sourceCard = read("components/ticker/TickerSignalsSourceCardClient.tsx");
  const tickerPage = read("app/ticker/[symbol]/page.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export async function getTickerSignalsSummary/);
  assert.match(api, /\/api\/tickers\/\$\{symbol\}\/signals-summary/);
  assert.match(api, /lookback_days: params\?\.lookback_days/);
  assert.match(api, /clientCachedJson<TickerSignalsSummaryResponse>/);
  assert.match(api, /`ticker-signals-summary:\$\{url\}`/);
  assert.match(client, /getTickerSignalsSummary\(symbol,/);
  assert.match(client, /lookback_days: lookbackDays/);
  assert.match(sourceCard, /getTickerSignalsSummary\(symbol,/);
  assert.match(sourceCard, /sourceFromSummary\(response, visibleItems, lookbackDays, fallbackSource\)/);
  assert.match(tickerPage, /getTickerSignalsSummary\(normalizedSymbol,/);
  assert.match(tickerPage, /lookback_days: SIGNAL_WINDOW_DAYS/);
  assert.match(tickerPage, /lookbackDays=\{SIGNAL_WINDOW_DAYS\}/);
  assert.match(tickerPage, /activityConfirmationScoreBundle \?\? confirmationScoreBundle/);
  assert.match(tickerPage, /activitySignalFreshness \?\? signalFreshness/);
  assert.match(tickerPage, /const insiderCardSource = confirmationBundle\.sources\.insiders/);
  assert.match(tickerPage, /const congressCardSource = confirmationBundle\.sources\.congress/);
  assert.match(tickerPage, /summaryCount\(summaryInsiders, "buy_count"\)/);
  assert.doesNotMatch(tickerPage, /sourceFromActivityCounts/);
  assert.doesNotMatch(client, /getSignalsAll|\/api\/signals\/all|limit:\s*100/);
  assert.doesNotMatch(tickerPage, /getSignalsAll|\/api\/signals\/all|signalsPromise/);
});

test("ticker upper Signals source card repairs stale inactive SSR from browser-authenticated summary rows", () => {
  const sourceCard = read("components/ticker/TickerSignalsSourceCardClient.tsx");
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(tickerPage, /<TickerSignalsSourceCardClient/);
  assert.match(tickerPage, /lookbackStartKey=\{lookbackStartDateKey\(confirmationLookbackDays\)\}/);
  assert.match(sourceCard, /const hasVisibleSignals = visibleItems\.length > 0/);
  assert.match(sourceCard, /if \(hasVisibleSignals\) \{/);
  assert.match(sourceCard, /body: "Signal conviction active"/);
  assert.match(sourceCard, /No qualifying signal entries found in the \$\{windowNoun\(lookbackDays\)\}/);
  assert.match(sourceCard, /source: "TickerSignalsSourceCard"/);
  assert.match(sourceCard, /error instanceof ApiError && \[401, 402, 403\]\.includes\(error\.status\)/);
  assert.match(sourceCard, /return "LOCKED"/);
  assert.match(sourceCard, /return "UNAVAILABLE"/);
});

test("ticker institutional source card renders unavailable for entitled missing provider state", () => {
  const institutionalCard = read("components/ticker/TickerInstitutionalSourceCardClient.tsx");
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(tickerPage, /<TickerInstitutionalSourceCardClient/);
  assert.match(tickerPage, /initialSource=\{confirmationBundle\.sources\.institutional_activity\}/);
  assert.match(tickerPage, /function sourceUnavailable/);
  assert.match(tickerPage, /if \(sourceUnavailable\(source\)\) return "UNAVAILABLE"/);
  assert.match(institutionalCard, /getTickerSignalsSummary\(symbol,/);
  assert.match(institutionalCard, /source: "TickerInstitutionalSourceCard"/);
  assert.match(institutionalCard, /canViewInstitutional\?: boolean/);
  assert.match(institutionalCard, /normalizeSourceForAccess/);
  assert.match(institutionalCard, /canViewInstitutional && sourceLocked\(source\)/);
  assert.match(institutionalCard, /nextEntitlement\?\.locked && !canViewInstitutional/);
  assert.match(institutionalCard, /return "UNAVAILABLE"/);
  assert.match(institutionalCard, /Institutional activity unavailable\./);
  assert.match(institutionalCard, /Institutional activity source is not configured\./);
  assert.match(tickerPage, /const institutionalCardLocked = !canViewProTickerContext && sourceIsLocked\(sourceEntitlements, "institutional_activity"\)/);
  assert.match(tickerPage, /canViewInstitutional=\{canViewProTickerContext\}/);
  assert.match(tickerPage, /requiredPlan="pro"/);
});

test("ticker server context relies on cookie-backed auth state without client token bridge", () => {
  const accountNav = read("components/auth/AccountNav.tsx");
  const api = read("lib/api.ts");
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(api, /function authHeaders\(sessionToken\?: string \| null\)/);
  assert.match(api, /return \{ Cookie: `\$\{backendSessionCookieName\}=\$\{sessionToken\}` \}/);
  assert.match(accountNav, /clearLegacyAuthStorage\(\)/);
  assert.doesNotMatch(accountNav, /syncServerAuthSession|localStorage\.getItem|router\.refresh/);
  assert.doesNotMatch(api, /syncServerAuthSession|response\.token|headers\.set\("Authorization"/);
  assert.match(tickerPage, /const authToken = authState\.token/);
  assert.match(tickerPage, /const signalSummaryRequest =\s*getTickerSignalsSummary\(normalizedSymbol/);
  assert.doesNotMatch(tickerPage, /canViewSignalActivity && authToken[\s\S]*?getTickerSignalsSummary\(normalizedSymbol/);
});

test("ticker context gates source cards instead of the whole context request", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export type TickerSourceEntitlement/);
  assert.match(api, /lock_state\?: "available" \| "requires_login" \| "premium_locked" \| "pro_locked" \| null/);
  assert.match(api, /source_entitlements\?: TickerSourceEntitlements \| null/);
  assert.match(tickerPage, /function tickerContextSourceEntitlements/);
  assert.match(tickerPage, /function displaySourceEntitlementsForTickerContext/);
  assert.match(tickerPage, /function displayConfirmationBundleForEntitlements/);
  assert.match(tickerPage, /const signalSummaryRequest =\s*getTickerSignalsSummary\(normalizedSymbol/);
  assert.doesNotMatch(tickerPage, /const signalSummaryRequest =\s*authToken\s*\?/);
  assert.doesNotMatch(tickerPage, /canViewSignalActivity && authToken[\s\S]*?getTickerSignalsSummary\(normalizedSymbol/);
  assert.match(tickerPage, /sourceEntitlements: signalsRes\.source_entitlements \?\? null/);
  assert.match(tickerPage, /const signalsCardLocked = sourceIsLocked\(sourceEntitlements, "signals"\)/);
  assert.match(tickerPage, /const institutionalCardLocked = !canViewProTickerContext && sourceIsLocked\(sourceEntitlements, "institutional_activity"\)/);
  assert.match(tickerPage, /const optionsFlowCardLocked = !canViewProTickerContext && sourceIsLocked\(sourceEntitlements, "options_flow"\)/);
  assert.match(tickerPage, /requiredPlan="premium"/);
  assert.match(tickerPage, /requiredPlan="pro"/);
  assert.match(tickerPage, /Premium feature/);
  assert.match(tickerPage, /Pro feature/);
});

test("ticker upper source cards let authenticated tier hints override stale locked summary metadata", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(tickerPage, /function displaySourceEntitlementsForTickerContext/);
  assert.match(tickerPage, /activityMeta\?\.locked && fallbackMeta\?\.locked === false/);
  assert.match(tickerPage, /merged\[source\] = fallbackMeta/);
  assert.match(tickerPage, /const sourceEntitlements = displaySourceEntitlementsForTickerContext\(/);
  assert.match(tickerPage, /allowAuthHintEntitlementOverride=\{authState\.hasAuthHint\}/);
  assert.match(tickerPage, /const signalsCardLocked = sourceIsLocked\(sourceEntitlements, "signals"\)/);
  assert.match(tickerPage, /const institutionalCardLocked = !canViewProTickerContext && sourceIsLocked\(sourceEntitlements, "institutional_activity"\)/);
  assert.match(tickerPage, /const optionsFlowCardLocked = !canViewProTickerContext && sourceIsLocked\(sourceEntitlements, "options_flow"\)/);
  assert.doesNotMatch(tickerPage, /preferFallbackSourceEntitlements/);
});

test("logged out ticker context keeps public sources visible and paid sources locked", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(tickerPage, /const hasAuthForEntitlementDisplay = Boolean\(authToken \|\| authState\.hasAuthHint\)/);
  assert.match(tickerPage, /tickerContextSourceEntitlements\(entitlements, hasAuthForEntitlementDisplay\)/);
  assert.match(tickerPage, /insiders: meta\("insiders", null, false\)/);
  assert.match(tickerPage, /congress: meta\("congress", null, false\)/);
  assert.match(tickerPage, /government_contracts: meta\("government_contracts", null, false\)/);
  assert.match(tickerPage, /signals: meta\("signals", "premium", true, "premium_locked"\)/);
  assert.match(tickerPage, /institutional_activity: meta\("institutional_activity", "pro", true, "pro_locked"\)/);
  assert.match(tickerPage, /options_flow: meta\("options_flow", "pro", true, "pro_locked"\)/);
  assert.match(tickerPage, /Locked source context/);
  assert.match(tickerPage, /Additional Premium\/Pro context is available for this ticker\./);
  assert.doesNotMatch(tickerPage, /function RequiresLoginSourceCard/);
  assert.doesNotMatch(tickerPage, /Sign in to view 30D confirmation/);
  assert.match(tickerPage, /lock_state: locked \? lockState/);
});

test("ticker government contracts activity loads in all mode and paginates", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");
  const api = read("lib/api.ts");

  assert.match(tickerPage, /source === "all" \|\| source === "government_contract"/);
  assert.match(tickerPage, /limit: GOVERNMENT_CONTRACTS_PAGE_SIZE/);
  assert.match(tickerPage, /page: contractsPage/);
  assert.match(tickerPage, /governmentContractsTotal\} contract\{governmentContractsTotal === 1 \? "" : "s"\}/);
  assert.match(tickerPage, /No government contracts in selected window\./);
  assert.match(tickerPage, /Government contract activity unavailable\./);
  assert.match(tickerPage, /Loading government contract activity\./);
  assert.match(tickerPage, /sectionId="government-contracts-activity"/);
  assert.match(tickerPage, /pageParam="contracts_page"/);
  assert.match(tickerPage, /page=\{governmentContractsPage\}/);
  assert.match(tickerPage, /hasNext=\{governmentContractsHasNext\}/);
  assert.match(api, /page: params\?\.page/);
  assert.match(api, /total\?: number/);
  assert.doesNotMatch(tickerPage, /governmentContractsDeferred/);
});

test("ticker tabs settle warming responses into public no-data copy", () => {
  const card = read("components/ticker/TickerContextCard.tsx");
  const api = read("lib/api.ts");

  assert.match(card, /rawStatus === "warming" \|\| rawStatus === "loading" && items\.length === 0 \? "no_data" : rawStatus === "empty" \? "no_data" : rawStatus/);
  assert.match(card, /NEWS_EMPTY_MESSAGE = "No recent news found\."/);
  assert.match(card, /FILINGS_EMPTY_MESSAGE = "No recent filings found\."/);
  assert.match(api, /function normalizeTickerItemsResponse/);
  assert.match(api, /arrayKeys: \["items", "news", "articles", "results", "data"\]/);
  assert.match(api, /arrayKeys: \["items", "press_releases", "pressReleases", "releases", "results", "data"\]/);
  assert.match(api, /arrayKeys: \["items", "filings", "sec_filings", "secFilings", "results", "data"\]/);
  assert.match(card, /status === "loading" \? FINANCIALS_LOADING_MESSAGE : response\.message/);
  assert.doesNotMatch(card, /FMP|provider|cache|402|heavy-route|budget/);
});

test("ticker page catches temporary profile failures and renders a shell fallback", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(tickerPage, /function fallbackTickerProfile/);
  assert.match(tickerPage, /function isRecoverableTickerProfileError/);
  assert.match(tickerPage, /error\.status === 503 \|\| error\.status >= 500/);
  assert.match(tickerPage, /Ticker data is loading\. Try refreshing shortly\./);
  assert.match(tickerPage, /profile: fallbackTickerProfile\(normalizedSymbol\)/);
  assert.match(tickerPage, /\[ticker-events\] unavailable/);
  const fallbackSource = tickerPage.slice(
    tickerPage.indexOf("function fallbackTickerProfile"),
    tickerPage.indexOf("function one"),
  );
  assert.doesNotMatch(fallbackSource, /heavy_route_saturated|Heavy endpoint|FMP|provider|cache/);
});

test("ticker error boundary renders branded recovery actions", () => {
  const errorBoundary = read("app/ticker/[symbol]/error.tsx");

  assert.match(errorBoundary, /Walnut ticker intelligence/);
  assert.match(errorBoundary, /This ticker page could not fully load\./);
  assert.match(errorBoundary, /Reload/);
  assert.match(errorBoundary, /Back to feed/);
  assert.match(errorBoundary, /href="\/\?mode=all"/);
});

test("ticker events tab loads filings and activity independently", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /const PRESS_LOADING_MESSAGE = "Loading press releases\."/);
  assert.match(card, /const ACTIVITY_EMPTY_MESSAGE = "No recent disclosure activity found\."/);
  assert.match(card, /const EVENTS_EMPTY_MESSAGE = "No recent filings or disclosure activity found\."/);
  assert.match(card, /getTickerSecFilings\(symbol,/);
  assert.doesNotMatch(card, /from: dateWindow\.from/);
  assert.doesNotMatch(card, /to: dateWindow\.to/);
  assert.match(card, /getEvents\(\{[\s\S]*?symbol,[\s\S]*?recent_days: 365,[\s\S]*?limit: 50/);
  assert.match(card, /showSecSection/);
  assert.match(card, /allEventsSourcesEmpty/);
  assert.match(card, /<EventsSection title="SEC Filings" meta="Latest available">/);
  assert.match(card, /<EventsSection title="Disclosure Activity" meta="365D">/);
  assert.doesNotMatch(card, /title="Filings \/ Disclosures"/);
});

test("ticker disclosure activity renders links from event url payload fields", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /function disclosureEventUrl\(event: EventItem\): string \| null/);
  assert.match(card, /"source_url"/);
  assert.match(card, /"filing_url"/);
  assert.match(card, /"report_url"/);
  assert.match(card, /"document_url"/);
  assert.match(card, /"sec_url"/);
  assert.match(card, /const sourceUrl = disclosureEventUrl\(event\)/);
  assert.match(card, /href=\{sourceUrl\}/);
  assert.match(card, /<span className="text-slate-500">-<\/span>/);
});

test("landing feed government contract profile lookup falls back without server error", () => {
  const page = read("app/page.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /"tickers" in data/);
  assert.match(page, /companyNames = \{\};/);
  assert.doesNotMatch(page, /console\.error\("\[feed\] ticker profiles unavailable for government contracts/);
});
