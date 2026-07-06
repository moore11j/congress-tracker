import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

test("feed filters use sort controls and remove confusing advanced filters", () => {
  const page = read("app/page.tsx");
  const filters = read("components/feed/FeedFiltersServer.tsx");

  for (const key of ["sort_by", "sort_dir"]) {
    assert.match(page, new RegExp(`"${key}"`));
    assert.match(filters, new RegExp(`"${key}"`));
    assert.match(filters, new RegExp(`name="${key}"`));
  }
  for (const key of ["filed_after_max", "pnl_min", "pnl_max", "signal_min", "asset_class", "min_amount", "max_amount"]) {
    assert.doesNotMatch(page, new RegExp(`"${key}"`));
    assert.doesNotMatch(filters, new RegExp(`name="${key}"`));
  }
  assert.match(filters, /Sort by/);
  assert.match(filters, /Direction/);
  assert.match(filters, /Member, insider, department, institution/);
  assert.match(filters, /selectValue="label"/);
  assert.doesNotMatch(filters, /value="etf_fund"/);
  assert.match(filters, /FeedRoleAutosuggestEnhancer/);
  assert.doesNotMatch(filters, /13F filing activity uses filing dates/);
});

test("congress and insider feed filters keep sort controls on the primary row", () => {
  const filters = read("components/feed/FeedFiltersServer.tsx");

  assert.match(
    filters,
    /mode === "congress" \? \(\s*<FilterRow>[\s\S]*?<SymbolField[\s\S]*?<NameField[\s\S]*?<TradeTypeField[\s\S]*?<PartyField[\s\S]*?<ChamberField[\s\S]*?<SortField[\s\S]*?<DirectionField[\s\S]*?<RecentDaysField[\s\S]*?<\/FilterRow>\s*\) : null/,
  );
  assert.match(
    filters,
    /mode === "insider" \? \(\s*<FilterRow>[\s\S]*?<SymbolField[\s\S]*?<NameField[\s\S]*?<TradeTypeField[\s\S]*?<RoleField[\s\S]*?<SortField[\s\S]*?<DirectionField[\s\S]*?<RecentDaysField[\s\S]*?<\/FilterRow>\s*\) : null/,
  );
  assert.doesNotMatch(filters, /mode === "congress" \? \(\s*<>\s*<FilterRow>/);
  assert.doesNotMatch(filters, /mode === "insider" \? \(\s*<>\s*<FilterRow>/);
});

test("feed autosuggest popovers render above result cards", () => {
  const filters = read("components/feed/FeedFiltersServer.tsx");
  const memberAutosuggest = read("components/feed/FeedMemberAutosuggestEnhancer.tsx");
  const symbolAutosuggest = read("components/feed/FeedSymbolAutosuggestEnhancer.tsx");
  const roleAutosuggest = read("components/feed/FeedRoleAutosuggestEnhancer.tsx");

  assert.match(filters, /relative z-30 overflow-visible/);
  for (const source of [memberAutosuggest, symbolAutosuggest, roleAutosuggest]) {
    assert.match(source, /relative z-\[120\]/);
    assert.match(source, /absolute z-\[1200\]/);
  }
});

test("visible feed results load client-side with bounded gain/loss enrichment", () => {
  const page = read("app/page.tsx");
  const client = read("components/feed/FeedResultsClient.tsx");
  const api = read("lib/api.ts");

  assert.match(page, /<FeedResultsClient/);
  assert.doesNotMatch(page, /getEvents/);
  assert.doesNotMatch(page, /\/api\/events/);
  assert.doesNotMatch(page, /requestSource:\s*"ssr"/);
  assert.match(client, /getEvents\(/);
  assert.match(client, /enrich_prices:\s*1/);
  assert.match(client, /include_net_flows:\s*0/);
  assert.match(client, /requestSource:\s*"client"/);
  assert.match(client, /routeFamily:\s*"feed"/);
  assert.doesNotMatch(page, /enrich_prices:\s*0/);
  assert.match(api, /const bypassPublicFetchCache = !authToken && requestSource === "ssr" && routeFamily === "feed";/);
  assert.match(api, /cache: authToken \|\| bypassPublicFetchCache \? "no-store" : "force-cache"/);
  assert.match(api, /next: authToken \|\| bypassPublicFetchCache \? \{ revalidate: 0 \} : \{ revalidate: 30 \}/);
});

test("feed cards hide net flow labels and gate premium metrics", () => {
  const card = read("components/feed/FeedCard.tsx");
  const list = read("components/feed/FeedList.tsx");
  const page = read("app/page.tsx");
  const mapper = read("lib/feedEventMapper.ts");
  const client = read("components/feed/FeedResultsClient.tsx");

  assert.doesNotMatch(card, /Member Net 30D:/);
  assert.doesNotMatch(card, /Insider Net 30D:/);
  assert.doesNotMatch(card, /Ticker Net 30D:/);
  assert.match(card, /canViewPremiumMetrics\?: boolean/);
  assert.match(card, /Gain\/loss and signal data is for Premium or Pro only\./);
  assert.match(card, /lockedPnlPlaceholder/);
  assert.match(card, /smartSignalPillClasses/);
  assert.match(list, /canViewPremiumMetrics=\{canViewPremiumMetrics\}/);
  assert.match(page, /hasEntitlement\(entitlements, "premium_feed_metrics"\)/);
  assert.match(mapper, /function redactPremiumFeedMetrics/);
  assert.match(mapper, /pnlValue < 0 \? -0\.1/);
  assert.match(mapper, /smart_score: signalValue === null \? null : undefined/);
  assert.match(client, /include_net_flows: 0/);
});

test("feed list defaults to compact table while preserving card view", () => {
  const list = read("components/feed/FeedList.tsx");
  const table = read("components/feed/FeedTable.tsx");
  const client = read("components/feed/FeedResultsClient.tsx");

  assert.match(list, /useState<FeedViewMode>\("table"\)/);
  assert.match(list, /const feedViewStorageKey = "walnut:feed:view"/);
  assert.match(list, /<FeedTable/);
  assert.match(list, /viewMode === "table"/);
  assert.match(list, /<FeedCard key=\{item\.id\}/);
  assert.match(list, /Table/);
  assert.match(list, /Cards/);
  assert.match(table, /Date/);
  assert.match(table, /Source/);
  assert.match(table, /Ticker/);
  assert.match(table, /Person \/ Entity/);
  assert.match(table, /Action/);
  assert.match(table, /Amount/);
  assert.match(table, /G\/L/);
  assert.match(table, /Signal/);
  assert.match(table, /Disclosure/);
  assert.match(table, /lg:hidden/);
  assert.match(table, /hidden lg:block/);
  assert.match(table, /table-fixed/);
  assert.match(table, /<colgroup>/);
  assert.doesNotMatch(table, /overflow-x-auto/);
  assert.doesNotMatch(table, /min-w-\[/);
  assert.match(table, /<FeedCard item=\{item\}/);
  assert.match(table, /expandedItemId === item\.id/);
  assert.match(table, /function institutionalActionLabel/);
  assert.match(table, /Reported Reduction/);
  assert.match(table, /Reported Increase/);
  assert.match(table, /Reported Exit/);
  assert.match(table, /function contractDescription/);
  assert.match(table, /Gov Contracts/);
  assert.match(table, /getInsiderDisplayName/);
  assert.match(client, /getEvents\(/);
  assert.equal((client.match(/getEvents\(/g) ?? []).length, 1);
  assert.doesNotMatch(client, /payload:\s*"full"|payload=full|getEventDetails/);
});

test("feed table keeps truthful gain loss states without internal wording", () => {
  const table = read("components/feed/FeedTable.tsx");

  assert.match(table, /function gainLossLabel/);
  assert.match(table, /status === "pending"/);
  assert.match(table, /status === "unavailable"/);
  assert.match(table, /status === "missing_trade_price"/);
  assert.match(table, /status === "missing_current_price"/);
  assert.match(table, /status === "missing_quantity"/);
  assert.match(table, /status === "ok" \? "Ready"/);
  assert.match(table, /canViewPremiumMetrics/);
  assert.match(table, /Locked/);
  assert.match(table, /gain_loss_amount/);
  assert.match(table, /gain_loss_percent/);
  assert.match(table, /Change N\/A/);
  assert.match(table, /recipient_name/);
  assert.doesNotMatch(table, /provider|vendor|cache|FMP/);
});

test("watchlist activity unlocks premium metrics for entitled users", () => {
  const entitlements = read("lib/entitlements.ts");
  const watchlistPage = read("app/watchlists/[id]/page.tsx");
  const watchlistContent = read("components/watchlists/WatchlistDetailContent.tsx");
  const watchlistRecentActivity = read("components/watchlists/WatchlistRecentActivity.tsx");
  const watchlistClient = read("components/watchlists/WatchlistDetailClient.tsx");

  assert.match(entitlements, /\| "premium_feed_metrics"/);
  assert.match(entitlements, /premium_feed_metrics: 0/);
  assert.match(entitlements, /premium_feed_metrics: 1/);
  assert.match(watchlistPage, /getEntitlements\(authToken, \{ source: "WatchlistDetailPage" \}/);
  assert.match(watchlistPage, /hasEntitlement\(entitlements, "premium_feed_metrics"\)/);
  assert.match(watchlistContent, /canViewPremiumMetrics=\{canViewPremiumMetrics\}/);
  assert.match(watchlistRecentActivity, /canViewPremiumMetrics=\{canViewPremiumMetrics\}/);
  assert.match(watchlistClient, /getEntitlements\(undefined, \{ source: "WatchlistDetailClient" \}/);
  assert.match(watchlistClient, /hasEntitlement\(entitlements, "premium_feed_metrics"\)/);
});

test("feed gain/loss tooltip uses simplified percentage copy", () => {
  const card = read("components/feed/FeedCard.tsx");
  const copy = read("lib/gainLossCopy.ts");
  const expectedBody =
    "This percentage represents the unrealized gain or loss since the trade date. It measures the difference between the trade price and the current market price. For example, a gain occurs if the current market price rises after a buy, or falls after a sale. Conversely, a loss occurs if the price falls after a buy, or rises after a sale.";

  assert.match(copy, /export const feedGainLossLabel = "Gain \/ loss \(%\)";/);
  assert.ok(copy.includes(expectedBody));
  assert.doesNotMatch(copy, /hypothetical copied trade|copied it/);

  assert.match(card, /title=\{feedGainLossLabel\}/);
  assert.match(card, /label: "Trade price"/);
  assert.doesNotMatch(card, /label: "Entry"/);
});

test("institutional feed cards use compact values and text activity labels", () => {
  const card = read("components/feed/FeedCard.tsx");
  const mapper = read("lib/feedEventMapper.ts");

  assert.match(card, /function formatMoneyCompactRange/);
  assert.match(card, /function formatSignedMoneyCompact/);
  assert.match(card, /const isInstitutionalExit = isInstitutional && String\(kind\)\.includes\("exit"\)/);
  assert.match(card, /isInstitutionalExit\s*\?\s*formatMoneyCompact\(0\)/);
  assert.match(card, /const institutionalActionToneClass/);
  assert.match(card, /text-rose-300/);
  assert.match(card, /text-emerald-300/);
  assert.match(card, /if \(valueDelta < 0\) return "Reported Reduction"/);
  assert.match(card, /return "Reported Activity"/);
  assert.doesNotMatch(card, /return "13F Filing"/);
  assert.match(card, /const showOutcomeMetrics = true/);
  assert.match(card, /isInstitutional \? "N\/A"/);
  assert.match(card, /Gain\/loss is not calculated for 13F rows/);
  assert.match(card, /institutionalSecuritySecondaryLabel/);
  assert.match(card, /Filed after:/);
  assert.match(card, /institutionalAmountLabel/);
  assert.doesNotMatch(card, /Prior value exited/);
  assert.match(card, /Change \{formatSignedMoneyCompact\(institutionalValueDelta\)\}/);
  assert.match(card, /isInstitutional \? "flex flex-col items-center gap-2 text-center md:grid md:\[grid-template-columns:minmax\(125px,170px\)_90px_60px\]/);
  assert.match(mapper, /companyNameForSymbol/);
  assert.match(mapper, /function institutionalDisplayName/);
  assert.match(mapper, /institutionalDisplayName\(event\.member_name\)/);
  assert.match(mapper, /normalized === "institutional activity"/);
  assert.match(mapper, /function institutionalTransactionLabel/);
  assert.match(mapper, /institutionalTransactionLabel\(event\.event_type, payload, event\.trade_type\)/);
  assert.doesNotMatch(mapper, /Multiple institutions/);
  assert.doesNotMatch(mapper, /`CIK \$\{event\.member_bioguide_id\}`/);
  assert.match(mapper, /feedProfileSymbols/);
});

test("global search UI advertises insider search and renders insider grouping", () => {
  const search = read("components/GlobalSearch.tsx");

  assert.match(search, /insider: "Insiders"/);
  assert.match(search, /insider: "Insider"/);
  assert.match(search, /members, insiders/);
});

test("global and landing search submit unknown text to results instead of raw ticker routes", () => {
  const globalSearch = read("components/GlobalSearch.tsx");
  const landingSearch = read("components/landing/LandingSearch.tsx");
  const navigation = read("lib/searchNavigation.ts");

  assert.match(globalSearch, /searchResultsHref\(trimmedQuery\)/);
  assert.match(globalSearch, /isHighConfidenceSearchResult/);
  assert.doesNotMatch(globalSearch, /router\.push\(`\/ticker\//);
  assert.doesNotMatch(globalSearch, /isTickerLikeQuery/);

  assert.match(landingSearch, /searchResultsHref\(trimmedQuery\)/);
  assert.match(landingSearch, /isHighConfidenceSearchResult/);
  assert.doesNotMatch(landingSearch, /\/ticker\/\$\{/);
  assert.doesNotMatch(landingSearch, /isTickerLikeQuery/);

  assert.match(navigation, /export function searchResultsHref/);
  assert.match(navigation, /export function isHighConfidenceSearchResult/);
});

test("search results page and invalid ticker fallback provide safe no-error search path", () => {
  const searchPage = read("app/search/page.tsx");
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(searchPage, /searchSuggest\(query, 20/);
  assert.match(searchPage, /Did you mean/);
  assert.match(searchPage, /No exact matches/);
  assert.doesNotMatch(searchPage, /getTickerProfile/);

  assert.match(tickerPage, /MissingTickerSearchFallback/);
  assert.match(tickerPage, /error instanceof ApiError && error\.status === 404/);
  assert.match(tickerPage, /\/search\?q=/);
});

test("insider profile preserves issuer-scoped search context", () => {
  const page = read("app/insider/[slug]/page.tsx");
  const api = read("lib/api.ts");

  assert.match(page, /one\(sp, "issuer"\)/);
  assert.match(page, /issuer && companyText/);
  assert.match(api, /issuer\?: string/);
  assert.match(api, /issuer: params\?\.issuer/);
});
