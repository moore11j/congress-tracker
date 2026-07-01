import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

test("feed saved views and URL params preserve advanced filters", () => {
  const page = read("app/page.tsx");
  const filters = read("components/feed/FeedFiltersServer.tsx");

  for (const key of ["filed_after_max", "pnl_min", "pnl_max", "signal_min"]) {
    assert.match(page, new RegExp(`"${key}"`));
    assert.match(filters, new RegExp(`"${key}"`));
    assert.match(filters, new RegExp(`name="${key}"`));
  }
  assert.match(filters, /value="etf_fund"/);
  assert.match(filters, /FeedRoleAutosuggestEnhancer/);
});

test("feed cards distinguish actor and ticker net flow labels", () => {
  const card = read("components/feed/FeedCard.tsx");

  assert.match(card, /Member Net 30D:/);
  assert.match(card, /Insider Net 30D:/);
  assert.match(card, /Ticker Net 30D:/);
  assert.match(card, /\(isInsider \|\| isCongress\) && symbol && symbolNet30d !== null/);
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
  const page = read("app/page.tsx");

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
  assert.match(page, /companyNameForSymbol/);
  assert.match(page, /function institutionalDisplayName/);
  assert.match(page, /institutionalDisplayName\(event\.member_name\)/);
  assert.match(page, /normalized === "institutional activity"/);
  assert.match(page, /function institutionalTransactionLabel/);
  assert.match(page, /institutionalTransactionLabel\(event\.event_type, payload, event\.trade_type\)/);
  assert.doesNotMatch(page, /Multiple institutions/);
  assert.doesNotMatch(page, /`CIK \$\{event\.member_bioguide_id\}`/);
  assert.match(page, /institutionalSymbols/);
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
