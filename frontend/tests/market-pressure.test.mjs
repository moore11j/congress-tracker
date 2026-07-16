import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const nav = read("components/AppTopNav.tsx");
const page = read("app/market-pressure/page.tsx");
const client = read("components/market-pressure/MarketPressureMapClient.tsx");
const contract = read("lib/marketPressure.ts");
const api = read("lib/api.ts");
const middleware = read("middleware.ts");

test("Maps appears before Pricing in the shared top navigation", () => {
  const marketPressureIndex = nav.indexOf('{ href: "/market-pressure", label: "Maps" }');
  const pricingIndex = nav.indexOf('{ href: "/pricing", label: "Pricing" }');
  assert.ok(marketPressureIndex > -1, "Maps nav item is missing");
  assert.ok(pricingIndex > -1, "Pricing nav item is missing");
  assert.ok(marketPressureIndex < pricingIndex, "Maps should appear before Pricing");
  assert.match(nav, /usePathname/);
});

test("Market Pressure uses the responsive shared nav rather than a duplicate mobile implementation", () => {
  assert.match(nav, /overflow-x-auto/);
  assert.match(nav, /topNavLinks\.map/);
  assert.equal((nav.match(/label: "Maps"/g) ?? []).length, 1);
});

test("/market-pressure route renders a discoverable page with metadata", () => {
  assert.match(page, /export const dynamic = "force-dynamic"/);
  assert.match(page, /title: "Market Pressure Map \| Walnut Markets"/);
  assert.match(page, /canonical: "\/market-pressure"/);
  assert.match(page, /optionalPageAuthState/);
  assert.doesNotMatch(page, /VerifiedSessionGuard/);
  assert.doesNotMatch(middleware, /"\/market-pressure"/);
});

test("Market Pressure active nav state covers exact and nested routes", () => {
  assert.match(nav, /function isActiveNavLink/);
  assert.match(nav, /path === basePath \|\| path\.startsWith\(`\$\{basePath\}\/`\)/);
  assert.match(nav, /aria-current=\{active \? "page" : undefined\}/);
});

test("Market Pressure controls are semantic keyboard-accessible buttons", () => {
  assert.match(client, /role="group" aria-label="Market Pressure time range"/);
  assert.match(client, /role="group" aria-label="Market Pressure universe"/);
  assert.match(client, /role="group" aria-label="Market Pressure view mode"/);
  assert.match(client, /type="button" className=\{segmentedButtonClass\(active\)\} aria-pressed=\{active\}/);
  assert.match(client, /focus-visible:ring-2/);
});

test("Market Pressure is gated as a Pro feature before protected fetches", () => {
  assert.match(page, /getEntitlements/);
  assert.match(page, /function canUseMarketPressure/);
  assert.match(page, /canUseMarketPressure\(entitlements, Boolean\(authState\.token\)\)/);
  assert.match(page, /Market Pressure is available with Pro/);
  assert.match(contract, /response\.status === 403/);
  assert.match(contract, /pro_required/);
  assert.match(client, /Upgrade to Pro/);
  assert.doesNotMatch(client, /LayerAccessPanel/);
  assert.doesNotMatch(client, /FAST, RELIABLE, COMPLETE data only/);
});

test("Market Pressure renders no mock ticker data in production", () => {
  assert.doesNotMatch(contract, /AAPL|MSFT|NVDA|TSLA|SPY|QQQ/);
  assert.doesNotMatch(client, /AAPL|MSFT|NVDA|TSLA|sample|mock|placeholder/i);
  assert.match(contract, /GET \/api\/market-pressure|marketPressureApiUrl|\/api\/market-pressure/);
  assert.doesNotMatch(contract, /pressureScore/);
});

test("Market Pressure visualization includes loading, no-data, error, and entitlement states", () => {
  assert.match(client, /function statusCopy/);
  assert.match(client, /data\.status === "loading"/);
  assert.match(client, /status: "no-data"/);
  assert.match(client, /data\.status === "error"/);
  assert.match(client, /data\.status === "entitlement"/);
  assert.match(client, /data\.status === "unsupported"/);
  assert.match(client, /data\.status === "auth-required"/);
  assert.match(client, /aria-busy=\{data\.status === "loading"\}/);
});

test("Market Pressure universe controls are driven by backend capability metadata", () => {
  assert.match(contract, /export type MarketPressureUniverse = "sp500" \| "nasdaq100" \| "etf" \| "all_us" \| "watchlist"/);
  assert.match(contract, /\{ value: "etf", label: "ETFs" \}/);
  assert.match(contract, /universeDetails\?: Record<MarketPressureUniverse, MarketPressureUniverseCapability>/);
  assert.match(contract, /membershipCount: number \| null/);
  assert.match(contract, /sourceLabel\?: string \| null/);
  assert.match(contract, /source: "security_master"/);
  assert.match(contract, /complete_us_equity_universe_not_available/);
  assert.match(client, /initialData\.capabilities\.universes\[option\.value\]/);
  assert.match(client, /ETF universe data is temporarily unavailable/);
  assert.doesNotMatch(client, /Source: \{universeDetails\.sourceLabel\}/);
  assert.doesNotMatch(client, /Membership source as of/);
});

test("Market Pressure defaults and falls back from backend capabilities", () => {
  assert.match(contract, /getMarketPressureCapabilities/);
  assert.match(contract, /\/api\/market-pressure\/capabilities/);
  assert.match(contract, /function selectMarketPressureUniverse/);
  assert.match(contract, /preferredUniverseOrder: MarketPressureUniverse\[\] = \["sp500", "nasdaq100", "etf", "watchlist"\]/);
  assert.match(contract, /normalized === "etf" \|\| normalized === "etfs" \|\| normalized === "etf_fund"/);
  assert.match(page, /selectMarketPressureUniverse\(capabilities, requestedQuery\.universe\)/);
  assert.match(page, /marketPressureUnavailableUniverseWarning\(requestedQuery\.universe, selectedUniverse\)/);
  assert.match(page, /\.\.\.data\.warnings, fallbackWarning/);
  assert.match(client, /requested_universe_unavailable:/);
  assert.match(client, /Index membership data is temporarily unavailable/);
  assert.match(client, /Manage Watchlists/);
});

test("Market Pressure typed adapter isolates the future backend endpoint", () => {
  assert.match(contract, /export type MarketPressureTile = \{/);
  assert.match(contract, /marketCap: number \| null/);
  assert.match(contract, /confirmationDirection: "bullish" \| "bearish" \| "neutral" \| "conflicted" \| "unavailable"/);
  assert.match(contract, /divergence:/);
  assert.match(contract, /export async function getMarketPressureMap/);
  assert.match(contract, /fetch\(url/);
  assert.match(contract, /marketPressureQueryString/);
});

test("Market Pressure share and analytics events are wired through first-party helpers", () => {
  assert.match(api, /export function recordProductEvent/);
  assert.match(client, /market_pressure_page_view/);
  assert.match(client, /market_pressure_time_range_changed/);
  assert.match(client, /market_pressure_universe_changed/);
  assert.match(client, /market_pressure_view_changed/);
  assert.match(client, /market_pressure_share_opened/);
  assert.match(client, /market_pressure_ticker_opened/);
  assert.match(client, /market_pressure_image_downloaded/);
  assert.match(client, /market_pressure_x_share_opened/);
  assert.match(client, /renderShareSvg/);
  assert.match(client, /twitter\.com\/intent\/tweet/);
});

test("Market Pressure Phase 3 visual semantics are source-driven", () => {
  assert.match(client, /function priceFillClass/);
  assert.match(client, /function priceFillHex/);
  assert.match(client, /#00a64a/);
  assert.match(client, /#ff202f/);
  assert.match(client, /function confirmationFrameClass/);
  assert.match(client, /border-dashed border-amber-200/);
  assert.match(client, /hidden_accumulation: "Accumulation"/);
  assert.match(client, /fragile_winner: "Fragile"/);
  assert.match(client, /tile\.divergence/);
  assert.match(client, /text-3xl/);
  assert.doesNotMatch(client, /pressureScore/);
  assert.doesNotMatch(client, /confirmationScore\s*[+\-*/]/);
});

test("Market Pressure map uses a sector-nested treemap layout", () => {
  assert.match(client, /type TreemapRect/);
  assert.match(client, /function layoutTreemap/);
  assert.match(client, /function tileWeight/);
  assert.match(client, /function sectorWeight/);
  assert.match(client, /function SectorHoverTooltip/);
  assert.match(client, /data-sector-treemap/);
  assert.match(client, /data-treemap-tile/);
  assert.match(client, /data-symbol=\{tile\.symbol\}/);
  assert.match(client, /const hideLabel = rect \?/);
  assert.match(client, /\{!hideLabel \? \(/);
  assert.match(client, /\{showPrice \? \(/);
  assert.match(client, /data-sector-hover-tooltip/);
  assert.match(client, /bg-slate-950\/95 px-3 py-3 text-sm shadow-xl/);
  assert.match(client, /<th className="px-2 py-2 text-left font-medium">Ticker<\/th>/);
  assert.match(client, /<th className="px-2 py-2 text-right font-medium">1D<\/th>/);
  assert.match(client, /<th className="px-2 py-2 text-right font-medium">Score<\/th>/);
  assert.match(client, /<th className="px-2 py-2 text-left font-medium">Direction<\/th>/);
  assert.match(client, /style=\{rect \? rectStyle\(rect\) : undefined\}/);
  assert.match(client, /rounded-none/);
  assert.match(client, /layoutTreemap\(sectors\.map/);
  assert.match(client, /weight: sectorWeight\(sectorGroup\)/);
  assert.match(client, /weight: tileWeight\(tile\)/);
  assert.doesNotMatch(client, /grid grid-cols-\[repeat\(auto-fill,minmax\(7\.6rem,1fr\)\)\]/);
});

test("Market Pressure tile flyout and share export avoid protected public JSON", () => {
  assert.match(client, /WalnutModal/);
  assert.match(client, /Evidence summary/);
  assert.match(client, /mt-2 grid gap-x-3 sm:grid-cols-2/);
  assert.match(client, /Open full ticker page/);
  assert.match(client, /AddTickerToWatchlist/);
  assert.match(client, /image\/svg\+xml/);
  assert.doesNotMatch(client, /public.*JSON/i);
});
