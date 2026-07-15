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

test("Market Pressure appears before Pricing in the shared top navigation", () => {
  const marketPressureIndex = nav.indexOf('{ href: "/market-pressure", label: "Market Pressure" }');
  const pricingIndex = nav.indexOf('{ href: "/pricing", label: "Pricing" }');
  assert.ok(marketPressureIndex > -1, "Market Pressure nav item is missing");
  assert.ok(pricingIndex > -1, "Pricing nav item is missing");
  assert.ok(marketPressureIndex < pricingIndex, "Market Pressure should appear before Pricing");
  assert.match(nav, /usePathname/);
});

test("Market Pressure uses the responsive shared nav rather than a duplicate mobile implementation", () => {
  assert.match(nav, /overflow-x-auto/);
  assert.match(nav, /topNavLinks\.map/);
  assert.equal((nav.match(/Market Pressure/g) ?? []).length, 1);
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
  assert.match(client, /initialData\.status === "entitlement" \|\| initialData\.status === "auth-required"/);
});

test("Market Pressure renders no mock ticker data in production", () => {
  assert.doesNotMatch(contract, /AAPL|MSFT|NVDA|TSLA|SPY|QQQ/);
  assert.doesNotMatch(client, /AAPL|MSFT|NVDA|TSLA|sample|mock|placeholder/i);
  assert.match(contract, /GET \/api\/market-pressure|marketPressureApiUrl|\/api\/market-pressure/);
  assert.doesNotMatch(contract, /pressureScore/);
});

test("Market Pressure visualization includes loading, no-data, error, and entitlement states", () => {
  assert.match(client, /loading: \{/);
  assert.match(client, /"no-data": \{/);
  assert.match(client, /error: \{/);
  assert.match(client, /entitlement: \{/);
  assert.match(client, /unsupported: \{/);
  assert.match(client, /"auth-required": \{/);
  assert.match(client, /aria-busy=\{state === "loading"\}/);
});

test("Market Pressure typed adapter isolates the future backend endpoint", () => {
  assert.match(contract, /export type MarketPressureTile = \{/);
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
  assert.match(client, /Download image will be enabled when the map renderer supports export\./);
  assert.match(client, /Share to X will be enabled after public map snapshots are implemented\./);
});
