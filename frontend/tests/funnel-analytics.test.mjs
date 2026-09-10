import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

function load(file, modules = {}, globals = {}) {
  const exports = {};
  const source = fs.readFileSync(file, "utf8");
  const js = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
  vm.runInNewContext(js, { exports, require: (name) => { if (!(name in modules)) throw Error(`Missing test module ${name}`); return modules[name]; }, URL, URLSearchParams, console, ...globals });
  return exports;
}
const routes = load("lib/funnelEvents.ts");
test("canonical names are unique and cover the required funnel", () => {
  assert.equal(new Set(routes.funnelEvents).size, routes.funnelEvents.length);
  for (const name of routes.funnelEvents) assert.match(name, /^[a-z]+(?:_[a-z]+)+$/);
  for (const name of ["outcomes_viewed", "signup_completed", "subscription_completed", "ticker_related_content_clicked"]) assert.ok(routes.funnelEvents.includes(name));
});
test("dynamic routes, dotted symbols, feed modes, marketing root and exclusions", () => {
  assert.equal(routes.routeFunnelEvent("/ticker/BRK.B").properties.ticker, "BRK.B");
  assert.equal(routes.routeFunnelEvent("/ticker/MSFT").name, "ticker_viewed");
  assert.equal(routes.routeFunnelEvent("/leaderboards/congress-traders").properties.leaderboard_type, "congress");
  assert.equal(routes.routeFunnelEvent("/strategies/congress-alpha").properties.strategy_id, "congress-alpha");
  assert.equal(routes.routeFunnelEvent("/", "mode=congress").name, "congress_trades_viewed");
  assert.equal(routes.routeFunnelEvent("/feed", "mode=institutional").name, "institutional_activity_viewed");
  assert.equal(routes.routeFunnelEvent("/", "", true).name, "homepage_viewed");
  assert.equal(routes.routeFunnelEvent("/"), null);
  assert.equal(routes.routeFunnelEvent("/strategies/methodology"), null);
  assert.equal(routes.routeFunnelEvent("/congress-trades"), null); // SEO article is not the live tape.
});
test("rerenders dedupe while navigation back and different tickers remain visits", () => {
  const visits = routes.createVisitTracker();
  assert.equal(visits.enter("/ticker/AAPL"), true);
  assert.equal(visits.enter("/ticker/AAPL"), false);
  assert.equal(visits.enter("/ticker/MSFT"), true);
  assert.equal(visits.enter("/ticker/AAPL"), true);
});
test("production guard rejects dev, preview, local, file copies and lookalike hosts", () => {
  function guard(hostname, protocol = "https:", env = "production", deployment = "production") {
    return load("lib/analyticsEnvironment.ts", {}, { window: { location: { hostname, protocol } }, process: { env: { NODE_ENV: env, NEXT_PUBLIC_VERCEL_ENV: deployment } } }).isProductionAnalyticsHost();
  }
  assert.equal(guard("walnutmarkets.com"), true);
  assert.equal(guard("app.walnutmarkets.com"), true);
  for (const host of ["localhost", "127.0.0.1", "staging.walnutmarkets.com", "preview.vercel.app", "walnutmarkets.com.evil.test"]) assert.equal(guard(host), false);
  assert.equal(guard("walnutmarkets.com", "file:"), false);
  assert.equal(guard("walnutmarkets.com", "https:", "development"), false);
  assert.equal(guard("walnutmarkets.com", "https:", "production", "preview"), false);
});
function facade({ production = true, consent = true, debug = false, throwTransport = false } = {}) {
  const calls = [];
  const analytics = load("lib/productAnalytics.ts", {
    "./api": { recordProductEvent: (payload) => { if (throwTransport) throw Error("blocked"); calls.push(["backend", payload]); } },
    "./googleAnalytics": { recordGoogleAnalyticsEvent: (...args) => calls.push(["ga", ...args]) },
    "./heycatch": { trackHeyCatchEvent: (...args) => calls.push(["heycatch", ...args]) },
    "./analyticsEnvironment": { isProductionAnalyticsHost: () => production },
    "./analyticsContext": { acquisitionProperties: () => ({ acquisition_source: "reddit" }), analyticsConsent: () => consent, analyticsIdentity: () => ({ authenticated: false, current_plan: "free" }), safeAnalyticsPath: value => new URL(value, "https://walnutmarkets.com").pathname },
    "./funnelEvents": routes,
  }, { window: { location: { pathname: "/ticker/NVDA" }, dispatchEvent: e => calls.push(["debug", e.detail]) }, CustomEvent: class { constructor(name, options) { this.detail = options.detail; } }, process: { env: { NEXT_PUBLIC_ANALYTICS_DEBUG: debug ? "1" : "0" } }, console: { info() {} } });
  return { ...analytics, calls };
}
test("facade fans out one canonical event, sanitizes paths and excludes PII", () => {
  const api = facade();
  assert.equal(api.trackEvent("ticker_viewed", { ticker: "NVDA", destination_page: "/pricing?token=secret", email: "a@b.test", entity_id: "a@b.test" }), true);
  assert.equal(api.calls.length, 3);
  const properties = api.calls[0][1].properties;
  assert.equal(properties.destination_page, "/pricing");
  assert.equal(properties.email, undefined);
  assert.equal(properties.entity_id, undefined);
  assert.equal(properties.acquisition_source, "reddit");
  assert.equal(api.calls[2][1], "ticker_viewed");
  assert.equal(api.trackEvent("invented_event"), false);
});
test("local debug never transmits; opt-out and storage/transport failure fail safely", () => {
  const local = facade({ production: false, debug: true });
  assert.equal(local.trackEvent("pricing_viewed"), true);
  assert.equal(local.calls.length, 1);
  assert.equal(local.calls[0][0], "debug");
  const denied = facade({ consent: false });
  assert.equal(denied.trackEvent("pricing_viewed"), false);
  assert.equal(denied.calls.length, 0);
  assert.doesNotThrow(() => facade({ throwTransport: true }).trackEvent("pricing_viewed"));
});
test("first-touch attribution survives navigation and session ID survives identity transitions", () => {
  let consent = true;
  const window = { location: { hostname: "app.walnutmarkets.com", search: "?utm_source=reddit&utm_campaign=launch" }, crypto: { randomUUID: () => "anonymous-session" } };
  const context = load("lib/analyticsContext.ts", { "./analyticsEnvironment": { isProductionAnalyticsHost: () => false }, "./privacyConsent": { hasPrivacyConsent: () => consent } }, { window, document: { cookie: "", referrer: "https://www.google.com/?q=private" } });
  assert.equal(context.acquisitionProperties().acquisition_source, "reddit");
  window.location.search = "";
  assert.equal(context.acquisitionProperties().utm_campaign, "launch");
  const sid = context.analyticsSessionId();
  context.setAnalyticsIdentity({ id: 42, current_plan: "premium" });
  assert.equal(context.analyticsSessionId(), sid);
  assert.equal(context.analyticsIdentity().authenticated, true);
  context.setAnalyticsIdentity(null);
  assert.equal(context.analyticsIdentity().current_plan, "free");
  assert.equal(context.safeAnalyticsPath("/login?password=secret"), "/login");
  assert.equal(context.sourceLabel("name@example.com"), null);
  consent = false;
  assert.equal(Object.keys(context.acquisitionProperties()).length, 0);
});
test("homepage CTAs and contextual gates preserve required destinations", () => {
  const home = fs.readFileSync("app/landing/page.tsx", "utf8");
  assert.match(home, /href=\{`\$\{appUrl\}\/screener`\}[^>]*>Open Screener/);
  assert.match(home, /View Leaderboards/);
  assert.match(home, /Explore Strategies/);
  const gates = fs.readFileSync("components/leaderboards/LeaderboardsDashboard.tsx", "utf8");
  assert.equal((gates.match(/rows.slice\(0, 3\)/g) || []).length, 4);
  assert.match(gates, /allowed \? <div className="flex items-center/);
  assert.match(gates, /tier="Pro" label="Unlock the complete institutional ranking"/);
  const pricing = fs.readFileSync("components/billing/PricingActions.tsx", "utf8");
  assert.match(pricing, /mode=register&return_to=/);
  assert.match(pricing, /signupQuery.set\("plan", tier\)/);
  assert.match(pricing, /signupQuery.set\("interval", billingInterval\)/);
});
test("retention success events do not fire when existing follows are loaded", () => {
  const source = fs.readFileSync("components/strategies/StrategyFollowButton.tsx", "utf8");
  const load = source.slice(source.indexOf("getStrategySubscription(slug)"), source.indexOf("async function saveSubscription"));
  assert.doesNotMatch(load, /trackEvent/);
  assert.match(source, /if \(!following && result.subscription.isActive\) trackEvent\("strategy_followed"/);
});

test("Opened sort orders dates both ways, uses deterministic ties, and leaves source rows intact", () => {
  const source = fs.readFileSync("components/outcomes/OutcomeLedgerClient.tsx", "utf8");
  const names = ["openedDateValue", "openedTime", "formatDirection", "sortedOutcomeSnapshots"];
  const snippets = names.map(name => {
    const start = source.indexOf(`function ${name}(`);
    const next = source.indexOf("\nfunction ", start + 1);
    return source.slice(start, next < 0 ? undefined : next).replace(`function ${name}`, `export function ${name}`);
  }).join("\n");
  const exports = {};
  vm.runInNewContext(ts.transpileModule(snippets, { compilerOptions: { module: ts.ModuleKind.CommonJS } }).outputText, { exports });
  const rows = [{ id: 1, ticker: "A", entry_session_date: "2026-07-10" }, { id: 2, ticker: "B", entry_session_date: "2026-07-06" }, { id: 3, ticker: "C", entry_session_date: "2026-07-10" }];
  assert.equal(exports.sortedOutcomeSnapshots(rows, { key: "opened", direction: "asc" }).map(r => r.id).join(), "2,1,3");
  assert.equal(exports.sortedOutcomeSnapshots(rows, { key: "opened", direction: "desc" }).map(r => r.id).join(), "3,1,2");
  assert.equal(rows.map(r => r.id).join(), "1,2,3");
  assert.match(source, /if \(!gatePremiumTable\(\)\) return;/);
  assert.match(source, /aria-sort=\{hasPremiumTable/);
  assert.match(source, /min-h-11 w-full/);
});
test("ticker discoveries are backed by activity or actual comparable tickers and preserve gates", () => {
  const page = fs.readFileSync("app/ticker/[symbol]/page.tsx", "utf8");
  assert.match(page, /insiderCardSource.present \? <TickerDiscoveryLink/);
  assert.match(page, /congressCardSource.present \? <TickerDiscoveryLink/);
  assert.match(page, /outcomes\?ticker=\$\{encodeURIComponent\(match.ticker\)\}/);
  assert.match(page, /enabled=\{!confirmationLocked && score !== null\}/);
  const institutional = fs.readFileSync("components/ticker/TickerInstitutionalSourceCardClient.tsx", "utf8");
  assert.match(institutional, /canViewInstitutional && source.present && !loading/);
  const gate = fs.readFileSync("components/billing/ContextualUpgrade.tsx", "utf8");
  assert.match(gate, /name="upgrade_prompt_viewed"/);
  assert.match(gate, /trackEvent\("upgrade_prompt_clicked"/);
});
