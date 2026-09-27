import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";
import * as jsx from "react/jsx-runtime";

function load(file, modules = {}, globals = {}) {
  const exports = {};
  const code = ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX } }).outputText;
  vm.runInNewContext(code, { exports, require: name => { assert.ok(name in modules, name); return modules[name]; }, URL, URLSearchParams, ...globals });
  return exports;
}
const returns = load("lib/returnPaths.ts");
const destinations = load("lib/upgradeDestination.ts", { "./returnPaths": returns });

test("upgrade destinations retain research state and compare anchor without forwarding credentials", () => {
  const context = destinations.upgradeReturnPath("/strategies", "?selected=insider-trend&period=1y&token=secret&utm_source=test", "#selected-strategy");
  const url = new URL(destinations.upgradePricingHref("Pro", context, true), "https://app.walnutmarkets.com");
  assert.equal(url.pathname, "/pricing");
  assert.equal(url.searchParams.get("plan"), "pro");
  assert.equal(url.searchParams.get("returnTo"), "/strategies?selected=insider-trend&period=1y#selected-strategy");
  assert.equal(url.hash, "#compare");
  assert.equal(destinations.upgradeReturnPath("/strategies/insider-trend", "?positions=history&history_page=2"), "/strategies/insider-trend?positions=history&history_page=2");
  for (const unsafe of ["https://evil.test", "//evil.test", "/\\evil.test", "/\nmalicious"]) {
    const href = new URL(destinations.upgradePricingHref("Premium", unsafe), "https://app.walnutmarkets.com");
    assert.equal(href.searchParams.get("returnTo"), returns.defaultPostLoginPath);
  }
});

test("shared gate emits matching feature/plan properties and one click while preserving the callback", () => {
  const calls = [];
  const module = load("components/billing/UpgradeLink.tsx", {
    "react/jsx-runtime": jsx,
    react: { useState: initial => [initial, () => {}], useEffect() {} },
    "next/navigation": { usePathname: () => "/ticker/MSFT" },
    "next/link": { default: "a" },
    "@/components/analytics/VisibleEvent": { VisibleEvent: "visible-event" },
    "@/lib/productAnalytics": { trackEvent: (...args) => calls.push(args) },
    "@/lib/upgradeDestination": destinations,
  });
  const impression = module.UpgradeImpression({ feature: "ticker_ownership", tier: "Pro", children: "test" });
  assert.equal(impression.props.name, "upgrade_prompt_viewed");
  assert.equal(impression.props.properties.gated_feature, "ticker_ownership");
  assert.equal(impression.props.properties.target_plan, "pro");
  const link = module.UpgradeLink({ feature: "ticker_ownership", tier: "Pro", children: "Upgrade", onClick: () => calls.push(["legacy"]) });
  assert.equal(new URL(link.props.href, "https://app.walnutmarkets.com").searchParams.get("returnTo"), "/ticker/MSFT");
  assert.equal(calls.length, 0);
  link.props.onClick({});
  assert.equal(calls.length, 2);
  assert.equal(calls[0][0], "upgrade_prompt_clicked");
  assert.equal(calls[0][1].target_plan, "pro");
  assert.equal(calls[0][1].gated_feature, impression.props.properties.gated_feature);
  assert.equal(calls[0][1].destination_page, "/pricing");
});

test("HeyCatch identity updates on entitlement and admin changes; logout clears identity dedupe", () => {
  const calls = [];
  const module = load("lib/heycatch.ts", {
    "@heycatch/sdk": { analytics: { init() {}, setIdentity: (...args) => calls.push(args), resetIdentity: () => calls.push(["reset"]) } },
    "./analyticsEnvironment": { isProductionAnalyticsHost: () => true },
    "./analyticsContext": { acquisitionProperties: () => ({}), analyticsConsent: () => true },
  }, { process: { env: { NEXT_PUBLIC_HEYCATCH_PROJECT_KEY: "hck_pk_fixture" } } });
  const user = { id: 42, current_plan: "free", entitlement_tier: "premium", role: "user" };
  module.identifyHeyCatchUser(user);
  module.identifyHeyCatchUser(user);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][1].plan, "premium");
  assert.equal(calls[0][1].is_internal, false);
  module.identifyHeyCatchUser({ ...user, is_admin: true });
  assert.equal(calls[1][1].is_internal, true);
  module.identifyHeyCatchUser({ ...user, entitlement_tier: "pro" });
  assert.equal(calls[2][1].plan, "pro");
  assert.equal(calls[2][1].is_internal, false);
  module.resetHeyCatchIdentity();
  module.identifyHeyCatchUser(user);
  assert.equal(calls.length, 5);
});

test("browser event identity does not retain an admin segment after logout", () => {
  const module = load("lib/analyticsContext.ts", { "./analyticsEnvironment": {}, "./privacyConsent": {} });
  module.setAnalyticsIdentity({ id: 1, is_admin: true, entitlement_tier: "pro" });
  assert.equal(module.analyticsIdentity().is_internal, true);
  module.setAnalyticsIdentity(null);
  assert.equal(module.analyticsIdentity().is_internal, false);
  assert.equal(module.analyticsIdentity().current_plan, "free");
});

test("visible upgrade impressions wait for identity and consent and dedupe rerenders", () => {
  let ready = false, consent = false, effect, observe, cleanup;
  const refs = [], calls = [], listeners = new Map();
  let index = 0;
  const events = { addEventListener: (name, callback) => listeners.set(name, callback), removeEventListener: name => listeners.delete(name) };
  class Observer { constructor(callback) { observe = callback; } observe() {} disconnect() {} }
  const module = load("components/analytics/VisibleEvent.tsx", {
    "react/jsx-runtime": jsx,
    react: { useRef: initial => refs[index++] ??= { current: initial }, useEffect: callback => { effect = callback; } },
    "next/navigation": { usePathname: () => "/ticker/MSFT" },
    "@/lib/productAnalytics": { trackEvent: (...args) => { if (!consent) return false; calls.push(args); return true; } },
    "@/lib/privacyConsent": { privacyConsentChangedEvent: "consent" },
    "@/lib/analyticsVisit": { analyticsVisitReadyEvent: "ready", isAnalyticsVisitReady: () => ready },
  }, { window: { ...events, IntersectionObserver: Observer }, document: { ...events, visibilityState: "visible" }, IntersectionObserver: Observer });
  const render = () => {
    cleanup?.(); index = 0;
    const node = module.VisibleEvent({ name: "upgrade_prompt_viewed", properties: { gated_feature: "ticker_ownership", target_plan: "pro" }, children: "Upgrade" });
    node.props.ref.current = {};
    cleanup = effect();
  };
  render();
  observe([{ isIntersecting: true }]);
  assert.equal(calls.length, 0);
  ready = true; listeners.get("ready")();
  assert.equal(calls.length, 0);
  consent = true; listeners.get("consent")();
  assert.equal(calls.length, 1);
  render(); observe([{ isIntersecting: true }]);
  assert.equal(calls.length, 1);
  cleanup();
  assert.equal(listeners.size, 0);
});
