import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

function load(file, modules = {}, globals = {}) {
  const exports = {};
  const js = ts.transpileModule(fs.readFileSync(file, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  vm.runInNewContext(js, { exports, require: name => modules[name] ?? {}, URL, URLSearchParams, setTimeout, clearTimeout, ...globals });
  return exports;
}
const returns = load("lib/returnPaths.ts");
const recovery = load("lib/authRecovery.ts", { "./returnPaths": returns });

// Run the real component handlers with persistent hooks and an inspectable JSX tree.
function harness(file, exportName, modules, window, props = {}) {
  const slots = [], effects = [];
  let cursor = 0;
  const hooks = {
    useState(initial) {
      const i = cursor++;
      if (!(i in slots)) slots[i] = typeof initial === "function" ? initial() : initial;
      return [slots[i], next => { slots[i] = typeof next === "function" ? next(slots[i]) : next; }];
    },
    useRef(initial) { const i = cursor++; return slots[i] ??= { current: initial }; },
    useMemo(fn) { return fn(); },
    useEffect(fn, deps) {
      const i = cursor++;
      if (!slots[i] || deps.some((value, j) => !Object.is(value, slots[i][j]))) { slots[i] = deps; effects.push(fn); }
    },
  };
  const jsx = (type, props) => ({ type, props });
  const component = load(file, { ...modules, react: hooks, "react/jsx-runtime": { jsx, jsxs: jsx }, "next/link": { default: "a" } }, { window, CustomEvent: class {} })[exportName];
  let tree;
  function nodes(node) {
    if (!node || typeof node !== "object") return [];
    if (Array.isArray(node)) return node.flatMap(child => nodes(child));
    return [node, ...nodes(node.props?.children)];
  }
  return {
    render() { cursor = 0; tree = component(props); return tree; },
    effects() { effects.splice(0).forEach(fn => fn()); },
    find(type, predicate = () => true) { return nodes(tree).find(node => node.type === type && predicate(node.props)); },
    nodes: () => nodes(tree),
  };
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test("auth deadline fails visibly and never resumes a timed-out navigation", async () => {
  let finish, navigated = false;
  const pending = new Promise(resolve => { finish = resolve; });
  const completion = recovery.withAuthTimeout(pending, 5).then(() => { navigated = true; });
  await assert.rejects(completion, /taking too long/);
  finish({ user: {} });
  await flush();
  assert.equal(navigated, false);
  assert.equal(await recovery.withAuthTimeout(Promise.resolve("ok"), 50), "ok");
  await assert.rejects(recovery.withAuthTimeout(Promise.reject(new Error("denied")), 50), /denied/);
});

test("Google recovery preserves only safe app destinations and tolerates blocked storage", () => {
  const data = new Map();
  const local = load("lib/authRecovery.ts", { "./returnPaths": returns }, { window: { sessionStorage: {
    setItem: (k,v) => data.set(k,v), getItem: k => data.get(k), removeItem: k => data.delete(k),
  } } });
  local.rememberGoogleReturnPath("/pricing?upgrade=premium");
  assert.equal(local.googleReturnPath(), "/pricing?upgrade=premium");
  local.rememberGoogleReturnPath("//untrusted.example");
  assert.equal(local.googleReturnPath(), returns.defaultPostLoginPath);
  local.clearGoogleReturnPath();
  assert.equal(data.size, 0);
  const unavailable = load("lib/authRecovery.ts", { "./returnPaths": returns }, { window: { get sessionStorage() { throw Error("blocked"); } } });
  unavailable.rememberGoogleReturnPath("/pricing");
  assert.equal(unavailable.googleReturnPath(), returns.defaultPostLoginPath);
});

function callback(search, api) {
  const navigation = [], scrubbed = [];
  const window = { location: { search, pathname: "/auth/google/callback", origin: "https://app.walnutmarkets.com", replace: value => navigation.push(value) }, history: { replaceState: (...args) => scrubbed.push(args) } };
  const h = harness("app/auth/google/callback/page.tsx", "default", {
    "@/lib/api": api, "@/lib/returnPaths": returns,
    "@/lib/authRecovery": { ...recovery, googleReturnPath: () => "/pricing?upgrade=premium", clearGoogleReturnPath() {} },
    "@/lib/heycatch": { identifyHeyCatchUser() {} },
  }, window);
  h.render(); h.effects();
  return { h, navigation, scrubbed };
}

test("cancelled Google login offers a fresh login preserving upgrade intent", () => {
  let calls = 0;
  const { h, navigation, scrubbed } = callback("?error=access_denied&state=state", { completeGoogleSignIn() { calls++; } });
  h.render();
  assert.match(h.find("h1").props.children, /cancelled/);
  assert.equal(h.find("h1").props.role, "alert");
  assert.equal(h.find("a").props.href, "/login?return_to=%2Fpricing%3Fupgrade%3Dpremium");
  assert.equal(calls, 0); assert.equal(navigation.length, 0);
  assert.equal(scrubbed[0][2], "/auth/google/callback");
});

test("OAuth exchanges once and navigates only after a verified session", async () => {
  let calls = 0, verify;
  const { h, navigation } = callback("?code=one-use-code&state=state", {
    completeGoogleSignIn: async () => { calls++; return { return_to: "/pricing?upgrade=premium" }; },
    verifyAuthenticatedSession: () => new Promise(resolve => { verify = resolve; }),
  });
  await flush(); h.render(); h.effects();
  assert.equal(calls, 1); assert.equal(navigation.length, 0);
  assert.equal(h.find("a"), undefined, "No Continue shortcut before verification");
  verify({ user: { id: 1 } }); await flush();
  assert.deepEqual(navigation, ["/pricing?upgrade=premium"]);
});

test("rejected Google session offers retry without a success redirect", async () => {
  const { h, navigation } = callback("?code=code&state=state", {
    completeGoogleSignIn: async () => ({ return_to: "/ticker/NVDA?follow=1" }),
    verifyAuthenticatedSession: async () => { throw new Error("Session unavailable"); },
  });
  await flush(); h.render();
  assert.match(h.find("a").props.href, /return_to=%2Fticker%2FNVDA%3Ffollow%3D1/);
  assert.equal(h.find("h1").props.role, "alert");
  assert.equal(navigation.length, 0);
});

function landing(results = []) {
  const window = { location: { pathname: "/", href: "https://walnutmarkets.com" } };
  const navigation = load("lib/searchNavigation.ts", { "./memberSlug": {} });
  const h = harness("components/landing/LandingSearch.tsx", "LandingSearch", {
    "@/lib/entitlements": { defaultEntitlements: {} },
    "@/lib/privacyConsent": { hasPrivacyConsent: () => true },
    "@/lib/googleAnalytics": { recordGoogleAnalyticsEvent() { throw Error("blocked analytics"); } },
    "@/lib/productAnalytics": { trackEvent() {} },
    "@/hooks/useFastSearchSuggest": { useFastSearchSuggest: () => ({ results, settled: true }) },
    "@/lib/searchNavigation": navigation,
  }, window, { appUrl: "https://app.walnutmarkets.com" });
  h.render();
  return { h, window };
}

test("homepage search has a usable native form before JavaScript hydrates", () => {
  const { h } = landing();
  assert.equal(h.find("form").props.action, "https://app.walnutmarkets.com/search");
  assert.equal(h.find("form").props.method, "get");
  assert.equal(h.find("input").props.name, "q");
  assert.equal(h.find("input").props.required, true);
  const config = fs.readFileSync("next.config.js", "utf8");
  const formPolicy = config.match(/form-action [^"\n]+/)[0];
  assert.ok(formPolicy.split(" ").includes(new URL(h.find("form").props.action).origin));
});

test("blank search stays put with feedback instead of bouncing to another homepage", () => {
  const { h, window } = landing();
  h.find("input").props.onChange({ target: { value: "   " } }); h.render();
  h.find("form").props.onSubmit({ preventDefault() {} }); h.render();
  assert.equal(window.location.href, "https://walnutmarkets.com");
  assert.match(h.find("p", props => props.role === "alert").props.children, /Enter a ticker/);
});

test("stock and fallback searches navigate even if analytics throws", () => {
  for (const [results, query, destination] of [
    [[{ kind: "ticker", id: "NVDA", symbol: "NVDA", label: "NVIDIA", href: "/ticker/NVDA" }], "NVDA", "/ticker/NVDA"],
    [[], "Example & Co", "/search?q=Example%20%26%20Co"],
  ]) {
    const { h, window } = landing(results);
    h.find("input").props.onChange({ target: { value: query } }); h.render();
    h.find("form").props.onSubmit({ preventDefault() {} }); h.render();
    assert.equal(window.location.href, `https://app.walnutmarkets.com${destination}`);
    assert.equal(h.find("button").props.disabled, true);
  }
});

test("monitoring pages change results, reset list scroll, and clear only selected checkboxes", async () => {
  const items = Array.from({ length: 12 }, (_, i) => ({ id: i + 1, symbol: `TEST${i}`, title: `Update ${i}`, timestamp: new Date(2026, 0, 20 - i).toISOString(), source_type: "watchlist", source_id: "1", is_unread: i % 2 === 0 }));
  const window = { localStorage: { getItem: () => null }, dispatchEvent() {} };
  const h = harness("components/monitoring/MonitoringDashboard.tsx", "MonitoringDashboard", {
    "next/dynamic": { default: () => "calendar" },
    "@/lib/api": {
      ApiError: class extends Error {}, hasClientAuthHint: () => false,
      getEntitlements: async () => ({}), getMonitoringInbox: async () => ({ items, sources: [], unread_total: 6 }),
      getMonitoringSources: async () => ({ sources: [] }), getMonitoringSourceCounts: async () => ({ counts: { sources: [] }, unread_total: 6 }),
      listWatchlists: async () => [],
    },
    "@/lib/entitlements": { defaultEntitlements: {}, hasEntitlement: () => true, limitFor: () => 100 },
    "@/lib/monitoringTitles": { displayMonitoringAlertTitle: item => item.title },
    "@/lib/savedViews": { parseSavedViewsStore: () => ({ views: [] }) },
  }, window, { initialWatchlists: [] });
  h.render(); h.effects(); await flush(); h.render(); h.effects(); h.render();
  const button = label => h.find("button", props => props.children === label);
  const checkboxes = () => h.nodes().filter(node => node.type === "input" && node.props.type === "checkbox");
  const list = h.find("div", props => props.ref && props["aria-busy"] !== undefined);
  list.props.ref.current = { scrollTop: 400 };
  assert.equal(button("Clear selection").props.disabled, true);
  assert.equal(checkboxes()[0].props["aria-label"], "Select Update 0");
  button("Next").props.onClick(); h.render(); h.effects();
  assert.equal(list.props.ref.current.scrollTop, 0);
  assert.equal(checkboxes()[0].props["aria-label"], "Select Update 5");
  button("Select all visible").props.onClick(); h.render();
  assert.equal(checkboxes().filter(node => node.props.checked).length, 5);
  button("Clear selection").props.onClick(); h.render();
  assert.equal(checkboxes().filter(node => node.props.checked).length, 0);
  assert.equal(button("Clear selection").props.disabled, true);
  button("Last").props.onClick(); h.render(); h.effects();
  assert.equal(checkboxes().length, 2);
  assert.equal(button("Next").props.disabled, true);
  button("unread").props.onClick(); h.render(); h.effects(); h.render();
  assert.equal(checkboxes().length, 5);
  assert.equal(button("Previous").props.disabled, true);
  button("Reset filters").props.onClick(); h.render(); h.effects(); h.render();
  assert.equal(button("Reset filters"), undefined);
  assert.equal(checkboxes()[0].props["aria-label"], "Select Update 0");
});
