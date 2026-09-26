import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import { createRequire } from "node:module";
import ts from "typescript";

const require = createRequire(import.meta.url);
function load(file, modules, globals = {}) {
  const exports = {};
  const js = ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
  vm.runInNewContext(js, { exports, URL, URLSearchParams, require: name => modules[name] ?? require(name), ...globals });
  return exports;
}
const paths = load("lib/returnPaths.ts", {});
function harness(returnTo) {
  let cursor = 0;
  const states = [], calls = [], navigation = [], events = [];
  const react = {
    useState(initial) { const i = cursor++; if (!(i in states)) states[i] = initial; return [states[i], value => { states[i] = typeof value === "function" ? value(states[i]) : value; }]; },
    useMemo: fn => fn(), useRef: value => ({ current: value }), useEffect() {},
  };
  const { LoginRegisterPanel } = load("components/auth/LoginRegisterPanel.tsx", {
    react, "next/link": { default: () => null },
    "next/navigation": { useRouter: () => ({ replace: path => navigation.push(path), refresh() {} }), useSearchParams: () => new URLSearchParams("mode=register") },
    "@/lib/api": { ApiError: class extends Error {}, register: async body => { calls.push(body); return {}; }, verifyAuthenticatedSession: async () => ({}), recordProductEvent() {}, getGoogleAuthUrl: async path => { calls.push({ google: path }); return { authorization_url: "https://example.com/oauth" }; } },
    "@/lib/returnPaths": paths, "@/lib/campaignAttribution": { campaignParamKeys: [] },
    "@/lib/productAnalytics": { trackEvent: name => events.push(name) },
  }, { window: { location: { origin: "https://app.walnutmarkets.com" } }, document: { referrer: "" } });
  const nodes = tree => tree == null || typeof tree !== "object" ? [] : Array.isArray(tree) ? tree.flatMap(nodes) : [tree, ...nodes(tree.props?.children)];
  const render = () => { cursor = 0; return nodes(LoginRegisterPanel({ returnTo })); };
  const set = (predicate, value) => render().find(predicate).props.onChange({ target: { value } });
  return { render, set, calls, navigation, events };
}

test("registration sends only email/password and opens the stock starting page", async () => {
  const h = harness();
  const fields = h.render().filter(n => n.type === "input" && n.props.autoComplete);
  assert.deepEqual(fields.map(n => n.props.autoComplete), ["email", "new-password"]);
  h.set(n => n.props?.autoComplete === "email", " reader@example.com ");
  h.set(n => n.props?.autoComplete === "new-password", "Password123!");
  h.render().find(n => n.type === "button" && n.props.children === "Show password").props.onClick();
  assert.equal(h.render().find(n => n.props?.autoComplete === "new-password").props.type, "text");
  await h.render().find(n => n.type === "form").props.onSubmit({ preventDefault() {} });
  assert.deepEqual(JSON.parse(JSON.stringify(h.calls)), [{ email: "reader@example.com", password: "Password123!" }]);
  assert.deepEqual(h.navigation, ["/welcome"]);
  assert.ok(h.events.includes("signup_submitted"));
});

test("validation blocks submission and both auth methods preserve ticker follow intent", async () => {
  const returnTo = "/ticker/NVDA?follow=1&utm_source=reddit";
  const h = harness(returnTo);
  await h.render().find(n => n.type === "form").props.onSubmit({ preventDefault() {} });
  assert.equal(h.calls.length, 0);
  assert.ok(h.events.includes("signup_validation_failed"));
  h.set(n => n.props?.autoComplete === "email", "reader@example.com");
  h.set(n => n.props?.autoComplete === "new-password", "Password123!");
  await h.render().find(n => n.type === "form").props.onSubmit({ preventDefault() {} });
  assert.deepEqual(h.navigation, [`/welcome?return_to=${encodeURIComponent(returnTo)}`]);
  const google = harness(returnTo);
  await google.render().find(n => n.type === "button" && Array.isArray(n.props.children) && n.props.children.includes("Continue with Google")).props.onClick();
  assert.equal(google.calls[0].google, `/welcome?return_to=${encodeURIComponent(returnTo)}`);
});
