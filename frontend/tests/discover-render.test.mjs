import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
function load(relative, context = {}) {
  const filename = path.join(root, relative);
  const source = ts.transpileModule(fs.readFileSync(filename, "utf8"), { compilerOptions: { jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true, module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  const module = { exports: {} };
  const localRequire = (id) => {
    if (id === "next/link") return ({ children, prefetch, ...props }) => React.createElement("a", props, children);
    if (id === "next/navigation") return { usePathname: () => context.pathname || "/" };
    if (id === "@/components/ui/HorizontalScrollAffordance") return { HorizontalScrollIndicators: () => null, useHorizontalScrollAffordance: () => ({ scrollRef: { current: null }, canScrollLeft: false, canScrollRight: false, updateScrollState() {} }) };
    if (id === "@/lib/api") return { getCachedLeaderboard: () => { throw new Error("Render must not fetch data"); } };
    if (id.startsWith("@/lib/")) return load(`${id.slice(2)}.ts`);
    return require(id);
  };
  vm.runInNewContext(source, { module, exports: module.exports, require: localRequire, Intl, Date, URL, process: { env: context.env || {} } }, { filename });
  return module.exports;
}
const { DiscoveryStocksView } = load("components/discover/DiscoveryStocks.tsx");
const snapshot = (items, extra = {}) => ({ items, preview: true, generated_at: "2026-11-20T20:00:00Z", ...extra });
const render = (value, extra = {}) => renderToStaticMarkup(React.createElement(DiscoveryStocksView, { snapshot: value, loading: false, failed: false, onRetry() {}, ...extra }));

test("public preview keeps supplied ranks and hides protected score even if included", () => {
  const html = render(snapshot([{ rank: 3, symbol: "NVDA", confirmation_score: 87.1234, company_name: "Nvidia" }], { locked_ranks: [1, 2] }));
  assert.match(html, />#3</);
  assert.doesNotMatch(html, />#1<|87\.1234/);
  assert.match(html, /Ranks 3–5|Create a free account/);
});
test("paid prepared score uses the shared direction label", () => {
  const html = render(snapshot([{ rank: 1, symbol: "NBIS", confirmation_score: 35, confirmation_direction: "bearish" }], { preview: false }));
  assert.match(html, /30-day Confirmation Score/);
  assert.match(html, /Weak Bearish/);
  assert.match(html, /href="\/ticker\/NBIS"/);
});
test("missing projection flag does not expose numeric scores", () => {
  assert.doesNotMatch(render(snapshot([{ rank: 1, symbol: "NVDA", confirmation_score: 87.1234 }], { preview: undefined })), /87\.1234/);
});
test("failure stays distinct from an empty successful snapshot", () => {
  assert.match(render(null, { failed: true }), /temporarily unavailable/);
  assert.match(render(null, { failed: true }), /Try again/);
  assert.match(render(snapshot([])), /No ranked stocks are available/);
  assert.doesNotMatch(render(snapshot([])), /temporarily unavailable/);
});
test("loading never reports no results or stale prior rows", () => {
  const html = render(snapshot([{ rank: 1, symbol: "NVDA" }]), { loading: true });
  assert.match(html, /aria-busy="true"/);
  assert.doesNotMatch(html, /No ranked|NVDA/);
});
test("stock links are validated and text is escaped instead of trusted as HTML", () => {
  const html = render(snapshot([{ rank: 3, symbol: "NVDA", company_name: "<script>unsafe</script>", ticker_url: "https://invalid.example" }, { rank: 4, symbol: "//bad" }]));
  assert.match(html, /&lt;script&gt;/);
  assert.doesNotMatch(html, /<script>|invalid\.example|href="\/\/bad"/);
  assert.match(html, /href="\/ticker\/NVDA"/);
});

test("workspace navigation flag preserves legacy destinations and exact default-off rollback", () => {
  const renderNav = (flag, pathname = "/") => {
    const { AppTopNav } = load("components/AppTopNav.tsx", { pathname, env: { NEXT_PUBLIC_WALNUT_WORKSPACE_ENABLED: flag } });
    return renderToStaticMarkup(React.createElement(AppTopNav));
  };
  const off = renderNav(undefined);
  assert.doesNotMatch(off, /href="\/(discover|my-stocks)"/);
  assert.equal(renderNav("0"), off);
  for (const route of ["/discover", "/my-stocks"]) {
    const html = renderNav("1", route);
    assert.match(html, new RegExp(`href="${route}" aria-current="page"`));
    assert.match(html, /href="\/\?mode=all"/);
    for (const label of ["Insights", "Signals", "Profiles", "Tools", "Company"]) assert.ok(html.includes(label), label);
    assert.doesNotMatch(html, /href="\/ask"/);
  }
});
