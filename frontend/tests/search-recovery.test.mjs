import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";

const expe = { kind: "ticker", id: "EXPE", symbol: "EXPE", label: "Expedia Group Inc", href: "/ticker/EXPE" };
const flush = async () => { for (let i = 0; i < 20; i++) await Promise.resolve(); };
function harness(searchSuggest) {
  let cursor = 0;
  const states = [];
  let effects = [];
  const exports = {};
  const jsx = (type, props) => ({ type, props });
  const modules = {
    react: {
      useEffect: fn => effects.push(fn), useMemo: fn => fn(),
      useState(value) { const i = cursor++; if (!(i in states)) states[i] = value; return [states[i], next => { states[i] = typeof next === "function" ? next(states[i]) : next; }]; },
    },
    "react/jsx-runtime": { jsx, jsxs: jsx }, "next/link": { default: "link" },
    "next/navigation": { useRouter: () => ({ push: () => {} }) },
    "@/lib/api": { searchSuggest },
    "@/lib/searchNavigation": { isHighConfidenceSearchResult: () => false, routeForSearchResult: r => r.href, searchResultsHref: q => `/search?q=${q}` },
  };
  vm.runInNewContext(ts.transpileModule(fs.readFileSync("app/search/SearchResultsClient.tsx", "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  }).outputText, { exports, require: name => modules[name], AbortController, Error, window: { setTimeout: () => 1, clearTimeout() {} } });
  return {
    render(query = "expedia") { cursor = 0; effects = []; return exports.SearchResultsClient({ initialQuery: query }); },
    start() { effects[0](); return effects[1](); },
  };
}
function nodes(tree) {
  if (!tree || typeof tree !== "object") return [];
  if (Array.isArray(tree)) return tree.flatMap(nodes);
  return [tree, ...nodes(tree.props?.children)];
}

test("immediate Enter search recovers through the same-origin API when direct host fails", async () => {
  const calls = [];
  const h = harness(async (query, limit, options) => {
    calls.push(options);
    if (calls.length === 1) throw new Error("Network failure");
    return { items: [expe] };
  });
  h.render(); const cleanup = h.start(); await flush();
  assert.equal(calls[1].sameOrigin, true);
  const tree = h.render();
  assert.ok(nodes(tree).some(n => n.props?.href === "/ticker/EXPE"));
  assert.ok(!nodes(tree).some(n => n.props?.role === "status"));
  cleanup();
});

test("optional deep-search failure or empty results cannot erase fast Expedia results", async () => {
  for (const fails of [true, false]) {
    const h = harness(async (query, limit, options) => {
      if (options.mode === "deep") { if (fails) throw new Error("slow enrichment"); return { items: [] }; }
      return { items: [expe] };
    });
    h.render(); const cleanup = h.start(); await flush();
    const tree = h.render();
    assert.ok(nodes(tree).some(n => n.props?.href === "/ticker/EXPE"));
    assert.ok(!nodes(tree).some(n => n.props?.role === "status"));
    cleanup();
  }
});

test("retrying the same query runs a new search and clears the failed state", async () => {
  let offline = true;
  const h = harness(async () => { if (offline) throw new Error("offline"); return { items: [expe] }; });
  h.render(); const cleanup = h.start(); await flush();
  const tree = h.render();
  assert.ok(nodes(tree).some(n => n.props?.role === "status"));
  const button = nodes(tree).find(n => n.type === "button" && n.props.children === "Try again");
  assert.ok(button); offline = false; button.props.onClick(); cleanup();
  h.render(); const cleanup2 = h.start(); await flush();
  assert.ok(nodes(h.render()).some(n => n.props?.href === "/ticker/EXPE"));
  cleanup2();
});

test("a new query clears results left over from the previous company", async () => {
  const h = harness(async query => query === "expedia" ? { items: [expe] } : new Promise(() => {}));
  h.render(); const cleanup = h.start(); await flush(); cleanup();
  h.render("microsoft"); const cleanup2 = h.start();
  assert.ok(!nodes(h.render("microsoft")).some(n => n.props?.href === "/ticker/EXPE"));
  cleanup2();
});
