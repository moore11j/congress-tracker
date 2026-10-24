import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);
let filterAccess = { canExpand: false, canSortExport: false };
const cache = new Map();
function loadTs(file) {
  if (cache.has(file)) return cache.get(file);
  const { outputText } = ts.transpileModule(fs.readFileSync(file, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
  });
  const module = { exports: {} };
  const localRequire = (name) => {
    if (name === "react" && file.endsWith("FeedFiltersServer.tsx")) return {
      ...React, useEffect: () => {}, useState: () => [filterAccess, () => {}],
    };
    if (name === "@/lib/api") return {};
    if (name === "next/link") return ({ children }) => React.createElement("span", {}, children);
    if (name.startsWith("@/components/")) return new Proxy({}, { get: () => () => null });
    if (name.startsWith("@/")) {
      const path = name.slice(2);
      return loadTs(fs.existsSync(path + ".ts") ? path + ".ts" : path + ".tsx");
    }
    return require(name);
  };
  new Function("require", "module", "exports", outputText)(localRequire, module, module.exports);
  cache.set(file, module.exports);
  return module.exports;
}
const controls = loadTs("lib/feedControls.ts");
const { FeedFiltersServer } = loadTs("components/feed/FeedFiltersServer.tsx");
const { FeedTable, sortFeedTableItems, feedExportRows } = loadTs("components/feed/FeedTable.tsx");
const item = (id, amount, pnl = null) => ({
  id, kind: "congress_trade", report_date: "2026-10-01", trade_date: "2026-09-25",
  security: { symbol: "AAPL", name: "Apple Inc." }, member: { name: "René Example" },
  amount_range_max: amount, amount_range_min: amount, gain_loss_percent: pnl,
  transaction_type: "purchase", payload: {}, url: "https://example.com/disclosure",
});

test("effective tier controls row sizes and separate Pro access", () => {
  for (const [tier, canExpand, canSortExport] of [
    ["free", false, false], ["premium", true, false], ["pro", true, true], ["admin", true, true],
  ]) {
    assert.deepEqual(controls.feedAccess({ tier }), { canExpand, canSortExport });
    assert.equal(controls.feedPageSize(1000, canExpand), canExpand ? 1000 : 25);
  }
  assert.deepEqual(controls.feedAccess({ tier: "pro", effective_tier: "free" }), { canExpand: false, canSortExport: false });
  for (const bad of [NaN, Infinity, -1, 0, 24, 999, 1001]) assert.equal(controls.feedPageSize(bad), 25);
});

test("filter submission, mode links and Reset preserve the selected paid page size", () => {
  for (const size of controls.feedPageSizes) {
    filterAccess = { canExpand: true, canSortExport: true };
    const html = renderToStaticMarkup(React.createElement(FeedFiltersServer, { mode: "congress", params: { symbol: "AAPL" }, pageSize: size }));
    assert.ok(html.includes('name="page_size" value="' + size + '"'));
    assert.ok(html.includes("mode=insider&amp;page_size=" + size));
    assert.ok(html.includes("/?mode=congress&amp;page_size=" + size));
    assert.doesNotMatch(html, /name="page" /);
    assert.doesNotMatch(html, /name="sort_by" disabled/);
  }
  filterAccess = { canExpand: false, canSortExport: false };
  const html = renderToStaticMarkup(React.createElement(FeedFiltersServer, { mode: "congress", params: {}, pageSize: 1000 }));
  assert.match(html, /name="page_size" value="25"/);
  assert.match(html, /name="sort_by" disabled/);
  assert.match(html, /name="sort_dir" disabled/);
});

test("table buttons gate sorting and expose ascending/descending state", () => {
  const props = { items: [item(1, 1000)], expandedItemId: null, onToggleExpanded: () => {}, canViewPremiumMetrics: true };
  const locked = renderToStaticMarkup(React.createElement(FeedTable, props));
  assert.equal((locked.match(/Pro required to sort/g) ?? []).length, 8);
  for (const dir of ["asc", "desc"]) {
    const html = renderToStaticMarkup(React.createElement(FeedTable, { ...props, canSortExport: true, sortBy: "amount", sortDir: dir }));
    assert.match(html, new RegExp('aria-sort="' + (dir === "asc" ? "ascending" : "descending") + '"'));
    assert.doesNotMatch(html, /Pro required to sort/);
  }
});

test("numeric sorts use values, put unavailable last in both directions and preserve input", () => {
  const rows = [item(1, 900), item(2, 10000), item(3, null), item(4, 900)];
  assert.deepEqual(sortFeedTableItems(rows, "amount", "asc").map(r => r.id), [4, 1, 2, 3]);
  assert.deepEqual(sortFeedTableItems(rows, "amount", "desc").map(r => r.id), [2, 4, 1, 3]);
  assert.deepEqual(rows.map(r => r.id), [1, 2, 3, 4]);
  const pnlRows = [item(1, 10, -2), item(2, 10, 8), item(3, 10)];
  assert.deepEqual(sortFeedTableItems(pnlRows, "pnl", "asc").map(r => r.id), [1, 2, 3]);
});

test("exports preserve displayed ordering, missing labels, distinct dates and disclosure URLs", () => {
  const rows = feedExportRows(sortFeedTableItems([item(1, 900), item(2, 10000)], "amount", "desc"));
  assert.deepEqual(rows.map(r => r[0]), ["2", "1"]);
  assert.equal(rows[0][2], "2026-09-25");
  assert.equal(rows[0][5], "René Example");
  assert.equal(rows[0][9], "Pending");
  assert.equal(rows[0][11], "https://example.com/disclosure");
  assert.equal(rows[0].length, 12);
});
