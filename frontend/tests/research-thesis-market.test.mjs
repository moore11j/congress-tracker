import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";
import React from "react";
import * as jsxRuntime from "react/jsx-runtime";
import { renderToStaticMarkup } from "react-dom/server";

const exports = {};
const js = ts.transpileModule(fs.readFileSync("components/research-memory/ResearchThesisMarket.tsx", "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
vm.runInNewContext(js, { exports, Intl, require: (name) => name === "react/jsx-runtime" ? jsxRuntime : { researchCheckTime: (value) => value } });
const point = { price: 100, price_as_of: "2026-09-25T20:00:00Z", price_source: "cached_close", day_change: 2, day_change_percent: 2, score: 60, score_as_of: "2026-09-25T18:00:00Z", is_stale: false };
const market = { baseline: { ...point, kind: "captured" }, current: { ...point, price: 110, score: 65 }, price_change: 10, price_change_percent: 10, score_change: 5, score_methodology_changed: false };
const render = (data) => renderToStaticMarkup(React.createElement(exports.ResearchThesisMarket, { market: data }));

test("ticker card distinguishes daily change, creation baseline, and since-creation change", () => {
  const html = render(market);
  for (const value of ["$110.00", "+$2.00", "+2.00%", "Price at creation", "$100.00", "Since thesis creation", "+$10.00", "+10.00%", "60/100", "65/100", "+5 pts"]) assert.ok(html.includes(value), value);
});

test("zero scores and losses display correctly without manufacturing missing returns", () => {
  const html = render({ ...market, baseline: { ...point, score: 0 }, current: { ...point, score: 0, day_change: -2, day_change_percent: -2 }, price_change: -10, price_change_percent: -10, score_change: 0 });
  assert.match(html, /0\/100/);
  assert.match(html, /-\$10\.00/);
  assert.match(html, /-10.00%/);
  const missing = render({ ...market, baseline: { ...point, price: null, score: null }, price_change: null, price_change_percent: null, score_change: null });
  assert.match(missing, /Unavailable/);
  assert.doesNotMatch(missing, /\$0.00|\+0.00%/);
});

test("historical references, old quotes, splits, and methodology changes are labeled", () => {
  const html = render({ ...market, baseline: { ...point, kind: "historical" }, current: { ...point, is_stale: true }, score_methodology_changed: true, score_change: null, price_change: null, price_change_percent: null, price_change_unavailable_reason: "split_review_required" });
  for (const value of ["Historical reference", "reconstructed", "Update overdue", "Scoring method changed", "Stock split", "excludes dividends"]) assert.ok(html.includes(value), value);
});
