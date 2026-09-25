import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import ts from "typescript";

const require = createRequire(import.meta.url);
const read = (name) => fs.readFileSync(`components/ticker/${name}.tsx`, "utf8");
function renderFunction(file, name, props = {}) {
  const source = ts.createSourceFile(file, read(file), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const declaration = source.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name);
  assert.ok(declaration, `Missing ${name}`);
  const code = ts.transpileModule(`export ${declaration.getText(source)}`, {
    compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  const module = { exports: {} };
  const Link = ({ children, prefetch, ...attributes }) => React.createElement("a", attributes, children);
  new Function("require", "exports", "Link", code)(require, module.exports, Link);
  return renderToStaticMarkup(React.createElement(module.exports[name], props));
}

test("Valuation upgrade label matches Ownership typography without altering its preview", () => {
  const valuation = renderFunction("TickerValuationTab", "ProBlur", { children: "Valuation preview" });
  const ownership = renderFunction("TickerOwnershipPanel", "LockedState");
  const typography = "text-sm font-semibold text-emerald-100";
  assert.ok(ownership.includes(typography));
  assert.ok(valuation.includes(`class="${typography}">Upgrade to Pro`));
  assert.doesNotMatch(valuation, /uppercase|tracking-/);
  assert.match(valuation, /Full valuation details/);
  assert.match(valuation, /blur-\[7px\]/);
  assert.match(valuation, /href="\/pricing"/);
});

test("Analyst Premium callout uses Macro layout and keeps the full feature description", () => {
  const html = renderFunction("TickerAnalystConsensusTab", "PremiumLocked");
  const macro = read("TickerContextCard");
  const container = "rounded-2xl border border-emerald-300/20 bg-emerald-300/10 p-5";
  const button = "mt-4 inline-flex rounded-xl border border-emerald-300/40 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15";
  for (const classes of [container, button]) {
    assert.ok(macro.includes(classes));
    assert.ok(html.includes(classes));
  }
  assert.match(html, /Analyst detail requires Premium/);
  assert.match(html, /Upgrade to Premium/);
  assert.match(html, /href="\/account\/billing"/);
  for (const copy of ["current summary", "full consensus trail", "rating distribution", "target dispersion", "trend changes", "upgrade\/downgrade history"]) assert.ok(html.includes(copy));
  assert.doesNotMatch(html, /grid-cols|17rem|Upgrade to Pro/);
});
