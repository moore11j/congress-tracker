import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);
function loadTs(relative) {
  const source = fs.readFileSync(relative, "utf8");
  const { outputText } = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true } });
  const module = { exports: {} };
  new Function("require", "module", "exports", outputText)((name) => name.startsWith("@/") ? loadTs(`${name.slice(2)}.ts`) : require(name), module, module.exports);
  return module.exports;
}
const { scoreRecalibrationsInRange } = loadTs("lib/confirmationRecalibrations.ts");
const { DecisionTrendChart } = loadTs("components/ticker/DecisionTrendChart.tsx");

test("recalibrations align with irregular daily observations and include endpoint dates", () => {
  const points = [{ date: "2026-09-14", score: 100 }, { date: "2026-09-16", score: 70 }, { date: "2026-09-18", score: 13 }];
  const original = structuredClone(points);
  assert.deepEqual(scoreRecalibrationsInRange(points).map(({ date, index, inspectionIndex }) => ({ date, index, inspectionIndex })), [
    { date: "2026-09-16", index: 1, inspectionIndex: 1 },
    { date: "2026-09-17", index: 1.5, inspectionIndex: 2 },
  ]);
  assert.deepEqual(points, original);
  assert.equal(scoreRecalibrationsInRange([{ date: "2026-09-16" }, { date: "2026-09-17" }]).length, 2);
});

test("annotations disappear outside the displayed window or without usable dates", () => {
  for (const points of [[], [{ date: "2026-09-17" }], [{ date: "2026-09-01" }, { date: "2026-09-15" }], [{ date: "2026-09-18" }, { date: "2026-10-01" }], [{ date: "bad" }, { date: "2026-09-17" }]]) {
    assert.deepEqual(scoreRecalibrationsInRange(points), []);
  }
});

test("rendered graph explains methodology changes accessibly without replacing history", () => {
  const history = [{ date: "2026-09-15", score: 100 }, { date: "2026-09-16", score: 70 }, { date: "2026-09-17", score: 13 }];
  const html = renderToStaticMarkup(React.createElement(DecisionTrendChart, { history, direction: "bullish" }));
  assert.equal((html.match(/data-recalibration=/g) ?? []).length, 2);
  assert.match(html, /Dashed amber lines mark score recalibrations/);
  assert.match(html, /Historical scores are unchanged/);
  assert.match(html, /Scores across these changes use different methods/);
  assert.match(html, /Score recalibrated/);
  assert.deepEqual(history.map((point) => point.score), [100, 70, 13]);
  const after = renderToStaticMarkup(React.createElement(DecisionTrendChart, { history: [{ date: "2026-09-18", score: 13 }, { date: "2026-09-19", score: 20 }], direction: "bullish" }));
  assert.doesNotMatch(after, /data-recalibration=|Score recalibrated/);
});
