import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

const source = fs.readFileSync("components/research/ResearchActions.tsx", "utf8");
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX } }).outputText;
const exports = {};
vm.runInNewContext(code, { exports, require: () => ({}), Intl });

test("research exports retain delayed quote timing and actual observation", () => {
  const result = exports.buildResearchOutputs({ kind: "ticker", symbol: "TEST", quote: {
    current_price: 100, change_percent: 2, is_delayed: true, delay_minutes: 15,
    as_of: "2026-11-06T20:44:00Z", price_kind: "minute_close",
  }});
  const text = result.bullets.join("\n");
  assert.match(text, /Last quote: \$100\.00 \(2\.00%\)/);
  assert.match(text, /Delayed 15 minutes\. Observed 2026-11-06T20:44:00Z/);
  assert.ok(result.briefLines.join("\n").includes("Delayed 15 minutes"));
});

test("research exports do not invent delay or observation metadata", () => {
  const result = exports.buildResearchOutputs({kind: "ticker", symbol: "TEST", quote: { current_price: 100 }});
  assert.doesNotMatch(result.bullets.join("\n"), /Delayed|Observed/);
  const missing = exports.buildResearchOutputs({kind: "ticker", symbol: "TEST", quote: { is_delayed: true, delay_minutes: 15 }});
  assert.match(missing.bullets.join("\n"), /Last quote: unavailable/);
});
