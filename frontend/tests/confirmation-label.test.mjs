import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import ts from "typescript";
const source = fs.readFileSync("lib/confirmationLabel.ts", "utf8");
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { confirmationLabel } = await import(`data:text/javascript;base64,${Buffer.from(code).toString("base64")}`);
test("directional lean labels distinguish weak evidence from missing and conflicted evidence", () => {
  assert.equal(confirmationLabel(16, "bullish", "inactive"), "Weak bullish lean");
  assert.equal(confirmationLabel(16, "bearish", "inactive"), "Weak bearish lean");
  assert.equal(confirmationLabel(0, "neutral", "inactive"), "Inactive");
  assert.equal(confirmationLabel(0, "mixed"), "Conflicted confirmation");
  assert.equal(confirmationLabel(null, "bullish"), "Unavailable");
  assert.equal(confirmationLabel(20, "bullish"), "Weak Bullish");
  assert.equal(confirmationLabel(67, "bullish"), "Strong Bullish");
});
