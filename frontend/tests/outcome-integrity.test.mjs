import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");
const outcomes = read("components/outcomes/OutcomeLedgerClient.tsx");
const api = read("lib/api.ts");

test("percentage formatting treats API values as percentage points exactly once", () => {
  assert.match(outcomes, /return `\$\{prefix\}\$\{value\.toFixed\(1\)\}%`/);
  assert.doesNotMatch(outcomes, /value \* 100/);
});

test("Outcome charts consume the exact event price-path endpoint", () => {
  assert.match(api, /\/api\/outcomes\/snapshots\/\$\{encodeURIComponent\(String\(snapshotId\)\)\}\/price-path/);
  assert.match(outcomes, /getOutcomePricePath\(selected\.id, Number\.parseInt\(horizon, 10\), controller\.signal\)/);
  assert.doesNotMatch(outcomes, /getTickerChartBundle\(selected\.ticker, 30/);
});

test("Outcome scatter plots raw API return without clipping or directional substitution", () => {
  assert.match(outcomes, /const returnValue = numericReturn\(outcome\?\.return_pct\)/);
  assert.match(outcomes, /const yExtent = Math\.ceil\(maxAbsoluteReturn \/ 5\) \* 5/);
  assert.doesNotMatch(outcomes, /Math\.max\(-25, Math\.min\(25, point\.returnValue\)\)/);
});

test("frontend never synthesizes missing Outcome returns", () => {
  assert.doesNotMatch(outcomes, /demoReturnForSnapshot/);
  assert.doesNotMatch(outcomes, /baseReturns =/);
  assert.match(outcomes, /data_integrity_status === "requires_reconstruction" \? "Audit hold"/);
});

test("null and missing horizons render as missing rather than zero", () => {
  assert.match(outcomes, /typeof outcome\.return_pct === "number"/);
  assert.match(outcomes, /return "-"/);
  assert.doesNotMatch(outcomes, /outcome\.return_pct \|\| 0/);
});
