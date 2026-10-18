import assert from "node:assert/strict";
import test from "node:test";
import { comparableHalfYear } from "../lib/financialPeriods.mjs";
const first = {date:"2025-06-30",periodStart:"2025-01-01",revenue:100};
const current = {date:"2026-06-30",periodStart:"2026-01-01",revenue:120};
test("half-year growth uses the same prior-year period even with no second-half row", () => {
  assert.equal(comparableHalfYear([first,current],1),first);
  assert.equal(comparableHalfYear([first,{date:"2025-12-31",periodStart:"2025-07-01"},current],2),first);
});
test("missing, duplicate and incompatible half-year comparisons stay unavailable", () => {
  assert.equal(comparableHalfYear([current],0),null);
  assert.equal(comparableHalfYear([first,{...first},current],2),null);
  assert.equal(comparableHalfYear([{...first,periodStart:"2025-04-01"},current],1),null);
  assert.equal(comparableHalfYear([{...first,date:"2024-06-30"},current],1),null);
  assert.equal(comparableHalfYear([first,{...current,periodStart:null}],1),null);
});
