import assert from "node:assert/strict";
import test from "node:test";
import { comparableHalfYear, financialAxisLabel } from "../lib/financialPeriods.mjs";
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


test("reported period axis labels preserve end month without inferring fiscal quarters", () => {
  assert.equal(financialAxisLabel("Half-year ended 2026-06-30"), "Jun '26");
  assert.equal(financialAxisLabel("Half-year ended 2025-12-31"), "Dec '25");
  assert.equal(financialAxisLabel("Quarter ended 2026-01-31"), "Jan '26");
  assert.equal(financialAxisLabel("Year ended 2025-09-30"), "Sep '25");
  assert.equal(financialAxisLabel("Quarter ended 2024-02-29"), "Feb '24");
});

test("existing fiscal labels and invalid source dates are not relabelled", () => {
  assert.equal(financialAxisLabel("Q2 2026"), "Q2 '26");
  assert.equal(financialAxisLabel("2025"), "2025");
  assert.equal(financialAxisLabel("FY 2027 Forecast"), "FY '27 Forecast");
  assert.equal(financialAxisLabel("Quarter ended 2026-02-30"), "Quarter ended 2026-02-30");
});
