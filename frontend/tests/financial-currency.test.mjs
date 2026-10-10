import assert from "node:assert/strict";
import test from "node:test";
import { formatStatementMoney } from "../lib/financialCurrency.mjs";

test("statement amounts retain reporting currency without FX conversion", () => {
  assert.equal(formatStatementMoney(309064000000, "DKK"), "DKK 309.1B");
  assert.equal(formatStatementMoney(1023670000000, "CNY"), "CNY 1.0T");
  assert.equal(formatStatementMoney(3809054300000, "TWD"), "TWD 3.8T");
  assert.equal(formatStatementMoney(-1500000, "DKK"), "-DKK 1.5M");
  assert.equal(formatStatementMoney(40054000), "$40.1M");
  assert.equal(formatStatementMoney(0, "CNY"), "CNY 0");
});

test("missing amounts and invalid currency are never labelled as dollars", () => {
  for (const value of [null, undefined, NaN, Infinity, "100"])
    assert.equal(formatStatementMoney(value, "DKK"), "-");
  assert.equal(formatStatementMoney(100, "unknown"), "Unavailable");
});
