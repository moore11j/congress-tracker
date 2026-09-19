import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";

const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync("lib/retirementCalculator.ts", "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText, { exports });
const { projectRetirement, validateRetirementPlan, annualizeReturn, defaultRetirementPlan } = exports;
const { realAnnualReturn, retirementDisplayValue } = exports;
const person = (overrides = {}) => ({ age: 40, retirementAge: 42, openingBalance: 10000, monthlySavings: 100, monthlyIncome: 100, ...overrides });
const plan = (overrides = {}) => ({ ...defaultRetirementPlan, you: person(), spouse: person({ age: 41 }), includeSpouse: false, annualReturn: 0, retirementReturn: 0, inflation: 0, taxRate: 0, retirementYears: 3, ...overrides });
const near = (a, b, tolerance = .001) => assert.ok(Math.abs(a - b) < tolerance, `${a} != ${b}`);

test("real returns use the exact inflation adjustment rather than subtracting percentages", () => {
  near(realAnnualReturn(7, 2.5), 4.390243902439024);
  near(realAnnualReturn(4, 2.5), 1.4634146341463414);
  near(realAnnualReturn(7, 0), 7);
  assert.ok(realAnnualReturn(2, 3) < 0);
});

test("nominal/real display changes purchasing power without changing the investment projection", () => {
  const result = projectRetirement(plan({ annualReturn: 7, inflation: 2.5 }), 2026);
  const balance = result.rows[2].total;
  assert.equal(retirementDisplayValue(balance, 2, 2.5, false), balance);
  near(retirementDisplayValue(balance, 2, 2.5, true), balance / 1.025 ** 2);
  assert.equal(retirementDisplayValue(balance, 2, 0, true), balance);
  assert.equal(retirementDisplayValue(balance, 0, 2.5, true), balance);
  assert.equal(result.rows[2].total, balance);
});

test("zero returns: deposits stop and withdrawals begin at each retirement", () => {
  const result = projectRetirement(plan(), 2026);
  near(result.atRetirement[0], 12400);
  near(result.rows[2].contributions, 1200);
  near(result.rows[2].withdrawals, 0);
  near(result.rows[3].contributions, 0);
  near(result.rows[3].withdrawals, 1200);
  near(result.rows.at(-1).total, 8800);
});
test("effective annual return matches monthly annuity closed form", () => {
  const result = projectRetirement(plan({ annualReturn: 12 }), 2026);
  const rate = Math.pow(1.12, 1 / 12) - 1;
  near(result.atRetirement[0], 10000 * 1.12 ** 2 + 100 * ((1 + rate) ** 24 - 1) / rate);
});
test("staggered retirements use balances at a shared date, including earlier withdrawals", () => {
  const result = projectRetirement(plan({ includeSpouse: true }), 2026);
  near(result.atRetirement[1], 11200);
  near(result.bothRetired.spouse, 10000);
  near(result.bothRetired.total, 22400);
  assert.notEqual(result.bothRetired.total, result.atRetirement[0] + result.atRetirement[1]);
});
test("already retired, empty accounts, and tax gross-up do not produce negative balances", () => {
  const result = projectRetirement(plan({ you: person({ retirementAge: 40, openingBalance: 1000 }), taxRate: 50 }), 2026);
  near(result.atRetirement[0], 1000);
  near(result.rows[1].withdrawals, 1000);
  near(result.rows[1].income, 500);
  near(result.rows[1].shortfall, 700);
  assert.equal(result.depletedMonth[0], 6);
  assert.ok(result.rows.every((row) => row.total >= 0));
});
test("negative returns and inflation reconcile annual account cash flows", () => {
  const result = projectRetirement(plan({ annualReturn: -20, retirementReturn: -10, inflation: 3, includeSpouse: true, taxRate: 15 }), 2026);
  result.rows.slice(1).forEach((row, index) => near(row.total, result.rows[index].total + row.contributions + row.growth - row.withdrawals));
  assert.ok(result.rows[2].withdrawals > 1200 / .85);
});
test("disabled spouse has no financial effect", () => {
  const a = projectRetirement(plan(), 2026);
  const b = projectRetirement(plan({ spouse: person({ openingBalance: 1e9, retirementAge: NaN }) }), 2026);
  assert.equal(a.bothRetired.total, b.bothRetired.total);
  assert.equal(b.rows.at(-1).spouse, 0);
});
test("validation rejects nonfinite, fractional age, reversed retirement and unsafe rates", () => {
  for (const invalid of [plan({ annualReturn: NaN }), plan({ annualReturn: -100 }), plan({ taxRate: 100 }), plan({ retirementYears: 2.5 }), plan({ you: person({ age: 40.5 }) }), plan({ you: person({ retirementAge: 39 }) }), plan({ you: person({ openingBalance: -1 }) })]) {
    assert.ok(validateRetirementPlan(invalid).length);
    assert.throws(() => projectRetirement(invalid, 2026));
  }
});
test("annualization requires at least a year and uses actual dates", () => {
  near(annualizeReturn(21, "2024-01-01", "2026-01-01"), (1.21 ** (365.25 / 731) - 1) * 100);
  for (const args of [[20, "2026-01-01", "2026-02-01"], [null, "2024-01-01", "2026-01-01"], [20, "bad", "2026-01-01"], [-101, "2024-01-01", "2026-01-01"]]) assert.equal(annualizeReturn(...args), null);
  assert.equal(annualizeReturn(0, "2024-01-01", "2026-01-01"), 0);
});
