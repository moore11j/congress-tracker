import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";

const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync("lib/retirementBacktest.ts", "utf8"), {
  compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
}).outputText, { exports });
const { congressRetirementBacktest, retirementBacktestReturn } = exports;
test("Congress selection uses backtester IDs, prefers canonical aliases, and excludes insiders", () => {
  const suggestions = [
    { category: "congress", label: "Nancy Pelosi", bioguide_id: " fmp_house_ca11 ", chamber: " HOUSE " },
    { category: "insider", label: "Nancy Insider", reporting_cik: "123" },
    { category: "congress", label: "Nancy Pelosi", bioguide_id: " p000197 ", chamber: "house" },
    { category: "congress", label: "Missing ID" },
  ];
  const members = exports.retirementCongressSources(suggestions);
  assert.equal(members.length, 1);
  assert.equal(members[0].id, "P000197");
  assert.equal(members[0].href, "/members/P000197");
  assert.equal(exports.retirementCongressSources(suggestions.toReversed())[0].id, "P000197");
});
const result = () => ({
  summary: { cagr_pct: 8.123456, positions_count: 12, skipped_positions_count: 0, price_fallback_positions_count: 0 },
  timeline: [{ date: "2023-09-20", active_positions: 0, invested_pct: 0 }, { date: "2024-09-20", active_positions: 2, invested_pct: 100 }, { date: "2026-09-18", active_positions: 1, invested_pct: 50 }],
  assumptions: ["Daily closes; no transaction costs or slippage."],
});

test("Congress request targets one member and uses disclosure timing without contributions", () => {
  const config = congressRetirementBacktest("P000197", "2026-09-20");
  assert.equal(config.member_id, "P000197");
  assert.equal(config.source_scope, "member");
  assert.equal(config.strategy_type, "congress");
  assert.equal(config.portfolio_model, "disclosure_date");
  assert.equal(config.contribution_amount, 0);
  assert.equal(config.hold_days, 90);
  assert.equal(config.rebalancing_frequency, "monthly");
  assert.equal(config.end_date, "2026-09-19");
  assert.equal((Date.parse(config.end_date) - Date.parse(config.start_date)) / 86400000, 1095);
});

test("UTC dates handle leap days and reject malformed provider dates", () => {
  assert.equal(congressRetirementBacktest("P000197", "2024-03-01").end_date, "2024-02-29");
  for (const day of ["invalid", "2026-02-30", "2026-9-20"]) assert.throws(() => congressRetirementBacktest("P000197", day));
});

test("import preserves the engine CAGR, actual priced dates, and assumptions", () => {
  const imported = retirementBacktestReturn(result());
  assert.equal(imported.rate, 8.123456);
  assert.equal(imported.period, "2023-09-20 to 2026-09-18");
  assert.match(imported.basis, /Simulated Congress-strategy/);
  assert.equal(imported.assumptions[0], result().assumptions[0]);
});

test("cash-only and unexecuted simulations cannot become a false 0% return", () => {
  for (const edit of [r => r.summary.positions_count = 0, r => r.timeline = [], r => r.timeline.forEach(p => p.invested_pct = 0)]) {
    const r = result(); edit(r); assert.throws(() => retirementBacktestReturn(r));
  }
});

test("short or invalid priced histories cannot be annualized", () => {
  for (const last of ["2024-01-01", "invalid", "2023-08-01"]) {
    const r = result(); r.timeline = [r.timeline[0], { ...r.timeline[1], date: last }];
    assert.throws(() => retirementBacktestReturn(r), /less than one year/);
  }
});

test("real zero and negative CAGR work; missing, nonfinite, and unsupported rates fail", () => {
  for (const rate of [0, -25, 100, -99]) { const r = result(); r.summary.cagr_pct = rate; assert.equal(retirementBacktestReturn(r).rate, rate); }
  for (const rate of [null, undefined, NaN, Infinity, -100, 101]) { const r = result(); r.summary.cagr_pct = rate; assert.throws(() => retirementBacktestReturn(r)); }
});

test("data omissions and price fallbacks remain visible before applying CAGR", () => {
  const r = result(); r.summary.skipped_positions_count = 2; r.summary.skipped_reasons = ["Missing close"];
  r.summary.price_fallback_positions_count = 3;
  const imported = retirementBacktestReturn(r);
  assert.match(imported.warnings[0], /2 positions skipped: Missing close/);
  assert.match(imported.warnings[1], /3 positions used nearby/);
});
