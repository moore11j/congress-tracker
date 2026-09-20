import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";
const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync("lib/optionsCalculator.ts", "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText, { exports });
const { optionValue, profitAt, expirationRisk, positionGreeks, createStrategy, daysUntil } = exports;
const model = { spot: 100, days: 365, volatility: 20, rate: 5, dividend: 0 };
const leg = (kind, side, strike, premium, quantity = 1) => ({ id: "1", kind, side, strike, premium, quantity, source: "Manual" });
const position = (legs, extra = {}) => ({ legs, shares: 0, stockEntry: 100, fee: 0, ...extra });
const near = (actual, expected, tolerance = .001) => assert.ok(Math.abs(actual - expected) < tolerance, `${actual} != ${expected}`);
test("published Black–Scholes example: S=K=100, r=5%, vol=20%, T=1", () => { near(optionValue("call", 100, model), 10.45058); near(optionValue("put", 100, model), 5.57353); });
test("dividend-adjusted put-call parity", () => { const m = { ...model, dividend: 3 }; near(optionValue("call", 110, m) - optionValue("put", 110, m), 100 * Math.exp(-.03) - 110 * Math.exp(-.05)); });
test("expiry, zero volatility, and zero underlying price are finite", () => { near(optionValue("call", 95, { ...model, days: 0 }), 5); near(optionValue("put", 110, { ...model, volatility: 0 }), 110 * Math.exp(-.05) - 100); near(optionValue("put", 100, { ...model, spot: 0 }), 100 * Math.exp(-.05)); });
test("long call risk, contract multiplier, and entry fees", () => { const p = position([leg("call", 1, 100, 5, 2)], { fee: .65 }); const r = expirationRisk(p); near(r.maxLoss, 1001.3); assert.equal(r.maxProfit, Infinity); near(r.breakEvens[0], 105.0065); near(profitAt(p, { ...model, days: 0, spot: 120 }), 2998.7); });
test("short naked call reports unlimited loss beyond chart", () => { const r = expirationRisk(position([leg("call", -1, 100, 5)])); assert.equal(r.maxLoss, Infinity); near(r.maxProfit, 500); near(r.breakEvens[0], 105); });
test("debit call spread caps both outcomes", () => { const r = expirationRisk(position([leg("call", 1, 100, 8), leg("call", -1, 110, 3)])); near(r.maxLoss, 500); near(r.maxProfit, 500); near(r.breakEvens[0], 105); });
test("credit put spread includes entry fees", () => { const r = expirationRisk(position([leg("put", -1, 100, 6), leg("put", 1, 90, 2)], { fee: 1 })); near(r.maxProfit, 398); near(r.maxLoss, 602); near(r.breakEvens[0], 96.02); });
test("covered call stock basis and put downside at zero", () => { const r = expirationRisk(position([leg("call", -1, 110, 2)], { shares: 100, stockEntry: 100 })); near(r.maxProfit, 1200); near(r.maxLoss, 9800); near(r.breakEvens[0], 98); const put = expirationRisk(position([leg("put", 1, 100, 5)])); near(put.maxProfit, 9500); near(put.maxLoss, 500); });
test("straddle and iron condor have two break-evens", () => { const r = expirationRisk(position([leg("put", 1, 100, 4), leg("call", 1, 100, 5)])); near(r.breakEvens[0], 91); near(r.breakEvens[1], 109); const c = expirationRisk(position([leg("put", 1, 90, 1), leg("put", -1, 95, 2), leg("call", -1, 105, 2), leg("call", 1, 110, 1)])); near(c.maxProfit, 200); near(c.maxLoss, 300); near(c.breakEvens[0], 93); near(c.breakEvens[1], 107); });
test("position Greeks have contract-scaled units", () => { const g = positionGreeks(position([leg("call", 1, 100, 10)]), model); near(g.delta, 63.683, .01); near(g.gamma, 1.876, .01); near(g.vega, 37.524, .01); assert.ok(g.theta < 0); });
test("scenario converges to exact payoff and does not mutate entries", () => { const p = createStrategy("bull-call", model), saved = JSON.stringify(p); near(profitAt(p, { ...model, days: 0, volatility: 100 }), profitAt(p, { ...model, days: 0, volatility: 0 })); assert.equal(JSON.stringify(p), saved); });
test("all templates produce finite modeled values and valid maximum loss", () => { for (const t of exports.strategyTemplates) { const p = createStrategy(t.id, model); assert.ok(Number.isFinite(profitAt(p, model))); assert.ok(expirationRisk(p).maxLoss >= 0); } });
test("calendar dates respect leap years and floor expired options at zero", () => { assert.equal(daysUntil("2028-02-28", "2028-03-01"), 2); assert.equal(daysUntil("2026-09-19", "2026-09-18"), 0); });

const listed = (kind, strike, close) => ({ ticker: `${kind}-${strike}`, kind, strike, close });
test("Alpaca closes retain provider attribution and never overwrite a manual premium", () => {
  const p = position([{ ...leg("call", 1, 100, 5), contract: "call-100", source: "Modeled entry" }]);
  const close = { ticker: "call-100", price: 7, as_of: "2026-09-18T04:00:00Z", source: "Alpaca" };
  assert.equal(exports.applyOptionClose(p, close).legs[0].source, "Alpaca close · 2026-09-18");
  p.legs[0].source = "Manual entry";
  assert.equal(exports.applyOptionClose(p, close).legs[0].premium, 5);
});
test("listed matching preserves a spread even when both targets are beyond the chain", () => {
  const result = exports.matchListedPosition(createStrategy("bull-call", { ...model, spot: 150 }), [listed("call", 95), listed("call", 100), listed("call", 105)], model);
  assert.equal(result.legs[0].strike, 100); assert.equal(result.legs[1].strike, 105);
  assert.equal(result.legs[0].contract, "call-100");
});
test("straddles use a common listed strike and zero is a valid closing price", () => {
  const result = exports.matchListedPosition(createStrategy("straddle", model), [listed("call", 100), listed("put", 101), listed("call", 105, { price: 0, as_of: "2026-09-18T20:00:00Z" }), listed("put", 105)], model);
  assert.equal(result.legs[0].strike, 105); assert.equal(result.legs[1].strike, 105);
  assert.equal(result.legs[0].premium, 0); assert.equal(result.legs[0].source, "Massive close · 2026-09-18");
});
test("insufficient listings leave an explicitly modeled position intact", () => {
  const original = createStrategy("bull-call", model);
  assert.equal(exports.matchListedPosition(original, [listed("call", 100)], model), original);
  assert.equal(exports.matchListedPosition(original, [], model), original);
});
test("matching a condor keeps all four strikes ordered without mutating inputs", () => {
  const original = createStrategy("iron-condor", model), saved = JSON.stringify(original);
  const contracts = [92, 97, 103, 108].flatMap(strike => [listed("call", strike), listed("put", strike)]);
  const result = exports.matchListedPosition(original, contracts, model);
  assert.equal(JSON.stringify(result.legs.map(l => l.strike)), "[92,97,103,108]");
  assert.equal(JSON.stringify(original), saved);
});
test("a delayed close updates only its modeled contract and preserves manual premiums", () => {
  const p = position([
    { ...leg("call", 1, 100, 1), id: "auto", contract: "call-100", source: "Modeled entry" },
    { ...leg("call", -1, 100, 7), id: "manual", contract: "call-100", source: "Manual entry" },
    { ...leg("call", 1, 110, 2), id: "changed", contract: "call-110", source: "Modeled entry" },
  ]);
  const next = exports.applyOptionClose(p, { ticker: "call-100", price: 4, as_of: "2026-09-18" });
  assert.equal(next.legs[0].premium, 4); assert.equal(next.legs[1].premium, 7); assert.equal(next.legs[2].premium, 2);
  assert.equal(p.legs[0].premium, 1);
});
