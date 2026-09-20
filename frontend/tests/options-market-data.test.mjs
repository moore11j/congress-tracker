import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";
const source = ts.transpileModule(fs.readFileSync("lib/optionsMarketData.ts", "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
function load(timer = setTimeout) { const exports = {}; vm.runInNewContext(source, { exports, Error, DOMException, setTimeout: timer, clearTimeout }); return exports; }
test("changing context cancels a queued request before it starts", async () => {
  const api = load(), controller = new AbortController();
  const pending = api.waitForOptions(61000, controller.signal);
  controller.abort();
  await assert.rejects(pending, { name: "AbortError" });
  await assert.rejects(api.waitForOptions(61000, controller.signal), { name: "AbortError" });
});
test("shared allowance retries are paced, bounded, and cancellable", async () => {
  const delays = [], api = load((fn, ms) => { delays.push(ms); return setTimeout(fn, 0); });
  let calls = 0, waits = 0;
  const limited = Object.assign(new Error("busy"), { status: 429 });
  await assert.rejects(api.loadOptionsData(async () => { calls++; throw limited; }, new AbortController().signal, () => waits++), /busy/);
  assert.equal(calls, 3); assert.equal(waits, 2); assert.deepEqual(delays, [61000, 61000]);
  const controller = new AbortController(); calls = 0;
  await assert.rejects(api.loadOptionsData(async () => { calls++; throw limited; }, controller.signal, () => controller.abort()), { name: "AbortError" });
  assert.equal(calls, 1);
});
test("access and no-trade errors are not retried as quota errors", async () => {
  for (const status of [404, 503]) {
    let calls = 0;
    await assert.rejects(load().loadOptionsData(async () => { calls++; throw Object.assign(new Error("unavailable"), { status }); }, new AbortController().signal, () => assert.fail("unexpected wait")), /unavailable/);
    assert.equal(calls, 1);
  }
});

test("batch queue prioritizes selected legs, caps at 100, and skips completed contracts", () => {
  const api = load(), contracts = Array.from({ length: 210 }, (_, i) => ({ ticker: String(i), strike: i, kind: "call" }));
  contracts[99].close = { price: 1 }; contracts[100].no_trade = true;
  const chain = { contracts, price_provider: "alpaca" };
  const batch = api.optionPriceBatch(chain, ["200"], 100, true);
  assert.equal(batch.length, 100);
  assert.equal(batch[0].ticker, "200");
  assert.ok(!batch.some(c => ["99", "100"].includes(c.ticker)));
  assert.equal(api.optionPriceBatch(chain, ["200"], 100, false).length, 1);
  assert.equal(api.optionPriceBatch({ ...chain, price_provider: "massive" }, [], 100, true).length, 1);
  assert.equal(api.optionPriceBatch(chain, [], 100, false).length, 0);
});
