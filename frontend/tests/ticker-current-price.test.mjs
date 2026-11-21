import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);
function moduleAt(file, imports = {}) {
  const code = ts.transpileModule(readFileSync(file, "utf8"), { compilerOptions: {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
  }}).outputText;
  const exports = {};
  vm.runInNewContext(code, { exports, require: name => imports[name] ?? require(name), Intl, Date,
    AbortController, setTimeout, clearTimeout, setInterval, clearInterval, ...imports.globals });
  return exports;
}
const helpers = moduleAt("lib/tickerHeaderPrice.ts");
const daily = { price: 96.92, date: "2026-11-19", changePercent: -9.39, relativeVolume: 1.38 };
// Retained public response from Nov20 22:53UTC; no transport is performed by tests.
const quote = { symbol: "NBIS", current_price: 84.5942, day_change_pct: -12.71749896822122,
  as_of: "2026-11-20T22:33:00+00:00", is_delayed: true, delay_minutes: 15,
  price_kind: "minute_close", is_stale: false };
const now = Date.parse("2026-11-20T22:55:00Z");
const shown = (q = quote, d = daily, failed = false) => helpers.tickerHeaderPrice("NBIS", q, d, failed, now);

test("today's delayed quote replaces yesterday's completed close without changing daily inputs", () => {
  const before = JSON.stringify(daily);
  const result = shown();
  assert.equal(result.price, 84.5942);
  assert.equal(result.changePercent, quote.day_change_pct);
  assert.equal(result.label, "Delayed quote");
  assert.equal(result.delay, "Delayed 15 minutes");
  assert.match(result.observed, /Nov 20, 2026.*5:33 PM EST/);
  assert.equal(JSON.stringify(daily), before);
});

test("rendered mobile header uses current price and separates daily volume date", () => {
  const { TickerPriceHeader } = moduleAt("components/ticker/TickerPriceHeader.tsx", {
    "@/lib/api": {}, "@/lib/tickerHeaderPrice": helpers,
  });
  const html = renderToStaticMarkup(React.createElement(TickerPriceHeader, { symbol: "NBIS", daily,
    initialQuote: { ...quote, change_percent: quote.day_change_pct, stale: false } }));
  assert.match(html, /84\.59/);
  assert.match(html, /-12\.72%/);
  assert.match(html, /Nov 20, 2026/);
  assert.match(html, /Delayed 15 minutes/);
  assert.match(html, /Daily volume vs 30D avg/);
  assert.match(html, /Nov 19, 2026/);
  assert.doesNotMatch(html, /96\.92|Latest available price/);
});

for (const [field, value] of [["symbol", "SPY"], ["current_price", 0], ["current_price", -1],
  ["current_price", NaN], ["current_price", Infinity], ["current_price", "84.59"],
  ["as_of", null], ["as_of", "invalid"], ["as_of", "2026-11-20T22:33:00"],
  ["as_of", "2026-11-21T22:33:00Z"], ["as_of", "2026-02-30T22:33:00Z"],
  ["as_of", "2026-11-18T22:33:00Z"]]) {
  test(`invalid/older quote ${field}=${String(value)} keeps explicitly dated daily fallback`, () => {
    const result = shown({ ...quote, [field]: value });
    assert.equal(result.price, 96.92);
    assert.equal(result.label, "Daily close");
    assert.equal(result.delay, null);
    assert.equal(result.observed, "Daily close as of Nov 19, 2026");
  });
}

test("missing and stale data never claim live/current refresh", () => {
  const empty = shown(null, {});
  assert.equal(empty.label, "Price unavailable");
  assert.equal(empty.price, null);
  for (const result of [shown({ ...quote, is_stale: true }), shown(quote, daily, true)]) {
    assert.equal(result.price, quote.current_price);
    assert.equal(result.label, "Last available quote");
    assert.equal(result.changePercent, null);
    assert.equal(result.stale, true);
  }
});

test("zero change, unknown delay and daily quote metadata retain their meaning", () => {
  assert.equal(shown({ ...quote, day_change_pct: 0 }).changePercent, 0);
  assert.equal(shown({ ...quote, day_change_pct: Infinity }).changePercent, null);
  assert.equal(shown({ ...quote, delay_minutes: null }).delay, "Delayed quote");
  assert.equal(shown({ ...quote, is_delayed: false }).delay, null);
  const result = shown({ ...quote, current_price: 96.92, price_kind: "daily_close", as_of: "2026-11-19T00:00:00Z", is_delayed: false });
  assert.equal(result.label, "Daily close");
  assert.equal(result.observed, "Daily close as of Nov 19, 2026");
});

for (const value of [null, "bad", "2026-02-30", "2026-11-19T00:00:00Z"]) {
  test(`invalid daily session ${value} cannot get a fabricated date/price`, () => {
    assert.equal(helpers.formatPriceSession(value), null);
    assert.equal(shown(null, { ...daily, date: value }).price, null);
  });
}

test("public quote request bypasses page cache, encodes scope and preserves cancellation", async () => {
  const source = readFileSync("lib/api.ts", "utf8");
  const ast = ts.createSourceFile("api.ts", source, ts.ScriptTarget.Latest, true);
  const declaration = ast.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === "getTickerMarketQuote");
  const code = ts.transpileModule(declaration.getText(ast).replace(/^export /, ""), {
    compilerOptions: { target: ts.ScriptTarget.ES2022 },
  }).outputText;
  const calls = [];
  let payload = { items: [{ ...quote, symbol: "SPY" }, quote] };
  const context = vm.createContext({ buildApiUrl: path => path,
    fetchPublicJson: async (url, init) => { calls.push({ url, init }); return payload; } });
  vm.runInContext(code, context);
  const signal = new AbortController().signal;
  assert.equal((await context.getTickerMarketQuote("NBIS", signal)).symbol, "NBIS");
  assert.equal(calls[0].url, "/api/market/quotes?symbols=NBIS");
  assert.equal(calls[0].init.cache, "no-store");
  assert.equal(calls[0].init.signal, signal);
  await context.getTickerMarketQuote("A&B");
  assert.match(calls[1].url, /A%26B$/);
  payload = { items: [{ ...quote, symbol: "SPY" }] };
  assert.equal(await context.getTickerMarketQuote("NBIS"), null);
  payload = { items: {} };
  assert.equal(await context.getTickerMarketQuote("NBIS"), null);
});

function mount() {
  let state, effect;
  const calls = [], intervals = [], deadlines = [], listeners = new Map();
  const document = { hidden: false, addEventListener: (name, fn) => listeners.set(name, fn),
    removeEventListener: name => listeners.delete(name) };
  const react = { ...React, useState: initial => {
    state = initial(); return [state, value => { state = typeof value === "function" ? value(state) : value; }];
  }, useEffect: fn => { effect = fn; } };
  const { TickerPriceHeader } = moduleAt("components/ticker/TickerPriceHeader.tsx", {
    react, "@/lib/tickerHeaderPrice": helpers, "@/lib/api": { getTickerMarketQuote: (symbol, signal) => new Promise((resolve, reject) => {
      calls.push({ symbol, signal, resolve, reject });
    }) }, globals: { document,
      setInterval: (fn, ms) => { intervals.push({ fn, ms }); return 1; }, clearInterval: () => {},
      setTimeout: (fn, ms) => { deadlines.push({ fn, ms }); return 2; }, clearTimeout: () => {},
    },
  });
  TickerPriceHeader({ symbol: "NBIS", daily });
  const unmount = effect();
  return { calls, intervals, deadlines, document, listeners, unmount, state: () => state };
}
const settle = async () => { await Promise.resolve(); await Promise.resolve(); };

test("quote refresh is bounded, coalesced, paused when hidden and resumed on visibility", async () => {
  const view = mount();
  assert.equal(view.calls.length, 1);
  assert.equal(view.intervals[0].ms, 60_000);
  assert.equal(view.deadlines[0].ms, 10_000);
  view.intervals[0].fn();
  assert.equal(view.calls.length, 1);
  view.calls[0].resolve(quote); await settle();
  assert.equal(view.state().quote.current_price, 84.5942);
  view.document.hidden = true; view.intervals[0].fn();
  assert.equal(view.calls.length, 1);
  view.document.hidden = false; view.listeners.get("visibilitychange")();
  assert.equal(view.calls.length, 2);
  view.calls[1].reject(new Error("offline")); await settle();
  assert.equal(view.state().failed, true);
  assert.equal(view.state().quote.current_price, 84.5942);
  view.unmount(); assert.equal(view.listeners.size, 0);
});

test("timeout/unmount abort and late response cannot update an unmounted ticker", async () => {
  const view = mount();
  view.deadlines[0].fn();
  assert.equal(view.calls[0].signal.aborted, true);
  view.unmount();
  view.calls[0].resolve(quote); await settle();
  assert.equal(view.state().quote, null);
});
