import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import ts from "typescript";
import { NextRequest, NextResponse } from "next/server.js";

function load(file, modules, globals = {}) {
  const exports = {};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(file, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText, { exports, require: (name) => modules[name], AbortController, Headers, URL, URLSearchParams, console: { info() {} }, process: { env: {} }, ...globals });
  return exports;
}

test("cold Expedia cache cannot block Safari, Android, desktop, or client navigation", async () => {
  let probes = 0;
  const { middleware } = load("middleware.ts", {
    "next/server": { NextRequest, NextResponse },
    "./lib/memberSlug": {}, "./lib/seoQuality": {}, "./lib/departments": {},
    "./lib/publicTickerReadiness": {
      publicTickerReady: async () => { probes++; return false; },
      unavailableTickerResponse: () => new Response("Unavailable", { status: 503 }),
    },
  });
  for (const userAgent of [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 Version/18.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 Chrome/140.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/140.0 Safari/537.36",
  ]) {
    for (const navigation of [{}, { RSC: "1" }, { "next-router-prefetch": "1" }]) {
      const response = await middleware(new NextRequest("https://app.walnutmarkets.com/ticker/EXPE", {
        headers: { host: "app.walnutmarkets.com", "user-agent": userAgent, ...navigation },
      }));
      assert.equal(response.status, 200);
      assert.equal(response.headers.get("x-middleware-next"), "1");
    }
  }
  assert.equal(probes, 0);
  const crawler = await middleware(new NextRequest("https://app.walnutmarkets.com/ticker/EXPE", {
    headers: { host: "app.walnutmarkets.com", "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1)" },
  }));
  assert.equal(crawler.status, 503);
  assert.equal(probes, 1);
});

function refreshHarness(fetcher) {
  let effect;
  const timers = new Map();
  let nextTimer = 0;
  const states = [];
  const refs = [];
  let cursor = 0;
  let refreshes = 0;
  const router = { refresh: () => refreshes++ };
  const jsx = (type, props) => ({ type, props });
  const { TickerLiveContextRefresh } = load("components/ticker/TickerLiveContextRefresh.tsx", {
    react: {
      useEffect: (fn) => { effect = fn; },
      useRef: (value) => { const i = cursor++; return refs[i] ??= { current: value }; },
      useState: (value) => { const i = cursor++; if (!(i in states)) states[i] = value; return [states[i], next => { states[i] = typeof next === "function" ? next(states[i]) : next; }]; },
    },
    "react/jsx-runtime": { jsx, jsxs: jsx },
    "next/navigation": { useRouter: () => router },
    "@/lib/api": { getTickerContextBundle: fetcher, requestTickerHydration: async () => ({}) },
  }, {
    window: { setTimeout: (fn, ms) => { const id = ++nextTimer; timers.set(id, { fn, ms }); return id; }, clearTimeout: id => timers.delete(id) },
  });
  return {
    render() { cursor = 0; return TickerLiveContextRefresh({ enabled: true, symbol: "EXPE", side: "all", lookbackDays: 365 }); },
    start: () => effect(),
    tick(ms) { const entry = [...timers].find(([, timer]) => timer.ms === ms); assert.ok(entry, `timer ${ms} exists`); timers.delete(entry[0]); entry[1].fn(); },
    get refreshes() { return refreshes; },
  };
}
const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
const bundle = { ticker: { symbol: "EXPE", identity_status: "resolved" } };

test("ticker silently retries transient failures then refreshes once", async () => {
  let calls = 0;
  const h = refreshHarness(async () => { if (++calls === 1) throw new Error("503"); return bundle; });
  assert.equal(h.render(), null);
  const cleanup = h.start();
  await flush();
  assert.equal(h.render(), null);
  h.tick(1000);
  await flush();
  assert.equal(calls, 2);
  assert.equal(h.refreshes, 1);
  cleanup();
  h.render(); h.start(); await flush();
  assert.equal(calls, 2, "server refresh must not create a request loop");
});

test("cancelled mount cannot poison a later ticker load", async () => {
  let calls = 0;
  const h = refreshHarness((symbol, { signal }) => {
    calls++;
    if (calls > 1) return Promise.resolve(bundle);
    return new Promise((resolve, reject) => signal.addEventListener("abort", () => reject(new Error("aborted"))));
  });
  h.render(); const cleanup = h.start(); cleanup(); await flush();
  h.render(); const cleanup2 = h.start(); await flush();
  assert.equal(calls, 2); assert.equal(h.refreshes, 1);
  cleanup2();
});

test("persistent failure is bounded and offers a working manual retry", async () => {
  let calls = 0;
  const h = refreshHarness(async () => { calls++; if (calls <= 3) throw new Error("offline"); return bundle; });
  h.render(); const cleanup = h.start(); await flush();
  h.tick(1000); await flush(); h.tick(2000); await flush();
  const output = h.render();
  assert.equal(calls, 3); assert.equal(output.props.role, "status");
  const button = output.props.children.find(child => child?.type === "button");
  button.props.onClick(); cleanup(); h.render(); const cleanup2 = h.start(); await flush();
  assert.equal(calls, 4); assert.equal(h.refreshes, 1); assert.equal(h.render(), null);
  cleanup2();
});

test("ticker renders partial data without the premature page-wide warning", () => {
  const page = fs.readFileSync("app/ticker/[symbol]/page.tsx", "utf8");
  assert.doesNotMatch(page, /\{shellFallbackMessage\s*\?\s*\(/);
  assert.match(page, /!hasResolvedTickerProfile\(profile\) && !userAgentLooksInteractiveBrowser/);
});
