import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import { createRequire } from "node:module";
import { PassThrough } from "node:stream";
import ts from "typescript";
import React from "react";
import { renderToPipeableStream } from "react-dom/server";

const require = createRequire(import.meta.url);
const cik = "0001050047";
const slug = `john-bolduc-${cik}`;
const publicSummary = {
  reporting_cik: cik, insider_name: "BOLDUC JOHN", primary_symbol: "WHF", primary_company_name: "WhiteHorse Finance, Inc.",
  primary_role: "Director", role_contexts: [], lookback_days: 90, total_trades: 2, unique_tickers: 1,
  buy_count: 1, sell_count: 1, gross_buy_value: 100, gross_sell_value: 100, net_flow: 0,
  latest_filing_date: "2026-08-01", latest_transaction_date: "2026-07-30",
};
const trade = { symbol: "WHF", trade_type: "Purchase", filing_date: "2026-08-01", transaction_date: "2026-07-30" };

function harness({ summary = publicSummary, trades = [trade], failure, auth = false, env = "production" } = {}) {
  const calls = [];
  class ApiError extends Error { constructor(status) { super(`HTTP ${status}`); this.status = status; } }
  const api = {
    ApiError,
    getInsiderSummary: async (...args) => { calls.push(["summary", args]); if (failure) throw new ApiError(failure); return summary; },
    getInsiderTrades: async (...args) => { calls.push(["trades", args]); return { reporting_cik: cik, lookback_days: 90, total: trades.length, items: trades }; },
    getInsiderAlphaSummary: async () => undefined,
    getSeoSnapshot: async () => { throw new Error("Metadata must not trust a separate snapshot"); },
  };
  const modules = new Map();
  function load(file) {
    if (modules.has(file)) return modules.get(file);
    const exports = {}; modules.set(file, exports);
    const js = ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX } }).outputText;
    vm.runInNewContext(js, {
      exports, console, URL, URLSearchParams, Headers, setTimeout, clearTimeout, process: { env: { VERCEL_ENV: env } },
      require(name) {
        if (name === "@/lib/api") return api;
        if (name === "next/headers") return { headers: async () => new Headers() };
        if (name === "next/navigation") return { notFound: () => { throw new Error("NEXT_NOT_FOUND"); }, redirect: (url) => { throw new Error(`NEXT_REDIRECT:${url}`); } };
        if (name === "next/link") return { default: ({ children, href }) => React.createElement("a", { href }, children) };
        if (name === "@/lib/serverAuth") return {
          requestMayHavePageAuthState: () => auth,
          optionalPageAuthState: async () => ({ token: "PRIVATE_SESSION_SENTINEL", hasAuthHint: true, entitlementHint: "pro" }),
        };
        if (name === "@/lib/wikipediaHeadshot") return { resolveWikipediaHeadshot: async () => null };
        if (name.startsWith("@/components/")) return new Proxy({}, { get: (_, component) =>
          (props) => React.createElement("div", { "data-component": component }, JSON.stringify(props)),
        });
        if (name.startsWith("@/")) return load(`${name.slice(2)}.ts`);
        if (name.startsWith("./lib/")) return load(`${name.slice(2)}.ts`);
        return require(name);
      },
    });
    return exports;
  }
  return { load, calls };
}

const props = (searchParams = {}, routeSlug = slug) => ({ params: Promise.resolve({ slug: routeSlug }), searchParams: Promise.resolve(searchParams) });
async function html(element) {
  return new Promise((resolve, reject) => {
    const output = new PassThrough(); let text = "";
    output.on("data", (chunk) => { text += chunk; }); output.on("end", () => resolve(text));
    const stream = renderToPipeableStream(element, { onAllReady: () => stream.pipe(output), onError: reject });
  });
}
const canonical = `https://app.walnutmarkets.com/insider/${slug}`;

test("substantive clean insider metadata uses the resolved public profile and self canonical", async () => {
  const { load } = harness();
  const metadata = await load("app/insider/[slug]/page.tsx").generateMetadata(props());
  assert.equal(metadata.robots.index, true); assert.equal(metadata.robots.follow, true);
  assert.equal(metadata.alternates.canonical, canonical);
  assert.match(metadata.title, /John Bolduc Insider Trades & SEC Form 4 Activity/);
});

test("all insider query state is noindex with one clean canonical, including repeated and unknown parameters", async () => {
  const { load } = harness(); const page = load("app/insider/[slug]/page.tsx");
  for (const query of [{ lookback: "180", chart: "performance" }, { issuer: "WHF" }, { symbol: "WHF" }, { recent_trades_page: "2" }, { chart: ["stock", "performance"] }, { other_filter: "value" }]) {
    const meta = await page.generateMetadata(props(query));
    assert.equal(meta.robots.index, false); assert.equal(meta.robots.follow, true);
    assert.equal(meta.alternates.canonical, canonical);
  }
  assert.equal((await page.generateMetadata(props({ utm_source: "google" }))).robots.index, true);
});

test("thin, unresolved, mismatched and unavailable profiles cannot inherit indexability", async () => {
  for (const options of [
    { summary: { ...publicSummary, total_trades: 0, unique_tickers: 0, latest_filing_date: null, latest_transaction_date: null }, trades: [] },
    { summary: { ...publicSummary, insider_name: "Unknown Insider" } },
    { summary: { ...publicSummary, reporting_cik: "0000000001" } },
    { summary: { ...publicSummary, status: "loading" } }, { failure: 503 },
  ]) {
    const metadata = await harness(options).load("app/insider/[slug]/page.tsx").generateMetadata(props());
    assert.equal(metadata.robots.index, false); assert.equal(metadata.robots.follow, true);
    assert.equal(metadata.alternates.canonical, canonical);
  }
});

test("invalid routes and genuine API 404s use missing handling while outages remain unavailable", async () => {
  await assert.rejects(harness().load("app/insider/[slug]/page.tsx").generateMetadata(props({}, "not-a-cik")), /NEXT_NOT_FOUND/);
  await assert.rejects(harness({ failure: 404 }).load("app/insider/[slug]/page.tsx").generateMetadata(props()), /NEXT_NOT_FOUND/);
  await assert.rejects(harness().load("app/insider/[slug]/page.tsx").default(props({}, "not-a-cik")), /NEXT_NOT_FOUND/);
  await assert.rejects(harness({ failure: 404 }).load("app/insider/[slug]/page.tsx").default(props()), /NEXT_NOT_FOUND/);
  const body = await html(await harness({ failure: 503 }).load("app/insider/[slug]/page.tsx").default(props()));
  assert.match(body, /Insider profile unavailable/);
  assert.doesNotMatch(body, /John Bolduc|Verified|total_trades|initialAlphaSummary/);
});

test("canonical alias redirects retain the CIK and every query value without injecting chart state", async () => {
  const page = harness().load("app/insider/[slug]/page.tsx");
  await assert.rejects(page.default(props({ lookback: "180", chart: "performance" }, `bolduc-john-${cik}`)),
    new RegExp(`NEXT_REDIRECT:/insider/${slug}\\?lookback=180&chart=performance`));
  await assert.rejects(page.default(props({}, cik)), new RegExp(`NEXT_REDIRECT:/insider/${slug}$`));
});

test("anonymous and authenticated profile HTML contain public data without session or private context", async () => {
  for (const auth of [false, true]) {
    const { load, calls } = harness({ auth });
    const body = await html(await load("app/insider/[slug]/page.tsx").default(props()));
    assert.match(body.replace(/<!--[\s\S]*?-->/g, ""), /John Bolduc Insider Activity/); assert.match(body, /WHF/);
    assert.doesNotMatch(body, /PRIVATE_SESSION_SENTINEL|entitlementHint|authToken/);
    for (const [kind, args] of calls) {
      const options = args[kind === "summary" ? 3 : 4];
      assert.equal(options.authToken, undefined); assert.equal(options.headers, undefined);
      assert.equal(options.stalePageCache, !auth);
    }
  }
  const locked = harness({ summary: { ...publicSummary, locked: true, private_context: "PRIVATE_GATED_SENTINEL" } });
  const body = await html(await locked.load("app/insider/[slug]/page.tsx").default(props()));
  assert.match(body, /Insider profile unavailable/);
  assert.doesNotMatch(body, /PRIVATE_GATED_SENTINEL|initialSummary/);
});

test("production allows insider crawl including parameters; preview robots remain blocked", async () => {
  const { NextRequest } = require("next/server");
  const request = (path) => new NextRequest(`https://app.walnutmarkets.com${path}`, { headers: { host: "app.walnutmarkets.com", "user-agent": "Googlebot" } });
  const middleware = harness().load("middleware.ts").middleware;
  const body = await (await middleware(request("/robots.txt"))).text();
  assert.match(body, /Allow: \//); assert.doesNotMatch(body, /Disallow:.*insider|Disallow:.*\?/);
  assert.equal((await middleware(request(`/insider/${slug}`))).headers.get("x-robots-tag"), null);
  assert.equal((await middleware(request(`/insider/${slug}?lookback=180&chart=performance`))).headers.get("x-robots-tag"), "noindex, follow");
  const preview = await harness({ env: "preview" }).load("middleware.ts").middleware(request("/robots.txt"));
  assert.equal(await preview.text(), "User-agent: *\nDisallow: /\n");
});

test("insider sitemap filters thin, malformed and duplicate candidates and reconstructs identity-safe paths", () => {
  const { insiderSitemapPages } = harness().load("lib/insiderSeo.ts");
  const candidate = { indexable: true, entity_type: "insider", entity_key: cik, canonical_path: "/insider/wrong-0000000001?chart=x", data_as_of: "2026-08-01", payload: { reporting_cik: cik, insider_name: "BOLDUC JOHN", recent_activity: [{ ...trade, transaction_type: "P" }] } };
  const pages = insiderSitemapPages([candidate, candidate, { ...candidate, indexable: false }, { ...candidate, payload: { ...candidate.payload, recent_activity: [null] } }, { ...candidate, payload: { ...candidate.payload, locked: true } }, { ...candidate, payload: { ...candidate.payload, recent_activity: [] } }, { ...candidate, payload: { ...candidate.payload, reporting_cik: "0000000001" } }]);
  assert.equal(pages.length, 1); assert.equal(pages[0].path, `/insider/${slug}`);
});
