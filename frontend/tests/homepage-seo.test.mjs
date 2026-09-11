import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
function load(file, env = { ...process.env, VERCEL_ENV: "production" }) {
  const exports = {};
  const source = fs.readFileSync(file, "utf8");
  const js = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  vm.runInNewContext(js, { exports, process: { env }, URL, URLSearchParams, Headers, console, require: (name) => {
    if (name.startsWith("./lib/")) return load(`${name.slice(2)}.ts`);
    return require(name);
  } });
  return exports;
}
const seo = load("lib/tickerSeo.ts");
const content = load("lib/homepageContent.ts").homepageContent;

test("homepage metadata fits snippets and keeps the requested positioning", () => {
  assert.equal(content.hero.title, "Find Top-Ranked Stocks. See Who Actually Outperformed.");
  assert.equal(content.metadata.title, "Stock Analysis, Congress Trades & Insider Data | Walnut Markets");
  assert.ok(content.metadata.title.length <= 65);
  assert.ok(content.metadata.description.length <= 165);
  assert.match(content.hero.description, /historically/);
  assert.match(content.hero.description, /backtested/);
});

test("ticker fallback rejects unavailable, mismatched and undated public snapshots", () => {
  const snapshot = {
    entity_type: "ticker", indexable: true,
    data_as_of: "2026-09-01T00:00:00Z",
    payload: { symbol: "AAPL", company_name: "Apple Inc.", sections: [{ heading: "Public data", body: "Test fixture" }] },
  };
  assert.equal(seo.usablePublicTickerSnapshot(snapshot, "AAPL"), true);
  for (const invalid of [null, { ...snapshot, indexable: false }, { ...snapshot, data_as_of: null }, { ...snapshot, data_as_of: "invalid" }, { ...snapshot, payload: { ...snapshot.payload, sections: [] } }]) {
    assert.equal(seo.usablePublicTickerSnapshot(invalid, "AAPL"), false);
  }
  assert.equal(seo.usablePublicTickerSnapshot(snapshot, "MSFT"), false);
  assert.equal(seo.hasResolvedTickerProfile({ ticker: { symbol: "AAPL", identity_status: "loading" } }), false);
  assert.equal(seo.hasResolvedTickerProfile(null), false);
});

test("ticker metadata distinguishes companies and avoids unavailable dataset claims in snapshot mode", () => {
  assert.match(seo.tickerSeoDescription("AAPL", "Apple Inc."), /Apple Inc\. \(AAPL\)/);
  assert.notEqual(seo.tickerSeoTitle("AAPL"), seo.tickerSeoTitle("MSFT"));
  assert.match(seo.tickerSeoDescription("AAPL", "Apple Inc.", true), /temporarily unavailable/);
  assert.doesNotMatch(seo.tickerSeoDescription("AAPL", "Apple Inc.", true), /fundamentals|technicals|institutional holdings/);
});

test("robots and redirects preserve production indexing while excluding previews", async () => {
  const { middleware } = load("middleware.ts");
  const { NextRequest } = require("next/server");
  const request = (host, path) => new NextRequest(`https://${host}${path}`, { headers: { host } });
  const robots = await middleware(request("walnutmarkets.com", "/robots.txt"));
  assert.match(await robots.text(), /Allow: \/\n/);
  const preview = await middleware(request("walnut-preview.vercel.app", "/robots.txt"));
  assert.equal(await preview.text(), "User-agent: *\nDisallow: /\n");
  const local = await middleware(request("localhost", "/landing"));
  assert.equal(local.headers.get("x-robots-tag"), "noindex, nofollow");
  const homepage = await middleware(request("walnutmarkets.com", "/"));
  assert.equal(homepage.headers.get("x-robots-tag"), null);
  const previewMiddleware = load("middleware.ts", { VERCEL_ENV: "preview" }).middleware;
  const previewDeployment = await previewMiddleware(request("walnutmarkets.com", "/robots.txt"));
  assert.equal(await previewDeployment.text(), "User-agent: *\nDisallow: /\n");
  assert.equal(previewDeployment.headers.get("x-robots-tag"), "noindex, nofollow");
  const alias = await middleware(request("walnutmarkets.com", "/landing?utm_source=test"));
  assert.equal(alias.status, 308);
  assert.equal(alias.headers.get("location"), "https://walnutmarkets.com/?utm_source=test");
  const sitemap = fs.readFileSync("public/sitemap.xml", "utf8");
  assert.doesNotMatch(sitemap, /<loc>[^<]*\/(leaderboards|watchlists|screener|monitoring)<\/loc>/);
});
