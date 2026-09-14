import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import {createRequire} from "node:module";
import test from "node:test";
import ts from "typescript";
import React from "react";
import {renderToStaticMarkup} from "react-dom/server";

const require = createRequire(import.meta.url);
function modules({fetch, consent = true, events = []} = {}) {
  const cache = new Map();
  function load(file) {
    if (cache.has(file)) return cache.get(file);
    const exports = {};
    cache.set(file, exports);
    const js = ts.transpileModule(fs.readFileSync(path.resolve(file), "utf8"), {
      compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX},
    }).outputText;
    vm.runInNewContext(js, {exports, process: {env: {}}, URL, URLSearchParams, Date, Intl, AbortController, setTimeout, clearTimeout,
      window: {location: {pathname: "/"}}, fetch,
      require(name) {
        if (name === "@/lib/api") return {API_BASE: "https://api.test"};
        if (name === "@/lib/googleAnalytics") return {recordGoogleAnalyticsEvent: (...args) => events.push(args)};
        if (name === "@/lib/privacyConsent") return {hasPrivacyConsent: () => consent};
        if (name === "@/components/landing/LandingSearch") return {LandingSearch: () => React.createElement("input", {"aria-label": "Ticker search"})};
        if (name === "@/components/landing/MarketingHeader") return {MarketingHeader: () => React.createElement("nav", null, "Navigation")};
        if (name.startsWith("@/")) {
          const base = name.slice(2);
          return load(fs.existsSync(`${base}.ts`) ? `${base}.ts` : `${base}.tsx`);
        }
        return require(name);
      },
    });
    return exports;
  }
  return load;
}
const model = modules()("lib/homepagePreview.ts");
const at = "2026-09-12T01:00:00Z";
const ranking = {top_stocks: {generated_at: at, filter_items: {secret: ["PRO_ONLY"]}, items: ["TSM", "AMZN", "BWFG", "SECRET4", "SECRET5"].map((symbol, i) => ({
  rank: i + 1, symbol, company_name: symbol, confirmation_score: 98.7654, key_drivers: ["Congress"], updated_at: at, premium_metric: "PRO_ONLY",
}))}};
function context(symbol, risks = true) {
  const evidence = (category, title) => ({category, title, description: `${title} details`});
  return {symbol, generated_at: at,
    source_entitlements: {fundamentals: {available: true, locked: false}, price_volume: {available: true, locked: false},
      institutional_activity: {available: true, locked: false}, analysts: {available: true, locked: false}},
    source_cards: {fundamentals: {as_of: "2025-12-31", metrics: {revenue_growth: {value: 0.12, display: "12%"}}}, price_volume: {latest_date: "2026-09-11"}},
    decision_layer: {symbol, confirmation: {score: 98.7654, history: ["SECRET_HISTORY"]},
      catalysts: [evidence("fundamentals", "Supportive fundamentals"), evidence("institutional_activity", "PRO_ONLY"), evidence("analysts", "PREMIUM_CONSENSUS")],
      risks: risks ? [evidence("price_volume", "Weak price confirmation")] : [],
      watch_items: [evidence("price_volume", "Monitor prices"), evidence("institutional_activity", "PRO_ONLY")],
    },
    similar_historical_setups: {return: "SECRET_RETURN"},
  };
}

test("public projection caps at three and omits scores, extra rows, filters and unknown fields", () => {
  const result = model.publicHomepageRanking(ranking);
  assert.equal(result.items.length, 3);
  assert.equal(result.items[1].symbol, "AMZN");
  assert.doesNotMatch(JSON.stringify(result), /98\.7654|PRO_ONLY|SECRET4|SECRET5|filter_items|confirmation_score/);
  const invalid = structuredClone(ranking);
  invalid.top_stocks.items[0].symbol = "<script>";
  assert.equal(model.publicHomepageRanking(invalid).items.length, 2);
  assert.deepEqual(JSON.parse(JSON.stringify(model.publicHomepageRanking(null))), {items: [], generatedAt: null});
});

test("research selects a real top-three company with both supporting and conflicting evidence", () => {
  const stocks = model.publicHomepageRanking(ranking).items;
  const examples = stocks.slice(0, 2).map((stock, index) => model.publicHomepageResearch(stock, context(stock.symbol, index === 1)));
  const selected = model.selectHomepageResearch(examples);
  assert.equal(selected.stock.symbol, "AMZN");
  assert.equal(selected.supporting[0].dataAsOf, "2025-12-31");
  assert.equal(selected.risks[0].dataAsOf, "2026-09-11");
  assert.equal(selected.supporting[0].details[0], "Revenue growth: 12%");
  assert.doesNotMatch(JSON.stringify(selected), /PRO_ONLY|PREMIUM_CONSENSUS|SECRET_HISTORY|SECRET_RETURN|98\.7654|confirmation_score/);
});

test("missing entitlements, locked sources, mismatched tickers and missing dates fail closed", () => {
  const stock = model.publicHomepageRanking(ranking).items[0];
  for (const patch of [{source_entitlements: {}}, {source_entitlements: {fundamentals: {available: true, locked: true}}}, {symbol: "WRONG"}, {generated_at: "invalid"}]) {
    assert.equal(model.publicHomepageResearch(stock, {...context(stock.symbol), ...patch}), null);
  }
  assert.equal(model.selectHomepageResearch([null]), null);
});

test("rendered homepage has eight workflow sections and serializes no paid metrics or fourth stock", async () => {
  const calls = [];
  const load = modules({fetch: async (url, options) => {
    calls.push({url: String(url), options});
    const pathname = new URL(url).pathname;
    if (pathname === "/api/leaderboards/preview") return {ok: true, json: async () => ranking};
    if (pathname === "/api/plan-config") return {ok: true, json: async () => ({plan_prices: []})};
    const symbol = pathname.split("/")[3];
    return {ok: true, json: async () => context(symbol, symbol === "AMZN")};
  }});
  const html = renderToStaticMarkup(await load("app/landing/page.tsx").default());
  assert.match(html, /Build Your Next Winning Portfolio/);
  assert.equal((html.match(/<section/g) || []).length, 8);
  assert.equal((html.match(/data-homepage-ranked-stock=/g) || []).length, 3);
  assert.match(html, /Why is AMZN near the top/);
  assert.match(html, /Supportive fundamentals/);
  assert.match(html, /Weak price confirmation/);
  assert.match(html, /https:\/\/app\.walnutmarkets\.com\/ticker\/AMZN/);
  assert.doesNotMatch(html, /SECRET|PRO_ONLY|PREMIUM_CONSENSUS|98\.7654|recalculat|audit|Research Memory|founder|Jarod/i);
  const sequence = ["data-walnut-homepage", 'id="top-stock-opportunities"', 'id="research-example"', 'id="whats-working"', 'id="monitoring"', 'id="confirmation-score"', 'id="pricing"', 'id="homepage-faq"'].map(value => html.indexOf(value));
  assert.ok(sequence.every((value, i) => value >= 0 && (i === 0 || value > sequence[i - 1])));
  const hero = html.slice(html.indexOf("data-walnut-homepage"), html.indexOf('id="top-stock-opportunities"'));
  assert.equal((hero.match(/bg-emerald-300 /g) || []).length, 1);
  assert.match(hero, />Open Screener<\/a>/);
  assert.match(hero, /View Leaderboards/);
  assert.match(hero, /Explore Strategies/);
  assert.ok(calls.every(call => !/\/top-stocks|\/strategies/.test(call.url)));
  assert.ok(calls.every(call => !call.options.headers.Cookie && !call.options.headers.Authorization));
});

test("source outages keep the working navigation and never fabricate an example", async () => {
  const load = modules({fetch: async () => {throw Error("Unavailable");}});
  const html = renderToStaticMarkup(await load("app/landing/page.tsx").default());
  assert.match(html, /ranked preview is unavailable/);
  assert.match(html, /source-backed example is not available/);
  assert.doesNotMatch(html, /data-homepage-ranked-stock=/);
  assert.match(html, />Open Screener<\/a>/);
});

test("CTA analytics preserve event names and consent without blocking navigation", () => {
  for (const consent of [false, true]) {
    const events = [];
    const {HomepageCtaLink} = modules({consent, events})("components/landing/HomepageCtaLink.tsx");
    for (const eventName of ["open_screener_click", "see_top_performers_click", "top_stocks_click", "analyze_stock_click", "strategy_click", "insider_profile_click"]) {
      const anchor = HomepageCtaLink({href: "/ticker/AMZN", eventName, children: "Research"});
      assert.equal(anchor.props.href, "/ticker/AMZN");
      anchor.props.onClick({defaultPrevented: false});
      if (consent) assert.equal(events.at(-1)[0], eventName);
    }
    assert.equal(events.length, consent ? 6 : 0);
  }
});
