import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("ticker chart checks hydration status before requesting chart bundle", () => {
  const chart = read("components/ticker/TickerChartLoader.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export async function getTickerHydrationStatus/);
  assert.match(api, /export async function requestTickerHydration/);
  assert.match(chart, /getTickerHydrationStatus\(symbol,/);
  assert.match(chart, /shouldRequestHydration\(status\)/);
  assert.match(chart, /requestedHydrationSymbols/);
  assert.match(chart, /requestTickerHydration\(symbol,/);
  assert.match(chart, /reason: "ticker_page_view"/);
  assert.match(chart, /getTickerChartBundle\(symbol, days,/);
  assert.match(chart, /CHART_HYDRATION_DELAY_MS/);
});

test("ticker context does not eagerly request heavy tab data on overview mount", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /if \(activeTab !== "news"\)/);
  assert.match(card, /if \(activeTab !== "financials"\)/);
  assert.match(card, /if \(activeTab !== "events"\)/);
  assert.match(card, /getTickerNews\(symbol,/);
  assert.match(card, /getTickerFinancials\(symbol,/);
  assert.match(card, /getTickerSecFilings\(symbol,/);
  assert.doesNotMatch(card, /requestTickerHydration\(symbol,/);
});

test("ticker signal activity uses ticker-specific summary instead of broad signals endpoint", () => {
  const client = read("components/ticker/TickerSignalActivityClient.tsx");
  const tickerPage = read("app/ticker/[symbol]/page.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export async function getTickerSignalsSummary/);
  assert.match(api, /\/api\/tickers\/\$\{symbol\}\/signals-summary/);
  assert.match(client, /getTickerSignalsSummary\(symbol,/);
  assert.doesNotMatch(client, /getSignalsAll|\/api\/signals\/all|limit:\s*100/);
  assert.doesNotMatch(tickerPage, /getSignalsAll|\/api\/signals\/all|signalsPromise/);
});

test("ticker government contracts all-mode copy is final no-data copy", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");

  assert.match(tickerPage, /No major government contracts\. No contracts above threshold in selected window\./);
  assert.doesNotMatch(tickerPage, /Government contracts load when the Gov Contracts filter is opened\./);
});

test("ticker tabs settle warming responses into public no-data copy", () => {
  const card = read("components/ticker/TickerContextCard.tsx");
  const api = read("lib/api.ts");

  assert.match(card, /rawStatus === "warming" \|\| rawStatus === "loading" && items\.length === 0 \? "no_data" : rawStatus === "empty" \? "no_data" : rawStatus/);
  assert.match(card, /NEWS_EMPTY_MESSAGE = "No recent news found\."/);
  assert.match(card, /FILINGS_EMPTY_MESSAGE = "No recent filings found\."/);
  assert.match(api, /function normalizeTickerItemsResponse/);
  assert.match(api, /arrayKeys: \["items", "news", "articles", "results", "data"\]/);
  assert.match(api, /arrayKeys: \["items", "press_releases", "pressReleases", "releases", "results", "data"\]/);
  assert.match(api, /arrayKeys: \["items", "filings", "sec_filings", "secFilings", "results", "data"\]/);
  assert.match(card, /status === "loading" \? FINANCIALS_LOADING_MESSAGE : response\.message/);
  assert.doesNotMatch(card, /FMP|provider|cache|402|heavy-route|budget/);
});

test("ticker events tab loads filings and activity independently", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /const PRESS_LOADING_MESSAGE = "Loading press releases\."/);
  assert.match(card, /const ACTIVITY_EMPTY_MESSAGE = "No recent activity found\."/);
  assert.match(card, /getTickerSecFilings\(symbol,/);
  assert.doesNotMatch(card, /from: dateWindow\.from/);
  assert.doesNotMatch(card, /to: dateWindow\.to/);
  assert.match(card, /getEvents\(\{ symbol, recent_days: 365, limit: 50/);
  assert.match(card, /showSecSection/);
  assert.match(card, /<EventsSection title="SEC Filings" meta="Latest available">/);
  assert.match(card, /<EventsSection title="Disclosure Activity" meta="365D">/);
  assert.doesNotMatch(card, /title="Filings \/ Disclosures"/);
});
