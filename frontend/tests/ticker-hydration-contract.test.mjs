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

  assert.match(card, /rawStatus === "warming" \? "empty" : rawStatus/);
  assert.match(card, /NEWS_EMPTY_MESSAGE = "No recent news found\."/);
  assert.match(card, /FILINGS_EMPTY_MESSAGE = "No recent filings found\."/);
  assert.match(card, /rawStatus === "warming" \? "Financial data is not available for this ticker yet\."/);
  assert.doesNotMatch(card, /FMP|provider|cache|402|heavy-route|budget/);
});
