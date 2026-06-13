import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("ticker context requests bounded hydration once per symbol", () => {
  const card = read("components/ticker/TickerContextCard.tsx");
  const api = read("lib/api.ts");

  assert.match(api, /export async function requestTickerHydration/);
  assert.match(card, /requestTickerHydration\(symbol,/);
  assert.match(card, /reason: "ticker_page_view"/);
  assert.match(card, /}, \[symbol\]\);/);
});

test("ticker context does not eagerly request heavy tab data on overview mount", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /if \(activeTab !== "news"\)/);
  assert.match(card, /if \(activeTab !== "financials"\)/);
  assert.match(card, /if \(activeTab !== "events"\)/);
  assert.match(card, /getTickerNews\(symbol,/);
  assert.match(card, /getTickerFinancials\(symbol,/);
  assert.match(card, /getTickerSecFilings\(symbol,/);
});

test("ticker tabs settle warming responses into public no-data copy", () => {
  const card = read("components/ticker/TickerContextCard.tsx");

  assert.match(card, /rawStatus === "warming" \? "empty" : rawStatus/);
  assert.match(card, /NEWS_EMPTY_MESSAGE = "No recent news found\."/);
  assert.match(card, /FILINGS_EMPTY_MESSAGE = "No recent filings found\."/);
  assert.match(card, /rawStatus === "warming" \? "Financial data is not available for this ticker yet\."/);
  assert.doesNotMatch(card, /FMP|provider|cache|402|heavy-route|budget/);
});
