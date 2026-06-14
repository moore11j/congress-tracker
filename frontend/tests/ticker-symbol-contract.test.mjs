import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("frontend ticker API calls normalize and encode route symbols", () => {
  const api = read("lib/api.ts");
  const ticker = read("lib/ticker.ts");

  assert.match(ticker, /function normalizeTickerSymbol/);
  assert.match(ticker, /\["\[SYMBOL\]", "SYMBOL", "UNKNOWN", "NULL", "NONE"\]/);
  assert.match(api, /import \{ normalizeTickerSymbol \} from "@\/lib\/ticker";/);
  assert.match(api, /function tickerPathSymbol\(symbol: string\)/);
  assert.match(api, /encodeURIComponent\(normalizeTickerSymbol\(symbol\) \?\? symbol\.trim\(\)\)/);
  assert.match(api, /\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/news/);
  assert.match(api, /\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/financials/);
  assert.match(api, /\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/chart-bundle/);
  assert.match(api, /\.map\(\(symbol\) => normalizeTickerSymbol\(symbol\)\)/);
  assert.doesNotMatch(api, /\.map\(\(symbol\) => symbol\.trim\(\)\.toUpperCase\(\)\)/);
});

test("ticker page uses base event rows and filters missing header metadata", () => {
  const page = read("app/ticker/[symbol]/page.tsx");

  assert.match(page, /enrich_prices:\s*0/);
  assert.match(page, /return \[ticker\.sector, ticker\.industry, ticker\.country, ticker\.exchange\]/);
  assert.match(page, /\.filter\(\(value\): value is string => Boolean\(value\)\)/);
  assert.match(page, /headerMetadata\.join\(" \/ "\)/);
});
