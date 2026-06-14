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
