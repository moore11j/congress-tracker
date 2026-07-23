import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const tickerPage = read("app/ticker/[symbol]/page.tsx");
const api = read("lib/api.ts");

test("ticker page uses context bundle for above-the-fold ticker context with old-path fallback", () => {
  assert.match(api, /export type TickerContextBundleResponse = TickerProfile &/);
  assert.match(api, /export async function getTickerContextBundle/);
  assert.match(api, /\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/context-bundle/);
  assert.match(api, /component: "context-bundle"/);
  assert.match(api, /requestSource: params\?\.requestSource \?\? \(typeof window === "undefined" \? "ssr" : "client"\)/);
  assert.match(tickerPage, /getTickerContextBundle\(normalizedSymbol/);
  assert.match(tickerPage, /source: "TickerContextBundle"/);
  assert.match(tickerPage, /requestSource: "ssr"/);
  assert.match(tickerPage, /getTickerProfile\(normalizedSymbol, \{ source: "TickerProfileFallback" \}\)/);
  assert.match(tickerPage, /const loadFreshSignalSummary = \(\) => \{/);
  assert.match(tickerPage, /if \(contextBundle\?\.signals_summary\) return Promise\.resolve\(contextBundle\.signals_summary\)/);
  assert.ok(
    tickerPage.indexOf("if (contextBundle?.signals_summary)") < tickerPage.indexOf("return getTickerSignalsSummary(normalizedSymbol"),
    "ticker page should reuse complete context-bundle summary before fetching a separate signals summary",
  );
  assert.match(tickerPage, /signalSummaryRequest: loadFreshSignalSummary\(\)/);
});
