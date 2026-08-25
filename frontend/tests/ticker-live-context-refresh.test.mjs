import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("ticker page uses a bounded server budget and background live refresh", () => {
  const page = readFileSync(join(process.cwd(), "app/ticker/[symbol]/page.tsx"), "utf8");
  const refresh = readFileSync(join(process.cwd(), "components/ticker/TickerLiveContextRefresh.tsx"), "utf8");

  assert.match(page, /const TICKER_CONTEXT_SSR_TIMEOUT_MS = 2500/);
  assert.match(page, /withinTickerLoadBudget\(getTickerContextBundle/);
  assert.match(page, /TickerLiveContextRefresh[\s\S]*enabled=\{Boolean\(shellFallbackMessage\) \|\| !showTickerName\}/);
  assert.match(refresh, /activeUser: true/);
  assert.match(refresh, /requestTickerHydration/);
  assert.match(refresh, /live: true/);
  assert.doesNotMatch(refresh, /Promise\.allSettled/);
  assert.match(refresh, /getTickerContextBundle\(symbol/);
  assert.match(refresh, /router\.refresh\(\)/);
});
