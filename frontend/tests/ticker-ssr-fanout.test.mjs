import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("anonymous unknown ticker SSR defers lower-page activity fanout but keeps context bundle", () => {
  const page = read("app/ticker/[symbol]/page.tsx");

  assert.match(page, /import \{ headers \} from "next\/headers"/);
  assert.match(page, /getTickerContextBundle\(normalizedSymbol/);
  assert.match(page, /source: "TickerContextBundle"/);
  assert.match(page, /function shouldDeferAnonymousTickerActivityDetails/);
  assert.match(page, /requestHeaders\.get\("next-router-prefetch"\) === "1"/);
  assert.match(page, /requestHeaders\.get\("x-middleware-prefetch"\) === "1"/);
  assert.match(page, /return !userAgentLooksInteractiveBrowser\(requestHeaders\.get\("user-agent"\)\)/);
  assert.match(page, /const activityDetailsRequested = one\(sp, "activity_details"\) === "1"/);
  assert.match(page, /if \(authToken \|\| hasAuthHint \|\| activityDetailsRequested\) return false/);

  const activityBlock = page.slice(page.indexOf("const activityPromise ="), page.indexOf("return (", page.indexOf("const activityPromise =")));
  const deferIndex = activityBlock.indexOf("if (deferTickerActivityDetails)");
  const eventsIndex = activityBlock.indexOf("getEvents({");
  const contractsIndex = activityBlock.indexOf("getTickerGovernmentContracts(");
  assert.ok(deferIndex >= 0, "ticker activity promise should have a deferred branch");
  assert.ok(eventsIndex > deferIndex, "events fanout should happen only after the deferred branch");
  assert.ok(contractsIndex > deferIndex, "government contract fanout should happen only after the deferred branch");
  assert.match(activityBlock, /signalSummaryRequest: contextBundle\?\.signals_summary \? Promise\.resolve\(contextBundle\.signals_summary\) : undefined/);
});

test("ticker deferred SSR renders section placeholders and hydrates details on visibility", () => {
  const page = read("app/ticker/[symbol]/page.tsx");
  const refresher = read("components/ticker/TickerDeferredActivityRefresh.tsx");

  assert.match(page, /<TickerDeferredActivityRefresh enabled=\{activityDetailsDeferred\} symbol=\{normalizedSymbol\} \/>/);
  assert.match(page, /activityDetailsDeferred \? "Loading Congress activity\." : "No Congress trades in the selected window\."/);
  assert.match(page, /activityDetailsDeferred \? "Loading insider activity\." : "No insider trades in the selected window\."/);
  assert.match(page, /activityDetailsDeferred \? "Loading government contract activity\." : "No government contracts in selected window\."/);
  assert.match(refresher, /"use client"/);
  assert.match(refresher, /IntersectionObserver/);
  assert.match(refresher, /document\.hidden/);
  assert.match(refresher, /url\.searchParams\.set\("activity_details", "1"\)/);
  assert.match(refresher, /router\.replace/);
});

test("ticker API attribution headers remain present for client lazy requests", () => {
  const api = read("lib/api.ts");

  assert.match(api, /headers\.set\("X-Walnut-Request-Source", requestSource\(\)\)/);
  assert.match(api, /headers\.set\("X-Walnut-Panel", panelFromSource\(attribution\.component\)\)/);
  assert.match(api, /headers\.set\("X-Walnut-Route-Family", url \? routeFamilyFromUrl\(url\) : routeFamilyFromPath\(attribution\.route\)\)/);
});
