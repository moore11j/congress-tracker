import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();

function read(path) {
  return readFileSync(join(root, path), "utf8");
}

test("anonymous unknown ticker SSR uses a lightweight shell before context bundle fanout", () => {
  const page = read("app/ticker/[symbol]/page.tsx");

  assert.match(page, /import \{ headers \} from "next\/headers"/);
  assert.match(page, /getTickerContextBundle\(normalizedSymbol/);
  assert.match(page, /source: "TickerContextBundle"/);
  assert.match(page, /function shouldUseAnonymousTickerSsrShell/);
  assert.match(page, /const useAnonymousTickerSsrShell = shouldUseAnonymousTickerSsrShell/);
  assert.match(page, /const activeTickerSsrRequest = !useAnonymousTickerSsrShell/);
  assert.match(page, /useAnonymousTickerSsrShell\s*\?\s*\{/);
  assert.match(page, /profile: fallbackTickerProfile\(normalizedSymbol\)/);
  assert.match(page, /function shouldDeferAnonymousTickerActivityDetails/);
  assert.match(page, /requestHeaders\.get\("next-router-prefetch"\) === "1"/);
  assert.match(page, /requestHeaders\.get\("x-middleware-prefetch"\) === "1"/);
  assert.match(page, /return !userAgentLooksInteractiveBrowser\(requestHeaders\.get\("user-agent"\)\)/);
  assert.match(page, /const activityDetailsRequested = one\(sp, "activity_details"\) === "1"/);
  assert.match(page, /if \(authToken \|\| hasAuthHint \|\| activityDetailsRequested\) return false/);
  assert.match(page, /const deferTickerActivityDetails = useAnonymousTickerSsrShell \|\| shouldDeferAnonymousTickerActivityDetails/);
  assert.match(page, /activeUser: activeTickerSsrRequest/);

  const bundleBlock = page.slice(page.indexOf("const contextBundleResult ="), page.indexOf("const profile = contextBundleResult.profile"));
  const shellIndex = bundleBlock.indexOf("useAnonymousTickerSsrShell");
  const bundleIndex = bundleBlock.indexOf("getTickerContextBundle(normalizedSymbol");
  assert.ok(shellIndex >= 0, "ticker page should compute a lightweight anonymous shell branch");
  assert.ok(bundleIndex > shellIndex, "context bundle fanout should happen only after the lightweight shell branch");

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
  const detailClient = read("components/ticker/TickerActivityDetailClient.tsx");

  assert.match(page, /<TickerDeferredActivityRefresh enabled=\{activityDetailsDeferred\} symbol=\{normalizedSymbol\} \/>/);
  assert.match(page, /<TickerActivityDetailClient kind="congress" symbol=\{normalizedSymbol\} lookbackDays=\{selectedLookbackDays\} side=\{side\} statusElementId="congress-activity-status" \/>/);
  assert.match(page, /<TickerActivityDetailClient kind="insider" symbol=\{normalizedSymbol\} lookbackDays=\{selectedLookbackDays\} side=\{side\} statusElementId="insider-activity-status" \/>/);
  assert.match(page, /activityDetailsDeferred \? "Loading government contract activity\." : "No government contracts in selected window\."/);
  assert.match(refresher, /"use client"/);
  assert.match(refresher, /IntersectionObserver/);
  assert.match(refresher, /document\.hidden/);
  assert.match(refresher, /url\.searchParams\.set\("activity_details", "1"\)/);
  assert.match(refresher, /router\.replace/);
  assert.match(detailClient, /requestSource: "visibility"/);
  assert.match(detailClient, /routeFamily: "ticker"/);
  assert.match(detailClient, /source: kind === "congress" \? "congress-detail" : "insider-detail"/);
  assert.match(detailClient, /No Congress trades in the selected window\./);
  assert.match(detailClient, /No insider trades in the selected window\./);
});

test("ticker API attribution headers remain present for client lazy requests", () => {
  const api = read("lib/api.ts");

  assert.match(api, /requestSource\(_requestSource\)/);
  assert.match(api, /typeof window === "undefined" \? "ssr" : "client"/);
  assert.match(api, /"X-Walnut-Active-User": "browser"/);
  assert.match(api, /headers\.set\("X-Walnut-Panel", panelFromSource\(attribution\.component\)\)/);
  assert.match(api, /_routeFamily \? safeHeaderValue\(_routeFamily\) : url \? routeFamilyFromUrl\(url\) : routeFamilyFromPath\(attribution\.route\)/);
});

test("middleware bypasses inactive anonymous terminal SSR before app render", () => {
  const middleware = read("middleware.ts");

  assert.match(middleware, /function isInteractiveBrowserUserAgent/);
  assert.match(middleware, /terminalShellResponse\(pathname, host, prefetch \? "prefetch" : bot \? "bot" : "inactive"\)/);
  assert.match(middleware, /!hasBackendSession && !hasAuthHint && \(prefetch \|\| bot \|\| !isInteractiveBrowserUserAgent\(userAgent\)\)/);
});
