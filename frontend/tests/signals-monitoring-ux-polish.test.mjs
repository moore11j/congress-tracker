import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const signalsPage = read("app/signals/page.tsx");
const signalsFiltersClient = read("components/signals/SignalsFiltersClient.tsx");
const signalsResultsClient = read("components/signals/SignalsResultsClient.tsx");
const savedViewsBar = read("components/saved-views/SavedViewsBar.tsx");
const monitoringDashboard = read("components/monitoring/MonitoringDashboard.tsx");
const api = read("lib/api.ts");

test("signals restores the user default saved view only when URL filters are absent", () => {
  assert.match(signalsPage, /const SIGNALS_SYSTEM_DEFAULT_PARAMS: Record<string, string> = \{/);
  assert.match(signalsPage, /mode: "congress"/);
  assert.match(signalsPage, /limit: "25"/);
  assert.match(signalsPage, /sort: "recent"/);
  assert.match(signalsPage, /<SignalsFiltersClient[\s\S]*defaultParams=\{SIGNALS_SYSTEM_DEFAULT_PARAMS\}/);
  assert.match(signalsFiltersClient, /<SavedViewsBar[\s\S]*surface="signals"[\s\S]*restoreOnLoad=\{true\}[\s\S]*defaultParams=\{defaultParams\}/);
  assert.match(savedViewsBar, /const targetId = store\.defaultViewIds\[surfaceKey\] \?\? store\.selectedViewIds\[surfaceKey\]/);
  assert.match(savedViewsBar, /if \(hasExplicitParams\(params, paramKeys\)\) return;/);
});

test("signals saved view dirty state compares normalized defaults", () => {
  assert.match(savedViewsBar, /function normalizedParamsSignature\(params: Record<string, string>, keys: readonly string\[\], defaults: Record<string, string>\)/);
  assert.match(savedViewsBar, /const activeViewIsDirty = Boolean\(activeView && activeView\.id !== restoreTargetViewId && normalizedParamsSignature\(activeView\.params, paramKeys, defaultParams\) !== currentSignature\)/);
  assert.match(savedViewsBar, /edited/);
});

test("signals mode and sort controls match the current product surface", () => {
  const modeOptions = signalsFiltersClient.match(/const modeOptions = \[([\s\S]*?)\] as const;/)?.[1] ?? "";
  assert.match(modeOptions, /\["congress", "Congress"\][\s\S]*\["insider", "Insider"\][\s\S]*\["institutional", "Institutional"\]/);
  assert.doesNotMatch(modeOptions, /\["all", "All"\]/);

  const sortOptions = signalsFiltersClient.match(/const sortOptions = \[([\s\S]*?)\] as const;/)?.[1] ?? "";
  assert.match(sortOptions, /\["recent", "Recent"\][\s\S]*\["amount", "Amount"\][\s\S]*\["multiple", "Multiple"\][\s\S]*\["smart", "Score"\]/);
  assert.doesNotMatch(sortOptions, /Confirm|Fresh|Conviction/);
  assert.doesNotMatch(signalsFiltersClient, /confirmationBand|confirmationDirection|minConfirmationSources|multiSourceOnly/);
  assert.doesNotMatch(signalsFiltersClient, />Confirm<|>Direction<|>Sources</);
});

test("signals side filter only renders useful choices", () => {
  assert.match(signalsFiltersClient, /const sideOptions = \[[\s\S]*\["all", "All"\][\s\S]*\["buy", "Buy"\][\s\S]*\["sell", "Sell"\]/);
  assert.doesNotMatch(signalsFiltersClient, /Award|InKind|In Kind|Exempt|\["award"|\["inkind"|\["exempt"|\["buy_or_sell"/);
});

test("signals table removes screener-only source confirmation and freshness columns", () => {
  for (const source of [signalsPage, signalsResultsClient]) {
    assert.doesNotMatch(source, /signals(?:-client)?-header-source/);
    assert.doesNotMatch(source, /signals(?:-client)?-header-confirmation/);
    assert.doesNotMatch(source, /signals(?:-client)?-header-freshness/);
    assert.doesNotMatch(source, /<th[\s\S]*?>\s*Source\s*<\/th>|label="Source"/);
    assert.doesNotMatch(source, /Conf\.|Fresh/);
    assert.doesNotMatch(source, /colSpan=\{11\}|columns=\{11\}|min-w-\[65rem\]/);
    assert.match(source, /colSpan=\{8\}/);
  }

  assert.match(signalsPage, /<SkeletonTable columns=\{8\} rows=\{8\}/);
});

test("monitoring source cards share styling and make whole source rows clickable", () => {
  assert.match(monitoringDashboard, /const monitoredSourceCardClassName = `\$\{compactInteractiveSurfaceClassName\}/);
  assert.match(monitoringDashboard, /function MonitoredSourceCard/);
  assert.match(monitoringDashboard, /<Link href=\{href\} prefetch=\{false\} onClick=\{onClick\} className=\{monitoredSourceCardClassName\}>/);
  assert.match(monitoringDashboard, /<MonitoredSourceCard[\s\S]*href=\{sourceHrefForWatchlist\(watchlist\)\}[\s\S]*subtitle=\{`Watchlist #\$\{watchlist\.id\}`\}/);
  assert.match(monitoringDashboard, /<MonitoredSourceCard[\s\S]*href=\{href\}[\s\S]*subtitle=\{`Saved screen/);
  assert.doesNotMatch(monitoringDashboard, /<Link href=\{sourceHrefForWatchlist\(watchlist\)\}[\s\S]*>\s*Open\s*<\/Link>/);
});

test("auth client still avoids writing a readable ct_session cookie", () => {
  const rememberBody = api.match(/function rememberAuthenticatedSession\(\) \{([\s\S]*?)\n\}/)?.[1] ?? "";

  assert.match(api, /export const backendSessionCookieName = "ct_session"/);
  assert.match(rememberBody, /document\.cookie = `\$\{authHintCookieName\}=1; Path=\/; SameSite=Lax; Max-Age=/);
  assert.doesNotMatch(rememberBody, /backendSessionCookieName|ct_session|token|Authorization|Bearer/);
});
