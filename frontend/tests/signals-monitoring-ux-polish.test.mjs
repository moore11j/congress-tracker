import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const signalsPage = read("app/signals/page.tsx");
const signalsFiltersClient = read("components/signals/SignalsFiltersClient.tsx");
const savedViewsBar = read("components/saved-views/SavedViewsBar.tsx");
const monitoringDashboard = read("components/monitoring/MonitoringDashboard.tsx");
const api = read("lib/api.ts");

test("signals restores the user default saved view only when URL filters are absent", () => {
  assert.match(signalsPage, /const SIGNALS_SYSTEM_DEFAULT_PARAMS: Record<string, string> = \{/);
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

test("signals side filter only renders useful choices", () => {
  const sideBlock = signalsFiltersClient.match(/<div className="text-xs text-slate-400">Side<\/div>[\s\S]*?<div className="text-xs text-slate-400 sm:ml-2">Sort<\/div>/)?.[0] ?? "";

  assert.match(sideBlock, /\["all", "All"\]/);
  assert.match(sideBlock, /\["buy", "Buy"\]/);
  assert.match(sideBlock, /\["sell", "Sell"\]/);
  assert.match(sideBlock, /\["buy_or_sell", "Buy\/Sell"\]/);
  assert.doesNotMatch(sideBlock, /Award|InKind|In Kind|Exempt|\["award"|\["inkind"|\["exempt"/);
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
