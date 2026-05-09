import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const layout = read("app/layout.tsx");
const accountNav = read("components/auth/AccountNav.tsx");
const signalsPage = read("app/signals/page.tsx");
const signalsResultsClient = read("components/signals/SignalsResultsClient.tsx");
const monitoringPage = read("app/monitoring/page.tsx");
const monitoringDashboard = read("components/monitoring/MonitoringDashboard.tsx");
const backtestingPage = read("app/backtesting/page.tsx");
const backtestingWorkbench = read("components/backtesting/BacktestingWorkbench.tsx");
const api = read("lib/api.ts");

test("top nav no longer exposes Watchlists while account dropdown does below Inbox", () => {
  assert.doesNotMatch(layout, /href="\/watchlists"[\s\S]*?Watchlists/);
  assert.match(accountNav, /href="\/monitoring"[\s\S]*?<span>Inbox<\/span>[\s\S]*?href="\/watchlists"[\s\S]*?Watchlists[\s\S]*?href="\/account\/settings"[\s\S]*?Account settings/);
  assert.match(accountNav, /href="\/account\/billing"[\s\S]*?Billing/);
  assert.match(accountNav, /href="\/admin\/settings"[\s\S]*?Admin/);
  assert.match(accountNav, /href="\/pricing"[\s\S]*?Pricing/);
  assert.match(accountNav, />\s*Sign out\s*<\/button>/);
});

test("signals sale rows render as Sell with normalized feed-like pill styling", () => {
  assert.match(signalsPage, /if \(t === "sale" \|\| t === "sell" \|\| t === "s-sale" \|\| t\.includes\("sale"\)\) return "sell"/);
  assert.match(signalsPage, /return \{ label: "Sell", klass: "border-rose-400\/30 bg-rose-400\/15 text-rose-100" \}/);
  assert.match(signalsResultsClient, /value\.includes\("sell"\) \|\| value\.includes\("sale"\)/);
  assert.match(signalsResultsClient, /return \{ label: "Sell", klass: "border-rose-400\/30 bg-rose-400\/15 text-rose-100" \}/);
  assert.doesNotMatch(signalsResultsClient, /label: titleCase\(tradeType \?\? kind\), klass: "border-white\/10 bg-white\/5 text-slate-200" \};[\s\S]*sale/);
});

test("signals smart Exceptional pill has enough compact width to avoid overflow", () => {
  assert.match(signalsPage, /label: "Exceptional"/);
  assert.match(signalsPage, /min-w-\[7\.75rem\][\s\S]*text-\[11px\]/);
  assert.match(signalsPage, /<col className="w-\[9\.25rem\]" \/>/);
  assert.match(signalsResultsClient, /label: "Exceptional"/);
  assert.match(signalsResultsClient, /min-w-\[7\.75rem\][\s\S]*text-\[11px\]/);
  assert.match(signalsResultsClient, /<col className="w-\[9\.25rem\]" \/>/);
});

test("monitoring shows skeletons instead of upgrade copy during likely-auth hydration", () => {
  assert.match(api, /authHintCookieName = "ct_auth_hint"/);
  assert.match(api, /export function hasClientAuthHint\(\)/);
  assert.match(monitoringPage, /initialAuthPending=\{!authToken\}/);
  assert.match(monitoringDashboard, /initialAuthPending/);
  assert.match(monitoringDashboard, /const \[entitlementsLoading, setEntitlementsLoading\] = useState\(initialAuthPending\)/);
  assert.match(monitoringDashboard, /initialAuthPending \|\| hasClientAuthHint\(\)/);
  assert.match(monitoringDashboard, /entitlementsLoading \? \(\s*<MonitoringPanelSkeleton \/>/);
  assert.match(monitoringDashboard, /!entitlementsLoading && hiddenSourceCount > 0/);
});

test("backtesting hides upgrade prompt during likely-auth hydration and unlocks after refresh", () => {
  assert.match(backtestingPage, /optionalPageAuthState/);
  assert.match(backtestingPage, /initialAuthPending=\{!authToken && authState\.hasAuthHint\}/);
  assert.match(backtestingWorkbench, /initialAuthPending/);
  assert.match(backtestingWorkbench, /initialAuthPending \|\| hasClientAuthHint\(\)/);
  assert.match(backtestingWorkbench, /setEntitlements\(nextEntitlements\)/);
  assert.match(backtestingWorkbench, /setPresets\(nextPresets\)/);
  assert.match(backtestingWorkbench, /loading \|\| entitlementsLoading \? <ResultSkeleton \/>/);
  assert.match(backtestingWorkbench, /: !canRun \? \(\s*<div className="space-y-4">[\s\S]*Unlock portfolio backtesting/);
});
