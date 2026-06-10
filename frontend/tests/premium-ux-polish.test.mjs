import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const layout = read("app/layout.tsx");
const appTopNav = read("components/AppTopNav.tsx");
const accountNav = read("components/auth/AccountNav.tsx");
const signalsPage = read("app/signals/page.tsx");
const signalsResultsClient = read("components/signals/SignalsResultsClient.tsx");
const monitoringPage = read("app/monitoring/page.tsx");
const monitoringDashboard = read("components/monitoring/MonitoringDashboard.tsx");
const backtestingPage = read("app/backtesting/page.tsx");
const backtestingWorkbench = read("components/backtesting/BacktestingWorkbench.tsx");
const tickerPage = read("app/ticker/[symbol]/page.tsx");
const tickerSignalActivityClient = read("components/ticker/TickerSignalActivityClient.tsx");
const watchlistsPage = read("app/watchlists/page.tsx");
const watchlistsDashboard = read("components/watchlists/WatchlistsDashboard.tsx");
const accountAccessPanel = read("components/billing/AccountAccessPanel.tsx");
const billingAccountPanel = read("components/billing/BillingAccountPanel.tsx");
const accountDisplay = read("lib/accountDisplay.ts");
const watchlistCreateForm = read("components/watchlists/WatchlistCreateForm.tsx");
const watchlistTickerManager = read("components/watchlists/WatchlistTickerManager.tsx");
const notificationPreferences = read("components/notifications/NotificationPreferences.tsx");
const api = read("lib/api.ts");

test("top nav no longer exposes Watchlists while account dropdown does below Inbox", () => {
  assert.doesNotMatch(layout, /href="\/watchlists"[\s\S]*?Watchlists/);
  assert.match(accountNav, /href="\/monitoring"[\s\S]*?<span>Inbox<\/span>[\s\S]*?href="\/watchlists"[\s\S]*?Watchlists[\s\S]*?href="\/account\/settings"[\s\S]*?Account Settings/);
  assert.match(accountNav, /href="\/account\/billing"[\s\S]*?Subscriptions & Billing/);
  assert.match(accountNav, /href="\/faq"[\s\S]*?FAQ/);
  assert.match(accountNav, /href="\/admin\/settings"[\s\S]*?Admin/);
  assert.match(appTopNav, /href: "\/pricing", label: "Pricing"/);
  assert.doesNotMatch(accountNav, /href="\/pricing"[\s\S]*?Pricing/);
  assert.match(accountNav, />\s*Sign out\s*<\/button>/);
});

test("logged-out account nav points to login registration", () => {
  assert.match(accountNav, /const label = useMemo\(\(\) => \(user \? `Hello, \$\{displayName\(user\)\}!` : !loaded && initialAuthHint \? "Checking session\.\.\." : "Login \/ Register"\), \[initialAuthHint, loaded, user\]\);/);
  assert.match(accountNav, /if \(!user && authUnavailable\) \{[\s\S]*?<Link[\s\S]*?href="\/login"[\s\S]*?>[\s\S]*?\{label\}[\s\S]*?<\/Link>/);
  assert.doesNotMatch(accountNav, /authUnavailable \? "Account"/);
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

test("watchlists shows skeletons instead of free upgrade copy during likely-auth hydration", () => {
  assert.match(watchlistsPage, /initialAuthPending=\{!authToken\}/);
  assert.match(watchlistsDashboard, /initialAuthPending/);
  assert.match(watchlistsDashboard, /const \[entitlementsLoading, setEntitlementsLoading\] = useState\(initialAuthPending\)/);
  assert.match(watchlistsDashboard, /initialAuthPending \|\| hasClientAuthHint\(\)/);
  assert.match(watchlistsDashboard, /if \(entitlementsLoading\) \{\s*return <WatchlistsSkeleton \/>;/);
  assert.match(watchlistsDashboard, /<WatchlistCreateForm[\s\S]*entitlements=\{entitlements\}/);
});

test("watchlist detail widgets keep entitlement unknown separate from free", () => {
  assert.match(watchlistTickerManager, /const \[entitlementsLoaded, setEntitlementsLoaded\] = useState\(false\)/);
  assert.match(watchlistTickerManager, /const canAddTickers = entitlementsLoaded && hasEntitlement\(entitlements, "watchlist_tickers"\)/);
  assert.match(watchlistTickerManager, /const atTickerLimit = entitlementsLoaded && rows\.length >= tickerLimit/);
  assert.match(watchlistTickerManager, /!entitlementsLoaded \? \(/);
  assert.match(watchlistTickerManager, /: !canAddTickers \|\| atTickerLimit \? \(/);
  assert.match(notificationPreferences, /const \[entitlementsLoaded, setEntitlementsLoaded\] = useState\(false\)/);
  assert.match(notificationPreferences, /const canUseDigests = entitlementsLoaded && hasEntitlement\(entitlements, "notification_digests"\)/);
  assert.match(notificationPreferences, /!entitlementsLoaded \? \(/);
  assert.match(notificationPreferences, /: !canUseDigests \? \(/);
});

test("account access and plan labels are clean and admin overrides free display", () => {
  assert.match(accountDisplay, /export function formatAccessLabel/);
  assert.match(accountDisplay, /return "Super Admin"/);
  assert.match(accountDisplay, /return "Admin"/);
  assert.doesNotMatch(accountAccessPanel, /Current access: \$\{entitlements\?\.tier \?\? "free"\}\$\{user\.is_admin \? " admin" : ""\}/);
  assert.match(accountAccessPanel, /Current access: \$\{formatAccessLabel\(user, entitlements\)\}\./);
  assert.match(billingAccountPanel, /getMe\(\{ force: true, source: "Billing" \}\)/);
  assert.match(billingAccountPanel, /await refreshBillingSubscription\(\)/);
  assert.match(billingAccountPanel, /getMe\(\{ force: true, source: "BillingRefresh" \}\)/);
  assert.match(billingAccountPanel, /refreshBillingSubscription/);
  assert.match(billingAccountPanel, /refreshedFromStripe = refreshResponse\.status === "refreshed"/);
  assert.match(billingAccountPanel, /\(!fromCheckout && refreshedFromStripe\)/);
  assert.match(billingAccountPanel, /loadBillingHistory/);
  assert.match(billingAccountPanel, /Plan updated\./);
  assert.match(billingAccountPanel, /accountPlanSummary\(user, entitlements\)/);
  assert.match(billingAccountPanel, /const \[authLoading, setAuthLoading\] = useState\(true\)/);
  assert.match(billingAccountPanel, /const \[entitlementLoading, setEntitlementLoading\] = useState\(true\)/);
  assert.match(billingAccountPanel, /if \(authLoading \|\| entitlementLoading\) \{\s*return <BillingAccountSkeleton \/>;/);
  assert.match(billingAccountPanel, /if \(!user\) \{/);
  assert.match(billingAccountPanel, /isNonRenewingPaid\(user\)/);
  assert.match(billingAccountPanel, /Your subscription is set to end on/);
  assert.match(billingAccountPanel, /createCustomerPortalSession/);
  assert.match(accountDisplay, /Full administrative access across Walnut Market Terminal\./);
  assert.match(accountDisplay, /label: "Free"/);
  assert.match(accountDisplay, /premium: "Premium"/);
  assert.match(accountDisplay, /pro: "Pro"/);
});

test("billing and admin pages avoid unauthenticated copy during likely-auth hydration", () => {
  const adminSettingsPanel = read("components/admin/AdminSettingsPanel.tsx");

  assert.match(accountAccessPanel, /hasClientAuthHint/);
  assert.match(accountAccessPanel, /const \[authLoading, setAuthLoading\] = useState\(\(\) => hasClientAuthHint\(\)\)/);
  assert.match(accountAccessPanel, /if \(authLoading\) \{\s*return <AccountAccessSkeleton \/>;/);
  assert.match(adminSettingsPanel, /function AdminPanelSkeleton/);
  assert.match(adminSettingsPanel, /const \[busy, setBusy\] = useState\(true\)/);
  assert.match(adminSettingsPanel, /const \[authResolved, setAuthResolved\] = useState\(false\)/);
  assert.match(adminSettingsPanel, /if \(!authResolved && busy\) \{\s*return <AdminPanelSkeleton \/>;/);
  assert.match(adminSettingsPanel, /error\.status === 401[\s\S]*return "Sign in required\."/);
  assert.match(adminSettingsPanel, /error\.status === 403[\s\S]*return "Access denied\."/);
});

test("large entitlement counts render with thousands separators", () => {
  assert.match(accountDisplay, /new Intl\.NumberFormat\("en-US", \{ maximumFractionDigits: 0 \}\)\.format\(value\)/);
  assert.match(billingAccountPanel, /formatInteger\(value\)/);
  assert.match(watchlistCreateForm, /formatInteger\(limitFor\(entitlements, "watchlists"\)\)/);
  assert.match(watchlistTickerManager, /formatInteger\(tickerLimit\)/);
});

test("backtesting hides upgrade prompt during likely-auth hydration and unlocks after refresh", () => {
  assert.match(backtestingPage, /requirePageAuthState/);
  assert.match(backtestingPage, /initialAuthPending=\{!authToken\}/);
  assert.match(backtestingWorkbench, /initialAuthPending/);
  assert.match(backtestingWorkbench, /initialAuthPending \|\| hasClientAuthHint\(\)/);
  assert.match(backtestingWorkbench, /setEntitlements\(nextEntitlements\)/);
  assert.match(backtestingWorkbench, /setPresets\(nextPresets\)/);
  assert.match(backtestingWorkbench, /loading \|\| entitlementsLoading \? <ResultSkeleton \/>/);
  assert.match(backtestingWorkbench, /: !canRun \? \(\s*<div className="space-y-4">[\s\S]*Unlock portfolio backtesting/);
});

test("ticker Signal Activity avoids login copy during auth-hint hydration", () => {
  assert.match(tickerPage, /optionalPageAuthState/);
  assert.match(tickerPage, /signalActivityAuthPending = shouldLoadSignals && !authToken && authState\.hasAuthHint/);
  assert.match(tickerPage, /hasEntitlement\(entitlements, "signals"\)/);
  assert.match(tickerPage, /signalGateForAuthenticatedFreeUser/);
  assert.match(tickerPage, /<TickerSignalActivityClient[\s\S]*signalsAuthPending/);
  assert.match(tickerSignalActivityClient, /SignalActivitySkeleton/);
  assert.match(tickerSignalActivityClient, /getSignalsAll\(\{[\s\S]*symbol/);
  assert.match(tickerSignalActivityClient, /Create an account or log in to unlock signal activity\./);
  assert.doesNotMatch(tickerPage, /Create a free account or log in to unlock premium ticker signals/);
});
