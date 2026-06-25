import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const middleware = read("middleware.ts");
const guard = read("components/auth/VerifiedSessionGuard.tsx");
const accountSettings = read("app/account/settings/page.tsx");
const accountBilling = read("app/account/billing/page.tsx");
const adminSettings = read("app/admin/settings/page.tsx");
const adminAiMarketing = read("app/admin/ai-marketing/page.tsx");
const screenerPage = read("app/screener/page.tsx");
const backtestingPage = read("app/backtesting/page.tsx");
const watchlistsPage = read("app/watchlists/page.tsx");
const watchlistDetailPage = read("app/watchlists/[id]/page.tsx");
const monitoringPage = read("app/monitoring/page.tsx");
const api = read("lib/api.ts");

test("middleware covers protected app routes while leaving public account flows open", () => {
  assert.match(middleware, /protectedPrefixes = \["\/admin", "\/account", "\/screener", "\/backtesting", "\/watchlists", "\/monitoring", "\/signals", "\/leaderboards"\]/);
  assert.match(middleware, /publicAccountPaths = new Set\(\["\/account\/verify-email", "\/account\/reactivate"\]\)/);
  assert.match(middleware, /hasBackendSession \|\| hasAuthHint/);
  assert.match(middleware, /"\/admin\/:path\*"/);
  assert.match(middleware, /"\/account\/:path\*"/);
  assert.match(middleware, /"\/screener"/);
  assert.match(middleware, /"\/backtesting"/);
});

test("verified session guard uses auth/me before rendering protected children", () => {
  assert.match(guard, /const source = requireAdmin \? "VerifiedSessionGuardAdmin" : "VerifiedSessionGuard"/);
  assert.match(guard, /const verifySession = \(\) => getMe\(\{ force: true, source \}\)/);
  assert.match(guard, /clearLegacyAuthStorage\(\)/);
  assert.match(guard, /let verifiedSessionInRuntime = false/);
  assert.match(guard, /hasVerifiedSessionHint\(requireAdmin\)/);
  assert.match(guard, /const hasVerifiedSessionRef = useRef\(state === "authorized"\)/);
  assert.match(guard, /\(initiallyAuthorized \|\| hasVerifiedSessionRef\.current\) && hasClientAuthHint\(\)/);
  assert.match(guard, /rememberVerifiedSession\(\)/);
  assert.match(guard, /clearVerifiedSessionHint\(\)/);
  assert.match(guard, /await delay\(350\)/);
  assert.match(guard, /if \(state === "authorized"\) return <>\{children\}<\/>/);
  assert.match(guard, /router\.replace\(signInHref\)/);
  assert.match(guard, /data-auth-guard-state=\{state\}/);
  assert.doesNotMatch(guard, /sessionStorage|localStorage\.getItem|Authorization|Bearer|ct:authToken/);
});

test("admin guard rejects non-admin users before admin shell renders", () => {
  assert.match(guard, /requireAdmin && !isAdminUser\(response\.user\)/);
  assert.match(guard, /setState\("forbidden"\)/);
  assert.match(adminSettings, /<VerifiedSessionGuard returnTo="\/admin\/settings" requireAdmin>/);
  assert.match(adminAiMarketing, /<VerifiedSessionGuard returnTo="\/admin\/ai-marketing" requireAdmin>/);
  assert.match(adminSettings, /<AdminSettingsPanel \/>/);
  assert.match(adminAiMarketing, /<AdminSettingsPanel initialTab="ai_marketing" \/>/);
});

test("account and billing shells wait for verified backend session", () => {
  assert.match(accountSettings, /<VerifiedSessionGuard returnTo="\/account\/settings">/);
  assert.match(accountSettings, /<AccountSettingsPanel \/>/);
  assert.match(accountBilling, /<VerifiedSessionGuard returnTo="\/account\/billing">/);
  assert.match(accountBilling, /<AccountAccessPanel \/>/);
  assert.match(accountBilling, /<BillingAccountPanel \/>/);
});

test("screener and backtesting shells wait for verified backend session", () => {
  assert.match(screenerPage, /const returnTo = buildReturnTo\("\/screener", sp\)/);
  assert.match(screenerPage, /<VerifiedSessionGuard returnTo=\{returnTo\} initiallyAuthorized=\{Boolean\(authToken\)\}>/);
  assert.match(screenerPage, /<ScreenerResultsClient/);
  assert.match(backtestingPage, /const returnTo = buildReturnTo\("\/backtesting", sp\)/);
  assert.match(backtestingPage, /<VerifiedSessionGuard returnTo=\{returnTo\}>/);
  assert.match(backtestingPage, /<BacktestingWorkbench/);
});

test("watchlist and monitoring shells wait for verified backend session", () => {
  assert.match(watchlistsPage, /<VerifiedSessionGuard returnTo="\/watchlists">/);
  assert.match(watchlistsPage, /<WatchlistsDashboard/);
  assert.match(watchlistDetailPage, /<VerifiedSessionGuard returnTo=\{returnTo\}>/);
  assert.match(watchlistDetailPage, /<WatchlistDetailClient/);
  assert.match(watchlistDetailPage, /<WatchlistDetailContent/);
  assert.match(monitoringPage, /<VerifiedSessionGuard returnTo="\/monitoring">/);
  assert.match(monitoringPage, /<MonitoringDashboard/);
});

test("cookie-only API client remains bearer-free", () => {
  assert.match(api, /credentials:\s*fetchInit\.credentials \?\? "include"/);
  assert.match(api, /clearLegacyAuthStorage/);
  assert.doesNotMatch(api, /headers\.set\("Authorization"/);
  assert.doesNotMatch(api, /Bearer \$\{/);
  assert.doesNotMatch(api, /localStorage\.getItem\(.*authToken/);
});
