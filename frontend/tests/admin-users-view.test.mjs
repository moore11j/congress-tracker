import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const adminUsersViewPath = path.join(process.cwd(), "components", "admin", "AdminUsersView.tsx");
const source = fs.readFileSync(adminUsersViewPath, "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");
const adminSettingsPanel = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminSettingsPanel.tsx"), "utf8");
const accountDisplay = fs.readFileSync(path.join(process.cwd(), "lib", "accountDisplay.ts"), "utf8");
const pageAnalyticsReport = fs.readFileSync(path.join(process.cwd(), "components", "admin", "PageAnalyticsReport.tsx"), "utf8");
const pageAnalyticsTracker = fs.readFileSync(path.join(process.cwd(), "components", "PageAnalyticsTracker.tsx"), "utf8");
const providerUsageReport = fs.readFileSync(path.join(process.cwd(), "components", "admin", "ProviderUsageReport.tsx"), "utf8");

test("admin users table renders distinct billing amount columns near plan", () => {
  assert.match(source, /<th className="px-3 py-3">Plan<\/th>\s*<th className="px-3 py-3">Billing interval<\/th>\s*<th className="px-3 py-3">Current price<\/th>\s*<th className="px-3 py-3">Total paid<\/th>\s*<th className="px-3 py-3">Last payment<\/th>/);
  assert.match(source, /displayCurrentPlanPrice\(user\)/);
  assert.match(source, /displayTotalPaid\(user\)/);
  assert.match(source, /displayLastPayment\(user\)/);
  assert.match(source, /displayBillingFrequency\(user\)/);
  assert.match(source, /colSpan=\{25\}/);
});

test("admin users table renders display-safe User ID before User Name", () => {
  assert.match(source, /<th className="px-3 py-3">User ID<\/th>\s*<th className="px-3 py-3">User name<\/th>/);
  assert.match(source, /formatUserDisplayId\(user\)/);
  assert.match(accountDisplay, /return `U-\$\{String\(user\.id\)\.padStart\(6, "0"\)\}`;/);
  assert.doesNotMatch(accountDisplay, /email/);
});

test("admin users billing helpers preserve currency and monthly annual labels", () => {
  assert.match(source, /new Intl\.NumberFormat\("en-US", \{ style: "currency", currency \}\)/);
  assert.match(source, /return `\$\{currency\} \$\{formatted\}`;/);
  assert.match(source, /current_plan_display/);
  assert.match(source, /total_paid_display/);
  assert.match(source, /last_payment_display/);
  assert.match(source, /return "Monthly";/);
  assert.match(source, /return "Annual";/);
  assert.match(source, /return "—";/);
});
test("admin users search renders with filters and debounces requests", () => {
  assert.match(source, /<span className="block font-medium text-slate-200">Search<\/span>[\s\S]*placeholder="Search ID, name, or email\.\.\."/);
  assert.match(source, /\{ value: "", label: "All non-deleted" \}/);
  assert.match(source, /\{ value: "all_with_deleted", label: "All including deleted" \}/);
  assert.match(source, /window\.setTimeout\(\(\) => \{[\s\S]*setPage\(1\);[\s\S]*setDebouncedSearch\(search\.trim\(\)\);[\s\S]*\}, 300\)/);
  assert.match(source, /search: debouncedSearch \|\| undefined/);
  assert.match(source, /onChange=\{\(event\) => setSearch\(event\.target\.value\)\}/);
  assert.match(source, /downloadAdminUsers\(format, \{[\s\S]*\.\.\.query,[\s\S]*page: undefined,[\s\S]*page_size: undefined/);
});

test("admin paid-plan override opens subscription price modal", () => {
  assert.match(source, /title="Set subscription price"/);
  assert.match(source, /Choose how this \{subscriptionPriceDialog\?\.tier === "pro" \? "Pro" : "Premium"\} subscription should be represented in Stripe/);
  assert.match(source, /Default plan price/);
  assert.match(source, /Custom price/);
  assert.match(source, /Free admin grant/);
  assert.match(source, /setSubscriptionPriceDialog\(\{ user, tier \}\)/);
});

test("admin paid-plan override sends explicit Stripe price mode payloads", () => {
  assert.match(apiSource, /export type AdminPlanPriceMode = "default" \| "custom" \| "free_admin_grant";/);
  assert.match(apiSource, /body: JSON\.stringify\(\{ tier, \.\.\.\(price \|\| \{\}\) \}\)/);
  assert.match(source, /return \{ price_mode: subscriptionPriceDraft\.mode \};/);
  assert.match(source, /price_mode: "custom" as const/);
  assert.match(source, /custom_price: \{[\s\S]*amount_cents: amountCents,[\s\S]*currency: subscriptionPriceDraft\.currency \|\| "USD",[\s\S]*interval: subscriptionPriceDraft\.interval/);
  assert.match(source, /payload\.price_mode = "free_admin_grant";/);
});

test("admin reports include first-party page analytics", () => {
  assert.match(adminSettingsPanel, /import \{ PageAnalyticsReport \} from "@\/components\/admin\/PageAnalyticsReport";/);
  assert.match(adminSettingsPanel, /<PageAnalyticsReport \/>/);
  assert.match(pageAnalyticsReport, /getAdminPageAnalytics\(\{ period, limit: 30 \}\)/);
  assert.match(pageAnalyticsReport, /Page analytics/);
});

test("admin provider usage uses Enterprise 500 calls per minute assumption", () => {
  assert.match(providerUsageReport, /FMP Enterprise guardrails/);
  assert.match(providerUsageReport, /function planAssumptionValue/);
  assert.match(providerUsageReport, /data\?\.plan_assumption\?\.name \|\| "Enterprise"/);
  assert.match(providerUsageReport, /data\?\.plan_assumption\?\.calls_per_minute \?\? 500/);
  assert.match(providerUsageReport, /<Metric label="Plan assumption" value=\{planAssumptionValue\(data\)\}/);
  assert.doesNotMatch(providerUsageReport, /Premium \/ 750 calls per minute/);
  assert.doesNotMatch(providerUsageReport, /FMP Premium/);
});

test("page analytics tracker strips query strings and sends route events", () => {
  assert.match(pageAnalyticsTracker, /usePathname/);
  assert.match(pageAnalyticsTracker, /parsed\.pathname/);
  assert.match(pageAnalyticsTracker, /recordPageView\(/);
  assert.doesNotMatch(pageAnalyticsTracker, /searchParams/);
});

test("admin panel refresh forwards active Users tab refresh token", () => {
  assert.match(adminSettingsPanel, /const \[usersRefreshToken, setUsersRefreshToken\] = useState\(0\)/);
  assert.match(adminSettingsPanel, /if \(activeTab === "users"\) \{[\s\S]*setUsersRefreshToken\(\(current\) => current \+ 1\);[\s\S]*return;/);
  assert.match(adminSettingsPanel, /onClick=\{refreshActiveTab\}/);
  assert.match(adminSettingsPanel, /<AdminUsersView refreshToken=\{usersRefreshToken\} \/>/);
  assert.match(source, /export function AdminUsersView\(\{ refreshToken = 0 \}: AdminUsersViewProps\)/);
  assert.match(source, /\}, \[query, refreshToken\]\);/);
  assert.match(adminSettingsPanel, /<StripeRow label="Webhook events" value=\{missingList\(settings\.stripe\.webhook_events\)\} \/>/);
});

test("admin delete surfaces Stripe customer cleanup result", () => {
  assert.match(apiSource, /export type AdminDeleteUserResponse = \{/);
  assert.match(apiSource, /delete_stripe_customer: deleteStripeCustomer \? "true" : "false"/);
  assert.match(apiSource, /fetchJson<AdminDeleteUserResponse>/);
  assert.match(source, /const result = await adminDeleteUser\(user\.id\)/);
  assert.match(source, /Stripe customer deleted\./);
  assert.match(source, /Stripe customer retained for billing history\./);
  assert.match(source, /Stripe customer cleanup will run before the account is removed\./);
});

test("admin user action menu guards against missing or detached anchors", () => {
  assert.match(source, /anchor: HTMLButtonElement \| null/);
  assert.match(source, /if \(!anchor\?\.isConnected\) return null;\s*const rect = anchor\.getBoundingClientRect\(\);/);
  assert.match(source, /const nextPosition = menuPosition\(anchor, menuRef\.current\?\.offsetHeight \?\? 260\);[\s\S]*if \(!nextPosition\) \{[\s\S]*onClose\(\);[\s\S]*return;/);
  assert.match(source, /const anchor = event\.currentTarget;[\s\S]*current\?\.userId === user\.id \? null : \{ userId: user\.id, anchor \}/);
});

test("admin user action menu can send password resets without exposing tokens", () => {
  assert.match(source, /adminSendPasswordReset/);
  assert.match(apiSource, /\/api\/admin\/users\/\$\{userId\}\/send-password-reset/);
  assert.match(source, /Send password reset/);
  assert.match(source, /Send password reset email to <span className="font-medium text-white">\{displayEmail\(user\)\}<\/span>\?/);
  assert.match(source, /Password reset email sent\./);
  assert.match(source, /Could not send password reset email\./);
  assert.doesNotMatch(source, /reset_url|resetUrl|password_reset_token|token_hash/);
});
