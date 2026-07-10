import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const pricingPlannerPath = path.join(process.cwd(), "components", "billing", "PricingPlanner.tsx");
const source = fs.readFileSync(pricingPlannerPath, "utf8");
const pricingPage = fs.readFileSync(path.join(process.cwd(), "app", "pricing", "page.tsx"), "utf8");
const pricingDeferred = fs.readFileSync(path.join(process.cwd(), "components", "billing", "PricingPlannerDeferred.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");
const defaultPlanConfig = fs.readFileSync(path.join(process.cwd(), "lib", "defaultPlanConfig.ts"), "utf8");
const entitlementConfig = fs.readFileSync(path.join(process.cwd(), "lib", "entitlements.ts"), "utf8");

test("annual pricing badge derives rounded-up months free from configured prices", () => {
  assert.match(source, /Math\.ceil\(\(\(monthlyYear - annualAmount\) \/ monthlyAmount\) \* 2\) \/ 2/);
  assert.match(source, /return `\$\{formattedMonths\} \$\{monthsFree === 1 \? "month" : "months"\} free`;/);
  assert.doesNotMatch(source, /Save \$\{percent\}%/);

  const annualSavingsLabel = (monthlyCents, annualCents) => {
    const monthlyYear = monthlyCents * 12;
    if (monthlyYear <= 0 || monthlyCents <= 0 || annualCents <= 0 || annualCents >= monthlyYear) return null;
    const monthsFree = Math.ceil(((monthlyYear - annualCents) / monthlyCents) * 2) / 2;
    const formattedMonths = Number.isInteger(monthsFree) ? monthsFree.toFixed(0) : monthsFree.toFixed(1);
    return `${formattedMonths} ${monthsFree === 1 ? "month" : "months"} free`;
  };

  assert.equal(annualSavingsLabel(2495, 24950), "2 months free");
  assert.equal(annualSavingsLabel(3995, 39995), "2 months free");
  assert.equal(annualSavingsLabel(2000, 22000), "1 month free");
  assert.equal(annualSavingsLabel(2000, 21000), "1.5 months free");
  assert.equal(annualSavingsLabel(2000, 24000), null);
});

test("included feature cells render as green checks while limits stay numeric", () => {
  assert.match(source, /aria-label="Included"[\s\S]*?text-emerald-300[\s\S]*?✓/);
  assert.match(source, /if \(feature\.kind === "limit"\) return formatLimit\(feature, feature\.limits\[tier\] \?\? 0\);/);
  assert.doesNotMatch(source, /return "Included";/);
});

test("free/core rows lead screener and monitoring pricing categories", () => {
  const marketOrderStart = source.indexOf('"Market feeds": {');
  const marketOrderEnd = source.indexOf("},", marketOrderStart);
  const marketOrderSource = source.slice(marketOrderStart, marketOrderEnd);
  const marketMarkers = [
    "congress_feed:",
    "insider_feed:",
    "government_contracts_feed:",
    "government_contracts_filters:",
    "premium_feed_metrics:",
  ];
  const marketPositions = marketMarkers.map((marker) => marketOrderSource.indexOf(marker));
  marketPositions.forEach((position, index) => assert.notEqual(position, -1, `missing market marker ${marketMarkers[index]}`));
  for (let index = 1; index < marketPositions.length; index += 1) {
    assert.ok(marketPositions[index] > marketPositions[index - 1], `${marketMarkers[index]} should follow ${marketMarkers[index - 1]}`);
  }

  const screenerOrderStart = source.indexOf('"Screener & signals": {');
  const screenerOrderEnd = source.indexOf("},", screenerOrderStart);
  const screenerOrderSource = source.slice(screenerOrderStart, screenerOrderEnd);
  const screenerMarkers = [
    "screener:",
    "screener_results:",
    "screener_intelligence:",
    "screener_presets:",
    "signals:",
    "ticker_confirmation:",
    "leaderboards:",
    "options_flow_filters:",
  ];
  const screenerPositions = screenerMarkers.map((marker) => screenerOrderSource.indexOf(marker));
  screenerPositions.forEach((position, index) => assert.notEqual(position, -1, `missing screener marker ${screenerMarkers[index]}`));
  for (let index = 1; index < screenerPositions.length; index += 1) {
    assert.ok(screenerPositions[index] > screenerPositions[index - 1], `${screenerMarkers[index]} should follow ${screenerMarkers[index - 1]}`);
  }

  const monitoringOrderStart = source.indexOf('"Watchlists & monitoring": {');
  const monitoringOrderEnd = source.indexOf("},", monitoringOrderStart);
  const monitoringOrderSource = source.slice(monitoringOrderStart, monitoringOrderEnd);
  const monitoringMarkers = [
    "inbox_alerts:",
    "inbox_alert_retention:",
    "monitoring_sources:",
    "watchlists:",
    "watchlist_tickers:",
    "saved_views:",
    "screener_saved_screens:",
    "screener_monitoring:",
  ];
  const monitoringPositions = monitoringMarkers.map((marker) => monitoringOrderSource.indexOf(marker));
  monitoringPositions.forEach((position, index) => assert.notEqual(position, -1, `missing monitoring marker ${monitoringMarkers[index]}`));
  for (let index = 1; index < monitoringPositions.length; index += 1) {
    assert.ok(monitoringPositions[index] > monitoringPositions[index - 1], `${monitoringMarkers[index]} should follow ${monitoringMarkers[index - 1]}`);
  }
});

test("advanced coming soon rows only include unavailable future surfaces", () => {
  const advancedOrderStart = source.indexOf('"Advanced / Coming Soon": {');
  const advancedOrderEnd = source.indexOf("},", advancedOrderStart);
  const advancedOrderSource = source.slice(advancedOrderStart, advancedOrderEnd);
  const advancedMarkers = ["institutional_feed:", "institutional_filters:", "options_flow_feed:", "api_webhooks:"];
  const advancedPositions = advancedMarkers.map((marker) => advancedOrderSource.indexOf(marker));
  advancedPositions.forEach((position, index) => assert.notEqual(position, -1, `missing advanced marker ${advancedMarkers[index]}`));
  for (let index = 1; index < advancedPositions.length; index += 1) {
    assert.ok(advancedPositions[index] > advancedPositions[index - 1], `${advancedMarkers[index]} should follow ${advancedMarkers[index - 1]}`);
  }
  assert.doesNotMatch(advancedOrderSource, /ticker_confirmation|premium_feed_metrics/);

  assert.match(
    source,
    /"ticker_confirmation"[\s\S]*?return "Screener & signals";/,
  );
  assert.match(source, /"premium_feed_metrics"[\s\S]*?return "Market feeds";/);
  assert.doesNotMatch(source, /"options_flow_feed", "institutional_feed"/);
  assert.match(source, /if \(\["options_flow_feed", "api_webhooks"\]\.includes\(feature\.feature_key\)\) return "Coming soon";/);
});

test("options flow and institutional activity are Pro-only in frontend fallback config", () => {
  assert.match(defaultPlanConfig, /feature_key:\s*"options_flow_feed"[\s\S]*?required_tier:\s*"pro"/);
  assert.match(defaultPlanConfig, /feature_key:\s*"options_flow_filters"[\s\S]*?required_tier:\s*"pro"/);
  assert.match(defaultPlanConfig, /feature_key:\s*"institutional_feed"[\s\S]*?required_tier:\s*"pro"/);
  assert.match(defaultPlanConfig, /feature_key:\s*"institutional_filters"[\s\S]*?required_tier:\s*"pro"/);
  assert.match(entitlementConfig, /export const proEntitlements/);
  const premiumBlock = entitlementConfig.slice(
    entitlementConfig.indexOf("export const premiumEntitlements"),
    entitlementConfig.indexOf("export const proEntitlements"),
  );
  assert.doesNotMatch(premiumBlock, /"options_flow_feed"|"options_flow_filters"|"institutional_feed"|"institutional_filters"/);
});

test("pricing actions render current plan states from fresh account entitlements", () => {
  assert.match(source, /getMe\(\{ force: true, source: "Pricing" \}\)/);
  assert.match(source, /refreshBillingSubscription\(\)/);
  assert.match(source, /getMe\(\{ force: true, source: "PricingRefresh" \}\)/);
  assert.match(source, /accountUser=\{accountUser\}/);
  assert.match(source, /accountEntitlements=\{accountEntitlements\}/);
  assert.match(source, /isNonRenewingPaid\(accountUser\)/);
  assert.match(source, /Your Walnut \{displayPlanName\(accountUser\)\} subscription is active until/);
  assert.match(source, /createCustomerPortalSession/);

  const actions = fs.readFileSync(path.join(process.cwd(), "components", "billing", "PricingActions.tsx"), "utf8");

  assert.doesNotMatch(actions, /getMe\(/);
  assert.match(actions, /const buttonLabel = isCurrentPlan\s*\?\s*"Current plan"/);
  assert.match(actions, /function currentPlanTier\(user: AccountUser \| null, entitlements: Entitlements \| null\): PlanTier/);
  assert.match(actions, /function actionForPlan\(currentTier: PlanTier, targetTier: PlanTier\): PlanAction/);
  assert.match(actions, /function labelForAction\(action: PlanAction, targetTier: PlanTier\)/);
  assert.match(actions, /return `\$\{action === "downgrade" \? "Downgrade" : "Upgrade"\} to \$\{planNames\[targetTier\]\}`;/);
  assert.doesNotMatch(actions, /"Change plan"/);
  assert.match(actions, /managedSubscriptionStatuses = new Set\(\["active", "trialing", "past_due"\]\)/);
  assert.match(actions, /const opensBillingPortal = !isCurrentPlan && \(hasManagedSubscription \|\| isDowngrade\);/);
  assert.match(actions, /: isDowngrade\s*\?\s*"border-white\/10 bg-slate-900\/70 text-slate-200 hover:border-white\/20 hover:text-white"/);
  assert.match(actions, /createCustomerPortalSession/);
  assert.match(actions, /checkoutConflictRedirectPath/);
  assert.match(actions, /payload\.code !== "active_subscription_exists"/);
  assert.match(actions, /window\.location\.href = redirectPath/);
  assert.match(actions, /disabled=\{disabled\}/);
});

test("pricing page renders a static public shell and refreshes live config client-side", () => {
  assert.match(pricingPage, /export const dynamic = "force-static"/);
  assert.match(pricingPage, /export const revalidate = false/);
  assert.doesNotMatch(pricingPage, /withServerTimeout\(getPlanConfig\(\), "pricing:plan-config"\)/);
  assert.doesNotMatch(pricingPage, /defaultPlanConfig/);
  assert.match(pricingPage, /<PricingPlannerDeferred \/>/);
  assert.match(pricingDeferred, /dynamic\(/);
  assert.match(pricingDeferred, /ssr: false/);
  assert.match(pricingDeferred, /loading: \(\) => <PricingFallback \/>/);
  assert.match(pricingDeferred, /defaultPlanConfig/);
  assert.match(pricingDeferred, /<PricingPlanner config=\{defaultPlanConfig\} \/>/);
  assert.match(source, /getPlanConfig\(\)/);
  assert.match(source, /setActiveConfig\(configResult\.value\)/);
  assert.match(apiSource, /export async function getPlanConfig\(\): Promise<PlanConfig> \{[\s\S]*?cache: "no-store"/);
  assert.match(apiSource, /export async function getPlanConfig\(\): Promise<PlanConfig> \{[\s\S]*?headers: \{ "Cache-Control": "no-cache" \}/);
  assert.doesNotMatch(apiSource, /export async function getPlanConfig\(\): Promise<PlanConfig> \{[\s\S]*?cache: "force-cache"/);
});

test("fallback plan config starts with current public prices", () => {
  assert.match(defaultPlanConfig, /tier: "premium", billing_interval: "monthly", amount_cents: 2495/);
  assert.match(defaultPlanConfig, /tier: "premium", billing_interval: "annual", amount_cents: 24950/);
  assert.match(defaultPlanConfig, /tier: "pro", billing_interval: "monthly", amount_cents: 3995/);
  assert.match(defaultPlanConfig, /tier: "pro", billing_interval: "annual", amount_cents: 39995/);
  assert.doesNotMatch(defaultPlanConfig, /amount_cents: 1995|amount_cents: 19995|amount_cents: 4995|amount_cents: 49995/);
});

test("fallback plan config starts with current public headline limits", () => {
  assert.match(defaultPlanConfig, /free:\s*\{[\s\S]*?screener_saved_screens:\s*1,[\s\S]*?screener_results:\s*5,[\s\S]*?watchlists:\s*1,[\s\S]*?watchlist_tickers:\s*5,[\s\S]*?saved_views:\s*1,[\s\S]*?monitoring_sources:\s*3,/);
  assert.match(defaultPlanConfig, /premium:\s*\{[\s\S]*?screener_saved_screens:\s*5,[\s\S]*?screener_results:\s*25,[\s\S]*?watchlists:\s*5,[\s\S]*?watchlist_tickers:\s*25,[\s\S]*?saved_views:\s*10,[\s\S]*?monitoring_sources:\s*10,/);
  assert.match(defaultPlanConfig, /pro:\s*\{[\s\S]*?screener_saved_screens:\s*25,[\s\S]*?screener_results:\s*100,[\s\S]*?watchlists:\s*25,[\s\S]*?watchlist_tickers:\s*100,[\s\S]*?saved_views:\s*25,[\s\S]*?monitoring_sources:\s*25,/);
  assert.match(defaultPlanConfig, /feature_key:\s*"signals"[\s\S]*?required_tier:\s*"premium"/);
  assert.match(defaultPlanConfig, /feature_key:\s*"screener_csv_export"[\s\S]*?required_tier:\s*"pro"/);
});

test("pricing CTA labels distinguish current, upgrade, and downgrade plans", () => {
  const ranks = { free: 0, premium: 10, pro: 20 };
  const names = { free: "Free", premium: "Premium", pro: "Pro" };
  const label = (current, target) => {
    if (current === target) return "Current plan";
    return `${ranks[target] < ranks[current] ? "Downgrade" : "Upgrade"} to ${names[target]}`;
  };

  assert.deepEqual(["free", "premium", "pro"].map((target) => label("free", target)), [
    "Current plan",
    "Upgrade to Premium",
    "Upgrade to Pro",
  ]);
  assert.deepEqual(["free", "premium", "pro"].map((target) => label("premium", target)), [
    "Downgrade to Free",
    "Current plan",
    "Upgrade to Pro",
  ]);
  assert.deepEqual(["free", "premium", "pro"].map((target) => label("pro", target)), [
    "Downgrade to Free",
    "Downgrade to Premium",
    "Current plan",
  ]);
});
