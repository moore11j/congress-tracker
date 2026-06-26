import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const pricingPlannerPath = path.join(process.cwd(), "components", "billing", "PricingPlanner.tsx");
const source = fs.readFileSync(pricingPlannerPath, "utf8");
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

  assert.equal(annualSavingsLabel(1995, 19995), "2 months free");
  assert.equal(annualSavingsLabel(4995, 49995), "2 months free");
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
  const screenerOrderStart = source.indexOf('"Screener & signals": {');
  const screenerOrderEnd = source.indexOf("},", screenerOrderStart);
  const screenerOrderSource = source.slice(screenerOrderStart, screenerOrderEnd);
  const screenerMarkers = ["screener:", "screener_results:", "screener_intelligence:", "screener_presets:", "signals:", "leaderboards:"];
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

test("advanced coming soon rows keep feed rows paired with their filters", () => {
  const advancedOrderStart = source.indexOf('"Advanced / Coming Soon": {');
  const advancedOrderEnd = source.indexOf("},", advancedOrderStart);
  const advancedOrderSource = source.slice(advancedOrderStart, advancedOrderEnd);
  const advancedMarkers = ["options_flow_feed:", "options_flow_filters:", "institutional_feed:", "institutional_filters:", "api_webhooks:"];
  const advancedPositions = advancedMarkers.map((marker) => advancedOrderSource.indexOf(marker));
  advancedPositions.forEach((position, index) => assert.notEqual(position, -1, `missing advanced marker ${advancedMarkers[index]}`));
  for (let index = 1; index < advancedPositions.length; index += 1) {
    assert.ok(advancedPositions[index] > advancedPositions[index - 1], `${advancedMarkers[index]} should follow ${advancedMarkers[index - 1]}`);
  }

  assert.match(
    source,
    /if \(\["screener", "screener_intelligence", "screener_presets", "screener_results", "signals", "leaderboards"\]\.includes\(featureKey\)\) return "Screener & signals";/,
  );
});

test("options flow and institutional activity are Pro-only in frontend fallback config", () => {
  assert.match(defaultPlanConfig, /premium:\s*\{[\s\S]*?options_flow_feed:\s*0,[\s\S]*?options_flow_filters:\s*0,[\s\S]*?institutional_feed:\s*0,[\s\S]*?institutional_filters:\s*0,/);
  assert.match(defaultPlanConfig, /pro:\s*\{[\s\S]*?options_flow_feed:\s*1,[\s\S]*?options_flow_filters:\s*1,[\s\S]*?institutional_feed:\s*1,[\s\S]*?institutional_filters:\s*1,/);
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
