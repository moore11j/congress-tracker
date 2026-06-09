import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const billingAccountPanel = fs.readFileSync(path.join(process.cwd(), "components", "billing", "BillingAccountPanel.tsx"), "utf8");
const pricingPlanner = fs.readFileSync(path.join(process.cwd(), "components", "billing", "PricingPlanner.tsx"), "utf8");

test("billing page refreshes Stripe and refetches account state on normal load", () => {
  assert.match(billingAccountPanel, /getMe\(\{ force: true, source: "Billing" \}\)/);
  assert.match(billingAccountPanel, /await refreshBillingSubscription\(\)/);
  assert.match(billingAccountPanel, /getMe\(\{ force: true, source: "BillingRefresh" \}\)/);
});

test("customer portal return waits for refresh-subscription before accepting updated plan", () => {
  assert.match(billingAccountPanel, /portal_return=1/);
  assert.match(billingAccountPanel, /const refreshResponse = await refreshBillingSubscription\(\)/);
  assert.match(billingAccountPanel, /refreshedFromStripe = refreshResponse\.status === "refreshed"/);
  assert.match(billingAccountPanel, /\(!fromCheckout && refreshedFromStripe\)/);
  assert.doesNotMatch(billingAccountPanel, /if \(!fromCheckout \|\| paidTier/);
});

test("pricing page bypasses cached account state after portal changes", () => {
  assert.match(pricingPlanner, /getMe\(\{ force: true, source: "Pricing" \}\)/);
  assert.match(pricingPlanner, /await refreshBillingSubscription\(\)/);
  assert.match(pricingPlanner, /getMe\(\{ force: true, source: "PricingRefresh" \}\)/);
});
