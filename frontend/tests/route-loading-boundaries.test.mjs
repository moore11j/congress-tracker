import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const routeLoadings = [
  ["app/insights/loading.tsx", /eyebrow="Insights"/],
  ["app/pricing/loading.tsx", /eyebrow="Pricing"/],
  ["app/market-pressure/loading.tsx", /Auditing index names/],
  ["app/monitoring/loading.tsx", /eyebrow="Monitoring"/],
  ["app/watchlists/loading.tsx", /eyebrow="Watchlists"/],
  ["app/watchlists/[id]/loading.tsx", /eyebrow="Watchlist"/],
  ["app/account/settings/loading.tsx", /eyebrow="Account Settings"/],
  ["app/account/billing/loading.tsx", /eyebrow="Subscriptions & Billing"/],
  ["app/admin/settings/loading.tsx", /eyebrow="Operations"/],
];

test("feed loading skeleton is scoped to the feed route", () => {
  assert.doesNotMatch(read("app/loading.tsx"), /Unified tape|UNIFIED TAPE|FeedLoadingMountProbe/);
  assert.match(read("app/feed/loading.tsx"), /eyebrow="Unified tape"/);
  assert.match(read("app/feed/loading.tsx"), /FeedLoadingMountProbe/);
});

test("affected routes define page-specific loading skeletons", () => {
  for (const [file, labelPattern] of routeLoadings) {
    const source = read(file);
    assert.match(source, labelPattern, `${file} should have route-specific loading context`);
    assert.doesNotMatch(source, /Unified tape|UNIFIED TAPE|FeedLoadingMountProbe/, `${file} should not reuse feed loading copy`);
  }
});
