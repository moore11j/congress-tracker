import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const routes = [
  "/stock-research-software",
  "/stock-analysis-platform",
  "/insider-trading-analysis-software",
  "/alternative-data-stock-analysis",
  "/institutional-activity-tracker",
];

const data = fs.readFileSync(path.join(root, "lib", "commercialFeaturePages.ts"), "utf8");
const component = fs.readFileSync(path.join(root, "components", "landing", "CommercialFeaturePage.tsx"), "utf8");
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const sitemap = fs.readFileSync(path.join(root, "public", "sitemap.xml"), "utf8");
const publicTools = fs.readFileSync(path.join(root, "lib", "publicResearchTools.ts"), "utf8");

test("commercial feature pages define five distinct public routes", () => {
  for (const route of routes) {
    assert.match(data, new RegExp(`pathname:\\s*"${route}"`));
    assert.ok(fs.existsSync(path.join(root, "app", route.slice(1), "page.tsx")), `${route} route file should exist`);
    assert.match(middleware, new RegExp(`"${route}"`));
    assert.match(sitemap, new RegExp(`https://walnutmarkets\\.com${route}`));
  }

  const descriptions = [...data.matchAll(/description:\s*"([^"]+)"/g)].map((match) => match[1]);
  assert.equal(new Set(descriptions).size, descriptions.length, "metadata descriptions should not be duplicated");
});

test("commercial feature renderer includes schema, real product evidence, and tracked CTAs", () => {
  assert.match(component, /commercialFeaturePageJsonLd/);
  assert.match(component, /application\/ld\+json/);
  assert.match(component, /CampaignEventOnMount/);
  assert.match(component, /seo_feature_page_view/);
  assert.match(component, /seo_feature_primary_cta_click/);
  assert.match(component, /seo_feature_secondary_cta_click/);
  assert.match(component, /\/landing\/compare-nvda-mu-production\.png/);
  assert.match(component, /Real Walnut interface capture/);
});

test("commercial feature copy keeps required limitations visible", () => {
  assert.match(data, /not illegal insider trading/i);
  assert.match(data, /not reliably on its own/i);
  assert.match(data, /do not show live buying or selling/i);
  assert.match(data, /No\. Public institutional filings are delayed and historical/);
  assert.match(data, /Options flow is treated as an availability-gated Pro data layer/);
  assert.doesNotMatch(data, /macro positioning.*confirmation score|confirmation score.*macro positioning/i);
  assert.doesNotMatch(data, /Unlock the power of|game-changing|revolutionary|fast-paced market|vibes/i);
});

test("commercial feature pages are linked from research tools", () => {
  for (const route of routes) assert.match(publicTools, new RegExp(`href:\\s*"${route}"`));
});
