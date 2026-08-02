import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const comparisonData = read("lib/comparisonPages.ts");
const comparisonView = read("components/landing/ComparisonPages.tsx");
const hubPage = read("app/compare/page.tsx");
const competitorPage = read("app/compare/[left]/page.tsx");
const middleware = read("middleware.ts");
const sitemap = read("public/sitemap.xml");

const slugs = [
  "walnut-markets-vs-stockanalysis",
  "walnut-markets-vs-insider-screener",
  "walnut-markets-vs-quiver-quant",
  "walnut-markets-vs-unusual-whales",
  "walnut-markets-vs-finviz",
  "walnut-markets-vs-capitol-trades",
  "walnut-markets-vs-trendspider",
];

test("comparison buildout exposes hub and seven reusable competitor pages", () => {
  assert.match(hubPage, /<ComparisonHubPage \/>/);
  assert.match(hubPage, /headers\(\)/);
  assert.match(hubPage, /x-walnut-public-landing/);
  assert.match(hubPage, /redirect\("\/compare\/_\/_"\)/);
  assert.match(competitorPage, /generateStaticParams/);
  assert.match(competitorPage, /comparisonPageList\.map/);
  assert.match(competitorPage, /marketingSeoPageMetadata/);
  assert.match(competitorPage, /CompetitorComparisonPageView/);

  for (const slug of slugs) {
    assert.match(comparisonData, new RegExp(`slug:\\s*"${slug}"`));
    assert.match(sitemap, new RegExp(`https://walnutmarkets\\.com/compare/${slug}`));
  }
});

test("comparison content keeps factual claims centralized with checked sources", () => {
  assert.match(comparisonData, /export type ComparisonFact/);
  assert.match(comparisonData, /sourceUrl: string/);
  assert.match(comparisonData, /checkedOn: string/);
  assert.match(comparisonData, /export const comparisonCheckedOn = "2026-08-01"/);
  assert.match(comparisonData, /claimsForOwnerReview/);
  assert.match(comparisonView, /Public sources checked/);
  assert.match(comparisonView, /checked \{item\.checkedOn\}/);
});

test("comparison pages use real product evidence and valid SEO surfaces", () => {
  assert.match(comparisonData, /compare-nvda-mu-production\.png/);
  assert.match(comparisonView, /not a fabricated dashboard/);
  assert.match(comparisonData, /comparisonPageJsonLd/);
  assert.match(comparisonData, /FAQPage/);
  assert.match(comparisonData, /BreadcrumbList/);
  assert.match(comparisonData, /SoftwareApplication/);
  assert.match(middleware, /function isPublicComparisonRoute\(pathname: string\): boolean/);
  assert.match(middleware, /normalized\.startsWith\("\/compare\/walnut-markets-vs-"\)/);
  assert.match(middleware, /function isMarketingComparisonSlugRoute\(pathname: string\): boolean/);
  assert.match(middleware, /host === appHost && isMarketingComparisonSlugRoute\(pathname\)/);
  assert.match(middleware, /marketingUrl\.hostname = canonicalMarketingHost/);
  assert.match(middleware, /isPublicComparisonRoute\(pathname\)/);
});

test("comparison copy avoids generic AI marketing filler", () => {
  const banned = [
    "Unlock the power of",
    "game-changing",
    "revolutionary",
    "fast-paced market",
    "vibes",
    "macro positioning.",
    "fake testimonials",
  ];

  for (const phrase of banned) {
    assert.doesNotMatch(comparisonData, new RegExp(phrase, "i"));
  }
});
