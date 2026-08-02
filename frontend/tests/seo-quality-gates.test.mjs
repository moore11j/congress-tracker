import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const seoQuality = read("lib/seoQuality.ts");
const evergreenSeo = read("lib/evergreenSeo.ts");
const sitemapRoutes = [
  "app/sitemap-tickers.xml/route.ts",
  "app/sitemap-members.xml/route.ts",
  "app/sitemap-insiders.xml/route.ts",
  "app/sitemap-institutions.xml/route.ts",
  "app/sitemap-departments.xml/route.ts",
  "app/sitemap-research.xml/route.ts",
  "app/sitemap-comparisons.xml/route.ts",
];

test("phase 3 defines explicit SEO quality gates and pilot pages", () => {
  for (const entityType of ["ticker", "member", "insider", "institution", "department", "research", "comparison"]) {
    assert.match(seoQuality, new RegExp(`${entityType}:\\s*\\[`), `${entityType} quality rules should be documented`);
  }
  for (const collection of ["tickers", "members", "insiders", "institutions", "departments", "research", "comparisons"]) {
    assert.match(seoQuality, new RegExp(`${collection}:\\s*\\[`), `${collection} pilot set should exist`);
  }
  assert.match(seoQuality, /rationale:/);
  assert.match(seoQuality, /lastmod:/);
});

test("segmented app sitemaps consume the centralized pilot registry", () => {
  for (const routePath of sitemapRoutes) {
    const source = read(routePath);
    assert.match(source, /seoPilotPages/);
    assert.match(source, /sitemapUrlset/);
    assert.doesNotMatch(source, /const PATHS|const TICKERS/);
  }
});

test("dynamic entity metadata uses noindex fallbacks for weak or unavailable pages", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");
  const memberPage = read("app/member/[slug]/page.tsx");
  const insiderPage = read("app/insider/[slug]/page.tsx");
  const institutionPage = read("app/institution/[cik]/page.tsx");
  const departmentPage = read("app/departments/[slug]/page.tsx");
  const comparePage = read("app/compare/[left]/[right]/page.tsx");

  assert.match(tickerPage, /tickerHasIndexableContent\(profile\)/);
  assert.match(memberPage, /noindexFollowMetadata/);
  assert.match(insiderPage, /insiderHasIndexableContent\(summary\)/);
  assert.match(institutionPage, /institutionHasIndexableContent\(profile\)/);
  assert.match(departmentPage, /departmentHasIndexableContent\(department\)/);
  assert.match(comparePage, /isApprovedSeoPilotPath\(canonicalPath\)/);
  assert.match(comparePage, /robots:\s*indexablePilot \?/);
});

test("sitemap XML includes lastmod for controlled pilot pages", () => {
  assert.match(seoQuality, /function sitemapUrlset/);
  assert.match(seoQuality, /<lastmod>\$\{page\.lastmod\}<\/lastmod>/);
});

test("evergreen SEO foundation requires editorial approval before indexing", () => {
  assert.match(evergreenSeo, /type EvergreenEditorialStatus = "draft" \| "approved" \| "published" \| "archived"/);
  assert.match(evergreenSeo, /evergreenSeoQualityRequirements/);
  assert.match(evergreenSeo, /evergreenSeoPilotPages/);
  assert.match(evergreenSeo, /editorialStatus === "published"/);
  assert.match(evergreenSeo, /isApprovedSeoPilotPath\(page\.canonicalPath\)/);
  assert.doesNotMatch(evergreenSeo, /Array\.from|for \(let i = 0; i < 1000/);
});
