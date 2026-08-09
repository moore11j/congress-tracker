import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const seoQuality = read("lib/seoQuality.ts");
const evergreenSeo = read("lib/evergreenSeo.ts");
const seoSnapshotBaseline = read("components/seo/SeoSnapshotBaseline.tsx");
const seoSnapshotService = fs.readFileSync(path.join(root, "..", "backend", "app", "services", "seo_snapshots.py"), "utf8");
const phase3Audit = fs.readFileSync(path.join(root, "..", "docs", "seo-phase3-audit.md"), "utf8");
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
  for (const entityType of ["ticker", "member", "insider", "institution", "department", "research", "comparison", "screener", "market"]) {
    assert.match(seoQuality, new RegExp(`${entityType}:\\s*\\[`), `${entityType} quality rules should be documented`);
  }
  for (const collection of ["tickers", "members", "insiders", "institutions", "departments", "research", "comparisons"]) {
    assert.match(seoQuality, new RegExp(`${collection}:\\s*\\[`), `${collection} pilot set should exist`);
  }
  assert.doesNotMatch(seoQuality, /screeners:\s*\[/);
  assert.doesNotMatch(seoQuality, /markets:\s*\[/);
  assert.match(seoQuality, /Private, user-specific, empty, paginated, or query-driven screens stay out of public sitemaps/);
  assert.match(seoQuality, /Thin sector shells, transient market states, and authenticated-only views stay noindex or excluded/);
  assert.match(seoQuality, /rationale:/);
  assert.match(seoQuality, /lastmod:/);
});

test("segmented app sitemaps consume controlled whitelist sources", () => {
  for (const routePath of sitemapRoutes.slice(0, 3)) {
    const source = read(routePath);
    assert.match(source, /getSeoSnapshotIndex/);
    assert.match(source, /sitemapUrlset/);
    assert.doesNotMatch(source, /const PATHS|const TICKERS/);
  }
  for (const routePath of sitemapRoutes.slice(3)) {
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

  assert.match(tickerPage, /getSeoSnapshot\("ticker"/);
  assert.match(memberPage, /noindexFollowMetadata/);
  assert.match(memberPage, /getSeoSnapshot\("member"/);
  assert.match(insiderPage, /getSeoSnapshot\("insider"/);
  assert.match(institutionPage, /institutionHasIndexableContent\(profile\)/);
  assert.match(departmentPage, /departmentHasIndexableContent\(department\)/);
  assert.match(comparePage, /isApprovedSeoPilotPath\(canonicalPath\)/);
  assert.match(comparePage, /robots:\s*indexablePilot \?/);
});

test("sitemap XML includes lastmod for controlled pilot pages", () => {
  assert.match(seoQuality, /function sitemapUrlset/);
  assert.match(seoQuality, /<lastmod>\$\{page\.lastmod\}<\/lastmod>/);
});

test("public snapshot presentation uses Walnut product shell copy", () => {
  assert.match(seoSnapshotBaseline, /Stock Research/);
  assert.match(seoSnapshotBaseline, /Congress Trading Profile/);
  assert.match(seoSnapshotBaseline, /Insider Trading Profile/);
  assert.match(seoSnapshotBaseline, /Data as of/);
  assert.match(seoSnapshotBaseline, /Institutional Activity/);
  assert.match(seoSnapshotBaseline, /Portfolio Simulation/);
  assert.match(seoSnapshotBaseline, /Ownership & Performance/);
  assert.doesNotMatch(seoSnapshotBaseline, />Stored Snapshot</);
  assert.doesNotMatch(seoSnapshotBaseline, />Ticker Research Snapshot</);
  assert.doesNotMatch(seoSnapshotBaseline, />Congress Activity Snapshot</);
  assert.doesNotMatch(seoSnapshotBaseline, />Insider Activity Snapshot</);
  assert.doesNotMatch(seoSnapshotService, /provider refresh|SEO snapshot|stored disclosure|Stored Market Data|Form 4 Activity Snapshot|Congress Trading Snapshot/);
});

test("evergreen SEO foundation requires editorial approval before indexing", () => {
  assert.match(evergreenSeo, /type EvergreenEditorialStatus = "draft" \| "approved" \| "published" \| "archived"/);
  assert.match(evergreenSeo, /evergreenSeoQualityRequirements/);
  assert.match(evergreenSeo, /evergreenSeoPilotPages/);
  assert.match(evergreenSeo, /editorialStatus === "published"/);
  assert.match(evergreenSeo, /isApprovedSeoPilotPath\(page\.canonicalPath\)/);
  assert.doesNotMatch(evergreenSeo, /Array\.from|for \(let i = 0; i < 1000/);
});

test("phase 3 audit covers held-back screener and market page families", () => {
  assert.match(phase3Audit, /Screener-related pages/);
  assert.match(phase3Audit, /Sector or market pages/);
  assert.match(phase3Audit, /No screener, sector, or market sitemap segment is created in Phase 3/);
  assert.match(phase3Audit, /Newly Excluded Or Held Back/);
});
