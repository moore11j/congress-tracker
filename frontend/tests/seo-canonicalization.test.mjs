import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const marketingMetadata = fs.readFileSync(path.join(root, "lib/marketingMetadata.ts"), "utf8");
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const sitemap = fs.readFileSync(path.join(root, "public/sitemap.xml"), "utf8");
const robots = fs.readFileSync(path.join(root, "public/robots.txt"), "utf8");
const seoRoutes = [
  "/stock-research-app",
  "/stock-analysis-tools",
  "/congress-trades",
  "/insider-trading-tracker",
  "/government-contracts",
  "/institutional-filings",
  "/stock-confirmation-score",
];

function readAppPage(route) {
  return fs.readFileSync(path.join(root, "app", route, "page.tsx"), "utf8");
}

test("marketing metadata uses non-www HTTPS canonicals", () => {
  assert.match(marketingMetadata, /WALNUT_MARKETING_URL = "https:\/\/walnutmarkets\.com"/);
  assert.match(marketingMetadata, /WALNUT_APP_URL = "https:\/\/app\.walnutmarkets\.com"/);
  assert.match(marketingMetadata, /new URL\(normalizedPath, `\$\{WALNUT_MARKETING_URL\}\/`\)\.toString\(\)/);
  assert.match(marketingMetadata, /canonical: marketingCanonicalUrl\("\/"\)/);
  assert.match(marketingMetadata, /url: marketingCanonicalUrl\("\/"\)/);
  assert.match(marketingMetadata, /function appCanonicalUrl\(pathname: string\)/);
  assert.doesNotMatch(marketingMetadata, /https?:\/\/www\.walnutmarkets\.com/);
});

test("app-owned information pages define self-referencing app canonical metadata", () => {
  for (const route of ["about", "faq", "pricing", "terms", "privacy", "contact"]) {
    assert.match(readAppPage(route), new RegExp(`appPageMetadata\\("/${route}"`));
  }
  for (const route of seoRoutes) {
    assert.match(readAppPage(route.slice(1)), /marketingSeoPageMetadata\(page\.pathname/);
  }
  assert.match(readAppPage("market-intelligence-terminal"), /permanentRedirect\("\/"\)/);
});

test("sitemap contains canonical URLs and no www or http variants", () => {
  const urls = Array.from(sitemap.matchAll(/<loc>([^<]+)<\/loc>/g), (match) => match[1]);
  assert.ok(urls.includes("https://walnutmarkets.com/"));
  for (const route of ["about", "faq", "pricing", "terms", "privacy"]) {
    assert.ok(urls.includes(`https://app.walnutmarkets.com/${route}`));
    assert.ok(!urls.includes(`https://walnutmarkets.com/${route}`));
  }
  for (const route of seoRoutes) {
    assert.ok(urls.includes(`https://walnutmarkets.com${route}`));
  }
  assert.ok(!urls.includes("https://walnutmarkets.com/market-intelligence-terminal"));
  assert.ok(urls.every((url) => url.startsWith("https://walnutmarkets.com/") || url.startsWith("https://app.walnutmarkets.com/")));
  assert.doesNotMatch(sitemap, /https?:\/\/www\.walnutmarkets\.com/);
  assert.doesNotMatch(sitemap, /http:\/\/walnutmarkets\.com/);
});

test("robots points crawlers to the canonical sitemap without blocking marketing pages", () => {
  assert.match(robots, /Sitemap: https:\/\/walnutmarkets\.com\/sitemap\.xml/);
  assert.doesNotMatch(robots, /Disallow: \/$/m);
  for (const route of ["/faq", "/pricing", "/terms", "/privacy"]) {
    assert.doesNotMatch(robots, new RegExp(`Disallow: ${route}`));
  }
  for (const route of seoRoutes) {
    assert.doesNotMatch(robots, new RegExp(`Disallow: ${route}`));
  }
});

test("obsolete market intelligence terminal route redirects permanently to homepage", () => {
  assert.match(middleware, /pathname === "\/market-intelligence-terminal"/);
  assert.match(middleware, /canonicalUrl\.pathname = "\/"/);
  assert.match(middleware, /canonicalUrl\.search = ""/);
  assert.match(middleware, /return NextResponse\.redirect\(canonicalUrl, 308\)/);
});

test("http and www marketing requests redirect permanently while preserving path and query", () => {
  assert.match(middleware, /legacyMarketingHosts = new Set\(\["walnut-intel\.com", "www\.walnut-intel\.com", "www\.walnutmarkets\.com"\]\)/);
  assert.match(middleware, /legacyAppHosts = new Set\(\["app\.walnut-intel\.com"\]\)/);
  assert.match(middleware, /const forwardedProto = request\.headers\.get\("x-forwarded-proto"\)/);
  assert.match(middleware, /const requestProto = forwardedProto \|\| request\.nextUrl\.protocol\.replace\(/);
  assert.match(middleware, /host === canonicalMarketingHost && requestProto === "http"/);

  const redirectBlock = middleware.match(/if \(legacyMarketingHosts\.has\(host\) \|\| isHttpCanonicalMarketingRequest\) \{[\s\S]*?return NextResponse\.redirect\(canonicalUrl, 301\);[\r\n\s]*\}/)?.[0] ?? "";
  assert.match(redirectBlock, /const canonicalUrl = request\.nextUrl\.clone\(\)/);
  assert.match(redirectBlock, /canonicalUrl\.protocol = "https:"/);
  assert.match(redirectBlock, /canonicalUrl\.hostname = canonicalMarketingHost/);
  assert.match(redirectBlock, /canonicalUrl\.port = ""/);
  assert.doesNotMatch(redirectBlock, /canonicalUrl\.(pathname|search) =/);
});

test("legacy app domain redirects permanently to canonical app host", () => {
  const redirectBlock = middleware.match(/if \(legacyAppHosts\.has\(host\)\) \{[\s\S]*?return NextResponse\.redirect\(canonicalUrl, 301\);[\r\n\s]*\}/)?.[0] ?? "";
  assert.match(redirectBlock, /canonicalUrl\.protocol = "https:"/);
  assert.match(redirectBlock, /canonicalUrl\.hostname = appHost/);
  assert.match(redirectBlock, /canonicalUrl\.port = ""/);
  assert.doesNotMatch(redirectBlock, /canonicalUrl\.(pathname|search) =/);
});
