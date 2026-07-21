import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const marketingMetadata = fs.readFileSync(path.join(root, "lib/marketingMetadata.ts"), "utf8");
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const sitemap = fs.readFileSync(path.join(root, "public/sitemap.xml"), "utf8");
const robots = fs.readFileSync(path.join(root, "public/robots.txt"), "utf8");

function readAppPage(route) {
  return fs.readFileSync(path.join(root, "app", route, "page.tsx"), "utf8");
}

test("marketing metadata uses non-www HTTPS canonicals", () => {
  assert.match(marketingMetadata, /WALNUT_MARKETING_URL = "https:\/\/walnutmarkets\.com"/);
  assert.match(marketingMetadata, /new URL\(normalizedPath, `\$\{WALNUT_MARKETING_URL\}\/`\)\.toString\(\)/);
  assert.match(marketingMetadata, /canonical: marketingCanonicalUrl\("\/"\)/);
  assert.match(marketingMetadata, /url: marketingCanonicalUrl\("\/"\)/);
  assert.doesNotMatch(marketingMetadata, /https?:\/\/www\.walnutmarkets\.com/);
});

test("public marketing pages define self-referencing canonical metadata", () => {
  assert.match(readAppPage("faq"), /marketingPageMetadata\("\/faq"/);
  assert.match(readAppPage("pricing"), /marketingPageMetadata\("\/pricing"/);
  assert.match(readAppPage("terms"), /marketingPageMetadata\("\/terms"/);
  assert.match(readAppPage("privacy"), /marketingPageMetadata\("\/privacy"/);
});

test("sitemap contains canonical URLs and no www or http variants", () => {
  const urls = Array.from(sitemap.matchAll(/<loc>([^<]+)<\/loc>/g), (match) => match[1]);
  assert.ok(urls.includes("https://walnutmarkets.com/"));
  assert.ok(urls.includes("https://walnutmarkets.com/faq"));
  assert.ok(urls.includes("https://walnutmarkets.com/pricing"));
  assert.ok(urls.includes("https://walnutmarkets.com/terms"));
  assert.ok(urls.includes("https://walnutmarkets.com/privacy"));
  assert.ok(urls.every((url) => url.startsWith("https://walnutmarkets.com/")));
  assert.doesNotMatch(sitemap, /https?:\/\/www\.walnutmarkets\.com/);
  assert.doesNotMatch(sitemap, /http:\/\/walnutmarkets\.com/);
});

test("robots points crawlers to the canonical sitemap without blocking marketing pages", () => {
  assert.match(robots, /Sitemap: https:\/\/walnutmarkets\.com\/sitemap\.xml/);
  assert.doesNotMatch(robots, /Disallow: \/$/m);
  for (const route of ["/faq", "/pricing", "/terms", "/privacy"]) {
    assert.match(robots, new RegExp(`Allow: ${route}`));
  }
});

test("http and www marketing requests redirect permanently while preserving path and query", () => {
  assert.match(middleware, /legacyMarketingHosts = new Set\(\["walnut-intel\.com", "www\.walnut-intel\.com", "www\.walnutmarkets\.com"\]\)/);
  assert.match(middleware, /const forwardedProto = request\.headers\.get\("x-forwarded-proto"\)/);
  assert.match(middleware, /const requestProto = forwardedProto \|\| request\.nextUrl\.protocol\.replace\(/);
  assert.match(middleware, /host === canonicalMarketingHost && requestProto === "http"/);

  const redirectBlock = middleware.match(/if \(legacyMarketingHosts\.has\(host\) \|\| isHttpCanonicalMarketingRequest\) \{[\s\S]*?return NextResponse\.redirect\(canonicalUrl, 301\);\n  \}/)?.[0] ?? "";
  assert.match(redirectBlock, /const canonicalUrl = request\.nextUrl\.clone\(\)/);
  assert.match(redirectBlock, /canonicalUrl\.protocol = "https:"/);
  assert.match(redirectBlock, /canonicalUrl\.hostname = canonicalMarketingHost/);
  assert.match(redirectBlock, /canonicalUrl\.port = ""/);
  assert.doesNotMatch(redirectBlock, /canonicalUrl\.(pathname|search) =/);
});
