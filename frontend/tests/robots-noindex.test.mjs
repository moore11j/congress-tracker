import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const robots = fs.readFileSync(path.join(root, "public/robots.txt"), "utf8");
const marketingMetadata = fs.readFileSync(path.join(root, "lib/marketingMetadata.ts"), "utf8");
const publicSeoRoutes = [
  "/congress-trades",
  "/insider-trading-tracker",
  "/government-contracts",
  "/institutional-filings",
  "/stock-confirmation-score",
  "/market-intelligence-terminal",
];
const publicLandingRoutes = [
  "/",
  "/landing",
  "/about",
  "/pricing",
  "/faq",
  "/terms",
  "/privacy",
  ...publicSeoRoutes,
];

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const disallowedRoutes = [
  "/insider/",
  "/member/",
  "/institution/",
  "/signals",
  "/screener",
  "/watchlists",
  "/monitoring",
  "/feed",
  "/account",
  "/billing",
  "/admin",
];

test("app and marketing robots disallow private terminal app routes", () => {
  assert.match(middleware, /function robotsTxtResponse\(host: string\)/);
  assert.match(middleware, /pathname === "\/robots\.txt"/);
  assert.match(middleware, /publicLandingHosts\.has\(host\)/);

  for (const route of disallowedRoutes) {
    assert.match(middleware, new RegExp(`"${route.replaceAll("/", "\\/")}"`));
    assert.match(robots, new RegExp(`Disallow: ${route.replaceAll("/", "\\/")}`));
  }
});

test("marketing robots keep marketing and public ticker pages indexable", () => {
  assert.match(middleware, /Allow: \//);
  assert.match(middleware, /Allow: \/landing/);
  assert.match(middleware, /Allow: \/about/);
  assert.match(middleware, /Allow: \/pricing/);
  assert.match(middleware, /Allow: \/faq/);
  assert.match(middleware, /Allow: \/terms/);
  assert.match(middleware, /Allow: \/privacy/);
  for (const route of publicSeoRoutes) {
    assert.match(middleware, new RegExp(`Allow: ${route}`));
  }
  assert.match(middleware, /Allow: \/ticker\//);
  assert.match(middleware, /Sitemap: https:\/\/walnutmarkets\.com\/sitemap\.xml/);
  assert.match(robots, /Allow: \/landing/);
  assert.match(robots, /Allow: \/about/);
  assert.match(robots, /Allow: \/pricing/);
  assert.match(robots, /Allow: \/terms/);
  assert.match(robots, /Allow: \/privacy/);
  for (const route of publicSeoRoutes) {
    assert.match(robots, new RegExp(`Allow: ${route}`));
  }
  assert.match(robots, /Allow: \/ticker\//);
  assert.doesNotMatch(robots, /Disallow: \/ticker\//);
});

test("landing pages are not noindexed or disallowed", () => {
  const noindexListMatch = middleware.match(/const noindexAppRoutePrefixes = \[([\s\S]*?)\];/);
  assert.ok(noindexListMatch, "middleware should define noindex app route prefixes");
  const noindexList = noindexListMatch[1];

  const disallowListMatch = middleware.match(/const robotsDisallowPaths = \[([\s\S]*?)\];/);
  assert.ok(disallowListMatch, "middleware should define robots disallow paths");
  const disallowList = disallowListMatch[1];

  for (const route of publicLandingRoutes.filter((route) => route !== "/")) {
    const routePattern = new RegExp(`"${escapeRegex(route)}\\/?",?`);
    assert.doesNotMatch(noindexList, routePattern, `${route} should not be in noindex app route prefixes`);
    assert.doesNotMatch(disallowList, routePattern, `${route} should not be in robots disallow paths`);
  }

  for (const route of publicLandingRoutes) {
    assert.doesNotMatch(robots, new RegExp(`Disallow: ${escapeRegex(route)}(?:\\r?\\n|$)`), `${route} should not be disallowed`);
  }

  assert.doesNotMatch(middleware, /<meta name="robots" content="noindex">/);
  assert.match(middleware, /pathname === "\/" && publicLandingHosts\.has\(host\)/);
  assert.doesNotMatch(marketingMetadata, /index:\s*false/);
  assert.match(marketingMetadata, /robots:\s*{\s*index:\s*true,\s*follow:\s*true,/);
});

test("app terminal routes receive noindex without blocking real users", () => {
  assert.match(middleware, /const noindexAppRoutePrefixes = \[/);
  assert.match(middleware, /host === appHost && isNoindexAppRoute\(pathname\)/);
  assert.match(middleware, /function withNoindex\(response: NextResponse\): NextResponse/);
  assert.match(middleware, /response\.headers\.set\("x-robots-tag", "noindex, nofollow"\)/);
  assert.match(middleware, /return shouldNoindex \? withNoindex\(response\) : response/);
  assert.match(middleware, /if \(!protectedRoute \|\| hasBackendSession \|\| hasAuthHint\)/);
});

test("legacy marketing domains redirect permanently and public ticker pages remain crawlable", () => {
  assert.match(middleware, /const publicStaticPaths = new Set\(\[/);
  for (const route of publicSeoRoutes) {
    assert.match(middleware, new RegExp(`"${route}"`));
  }
  assert.match(middleware, /legacyMarketingHosts = new Set\(\["walnut-intel\.com", "www\.walnut-intel\.com", "www\.walnutmarkets\.com"\]\)/);
  assert.match(middleware, /return NextResponse\.redirect\(canonicalUrl, 301\)/);
  assert.match(middleware, /canonicalUrl\.hostname = canonicalMarketingHost/);
  assert.match(middleware, /canonicalUrl\.port = ""/);
  assert.match(middleware, /matcher: \["\/\(\(\?!_next\/static\|_next\/image\|favicon\.ico\|apple-icon\.png\|icon\.png\)\.\*\)"\]/);
  assert.match(middleware, /function isPublicTickerRoute\(pathname: string\): boolean/);
  assert.match(middleware, /function isPublicMarketingAsset\(pathname: string\): boolean/);
  assert.match(middleware, /canonicalMarketingHosts\.has\(host\) && isPublicMarketingAsset\(pathname\)/);
  assert.match(middleware, /canonicalMarketingHosts\.has\(host\) && isPublicTickerRoute\(pathname\)/);
  assert.match(middleware, /const shouldNoindex = host === appHost && isNoindexAppRoute\(pathname\)/);
  assert.match(middleware, /publicLandingHosts\.has\(host\) && !publicStaticPaths\.has\(pathname\) && !isPublicResearchRoute\(pathname\) && !publicAccountPaths\.has\(pathname\)/);
  assert.match(middleware, /appUrl\.host = appHost/);
  assert.match(middleware, /return NextResponse\.redirect\(appUrl, 307\)/);
  assert.match(middleware, /isTerminalRoute\(pathname\) && !isPublicTickerRoute\(pathname\) && !hasBackendSession/);
});
