import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const robots = fs.readFileSync(path.join(root, "public/robots.txt"), "utf8");
const marketingMetadata = fs.readFileSync(path.join(root, "lib/marketingMetadata.ts"), "utf8");
const publicSeoRoutes = [
  "/stock-research-app",
  "/stock-analysis-tools",
  "/compare",
  "/congress-trades",
  "/insider-trading-tracker",
  "/government-contracts",
  "/institutional-filings",
  "/stock-confirmation-score",
];
const redirectedMarketingRoutes = ["/market-intelligence-terminal"];
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
  "/api/",
  "/account",
  "/billing",
  "/settings",
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
  for (const route of publicSeoRoutes) {
    assert.match(middleware, new RegExp(`"${route}"`));
  }
  for (const route of redirectedMarketingRoutes) {
    assert.match(middleware, new RegExp(`pathname === "${route}"`));
  }
  assert.match(middleware, /Sitemap: https:\/\/walnutmarkets\.com\/sitemap\.xml/);
  assert.match(middleware, /Sitemap: https:\/\/app\.walnutmarkets\.com\/sitemap-index\.xml/);
  assert.match(robots, /Allow: \//);
  for (const route of publicSeoRoutes) {
    assert.doesNotMatch(robots, new RegExp(`Disallow: ${route}`));
  }
  assert.doesNotMatch(robots, /Disallow: \/ticker\//);
  assert.doesNotMatch(robots, /Disallow: \/member\//);
  assert.doesNotMatch(robots, /Disallow: \/insider\//);
  assert.doesNotMatch(robots, /Disallow: \/institution\//);
  assert.doesNotMatch(robots, /Disallow: \/departments\//);
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

test("private app routes receive noindex without blocking real users", () => {
  assert.match(middleware, /const noindexAppRoutePrefixes = \[/);
  assert.match(middleware, /host === appHost && isNoindexAppRoute\(pathname\)/);
  assert.match(middleware, /function withNoindex\(response: NextResponse\): NextResponse/);
  assert.match(middleware, /response\.headers\.set\("x-robots-tag", "noindex, follow"\)/);
  assert.match(middleware, /return shouldNoindex \? withNoindex\(response\) : response/);
  assert.match(middleware, /if \(!protectedRoute \|\| hasBackendSession \|\| hasAuthHint\)/);
  assert.match(middleware, /if \(prefix === "\/"\) return normalized === "\/"/);
  const noindexList = middleware.match(/const noindexAppRoutePrefixes = \[([\s\S]*?)\];/)?.[1] ?? "";
  for (const publicRoute of ["/ticker/", "/member/", "/insider/", "/institution/", "/departments/"]) {
    assert.doesNotMatch(noindexList, new RegExp(`"${escapeRegex(publicRoute)}"`));
  }
});

test("legacy marketing domains redirect permanently and public ticker pages remain crawlable", () => {
  assert.match(middleware, /const publicStaticPaths = new Set\(\[/);
  for (const route of publicSeoRoutes) {
    assert.match(middleware, new RegExp(`"${route}"`));
  }
  assert.match(middleware, /legacyMarketingHosts = new Set\(\["walnut-intel\.com", "www\.walnut-intel\.com", "www\.walnutmarkets\.com"\]\)/);
  assert.match(middleware, /legacyAppHosts = new Set\(\["app\.walnut-intel\.com"\]\)/);
  assert.match(middleware, /return NextResponse\.redirect\(canonicalUrl, 301\)/);
  assert.match(middleware, /canonicalUrl\.hostname = canonicalMarketingHost/);
  assert.match(middleware, /canonicalUrl\.port = ""/);
  assert.match(middleware, /matcher: \["\/\(\(\?!_next\/static\|_next\/image\|favicon\.ico\|apple-icon\.png\|icon\.png\)\.\*\)"\]/);
  assert.match(middleware, /function isPublicTickerRoute\(pathname: string\): boolean/);
  assert.match(middleware, /function isPublicMarketingAsset\(pathname: string\): boolean/);
  assert.match(middleware, /canonicalMarketingHosts\.has\(host\) && isPublicMarketingAsset\(pathname\)/);
  assert.match(middleware, /canonicalMarketingHosts\.has\(host\) && isPublicTickerRoute\(pathname\)/);
  assert.match(middleware, /const shouldNoindex = host === appHost && isNoindexAppRoute\(pathname\)/);
  assert.match(middleware, /publicLandingHosts\.has\(host\) && !publicStaticPaths\.has\(pathname\) && !isPublicResearchRoute\(pathname\) && !isPublicComparisonRoute\(pathname\) && !publicAccountPaths\.has\(pathname\)/);
  assert.match(middleware, /appUrl\.host = appHost/);
  assert.match(middleware, /return NextResponse\.redirect\(appUrl, 307\)/);
  assert.match(middleware, /isTerminalRoute\(pathname\) && !isPublicTickerRoute\(pathname\) && !hasBackendSession/);
});
