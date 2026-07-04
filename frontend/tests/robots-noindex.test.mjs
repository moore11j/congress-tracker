import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const robots = fs.readFileSync(path.join(root, "public/robots.txt"), "utf8");

const disallowedRoutes = [
  "/ticker/",
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

test("app and marketing robots disallow terminal app routes", () => {
  assert.match(middleware, /function robotsTxtResponse\(host: string\)/);
  assert.match(middleware, /pathname === "\/robots\.txt"/);
  assert.match(middleware, /publicLandingHosts\.has\(host\)/);

  for (const route of disallowedRoutes) {
    assert.match(middleware, new RegExp(`"${route.replaceAll("/", "\\/")}"`));
    assert.match(robots, new RegExp(`Disallow: ${route.replaceAll("/", "\\/")}`));
  }
});

test("marketing robots keep marketing pages indexable", () => {
  assert.match(middleware, /Allow: \//);
  assert.match(middleware, /Allow: \/pricing/);
  assert.match(middleware, /Allow: \/faq/);
  assert.match(middleware, /Allow: \/terms/);
  assert.match(middleware, /Allow: \/privacy/);
  assert.match(middleware, /Sitemap: https:\/\/walnutmarkets\.com\/sitemap\.xml/);
  assert.match(robots, /Allow: \/pricing/);
  assert.match(robots, /Allow: \/terms/);
  assert.match(robots, /Allow: \/privacy/);
});

test("app terminal routes receive noindex without blocking real users", () => {
  assert.match(middleware, /const noindexAppRoutePrefixes = \[/);
  assert.match(middleware, /host === appHost && isNoindexAppRoute\(pathname\)/);
  assert.match(middleware, /function withNoindex\(response: NextResponse\): NextResponse/);
  assert.match(middleware, /response\.headers\.set\("x-robots-tag", "noindex, nofollow"\)/);
  assert.match(middleware, /return shouldNoindex \? withNoindex\(response\) : response/);
  assert.match(middleware, /if \(!protectedRoute \|\| hasBackendSession \|\| hasAuthHint\)/);
});

test("marketing pages are not accidentally noindexed and public ticker redirects remain", () => {
  assert.match(middleware, /publicStaticPaths = new Set\(\["\/landing", "\/pricing", "\/terms", "\/privacy", "\/faq"\]\)/);
  assert.match(middleware, /const shouldNoindex = host === appHost && isNoindexAppRoute\(pathname\)/);
  assert.match(middleware, /publicLandingHosts\.has\(host\) && !publicStaticPaths\.has\(pathname\) && !publicAccountPaths\.has\(pathname\)/);
  assert.match(middleware, /appUrl\.host = appHost/);
  assert.match(middleware, /return NextResponse\.redirect\(appUrl, 307\)/);
});
