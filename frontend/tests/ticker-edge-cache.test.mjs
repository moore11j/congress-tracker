import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("anonymous interactive ticker pages opt into a short edge cache without changing authenticated routes", () => {
  const middleware = readFileSync(join(process.cwd(), "middleware.ts"), "utf8");

  assert.match(middleware, /const publicTickerEdgeCacheControl = "public, s-maxage=60, stale-while-revalidate=300"/);
  assert.match(middleware, /function publicTickerEdgeCacheResponse\(\): NextResponse/);
  assert.match(middleware, /response\.headers\.set\("x-walnut-ticker-edge-cache", "public"\)/);
  assert.match(middleware, /isPublicTickerRoute\(pathname\)[\s\S]*!hasBackendSession[\s\S]*!hasAuthHint[\s\S]*!prefetch[\s\S]*isInteractiveBrowserUserAgent\(userAgent\)/);
});
