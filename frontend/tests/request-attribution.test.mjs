import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const api = fs.readFileSync(path.join(root, "lib/api.ts"), "utf8");

test("api requests propagate safe walnut attribution headers", () => {
  assert.match(api, /headers\.set\("X-Walnut-Route"/);
  assert.match(api, /headers\.set\("X-Walnut-Page-Route"/);
  assert.match(api, /headers\.set\("X-Walnut-Component"/);
  assert.match(api, /headers\.set\("X-Walnut-Panel"/);
  assert.match(api, /headers\.set\("X-Walnut-Route-Family"/);
  assert.match(api, /headers\.set\("X-Walnut-Request-Source"/);
});

test("api attribution does not add raw auth or cookie headers", () => {
  const attributionBlock = api.slice(api.indexOf("function withRequestAttribution"), api.indexOf("function requestInitWithEntitlements"));
  assert.doesNotMatch(attributionBlock, /Cookie/i);
  assert.doesNotMatch(attributionBlock, /Authorization/i);
  assert.doesNotMatch(attributionBlock, /token/i);
});

test("api attribution classifies major route families", () => {
  assert.match(api, /startsWith\("\/api\/market\/quotes"\).*market_quotes/);
  assert.match(api, /startsWith\("\/api\/tickers\/"\).*ticker/);
  assert.match(api, /startsWith\("\/api\/insiders\/"\).*insider/);
  assert.match(api, /startsWith\("\/api\/members\/"\).*member/);
  assert.match(api, /startsWith\("\/api\/institutions\/"\).*institution/);
  assert.match(api, /startsWith\("\/api\/events"\).*feed/);
});

test("api attribution supports ticker route-family overrides and rejects JSON 204", () => {
  assert.match(api, /routeFamily\?: string/);
  assert.match(api, /_routeFamily \? safeHeaderValue\(_routeFamily\) : url \? routeFamilyFromUrl\(url\) : routeFamilyFromPath\(attribution\.route\)/);
  assert.match(api, /if \(response\.status === 204\) \{\s*throw new ApiError\(\{ status: response\.status/s);
});
