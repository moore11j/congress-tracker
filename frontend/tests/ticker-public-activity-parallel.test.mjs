import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("anonymous ticker activity requests begin alongside the context bundle", () => {
  const page = readFileSync(join(process.cwd(), "app/ticker/[symbol]/page.tsx"), "utf8");

  const earlyRequests = page.indexOf("const earlyPublicActivityRequests = publicStalePageCache");
  const contextBundle = page.indexOf("const contextBundleResult = useAnonymousTickerSsrShell");
  assert.ok(earlyRequests >= 0, "public ticker activity requests should be defined");
  assert.ok(contextBundle > earlyRequests, "public activity requests should start before the context bundle is awaited");
  assert.match(page, /earlyPublicActivityRequests\?\.congress \?\?/);
  assert.match(page, /earlyPublicActivityRequests\?\.insider \?\?/);
  assert.match(page, /earlyPublicActivityRequests\?\.government \?\?/);
});
