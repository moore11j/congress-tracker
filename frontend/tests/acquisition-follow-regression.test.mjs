import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

function load(file, modules = {}, globals = {}) {
  const exports = {};
  const js = ts.transpileModule(fs.readFileSync(file, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
  }).outputText;
  vm.runInNewContext(js, { exports, require: name => modules[name], URL, URLSearchParams, console, ...globals });
  return exports;
}

const returns = load("lib/returnPaths.ts");

test("follow intent survives encoded registration and Google return paths with existing filters", () => {
  const path = returns.tickerFollowReturnPath("/ticker/BRK.B", "range=90&utm_source=reddit&utm_content=post_1");
  for (const mode of ["register", "login"]) {
    const auth = new URL(`/login?mode=${mode}&return_to=${encodeURIComponent(path)}`, "https://app.walnutmarkets.com");
    const afterAuth = new URL(returns.safeAppReturnPath(auth.searchParams.get("return_to")), auth.origin);
    assert.equal(afterAuth.pathname, "/ticker/BRK.B");
    assert.equal(afterAuth.searchParams.get("follow"), "1");
    assert.equal(afterAuth.searchParams.get("range"), "90");
    assert.equal(afterAuth.searchParams.get("utm_content"), "post_1");
  }
  const repeated = returns.tickerFollowReturnPath("/ticker/AAPL", "follow=0&follow=1");
  assert.equal(new URL(repeated, "https://app.walnutmarkets.com").searchParams.getAll("follow").length, 1);
  assert.equal(returns.tickerFollowReturnPath("//outside.example/", ""), returns.defaultPostLoginPath);
});

function context({ referrer = "", search = "", shared = "" } = {}) {
  const window = { location: { hostname: "app.walnutmarkets.com", search }, crypto: { randomUUID: () => "test-session" } };
  const document = { cookie: shared, referrer };
  return load("lib/analyticsContext.ts", {
    "./analyticsEnvironment": { isProductionAnalyticsHost: () => false },
    "./privacyConsent": { hasPrivacyConsent: () => true },
  }, { window, document });
}

test("Google authentication and tools are not organic search; actual search remains organic", () => {
  for (const [referrer, expected] of [
    ["https://accounts.google.com/o/oauth2/v2/auth", "direct"],
    ["https://search.google.com/search-console", "referral"],
    ["https://tagassistant.google.com/", "referral"],
    ["https://www.google.com/search?q=AAPL", "google_organic"],
    ["https://www.google.co.uk/search?q=AAPL", "google_organic"],
    ["https://www.reddit.com/r/stocks/", "reddit"],
    ["https://google.com.evil.example/", "referral"],
  ]) assert.equal(context({ referrer }).acquisitionProperties().acquisition_source, expected);
});

test("post-level attribution survives the OAuth return in the shared acquisition cookie", () => {
  const entry = context({ search: "?utm_source=reddit&utm_medium=organic_social&utm_campaign=research&utm_content=nbis_post_1" }).acquisitionProperties();
  const shared = `walnut_acquisition=${encodeURIComponent(JSON.stringify(entry))}; ct_analytics_sid=test-session`;
  const callback = context({ referrer: "https://accounts.google.com/", shared });
  callback.setAnalyticsIdentity({ id: 42, current_plan: "free" });
  assert.equal(callback.acquisitionProperties().acquisition_source, "reddit");
  assert.equal(callback.acquisitionProperties().utm_content, "nbis_post_1");
  assert.equal(callback.analyticsSessionId(), "test-session");
  assert.equal(context({ search: "?utm_content=name%40example.com" }).acquisitionProperties().utm_content, null);
});
