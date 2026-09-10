import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

function load(file, modules = {}, globals = {}) {
  const exports = {};
  const js = ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
  vm.runInNewContext(js, { exports, require: name => modules[name], URL, URLSearchParams, Date, console, setTimeout, clearTimeout, ...globals });
  return exports;
}

test("installed HeyCatch server SDK serializes the exact canonical business event", () => {
  const script = `
    const {gunzipSync}=await import("node:zlib");
    const calls=[];
    globalThis.fetch=async(url,options)=>{const bytes=Buffer.from(await new Response(options.body).arrayBuffer()); const text=(bytes[0]===31&&bytes[1]===139?gunzipSync(bytes):bytes).toString(); calls.push({url:String(url),body:JSON.parse(text)});return new Response('{"status":1}',{status:200});};
    const {analytics}=await import('@heycatch/sdk/server');
    analytics.init({projectKey:'hck_pk_readiness_fixture_only'});
    await analytics.trackEvent('subscription_completed',{current_plan:'premium',acquisition_source:'reddit'},{userId:'42'});
    console.log('WIRE:'+JSON.stringify(calls));
  `;
  const child = spawnSync(process.execPath, ["--input-type=module", "-e", script], { encoding: "utf8", timeout: 15000 });
  assert.equal(child.status, 0, child.stderr);
  const line = child.stdout.split("\n").find(line => line.startsWith("WIRE:"));
  const calls = JSON.parse(line.slice(5));
  assert.equal(calls.length, 1, child.stderr);
  const body = calls[0].body;
  const event = body.batch?.[0] || body;
  assert.equal(event.event, "subscription_completed");
  assert.equal(event.distinct_id ?? event.properties.distinct_id, "42");
  assert.equal(event.properties.heycatch_custom_user_event, true);
  assert.equal(event.properties.heycatch_project_key, "hck_pk_readiness_fixture_only");
  assert.equal(event.properties.current_plan, "premium");
  assert.equal(event.properties.$groups.project, "hck_pk_readiness_fixture_only");
});

test("installed browser SDK preserves custom event names and tags them for discovery", () => {
  // Execute the actual bundled trackEvent body with a capture spy, not Walnut's facade.
  const sdk = fs.readFileSync("node_modules/@heycatch/sdk/dist/index.js", "utf8");
  const body = sdk.match(/function al\(t,e,r\)\{([\s\S]*?)\}function ul/)[1];
  const calls = [];
  const send = vm.runInNewContext(`(function(t,e,r){${body}})`, { ft: () => true, Re: () => true, Fe: { capture: (...args) => calls.push(args) }, Ih: () => ({}), console });
  for (const name of ["homepage_viewed", "screener_opened", "ticker_viewed", "leaderboard_viewed", "pricing_viewed", "signup_started", "signup_completed", "checkout_started"]) {
    send(name, { route: "/fixture", authenticated: false });
    const [actual, props] = calls.at(-1);
    assert.equal(actual, name);
    assert.equal(props.heycatch_custom_user_event, true);
    assert.equal(props.route, "/fixture");
  }
});

test("GA supported getters provide bounded consented linkage without invented IDs", async () => {
  let consent = true;
  const module = load("lib/googleAnalytics.ts", {
    "@/lib/analyticsEnvironment": { isProductionAnalyticsHost: () => true },
    "@/lib/privacyConsent": { hasPrivacyConsent: () => consent },
  }, { window: { gtag: (command, id, field, callback) => { assert.equal(command, "get"); callback(field === "client_id" ? "123.456" : 1789010000); } } });
  assert.equal(JSON.stringify(await module.getGoogleAnalyticsContext()), '{"client_id":"123.456","session_id":"1789010000"}');
  consent = false;
  assert.equal(await module.getGoogleAnalyticsContext(), undefined);
});

test("signed Node bridge rejects tampering and requests a durable backend claim before SDK send", async () => {
  const secret = "qa-only-secret-".repeat(3), calls = [];
  const module = load("app/api/internal/paid-analytics/route.ts", {
    "node:crypto": await import("node:crypto"),
    "@heycatch/sdk/server": { analytics: { init() {}, trackEvent: async (...args) => calls.push(args) } },
  }, { process: { env: { NODE_ENV: "production", ANALYTICS_FORWARDING_SECRET: secret, NEXT_PUBLIC_HEYCATCH_PROJECT_KEY: "hck_pk_readiness_fixture_only" } },
    Buffer, Response, AbortSignal,
    fetch: async () => calls.length ? new Response(null, { status: 204 }) : Response.json({ user_id: "42", properties: { current_plan: "premium" } }),
  });
  const body = '{"event_id":1}', timestamp = String(Math.floor(Date.now() / 1000));
  const sig = createHmac("sha256", secret).update(`${timestamp}.dispatch.${body}`).digest("hex");
  const request = (signature, text = body) => new Request("https://app.walnutmarkets.com/api/internal/paid-analytics", { method: "POST", body: text,
    headers: { "x-walnut-timestamp": timestamp, "x-walnut-signature": signature } });
  assert.equal((await module.POST(request("invalid"))).status, 401);
  assert.equal((await module.POST(request(sig, '{"event_id":2}'))).status, 401);
  assert.equal((await module.POST(request(sig))).status, 200);
  assert.equal((await module.POST(request(sig))).status, 204);
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0], "subscription_completed");
  assert.equal(calls[0][2].userId, "42");
});

test("production-domain cookie contract survives host hops, auth, and inactivity", () => {
  let now = 1789010000000, counter = 0;
  const jar = [], writes = [];
  class Clock extends Date { static now() { return now; } }
  function surface(host, search = "") {
    const url = new URL(`https://${host}/${search}`), storage = new Map();
    const window = { location: url, crypto: { randomUUID: () => `session-${++counter}` }, localStorage: { getItem: k => storage.get(k) ?? null, setItem: (k,v) => storage.set(k,v) }, dispatchEvent() {} };
    const document = { referrer: "" };
    Object.defineProperty(document, "cookie", {
      get: () => jar.filter(c => c.expires > now && (c.hostOnly ? c.domain === host : host === c.domain || host.endsWith(`.${c.domain}`))).map(c => `${c.name}=${c.value}`).join("; "),
      set: raw => {
        writes.push(raw);
        const [pair,...parts] = raw.split("; "), eq = pair.indexOf("=");
        const attrs = Object.fromEntries(parts.map(p => { const [k,...v] = p.split("="); return [k.toLowerCase(),v.join("=")]; }));
        const domain = attrs.domain || host, name = pair.slice(0,eq), hostOnly = !attrs.domain;
        const prior = jar.findIndex(c => c.name === name && c.domain === domain && c.hostOnly === hostOnly);
        if (prior !== -1) jar.splice(prior,1);
        jar.push({ name, value: pair.slice(eq+1), domain, hostOnly, expires: now + Number(attrs["max-age"]) * 1000 });
      },
    });
    const privacy = load("lib/privacyConsent.ts", {}, { window, document, navigator: {}, Event: class {}, Date: Clock });
    const context = load("lib/analyticsContext.ts", { "./analyticsEnvironment": { isProductionAnalyticsHost: () => true }, "./privacyConsent": privacy }, { window, document, Date: Clock });
    return { context, privacy, window };
  }
  const marketing = surface("walnutmarkets.com", "?utm_source=reddit&utm_campaign=test");
  marketing.privacy.writePrivacyConsent({ analytics: true, marketing: false });
  const source = marketing.context.acquisitionProperties(), id = marketing.context.analyticsSessionId();
  const app = surface("app.walnutmarkets.com");
  assert.equal(app.privacy.hasPrivacyConsent("analytics"), true);
  assert.equal(app.context.analyticsSessionId(), id);
  assert.equal(app.context.acquisitionProperties().utm_campaign, source.utm_campaign);
  app.context.setAnalyticsIdentity({ id: 42, current_plan: "premium" });
  app.window.location.pathname = "/pricing";
  assert.equal(app.context.analyticsSessionId(), id);
  assert.equal(app.context.acquisitionProperties().acquisition_source, "reddit");
  const acquisitions = writes.filter(w => w.startsWith("walnut_acquisition="));
  assert.ok(acquisitions.every(w => /Domain=walnutmarkets.com; Max-Age=1800; SameSite=Lax; Secure/.test(w)));
  now += 1801000;
  assert.equal(app.context.acquisitionProperties().acquisition_source, "direct");
  assert.notEqual(app.context.analyticsSessionId(), id);
  app.privacy.writePrivacyConsent({ analytics: false, marketing: false });
  assert.equal(marketing.privacy.hasPrivacyConsent("analytics"), false);
  assert.equal(marketing.context.analyticsSessionId(), "");
  assert.ok(!jar.some(c => c.name === "ct_analytics_sid" && c.expires > now));
});


test("visible impressions wait for route readiness across rerenders and A-B-A", () => {
  const events = [];
  const module = load("lib/analyticsVisit.ts", {}, { window: { dispatchEvent: event => events.push(event.type) }, Event });
  module.beginAnalyticsVisit("/leaderboards");
  assert.equal(module.isAnalyticsVisitReady("/leaderboards"), false);
  module.finishAnalyticsVisit("/leaderboards");
  assert.equal(module.isAnalyticsVisitReady("/leaderboards"), true);
  module.beginAnalyticsVisit("/leaderboards");
  assert.equal(module.isAnalyticsVisitReady("/leaderboards"), true);
  module.beginAnalyticsVisit("/pricing");
  module.finishAnalyticsVisit("/leaderboards"); // a cancelled old lookup cannot release impressions
  assert.equal(module.isAnalyticsVisitReady("/pricing"), false);
  module.finishAnalyticsVisit("/pricing");
  module.beginAnalyticsVisit("/leaderboards");
  assert.equal(module.isAnalyticsVisitReady("/leaderboards"), false);
  module.finishAnalyticsVisit("/leaderboards");
  assert.equal(events.length, 3);
});
