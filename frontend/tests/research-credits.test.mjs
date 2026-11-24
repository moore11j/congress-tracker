import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import { createRequire } from "node:module";
import test from "node:test";
import ts from "typescript";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);
function load(file, modules = {}, globals = {}) {
  const exports = {};
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(file, "utf8"), {compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX,
  }}).outputText, {exports, URL, Headers, AbortSignal, AbortController, process:{env:{}}, ...globals,
    require: name => name in modules ? modules[name] : require(name)});
  return exports;
}
const credits = load("lib/researchCredits.ts");
const balanceHook = {useResearchCreditBalance: () => ({balance:null,status:"Balance not connected"}), refreshResearchCredits() {}};
test("custom amounts use exact cents and reject ambiguous or out of range inputs", () => {
  for (const [text, cents] of [["5",500], ["10.02",1002], [" 12.5 ",1250], ["1000",100000]])
    assert.equal(credits.creditAmountCents(text), cents);
  for (const text of ["", "5.01", "10.01", "4.99", "1000.01", "1e2", "NaN", "-5", "5.001", "5,00", "$5", "5."])
    assert.equal(credits.creditAmountCents(text), null);
});
test("credit proxy requires its own sandbox host and never falls back to the live API", () => {
  const env = {WALNUT_CREDITS_SANDBOX_ENABLED:"1", WALNUT_CREDITS_SANDBOX_API_URL:"https://sandbox.example",
    API_BASE_URL:"https://congress-tracker-api.fly.dev"};
  assert.equal(credits.creditSandboxBackend(env), "https://sandbox.example");
  assert.equal(credits.creditSandboxBackend({...env, VERCEL_ENV:"production"}), null);
  assert.equal(credits.creditSandboxBackend({...env, WALNUT_CREDITS_SANDBOX_ENABLED:"0"}), null);
  for (const host of ["", "https://congress-tracker-api.fly.dev", "https://app.walnutmarkets.com", "https://user:pass@sandbox.example", "https://sandbox.example/api", "https://sandbox.example?x=1", "http://sandbox.example"])
    assert.equal(credits.creditSandboxBackend({...env, WALNUT_CREDITS_SANDBOX_API_URL:host}), null);
});
test("preview renders honest unconfirmed balance, existing plan prices and disabled checkout", () => {
  const {ResearchCreditsPanel} = load("components/chat/ResearchCreditsPanel.tsx", {
    "next/link":{default:({children,...props})=>React.createElement("a",props,children)},
    "@/lib/researchCredits":credits, "@/lib/useResearchCreditBalance":balanceHook,
  });
  const html = renderToStaticMarkup(React.createElement(ResearchCreditsPanel));
  for (const text of ["Billing preview", "500 credits for $10.00", "$40 at one-time credit prices", "250 credits in your first month", "Then 100 credits each month", "$80 at one-time credit prices", "25 credits", "100 credits", "400 credits", "$24.95", "$39.95", "$249.50", "$399.95", "Test payments only", "No live subscription changes", "Subscription credits require configured Stripe webhooks"])
    assert.ok(html.includes(text), text);
  assert.match(html, /disabled=""[^>]*>Test checkout/);
  assert.doesNotMatch(html, /100 available|1,500 available|4,000 available/);
});
test("composer quotes each selected task, describes scope and never invents a balance", () => {
  const {ResearchCreditQuote} = load("components/chat/ResearchCreditQuote.tsx", {"@/lib/researchCredits":credits,"@/lib/useResearchCreditBalance":balanceHook});
  for (const [mode,cost] of [["browse",0],["answer",25],["analysis",100],["report",400]]) {
    const html=renderToStaticMarkup(React.createElement(ResearchCreditQuote,{mode,onModeChange(){}}));
    assert.ok(html.includes(cost ? `${cost} credits per task` : "0 credits"));
    if (cost) {
      assert.ok(html.includes("Balance not connected"));
      assert.ok(html.includes("one brief clarification"));
      assert.ok(html.includes("sending is disabled"));
      assert.doesNotMatch(html,/\d+ credits available/);
    }
  }
});
function proxy(env, fetcher) {
  class Response {
    constructor(body, init) {this.body=body;this.status=init.status;this.headers=init.headers;}
    static json(body, init) {return new Response(body, init);}
  }
  return load("app/api/ask/credits/[action]/route.ts", {
    "next/server":{NextResponse:Response}, "@/lib/researchCredits":credits, "@/lib/useResearchCreditBalance":balanceHook,
  }, {process:{env},fetch:fetcher});
}
const request = (method="GET", origin="https://preview.example", body="{}") => ({method,
  headers:new Headers({origin,cookie:"sandbox_session=fixture"}),
  nextUrl:new URL("https://preview.example/api/ask/credits/checkout"),text:async()=>body});
test("unconfigured, disallowed and cross-origin credit routes make no upstream requests", async () => {
  let calls=0; const fetcher=async()=>{calls++;throw Error("unexpected fetch");};
  const env={WALNUT_CREDITS_SANDBOX_ENABLED:"1",WALNUT_CREDITS_SANDBOX_API_URL:"https://sandbox.example"};
  assert.equal((await proxy({},fetcher).GET(request(),{params:Promise.resolve({action:"balance"})})).status,503);
  assert.equal((await proxy(env,fetcher).POST(request("POST","https://evil.example"),{params:Promise.resolve({action:"checkout"})})).status,403);
  assert.equal((await proxy(env,fetcher).POST(request("POST"),{params:Promise.resolve({action:"grant"})})).status,404);
  assert.equal((await proxy(env,fetcher).POST(request("POST",undefined,"x".repeat(2001)),{params:Promise.resolve({action:"checkout"})})).status,413);
  assert.equal(calls,0);
});
test("sandbox proxy forwards only to explicit sandbox and rejects redirects", async () => {
  const env={WALNUT_CREDITS_SANDBOX_ENABLED:"1",WALNUT_CREDITS_SANDBOX_API_URL:"https://sandbox.example"};
  let call;
  const result=await proxy(env,async(url,options)=>{call={url,options};return {status:200,text:async()=>'{"sandbox":true}'};})
    .GET(request(),{params:Promise.resolve({action:"balance"})});
  assert.equal(result.status,200);
  assert.equal(call.url,"https://sandbox.example/api/research-credits/balance");
  assert.equal(call.options.redirect,"error"); assert.equal(call.options.cache,"no-store");
  assert.equal(call.options.headers.get("cookie"),"sandbox_session=fixture");
  assert.equal(result.headers["cache-control"],"no-store");
});
