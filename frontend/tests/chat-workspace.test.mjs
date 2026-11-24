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
  vm.runInNewContext(ts.transpileModule(fs.readFileSync(file, "utf8"), { compilerOptions: {
    module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX,
  } }).outputText, { exports, URL, URLSearchParams, AbortController, process: { env: {} }, ...globals,
    require: name => name in modules ? modules[name] : require(name),
  });
  return exports;
}
const ticker = load("lib/ticker.ts");
const navigation = load("lib/chatNavigation.ts", { "./ticker": ticker });
const nodes = tree => !tree || typeof tree !== "object" ? [] : Array.isArray(tree) ? tree.flatMap(nodes) : [tree, ...nodes(tree.props?.children)];

test("follow-up commands retain company context without pretending to answer an open question", () => {
  for (const [prompt, section] of [["Show financials", "financials"], ["Show the chart", "chart"], ["What are the risks?", "overview"], ["Show news", "news"], ["Open ownership", "ownership"]]) {
    const intent = navigation.chatIntent(prompt, true);
    assert.equal(intent.kind, "section"); assert.equal(intent.section, section);
    assert.equal(navigation.chatIntent(prompt, false).kind, "search");
  }
  assert.equal(navigation.chatIntent("Why is the business risky?", true).kind, "search");
  assert.equal(navigation.chatIntent("Show ranked stocks", false).kind, "rankings");
  assert.equal(navigation.chatIntent("Research Microsoft", false).query, "Microsoft");
});
test("research navigation only generates validated internal ticker routes and allowed sections", () => {
  assert.equal(navigation.chatResearchHref("BRK.B", "financials"), "/ask?symbol=BRK-B#financials");
  for (const value of ["//outside.example", "NVDA?admin=1", "<script>", "AAPL#news", "https://bad"]) assert.equal(navigation.chatResearchHref(value), null);
  assert.equal(navigation.chatResearchHref("NBIS", "//evil"), null);
});
test("chat-only chrome does not remove legacy navigation from other routes", () => {
  for (const path of ["/ask", "/ticker/NBIS", "/my-stocks", "/admin", "/"]) {
    const { RouteChrome } = load("components/RouteChrome.tsx", { "next/navigation": { usePathname: () => path } });
    const header = renderToStaticMarkup(React.createElement(RouteChrome, {kind:"header"}, "legacy header"));
    assert.equal(header, path === "/ask" ? "" : "legacy header");
    assert.match(renderToStaticMarkup(React.createElement(RouteChrome, {kind:"main"}, "content")), /<main[^>]*>content<\/main>/);
  }
});
test("server chat embeds the unchanged authenticated ticker renderer and never forwards query entitlement hints", async () => {
  const tickerPage = () => null, chat = () => null;
  const page = load("app/ask/page.tsx", {
    "@/app/ticker/[symbol]/page": { default: tickerPage },
    "@/components/discover/DiscoveryStocks": { DiscoveryStocks: () => null },
    "@/components/chat/WalnutChatWorkspace": { WalnutChatWorkspace: chat }, "@/lib/ticker": ticker,
  });
  const result = await page.default({ searchParams: Promise.resolve({ symbol: "BRK.B", tier:"admin", authToken:"untrusted", view:"rankings" }) });
  assert.equal(result.type, chat);
  assert.equal(result.props.symbol, "BRK-B");
  const embedded = nodes(result).find(n => n.type === tickerPage);
  assert.equal((await embedded.props.params).symbol, "BRK-B");
  assert.equal(JSON.stringify(await embedded.props.searchParams), "{}");
  assert.equal(page.dynamic, "force-dynamic");
  assert.equal(page.metadata.robots.index, false);
  const invalid = await page.default({ searchParams: Promise.resolve({ symbol:"//evil" }) });
  assert.equal(invalid.props.symbol, null);
  assert.equal(nodes(invalid).some(n => n.type === tickerPage), false);
});

function harness(search, symbol = null) {
  let cursor = 0; const slots = [], calls = [], urls = [], timers = new Map();
  const refs = [];
  const react = {
    useState(initial) { const i = cursor++; if (!(i in slots)) slots[i] = initial; return [slots[i], value => { slots[i] = typeof value === "function" ? value(slots[i]) : value; }]; },
    useRef(initial) { const i = cursor++; if (!(i in refs)) refs[i] = { current: initial }; return refs[i]; },
    useEffect() {}, useTransition: () => [false, fn => fn()],
  };
  const jsx = (type, props) => ({ type, props });
  const Component = load("components/chat/WalnutChatWorkspace.tsx", {
    react, "react/jsx-runtime": {jsx, jsxs:jsx}, "next/link": {default:"a"},
    "next/navigation": {useRouter: () => ({push: value => urls.push(value)})},
    "lucide-react": new Proxy({}, {get:(_,key)=>String(key)}),
    "@/components/auth/AccountNav": {AccountNav:"AccountNav"},
    "@/components/WalnutBrandMark": {WalnutBrandMark:"Brand"},
    "@/lib/api": {searchSuggest: async (...args) => {calls.push(args); return search(...args);}},
    "@/lib/chatNavigation": navigation, "@/lib/ticker": ticker,
  }, {setTimeout: fn => {const id=timers.size+1;timers.set(id,fn);return id;},clearTimeout:id=>timers.delete(id), window:{location:{hash:""}}, document:{querySelector:()=>null}}).WalnutChatWorkspace;
  const render = () => {cursor=0;return Component({symbol,rankings:false,children:"server research"});};
  return {calls,urls,timers,render, async submit(text) {nodes(render()).find(n=>n.type==="textarea").props.onChange({target:{value:text}}); const form=nodes(render()).find(n=>n.type==="form"); form.props.onSubmit({preventDefault(){}}); for(let i=0;i<15;i++) await Promise.resolve();}};
}
test("company submission performs one same-origin lookup and ignores returned external href", async () => {
  const h=harness(async()=>({items:[{kind:"ticker",symbol:"NBIS",label:"Nebius",href:"https://evil.example"}]}));
  h.render(); assert.equal(h.calls.length,0);
  await h.submit("Research NBIS");
  assert.equal(h.calls.length,1); assert.equal(h.calls[0][0],"NBIS"); assert.equal(h.calls[0][2].sameOrigin,true);
  assert.deepEqual(h.urls,["/ask?symbol=NBIS#overview"]); assert.equal(h.timers.size,0);
});
test("ambiguous company matches require selection rather than opening a guessed company", async () => {
  const h=harness(async()=>({items:[{kind:"ticker",symbol:"AAA",label:"Alpha one"},{kind:"ticker",symbol:"BBB",label:"Alpha two"}]}));
  await h.submit("Alpha"); assert.deepEqual(h.urls,[]);
  assert.match(JSON.stringify(h.render()),/Choose the company/);
});
test("lookup failure preserves the research and supplies a retryable explanation", async () => {
  const h=harness(async()=>{throw Error("offline")},"NBIS"); await h.submit("Research MSFT");
  assert.deepEqual(h.urls,[]); assert.match(JSON.stringify(h.render()),/temporarily unavailable/); assert.match(JSON.stringify(h.render()),/server research/); assert.equal(h.timers.size,0);
});
test("new research cancels an outstanding lookup and ignores its late result", async () => {
  let resolve; const h=harness(()=>new Promise(r=>resolve=r)); await h.submit("Research NBIS");
  const newButton=nodes(h.render()).find(n=>n.type==="button" && JSON.stringify(n.props.children).includes("New research"));
  newButton.props.onClick(); assert.equal(h.calls[0][2].signal.aborted,true);
  resolve({items:[{kind:"ticker",symbol:"NBIS",label:"NBIS"}]}); for(let i=0;i<15;i++) await Promise.resolve();
  assert.deepEqual(h.urls,["/ask"]);
});
