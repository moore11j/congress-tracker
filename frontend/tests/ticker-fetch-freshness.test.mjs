import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";

const source = readFileSync(new URL("../lib/api.ts", import.meta.url), "utf8");
const ast = ts.createSourceFile("api.ts", source, ts.ScriptTarget.Latest, true);
const functions = ["tickerPageFetchInit", "getTickerContextBundle", "getTickerSignalsSummary", "getTickerProfile"];
const variables = ["TICKER_CONTEXT_BUNDLE_CACHE_VERSION", "tickerContextBundleServerInflight"];
const declarations = ast.statements.filter(node =>
  ts.isFunctionDeclaration(node) && functions.includes(node.name?.text)
  || ts.isVariableStatement(node) && node.declarationList.declarations.some(decl => variables.includes(decl.name.getText(ast))));
assert.equal(declarations.length, 6);
const script = ts.transpileModule(declarations.map(node => node.getText(ast).replace(/^export /, "")).join("\n"), {
  compilerOptions: { target: ts.ScriptTarget.ES2022 },
}).outputText;

function setup() {
  const calls = [];
  const context = vm.createContext({
    Map,
    buildApiUrl: (path, params) => ({ path, params }),
    tickerPathSymbol: symbol => symbol,
    authHeaders: token => token ? { Authorization: "test-only" } : {},
    fetchJson: async (url, init) => { calls.push({ url, init }); return {}; },
    clientCachedJson: async (_key, _signal, read) => read(),
    serverCachedJson: async (_key, read) => read(),
    TICKER_SIGNALS_SUMMARY_CACHE_TTL_MS: 30000,
  });
  vm.runInContext(script, context);
  return { calls, context };
}

for (const name of ["getTickerContextBundle", "getTickerSignalsSummary"]) {
  test(`${name} refreshes public snapshots without caching authenticated or active requests`, async () => {
    const { calls, context } = setup();
    for (const options of [{ stalePageCache: true }, { stalePageCache: true, authToken: "test-token" },
      { stalePageCache: true, activeUser: true }, {}]) await context[name]("AAPL", options);
    assert.equal(calls[0].init.cache, "force-cache");
    assert.equal(calls[0].init.next.revalidate, 300);
    assert.equal(calls[0].url.params.context_version, 14);
    for (const call of calls.slice(1)) {
      assert.equal(call.init.cache, "no-store");
      assert.equal(call.init.next.revalidate, 0);
    }
    if (name === "getTickerContextBundle") {
      assert.equal(calls[0].url.params.cached_only, "true");
      assert.ok(calls.slice(1).every(call => call.url.params.cached_only === undefined));
    }
  });
}

test("profile fallback retires the old daily cache and retains live bypass", async () => {
  const { calls, context } = setup();
  await context.getTickerProfile("AAPL", { stalePageCache: true });
  await context.getTickerProfile("AAPL");
  assert.equal(calls[0].init.next.revalidate, 300);
  assert.equal(calls[0].url.params.context_version, 14);
  assert.equal(calls[1].init.cache, "no-store");
});
