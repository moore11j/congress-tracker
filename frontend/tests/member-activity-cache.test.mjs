import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";

const source = readFileSync(new URL("../lib/api.ts", import.meta.url), "utf8");
const ast = ts.createSourceFile("api.ts", source, ts.ScriptTarget.Latest, true);
const names = ["memberActivityFetchInit", "getMemberProfileBySlug", "getMemberAlphaSummary", "getMemberTrades"];
const declarations = ast.statements.filter((node) =>
  ts.isFunctionDeclaration(node) && names.includes(node.name?.text)
  || ts.isVariableStatement(node) && node.declarationList.declarations.some((decl) => decl.name.getText(ast) === "MEMBER_ACTIVITY_CACHE_VERSION"));
assert.equal(declarations.length, 5);
const script = ts.transpileModule(declarations.map((node) => node.getText(ast).replace(/^export /, "")).join("\n"), {
  compilerOptions: { target: ts.ScriptTarget.ES2022 },
}).outputText;

for (const name of names.slice(1)) {
  test(`${name} retires pre-cutover caches and bounds public staleness without caching live requests`, async () => {
    const calls = [];
    const context = vm.createContext({
      buildApiUrl: (path, params) => ({ path, params }),
      fetchJson: async (url, init) => { calls.push({ url, init }); return {}; },
    });
    vm.runInContext(script, context);
    await context[name]("W000802", { stalePageCache: true });
    await context[name]("W000802");
    assert.equal(calls[0].init.cache, "force-cache");
    assert.ok(calls[0].init.next.revalidate > 0 && calls[0].init.next.revalidate <= 300);
    assert.ok(calls[0].url.params.activity_version > 0);
    assert.equal(calls[1].init.cache, "no-store");
    assert.equal(calls[1].init.next.revalidate, 0);
    assert.equal(calls[1].url.params.activity_version, calls[0].url.params.activity_version);
  });
}
