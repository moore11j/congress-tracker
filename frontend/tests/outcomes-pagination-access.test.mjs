import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import ts from "typescript";

const source = readFileSync("components/outcomes/OutcomeLedgerClient.tsx", "utf8");
const start = source.indexOf("  function gatePremiumTable()");
const end = source.indexOf("\n  return (", start);
const code = ts.transpileModule(source.slice(start, end), { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
for (const paid of [false, true]) {
  test(`${paid ? "Premium" : "Free"} Next, Last and row-size clicks share the access gate`, () => {
    let page = 0, modal = false, size = 10;
    const context = vm.createContext({
      hasPremiumTable: paid, pageCount: 50,
      setTableGateOpen: value => { modal = value; },
      setPage: value => { page = typeof value === "function" ? value(page) : value; },
      setPageSize: value => { size = value; },
      setSort: () => {},
    });
    vm.runInContext(code, context);
    context.movePage(1);
    assert.equal(page, paid ? 1 : 0);
    assert.equal(modal, !paid);
    context.jumpToPage(49);
    assert.equal(page, paid ? 49 : 0);
    context.movePage(10);
    assert.equal(page, paid ? 49 : 0);
    context.handlePageSize(25);
    assert.equal(size, paid ? 25 : 10);
  });
}
