import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import ts from "typescript";

const source = readFileSync("lib/institution.ts", "utf8");
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 } }).outputText;
const module = { exports: {} };
new Function("exports", "module", compiled)(module.exports, module);
const { institutionDisplayName, institutionHref } = module.exports;

test("institution names use readable capitalization without breaking brands, acronyms or links", () => {
  for (const [raw, expected] of [
    ["GOLDMAN SACHS GROUP INC", "Goldman Sachs Group Inc"],
    ["BLACKROCK, INC.", "BlackRock, Inc."],
    ["JPMORGAN CHASE & CO", "JPMorgan Chase & Co"],
    ["FMR LLC", "FMR LLC"], ["UBS GROUP AG", "UBS Group AG"],
    ["BANK OF AMERICA CORP", "Bank of America Corp"],
    ["BNY MELLON", "BNY Mellon"], ["Baillie Gifford & Co", "Baillie Gifford & Co"],
    ["", null], [null, null],
  ]) {
    assert.equal(institutionDisplayName(raw), expected);
    assert.equal(institutionDisplayName(expected), expected);
  }
  assert.equal(institutionHref("19617"), "/institution/0000019617");
});

test("institution displays share the formatter across feed, signals, profiles and ownership", () => {
  for (const path of ["components/feed/FeedCard.tsx", "components/feed/FeedTable.tsx",
    "components/ticker/TickerOwnershipPanel.tsx", "app/signals/page.tsx",
    "app/institution/[cik]/page.tsx", "app/ticker/[symbol]/page.tsx", "lib/feedEventMapper.ts"]) {
    assert.match(readFileSync(path, "utf8"), /institutionDisplayName\(/, path);
  }
});
