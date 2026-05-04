import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const insiderRolePath = path.join(process.cwd(), "lib", "insiderRole.ts");
const source = fs.readFileSync(insiderRolePath, "utf8");

test("vice president role normalization is checked before generic president", () => {
  const evpIndex = source.indexOf("EXECUTIVE VICE PRESIDENT");
  const svpIndex = source.indexOf("SENIOR VICE PRESIDENT");
  const vpIndex = source.indexOf("VICE PRESIDENT");
  const presidentIndex = source.indexOf("\\bPRESIDENT\\b");

  assert.notEqual(evpIndex, -1, "EVP role matcher should exist");
  assert.notEqual(svpIndex, -1, "SVP role matcher should exist");
  assert.notEqual(vpIndex, -1, "VP role matcher should exist");
  assert.notEqual(presidentIndex, -1, "generic president matcher should exist");
  assert.ok(vpIndex < presidentIndex, "VP matcher should run before generic president matcher");
});

test("development sanity cases include common VP variants", () => {
  for (const title of ["Vice President", "Vice-President", "Vice President, Finance"]) {
    assert.match(source, new RegExp(`\\["${title.replace("-", "[ -]")}", "VP"\\]`));
  }
  assert.match(source, /Senior Vice President", "SVP"/);
  assert.match(source, /Executive Vice President", "EVP"/);
});
