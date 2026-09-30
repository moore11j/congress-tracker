import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";

const exports = {};
vm.runInNewContext(ts.transpileModule(fs.readFileSync("lib/researchDistribution.ts", "utf8"), {compilerOptions: {module: ts.ModuleKind.CommonJS}}).outputText, {exports, URL});
const draft = exports.researchDistributionDraft;

test("sharing uses edited public copy and an attributed same-site article link", () => {
  const result = draft({slug: "nvda-update", title: "Title", preview_body: "**Edited finding**", risks: ["Filings lag."], key_points: ["Not selected"]}, "reddit");
  assert.match(result, /^Edited finding\n\nFilings lag\./);
  const link = new URL(result.split("Read the research on Walnut: ")[1]);
  assert.equal(link.hostname, "walnutmarkets.com");
  assert.equal(link.pathname, "/research/nvda-update");
  assert.equal(link.searchParams.get("utm_source"), "reddit");
  assert.equal(link.searchParams.get("utm_content"), "nvda-update");
  assert.doesNotMatch(result, /\*\*|Not selected/);
});

test("paid brief packet does not copy gated key points or risks; platform and path are constrained", () => {
  const result = draft({slug: "//evil.test/path", title: "Public title", premium_required: true, key_points: ["PRIVATE"], risks: ["PRIVATE RISK"]}, "evil");
  assert.doesNotMatch(result, /PRIVATE/);
  const link = new URL(result.split("Read the research on Walnut: ")[1]);
  assert.equal(link.hostname, "walnutmarkets.com");
  assert.equal(link.searchParams.get("utm_source"), "reddit");
  assert.match(link.pathname, /^\/research\/%2F/);
});
