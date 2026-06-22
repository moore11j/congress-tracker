import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const viewSource = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminAiMarketingView.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");

test("manual AI outreach form accepts pasted text as the source body", () => {
  assert.match(viewSource, /Post\/comment text or thread excerpt/);
  assert.match(viewSource, /!manualUrl\.trim\(\) && !manualText\.trim\(\)/);
  assert.match(viewSource, /Paste a source URL or post\/comment text first\./);
  assert.match(viewSource, /url: manualUrl\.trim\(\) \|\| null/);
  assert.match(viewSource, /text: manualText\.trim\(\) \|\| null/);
  assert.match(viewSource, /source_url_provided !== false/);
  assert.doesNotMatch(viewSource, /Paste a Reddit, X, or Facebook URL first\./);
});

test("manual AI outreach API and errors preserve backend validation detail", () => {
  assert.match(apiSource, /url\?: string \| null/);
  assert.match(apiSource, /function structuredDetailMessage/);
  assert.match(apiSource, /Array\.isArray\(detail\)/);
  assert.match(apiSource, /return messages\.join\(" "\)/);
});
