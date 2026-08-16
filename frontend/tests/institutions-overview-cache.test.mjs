import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("anonymous Institutions overviews use the shared server cache without sharing authenticated data", () => {
  const api = readFileSync(join(process.cwd(), "lib/api.ts"), "utf8");
  const start = api.indexOf("export async function getInstitutionsOverview");
  const end = api.indexOf("export async function getDepartmentsOverview", start);
  const source = api.slice(start, end);

  assert.match(source, /const request = \(\) => fetchJson<InstitutionsOverviewResponse>/);
  assert.match(source, /if \(params\?\.authToken \|\| params\?\.signal\) return request\(\)/);
  assert.match(source, /serverCachedJson\(`institutions-overview:\$\{url\}`, request\)/);
});
