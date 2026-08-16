import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("Departments overview shares public server renders without weakening abort behavior", () => {
  const api = readFileSync(join(process.cwd(), "lib/api.ts"), "utf8");
  const start = api.indexOf("export async function getDepartmentsOverview");
  const end = api.indexOf("export async function getDepartments():", start);
  const source = api.slice(start, end);

  assert.match(source, /const request = \(\) => fetchJson<DepartmentsOverviewResponse>/);
  assert.match(source, /if \(params\?\.signal\) return request\(\)/);
  assert.match(source, /serverCachedJson\(`departments-overview:\$\{url\}`, request\)/);
});
