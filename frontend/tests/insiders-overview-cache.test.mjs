import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("Insiders overview shares public server renders without weakening abort behavior", () => {
  const api = readFileSync(join(process.cwd(), "lib/api.ts"), "utf8");
  const start = api.indexOf("export async function getInsidersOverview");
  const end = api.indexOf("export async function getInstitutionsOverview", start);
  const source = api.slice(start, end);

  assert.match(source, /const request = \(\) => fetchJson<InsidersOverviewResponse>/);
  assert.match(source, /if \(params\?\.signal\) return request\(\)/);
  assert.match(source, /serverCachedJson\(`insiders-overview:\$\{url\}`, request\)/);
});
