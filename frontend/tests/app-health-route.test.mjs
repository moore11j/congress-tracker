import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const routePath = path.join(root, "app/health/route.ts");
const route = fs.readFileSync(routePath, "utf8");

test("app health route is lightweight and public", () => {
  assert.match(route, /export const dynamic = "force-static"/);
  assert.match(route, /export function GET\(\)/);
  assert.match(route, /status: "ok"/);
  assert.match(route, /surface: "app"/);
  assert.match(route, /status: 200/);
});

test("app health route does not call backend, DB, or auth", () => {
  assert.doesNotMatch(route, /\bfetch\s*\(/);
  assert.doesNotMatch(route, /API_BASE|DATABASE|Session|auth|cookie|ct_session/i);
  assert.doesNotMatch(route, /from ["']@\/lib\/api|from ["']@\/lib\/auth|from ["']@\/lib\/backend/i);
});
