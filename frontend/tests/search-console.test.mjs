import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
const ui = fs.readFileSync("components/admin/SearchConsolePanel.tsx", "utf8");
const callback = fs.readFileSync("app/auth/google/callback/page.tsx", "utf8");
test("Google connection discloses scope, missing volume and review-only feedback", () => {
  for (const text of ["Read-only access", "No Gmail or Drive access", "Keyword Planner volumes are managed separately", "never automatically rewritten or republished", "Sync performance now", "window.confirm"]) assert.ok(ui.includes(text));
});
test("Search Console callback is separate from login and clears the code URL", () => {
  assert.match(callback, /state\?\.startsWith\("gsc_"\)/);
  assert.match(callback, /history.replaceState/);
  assert.match(callback, /completeSearchConsole\(code, state\)/);
  assert.match(callback, /params.has\("error"\)/);
});
