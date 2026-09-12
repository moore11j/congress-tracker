import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
const ui = fs.readFileSync("components/admin/KeywordPlannerPanel.tsx", "utf8");
const callback = fs.readFileSync("app/auth/google/callback/page.tsx", "utf8");

test("Keyword Planner discloses broad OAuth permission and read-only implementation", () => {
  for (const text of ["broad Google Ads permission", "viewing and managing", "cannot create campaigns", "separate from Search Console", "cached for 30 days", "targeting.country", "Unavailable", "close_variants", "window.confirm"]) assert.ok(ui.includes(text), text);
});
test("Ads callback has its own route and does not fall through to login", () => {
  assert.match(callback, /state\?\.startsWith\("gads_"\)/);
  assert.match(callback, /completeKeywordPlanner\(code, state\)/);
  const branch = callback.slice(callback.indexOf('if (state?.startsWith("gads_"))'), callback.indexOf('if (state?.startsWith("gsc_"))'));
  assert.match(branch, /history.replaceState/);
  assert.match(branch, /params.has\("error"\)/);
  assert.match(branch, /return;/);
});
