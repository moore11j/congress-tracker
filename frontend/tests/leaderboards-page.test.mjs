import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const page = fs.readFileSync(path.join(root, "app/leaderboards/page.tsx"), "utf8");
const dashboard = fs.readFileSync(path.join(root, "components/leaderboards/LeaderboardsDashboard.tsx"), "utf8");
const api = fs.readFileSync(path.join(root, "lib/api.ts"), "utf8");

test("leaderboards provides one SSR dashboard with the four required sections", () => {
  assert.match(page, /<h1[^>]*>Leaderboards<\/h1>/);
  for (const section of ["top-stocks", "congress", "insiders", "institutions"]) assert.match(dashboard, new RegExp(`id="${section}"`));
  assert.match(page, /canViewPerformance/);
  assert.match(page, /canViewInstitutions/);
  assert.match(dashboard, /grid-cols-1 gap-4 2xl:grid-cols-2/);
  assert.match(dashboard, /How Stocks Are Ranked/);
});

test("leaderboards analytics and cached API contract are explicit", () => {
  for (const eventName of ["leaderboards_view", "leaderboard_section_view", "leaderboard_sort_change", "leaderboard_stock_click", "leaderboard_member_click", "leaderboard_institution_click"]) assert.match(dashboard, new RegExp(eventName));
  assert.match(api, /CachedLeaderboardSection/);
  assert.match(api, /prepared daily leaderboard snapshot/);
  assert.match(api, /getLeaderboardDashboard/);
  assert.match(page, /getLeaderboardDashboard/);
  assert.doesNotMatch(page, /getEntitlements/);
});
