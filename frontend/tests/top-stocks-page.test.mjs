import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const page = fs.readFileSync(path.join(root, "app/top-stocks/page.tsx"), "utf8");
const landing = fs.readFileSync(path.join(root, "app/landing/page.tsx"), "utf8");
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");
const leaderboards = fs.readFileSync(path.join(root, "app/leaderboards/page.tsx"), "utf8");
const dashboard = fs.readFileSync(path.join(root, "components/leaderboards/LeaderboardsDashboard.tsx"), "utf8");

test("Top Stocks redirects to the canonical crawlable Leaderboards dashboard", () => {
  assert.match(page, /redirect\("\/leaderboards#top-stocks"\)/);
  assert.match(leaderboards, /Stock, Congress, Insider & Institution Leaderboards/);
  assert.match(leaderboards, /<h1[^>]*>Leaderboards<\/h1>/);
  assert.match(leaderboards, /getCachedLeaderboard\("top-stocks"/);
  assert.match(dashboard, /id="top-stocks"/);
  assert.match(dashboard, /Top Stocks/);
  assert.match(landing, /topStocks\.items\.slice\(0, 5\)/);
  assert.match(landing, /leaderboards#top-stocks/);
  assert.match(middleware, /leaderboardsUrl\.hash = "top-stocks"/);
});

test("The Leaderboards page only reads prepared snapshots", () => {
  assert.match(leaderboards, /getCachedLeaderboard\("congress_members"/);
  assert.match(leaderboards, /getCachedLeaderboard\("insiders"/);
  assert.match(leaderboards, /getCachedLeaderboard\("institutions"/);
  assert.doesNotMatch(leaderboards, /\/api\/screener/);
  assert.doesNotMatch(leaderboards, /build_screener/);
});
