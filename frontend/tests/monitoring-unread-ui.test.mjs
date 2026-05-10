import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const accountNavPath = path.join(process.cwd(), "components", "auth", "AccountNav.tsx");
const monitoringPath = path.join(process.cwd(), "components", "monitoring", "MonitoringDashboard.tsx");
const watchlistListPath = path.join(process.cwd(), "components", "watchlists", "WatchlistList.tsx");
const watchlistDetailPath = path.join(process.cwd(), "components", "watchlists", "WatchlistDetailContent.tsx");
const apiPath = path.join(process.cwd(), "lib", "api.ts");

const accountNavSource = fs.readFileSync(accountNavPath, "utf8");
const monitoringSource = fs.readFileSync(monitoringPath, "utf8");
const watchlistListSource = fs.readFileSync(watchlistListPath, "utf8");
const watchlistDetailSource = fs.readFileSync(watchlistDetailPath, "utf8");
const apiSource = fs.readFileSync(apiPath, "utf8");

test("account nav renders unread badges only when unread count is positive", () => {
  const positiveBadgeGuards = accountNavSource.match(/unreadCount > 0 \?/g) ?? [];

  assert.ok(positiveBadgeGuards.length >= 2, "account trigger and inbox link should both guard badges behind unreadCount > 0");
  assert.match(accountNavSource, /<button[\s\S]*aria-haspopup="menu"[\s\S]*aria-expanded=\{menuOpen\}/);
  assert.match(accountNavSource, /onMouseEnter=\{\(\) => setMenuOpen\(true\)\}/);
  assert.match(accountNavSource, /onClick=\{\(\) => setMenuOpen\(\(open\) => !open\)\}/);
  assert.match(accountNavSource, /pointer-events-none absolute -right-1 -top-1/);
  assert.match(accountNavSource, /href="\/monitoring"[\s\S]*?<span>Inbox<\/span>[\s\S]*?\{unreadLabel\}/);
  assert.match(accountNavSource, /bg-red-500/);
  assert.match(accountNavSource, /unreadCount > 9 \? "9\+"/);
  assert.doesNotMatch(accountNavSource, /99\+/);
});

test("monitoring inbox exposes selectable item read controls without ambiguous source unread actions", () => {
  assert.match(monitoringSource, /markMonitoringItemsRead/);
  assert.match(monitoringSource, /markMonitoringItemsUnread/);
  assert.match(monitoringSource, /type="checkbox"/);
  assert.match(monitoringSource, />\s*Select all\s*<\/button>/);
  assert.match(monitoringSource, />\s*Clear selection\s*<\/button>/);
  assert.match(monitoringSource, />\s*Mark selected read\s*<\/button>/);
  assert.match(monitoringSource, />\s*Mark selected unread\s*<\/button>/);
  assert.match(monitoringSource, /disabled=\{!hasSelection \|\| Boolean\(pendingReadAction\)\}/);
  assert.match(monitoringSource, />\s*Mark unread\s*<\/button>/);
  assert.match(monitoringSource, /No unread monitoring updates\./);
  assert.doesNotMatch(monitoringSource, /markMonitoringSourceUnread/);
  assert.doesNotMatch(monitoringSource, /No read items to mark unread\./);
  assert.doesNotMatch(monitoringSource, /Unable to mark this source unread\./);
  assert.match(monitoringSource, /role="status"/);
  assert.match(monitoringSource, /window\.dispatchEvent\(new Event\("ct:monitoring-unread-updated"\)\)/);
  assert.match(monitoringSource, /refreshWatchlists\(\)/);
});

test("api client includes read and unread monitoring mutations", () => {
  assert.match(apiSource, /\/api\/monitoring\/items\/mark-read/);
  assert.match(apiSource, /\/api\/monitoring\/items\/mark-unread/);
  assert.match(apiSource, /\/api\/monitoring\/sources\/\$\{encodeURIComponent\(sourceId\)\}\/mark-read/);
  assert.match(apiSource, /\/api\/monitoring\/sources\/\$\{encodeURIComponent\(sourceId\)\}\/mark-unread/);
  assert.match(apiSource, /\/api\/monitoring\/alerts\/\$\{encodeURIComponent\(String\(alertId\)\)\}\/read/);
  assert.match(apiSource, /\/api\/monitoring\/alerts\/\$\{encodeURIComponent\(String\(alertId\)\)\}\/unread/);
});

test("watchlist surfaces render canonical unread count fields", () => {
  assert.match(watchlistListSource, /watchlist\.unread_count \?\? watchlist\.unseen_count/);
  assert.match(watchlistListSource, /ct:monitoring-unread-updated/);
  assert.match(watchlistDetailSource, /watchlist\.unread_count \?\? watchlist\.unseen_count/);
  assert.doesNotMatch(watchlistDetailSource, /WatchlistSeenMarker/);
});
