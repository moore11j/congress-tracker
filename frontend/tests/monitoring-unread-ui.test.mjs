import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const accountNavPath = path.join(process.cwd(), "components", "auth", "AccountNav.tsx");
const monitoringPath = path.join(process.cwd(), "components", "monitoring", "MonitoringDashboard.tsx");
const apiPath = path.join(process.cwd(), "lib", "api.ts");

const accountNavSource = fs.readFileSync(accountNavPath, "utf8");
const monitoringSource = fs.readFileSync(monitoringPath, "utf8");
const apiSource = fs.readFileSync(apiPath, "utf8");

test("account nav renders unread badges only when unread count is positive", () => {
  const positiveBadgeGuards = accountNavSource.match(/unreadCount > 0 \?/g) ?? [];

  assert.ok(positiveBadgeGuards.length >= 2, "account trigger and inbox link should both guard badges behind unreadCount > 0");
  assert.match(accountNavSource, /className="relative block px-2 py-1 pr-5/);
  assert.match(accountNavSource, /href="\/monitoring"[\s\S]*?<span>Inbox<\/span>[\s\S]*?\{unreadLabel\}/);
  assert.match(accountNavSource, /bg-red-500/);
});

test("monitoring inbox exposes explicit source and item read controls", () => {
  assert.match(monitoringSource, /markMonitoringSourceRead/);
  assert.match(monitoringSource, /markMonitoringSourceUnread/);
  assert.match(monitoringSource, /markMonitoringAlertRead/);
  assert.match(monitoringSource, /markMonitoringAlertUnread/);
  assert.match(monitoringSource, />\s*Mark read\s*<\/button>/);
  assert.match(monitoringSource, />\s*Mark unread\s*<\/button>/);
  assert.match(monitoringSource, /window\.dispatchEvent\(new Event\("ct:monitoring-unread-updated"\)\)/);
});

test("api client includes read and unread monitoring mutations", () => {
  assert.match(apiSource, /\/api\/monitoring\/sources\/\$\{encodeURIComponent\(sourceId\)\}\/mark-read/);
  assert.match(apiSource, /\/api\/monitoring\/sources\/\$\{encodeURIComponent\(sourceId\)\}\/mark-unread/);
  assert.match(apiSource, /\/api\/monitoring\/alerts\/\$\{encodeURIComponent\(String\(alertId\)\)\}\/read/);
  assert.match(apiSource, /\/api\/monitoring\/alerts\/\$\{encodeURIComponent\(String\(alertId\)\)\}\/unread/);
});
