import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

test("Monitoring defers the interactive event calendar without removing it", () => {
  const dashboard = readFileSync(join(process.cwd(), "components/monitoring/MonitoringDashboard.tsx"), "utf8");

  assert.match(dashboard, /import dynamic from "next\/dynamic"/);
  assert.match(dashboard, /const EventCalendarPanel = dynamic\(/);
  assert.match(dashboard, /EventCalendarPanel\"\)\.then/);
  assert.match(dashboard, /ssr: false/);
  assert.match(dashboard, /<EventCalendarPanel canUseEventCalendar=\{canUseEventCalendar\}/);
});
