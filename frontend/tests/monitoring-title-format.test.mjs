import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const monitoringPath = path.join(process.cwd(), "components", "monitoring", "MonitoringDashboard.tsx");
const monitoringTitlesPath = path.join(process.cwd(), "lib", "monitoringTitles.ts");

const monitoringSource = fs.readFileSync(monitoringPath, "utf8");
const titlesSource = fs.readFileSync(monitoringTitlesPath, "utf8");

test("monitoring titles normalize insider names and trade sides without leaking provider labels", () => {
  assert.match(monitoringSource, /displayMonitoringAlertTitle\(item\)/);
  assert.match(monitoringSource, /buildMonitoringEventTitle\(event, event\.payload \?\? \{\}\)/);
  assert.match(titlesSource, /reporting_owner_name/);
  assert.match(titlesSource, /reportingOwnerName/);
  assert.match(titlesSource, /owner_name/);
  assert.match(titlesSource, /insider_name/);
  assert.match(titlesSource, /person_name/);
  assert.match(titlesSource, /"Insider"/);
  assert.match(titlesSource, /"fmp"/);
  assert.match(titlesSource, /"provider"/);
  assert.match(titlesSource, /"source"/);
  assert.match(titlesSource, /"buy"/);
  assert.match(titlesSource, /"acquired"/);
  assert.match(titlesSource, /"sale"/);
  assert.match(titlesSource, /"disposition"/);
});
