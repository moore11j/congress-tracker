import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const eventCalendarPath = path.join(process.cwd(), "components", "monitoring", "EventCalendarPanel.tsx");
const eventCalendarSource = fs.readFileSync(eventCalendarPath, "utf8");

test("event calendar visual filters persist to local storage", () => {
  assert.match(eventCalendarSource, /calendarFilterStorageKey = "walnut:event-calendar:filters:v1"/);
  assert.match(eventCalendarSource, /function readPersistedCalendarFilters/);
  assert.match(eventCalendarSource, /function writePersistedCalendarFilters/);
  assert.match(eventCalendarSource, /setActiveKinds\(mergeBooleanFilters\(defaultKindFilters, saved\.kinds\)\)/);
  assert.match(eventCalendarSource, /setActiveEconomicCategories\(mergeBooleanFilters\(defaultEconomicCategoryFilters, saved\.economicCategories\)\)/);
  assert.match(eventCalendarSource, /writePersistedCalendarFilters\(activeKinds, activeEconomicCategories\)/);
});
