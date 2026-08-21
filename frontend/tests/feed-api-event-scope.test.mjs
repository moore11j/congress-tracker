import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const api = readFileSync(join(process.cwd(), "lib", "api.ts"), "utf8");

test("the all feed paginates activity events without affecting other event surfaces", () => {
  const eventTypes = api.slice(
    api.indexOf("export const FEED_ALL_EVENT_TYPES"),
    api.indexOf("export function normalizeEventType"),
  );

  assert.match(eventTypes, /"congress_trade"/);
  assert.match(eventTypes, /"congress_treasury_trade"/);
  assert.match(eventTypes, /"congress_crypto_trade"/);
  assert.match(eventTypes, /"insider_trade"/);
  assert.match(eventTypes, /"government_contract"/);
  assert.match(eventTypes, /\.\.\.INSTITUTIONAL_ACTIVITY_EVENT_TYPES/);
  assert.doesNotMatch(eventTypes, /news_article|press_release/);

  assert.match(api, /tape === "all" && routeFamily === "feed"/);
  assert.match(api, /nextParams\.event_type = FEED_ALL_EVENT_TYPES\.join\(","\);/);
});
