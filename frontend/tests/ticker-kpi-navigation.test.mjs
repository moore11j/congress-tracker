import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const tickerPage = fs.readFileSync(path.join(root, "app/ticker/[symbol]/page.tsx"), "utf8");

test("ticker removes KPI tile row and moves buy/sell counts into activity headers", () => {
  assert.doesNotMatch(tickerPage, /<TickerKpiNavigation|Latest Signal Conviction Score|unique-congress-traders|unique-insiders/);
  const activityHeaderStats = tickerPage.slice(tickerPage.indexOf("function ActivityHeaderStats"), tickerPage.indexOf("function InstitutionalActivityCard"));
  assert.match(activityHeaderStats, /label="Buys"[\s\S]*label="Sells"/);
  assert.match(tickerPage, /source="congress"[\s\S]*buys=\{congressBuys\}[\s\S]*sells=\{congressSells\}/);
  assert.match(tickerPage, /source="insider"[\s\S]*buys=\{insiderBuys\}[\s\S]*sells=\{insiderSells\}/);
});
