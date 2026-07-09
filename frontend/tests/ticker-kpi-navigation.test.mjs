import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const tickerPage = fs.readFileSync(path.join(root, "app/ticker/[symbol]/page.tsx"), "utf8");

test("ticker KPI row replaces net disclosed flow with institutional activity", () => {
  const tileSection = tickerPage.slice(tickerPage.indexOf("<TickerKpiNavigation"), tickerPage.indexOf("<TickerChartLoader"));
  assert.doesNotMatch(tileSection, /net-disclosed-flow|Net disclosed flow/);
  assert.match(tileSection, /key: "insider-sells"[\s\S]*key: "institutional-activity-count"[\s\S]*key: "unique-congress-traders"/);
});
