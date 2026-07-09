import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const chartSource = fs.readFileSync(path.join(root, "components/ticker/PremiumTickerChart.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(root, "lib/api.ts"), "utf8");

test("daily price terminal has a development/test consistency guard", () => {
  assert.match(apiSource, /latest_close\?: number \| null/);
  assert.match(apiSource, /previous_close\?: number \| null/);
  assert.match(chartSource, /export function assertDailyPriceTerminalConsistency/);
  assert.match(chartSource, /bundle\?\.prices\?\.\[bundle\.prices\.length - 1\]/);
  assert.match(chartSource, /Math\.abs\(currentPrice - latestClose\) <= 0\.01/);
  assert.match(chartSource, /process\.env\.NODE_ENV === "test"/);
  assert.match(chartSource, /console\.error\(message\)/);
  assert.match(chartSource, /assertDailyPriceTerminalConsistency\(bundle\)/);
});
