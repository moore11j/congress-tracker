import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const page = fs.readFileSync(path.join(root, "app", "ticker", "[symbol]", "page.tsx"), "utf8");
const signalActivity = fs.readFileSync(path.join(root, "components", "ticker", "TickerSignalActivityClient.tsx"), "utf8");
const signalSource = fs.readFileSync(path.join(root, "components", "ticker", "TickerSignalsSourceCardClient.tsx"), "utf8");
const chartLoader = fs.readFileSync(path.join(root, "components", "ticker", "TickerChartLoader.tsx"), "utf8");

test("ticker shell fallback keeps secondary loads behind the live context refresh", () => {
  assert.match(page, /deferHeavyTickerLoads=\{Boolean\(shellFallbackMessage\)\}/);
  assert.match(page, /<TickerSignalActivityClient[\s\S]*deferLoad=\{deferHeavyTickerLoads\}/);
  assert.match(page, /<TickerSignalsSourceCardClient[\s\S]*deferLoad=\{deferHeavyTickerLoads\}/);
  assert.match(page, /<TickerChartLoader[^>]*deferLoad=\{deferHeavyTickerLoads\}/);
  assert.match(signalActivity, /if \(deferLoad\) \{/);
  assert.match(signalSource, /if \(deferLoad\) \{/);
  assert.match(chartLoader, /if \(deferLoad\) return;/);
});
