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

test("daily price terminal exposes advanced chart controls", () => {
  assert.match(apiSource, /export type TickerChartVolumePoint/);
  assert.match(apiSource, /export type TickerChartCandlePoint/);
  assert.match(apiSource, /volumes\?: TickerChartVolumePoint\[\]/);
  assert.match(apiSource, /candles\?: TickerChartCandlePoint\[\]/);
  assert.match(chartSource, /type ChartMode = "line" \| "candles"/);
  assert.match(chartSource, /CandlestickSeries/);
  assert.match(chartSource, /HistogramSeries/);
  assert.match(chartSource, /volumeProfileBuckets/);
  assert.match(chartSource, /SMA 20/);
  assert.match(chartSource, /Bollinger/);
  assert.match(chartSource, /VWAP/);
  assert.match(chartSource, /TickerChartCompare/);
});
