import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const marketSnapshot = read("components/insights/MarketSnapshot.tsx");

test("insights market snapshot renders indexes and sectors independently", () => {
  assert.match(marketSnapshot, /const indexes = snapshot\.indexes \?\? \[\]/);
  assert.match(marketSnapshot, /const sectorPerformance = snapshot\.sector_performance \?\? \[\]/);
  assert.match(marketSnapshot, /indexes\.length === 0 \? \(/);
  assert.match(marketSnapshot, /indexes\.map\(\(item\) =>/);
  assert.match(marketSnapshot, /Major indexes · 1D change/);
  assert.match(marketSnapshot, /ETF proxy · 1D change/);
  assert.match(marketSnapshot, /item\.is_proxy \? `\$\{item\.symbol\} proxy` : item\.symbol/);
  assert.match(marketSnapshot, /<SectorList items=\{sectorPerformance\} \/>/);
  assert.match(marketSnapshot, /Sector performance · 1D average change/);
  assert.match(marketSnapshot, /if \(items\.length === 0\) return <UnavailableState \/>;/);
});
