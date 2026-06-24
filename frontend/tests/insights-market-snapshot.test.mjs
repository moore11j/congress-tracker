import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const marketSnapshot = read("components/insights/MarketSnapshot.tsx");
const marketSnapshotLib = read("lib/marketSnapshot.ts");

test("insights market snapshot renders the requested 4x2 block order", () => {
  assert.match(marketSnapshot, /const worldIndexes = indexesToInstruments\(snapshot\.world_indexes, FALLBACK_WORLD_INDEXES\)/);
  assert.match(marketSnapshot, /const usIndexes = indexesToInstruments\(snapshot\.indexes, FALLBACK_US_INDEXES\)/);
  assert.match(marketSnapshot, /const currencies = instrumentsOrFallback\(snapshot\.currencies, FALLBACK_CURRENCIES\)/);
  assert.match(marketSnapshot, /const commodities = instrumentsOrFallback\(snapshot\.commodities, FALLBACK_COMMODITIES\)/);
  assert.match(marketSnapshot, /const crypto = instrumentsOrFallback\(snapshot\.crypto, FALLBACK_CRYPTO\)/);
  assert.match(marketSnapshot, /grid auto-rows-fr gap-4 md:grid-cols-2 lg:grid-cols-4/);

  const order = [
    'title="Global Markets"',
    'title="Currencies"',
    'title="Commodities"',
    'title="Crypto"',
    'title="US Macro"',
    'title="US Treasury"',
    'title="US Markets"',
    'title="Sectors"',
  ].map((needle) => marketSnapshot.indexOf(needle));
  assert.deepEqual(
    order,
    [...order].sort((a, b) => a - b),
    "snapshot cards should render in the requested row-major order",
  );

  assert.match(marketSnapshot, /subtitle="Coming Soon"/);
  assert.match(marketSnapshot, /unavailableText="—"/);
  assert.match(marketSnapshot, /subtitle="Yield and Daily Change"/);
  assert.match(marketSnapshot, /<MacroPointList items=\{economics\} showChange \/>/);
  assert.doesNotMatch(marketSnapshot, /1D change unavailable/);
  assert.doesNotMatch(marketSnapshot, /1D avg change/);
  assert.doesNotMatch(marketSnapshot, /AUD\/USD/);
  assert.doesNotMatch(marketSnapshot, /USD\/CHF/);
  assert.doesNotMatch(marketSnapshot, /Crude Oil WTI/);
  assert.doesNotMatch(marketSnapshot, /Natural Gas/);

  const treasuryFallback = marketSnapshot.slice(
    marketSnapshot.indexOf("const FALLBACK_TREASURY"),
    marketSnapshot.indexOf("function formatValue"),
  );
  const treasuryOrder = [
    '"3M Treasury"',
    '"2Y Treasury"',
    '"5Y Treasury"',
    '"10Y Treasury"',
    '"30Y Treasury"',
  ].map((needle) => treasuryFallback.indexOf(needle));
  assert.ok(treasuryOrder.every((index) => index >= 0), "treasury fallback should include all required maturities");
  assert.deepEqual(
    treasuryOrder,
    [...treasuryOrder].sort((a, b) => a - b),
    "treasury fallback should render maturities from shortest to longest",
  );

  const macroFallback = marketSnapshot.slice(
    marketSnapshot.indexOf("const FALLBACK_MACRO"),
    marketSnapshot.indexOf("const FALLBACK_TREASURY"),
  );
  assert.match(macroFallback, /"Core CPI"/);
  assert.match(macroFallback, /"Debt\/GDP"/);
  assert.doesNotMatch(macroFallback, /"CPI"/);
  assert.doesNotMatch(macroFallback, /"GDP"/);
});

test("insights market snapshot hides provider and cache internals", () => {
  assert.doesNotMatch(marketSnapshot, /snapshot\.source/);
  assert.doesNotMatch(marketSnapshot, /FRED/);
  assert.doesNotMatch(marketSnapshot, /cache-first/i);
  assert.doesNotMatch(marketSnapshot, /cached/i);
  assert.doesNotMatch(marketSnapshot, /fmp/i);
  assert.doesNotMatch(marketSnapshot, /Launch Disabled/);
  assert.doesNotMatch(marketSnapshot, /Global ETF Proxies/);
  assert.doesNotMatch(marketSnapshot, /Canada ETF Proxy/);
  assert.doesNotMatch(marketSnapshot, /France ETF Proxy/);
  assert.doesNotMatch(marketSnapshot, /EWC/);
  assert.doesNotMatch(marketSnapshot, /EWQ/);
  assert.doesNotMatch(marketSnapshot, /CPER/);
  assert.match(marketSnapshot, /Latest available/);

  assert.doesNotMatch(marketSnapshotLib, /Global ETF Proxies/);
  assert.doesNotMatch(marketSnapshotLib, /Launch Disabled/);
  assert.doesNotMatch(marketSnapshotLib, /cached ETF/i);
  assert.doesNotMatch(marketSnapshotLib, /Currency data is disabled/i);
  assert.doesNotMatch(marketSnapshotLib, /Crypto data is disabled/i);
  assert.doesNotMatch(marketSnapshotLib, /Canada ETF Proxy/);
  assert.doesNotMatch(marketSnapshotLib, /France ETF Proxy/);
  assert.doesNotMatch(marketSnapshotLib, /EWC/);
  assert.doesNotMatch(marketSnapshotLib, /EWQ/);
  assert.doesNotMatch(marketSnapshotLib, /CPER/);
});

test("insights market snapshot uses clean global and commodity instruments", () => {
  for (const symbol of ["VFV", "ISF", "IJP", "EWG", "MCHI", "USO", "COPX"]) {
    assert.match(marketSnapshot, new RegExp(`symbol: "${symbol}"`));
    assert.match(marketSnapshotLib, new RegExp(`symbol: "${symbol}"`));
  }

  for (const label of ["Canada", "United Kingdom", "Japan", "Germany", "China", "Oil", "Copper"]) {
    assert.match(marketSnapshot, new RegExp(`label: "${label}"`));
    assert.match(marketSnapshotLib, new RegExp(`label: "${label}"`));
  }
});
