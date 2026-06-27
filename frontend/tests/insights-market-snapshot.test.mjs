import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const marketSnapshot = read("components/insights/MarketSnapshot.tsx");
const marketSnapshotLib = read("lib/marketSnapshot.ts");
const insightsClient = read("components/insights/InsightsMarketSnapshotClient.tsx");
const categoryClient = read("components/insights/MarketSnapshotCategoryClient.tsx");
const api = read("lib/api.ts");

test("insights market snapshot renders the requested 4x2 block order", () => {
  assert.match(marketSnapshot, /const worldIndexes = indexesToInstruments\(snapshot\.world_indexes, FALLBACK_WORLD_INDEXES\)/);
  assert.match(marketSnapshot, /const usIndexes = indexesToInstruments\(snapshot\.indexes, FALLBACK_US_INDEXES\)/);
  assert.match(marketSnapshot, /const currencies = instrumentsOrFallback\(snapshot\.currencies, FALLBACK_CURRENCIES\)/);
  assert.match(marketSnapshot, /const commodities = instrumentsOrFallback\(snapshot\.commodities, FALLBACK_COMMODITIES\)/);
  assert.match(marketSnapshot, /const crypto = instrumentsOrFallback\(snapshot\.crypto, FALLBACK_CRYPTO\)/);
  assert.match(marketSnapshot, /grid auto-rows-fr gap-4 md:grid-cols-2 lg:grid-cols-4/);
  assert.match(insightsClient, /getInsightsOverview/);
  assert.match(categoryClient, /getInsightsOverview/);
  assert.match(api, /\/api\/insights\/overview/);

  const order = [
    'title="Global Markets"',
    'title="Commodities"',
    'title="Currencies"',
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

  assert.doesNotMatch(marketSnapshot, /subtitle="Coming Soon"/);
  assert.match(marketSnapshot, /unavailableText="-"/);
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

test("insights market snapshot hides provider and internal source terms", () => {
  for (const source of [marketSnapshot, marketSnapshotLib, insightsClient, categoryClient]) {
    assert.doesNotMatch(source, /snapshot\.source/);
    assert.doesNotMatch(source, /fmp/i);
    assert.doesNotMatch(source, /fred\s*cache/i);
    assert.doesNotMatch(source, /proxy/i);
    assert.doesNotMatch(source, /launch disabled/i);
  }

  for (const source of [marketSnapshot, marketSnapshotLib]) {
    assert.doesNotMatch(source, /Global ETF Proxies/);
    assert.doesNotMatch(source, /Canada ETF Proxy/);
    assert.doesNotMatch(source, /France ETF Proxy/);
    assert.doesNotMatch(source, /EWC/);
    assert.doesNotMatch(source, /EWQ/);
    assert.doesNotMatch(source, /CPER/);
    assert.doesNotMatch(source, /GLD/);
    assert.doesNotMatch(source, /SLV/);
    assert.doesNotMatch(source, /USO/);
    assert.doesNotMatch(source, /COPX/);
  }

  assert.match(marketSnapshot, /Latest available/);
});

test("insights market snapshot uses requested global and commodity instruments", () => {
  for (const symbol of ["MCHI", "EWG", "IJP", "ISF", "VFV", "GCUSD", "SILUSD", "BZUSD", "HGUSD"]) {
    assert.match(marketSnapshot, new RegExp(`symbol: "${symbol}"`));
    assert.match(marketSnapshotLib, new RegExp(`symbol: "${symbol}"`));
  }

  for (const label of [
    "China \\\\u2014 MCHI",
    "Germany \\\\u2014 EWG",
    "Japan \\\\u2014 IJP",
    "UK \\\\u2014 ISF",
    "Canada \\\\u2014 VFV",
    "Gold \\\\u2014 GCUSD",
    "Silver \\\\u2014 SILUSD",
    "Brent Crude Oil \\\\u2014 BZUSD",
    "Copper \\\\u2014 HGUSD",
  ]) {
    assert.match(marketSnapshot, new RegExp(`label: "${label}"`));
    assert.match(marketSnapshotLib, new RegExp(`label: "${label}"`));
  }
});
