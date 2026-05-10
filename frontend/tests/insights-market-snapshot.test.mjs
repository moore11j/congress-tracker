import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const marketSnapshot = read("components/insights/MarketSnapshot.tsx");

test("insights market snapshot renders the requested 4x2 block order", () => {
  assert.match(marketSnapshot, /const worldIndexes = indexesToInstruments\(snapshot\.world_indexes, FALLBACK_WORLD_INDEXES\)/);
  assert.match(marketSnapshot, /const usIndexes = indexesToInstruments\(snapshot\.indexes, FALLBACK_US_INDEXES\)/);
  assert.match(marketSnapshot, /const currencies = instrumentsOrFallback\(snapshot\.currencies, FALLBACK_CURRENCIES\)/);
  assert.match(marketSnapshot, /const commodities = instrumentsOrFallback\(snapshot\.commodities, FALLBACK_COMMODITIES\)/);
  assert.match(marketSnapshot, /const crypto = instrumentsOrFallback\(snapshot\.crypto, FALLBACK_CRYPTO\)/);
  assert.match(marketSnapshot, /grid auto-rows-fr gap-4 md:grid-cols-2 lg:grid-cols-4/);

  const order = [
    'title="World Indexes"',
    'title="Currencies"',
    'title="Commodities"',
    'title="Crypto"',
    'title="US Macro"',
    'title="US Treasury"',
    'title="US Indexes"',
    'title="US Sectors"',
  ].map((needle) => marketSnapshot.indexOf(needle));
  assert.deepEqual(
    order,
    [...order].sort((a, b) => a - b),
    "snapshot cards should render in the requested row-major order",
  );

  assert.match(marketSnapshot, /subtitle="1D average change"/);
  assert.match(marketSnapshot, /subtitle="Yield and daily change"/);
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
});
