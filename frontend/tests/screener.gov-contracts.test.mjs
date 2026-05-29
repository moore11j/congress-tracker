import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const screenerPagePath = path.join(process.cwd(), "app", "screener", "page.tsx");
const source = fs.readFileSync(screenerPagePath, "utf8");

test("screener default result headers stay limited to core market columns", () => {
  const theadStart = source.indexOf("<thead");
  const theadEnd = source.indexOf("</thead>", theadStart);
  const tableHeaderSource = source.slice(theadStart, theadEnd);
  const headerMarkers = [
    'label="Symbol"',
    ">Company<",
    ">Sector<",
    'label="Market cap"',
    'label="Price"',
    'label="Volume"',
    'label="Beta"',
  ];
  const positions = headerMarkers.map((marker) => tableHeaderSource.indexOf(marker));
  positions.forEach((position, index) => {
    assert.notEqual(position, -1, `missing header marker ${headerMarkers[index]}`);
  });
  for (let index = 1; index < positions.length; index += 1) {
    assert.ok(
      positions[index] > positions[index - 1],
      `${headerMarkers[index]} should appear after ${headerMarkers[index - 1]}`,
    );
  }
  assert.match(tableHeaderSource, /activeColumns\.includes\("congress"\)/);
  assert.match(tableHeaderSource, /activeColumns\.includes\("trailing_pe"\)/);
  assert.match(tableHeaderSource, /activeColumns\.includes\("rel_volume"\)/);
});

test("screener exposes collapsible intelligence, technical, and fundamental filters", () => {
  assert.match(source, /title="Intelligence Filters"/);
  assert.match(source, /title="Technical Filters"/);
  assert.match(source, /title="Fundamental Filters"/);
  assert.match(source, /minName="rel_volume_min"/);
  assert.match(source, /minName="price_move_min"/);
  assert.match(source, /minName="rsi_min"/);
  assert.match(source, /name="macd_state"/);
  assert.match(source, /name="trend_state"/);
  assert.match(source, /minName="trailing_pe_min"/);
  assert.match(source, /minName="forward_pe_min"/);
  assert.match(source, /minName="revenue_growth_min"/);
  assert.match(source, /minName="eps_growth_min"/);
  assert.match(source, /maxName="debt_equity_max"/);
});

test("technical inputs use placeholders and submit only entered values", () => {
  assert.match(source, /placeholderMin="1" placeholderMax="2"/);
  assert.match(source, /placeholderMin="-10" placeholderMax="10"/);
  assert.match(source, /placeholderMin="30" placeholderMax="70"/);
  assert.match(source, /const value = getParam\(sp, key\);[\s\S]*?if \(value\.trim\(\)\) params\[key\] = value\.trim\(\);/);
  assert.match(source, /const trimmed = NUMERIC_PARAM_KEYS\.has\(key\) \? stripNumberFormatting\(value\) : String\(value\)\.trim\(\);[\s\S]*?if \(trimmed\) url\.searchParams\.set\(key, trimmed\);/);
  assert.match(source, /<Link href="\/screener" className=\{ghostButtonClassName\} prefetch=\{false\}>/);
});

test("gov contracts rendering distinguishes unavailable, inactive, and active rows", () => {
  assert.match(
    source,
    /availabilityStatus === "unavailable"[\s\S]*?Unavailable/,
    "gov contracts cell should render Unavailable when backend marks the overlay unavailable",
  );
  assert.match(
    source,
    /if \(!row\.government_contracts_active\) return <span className="text-sm text-slate-500">—<\/span>/,
    "inactive gov contracts rows should render an em dash",
  );
  assert.match(
    source,
    /formatCurrencyCompact\(row\.government_contracts_total_amount\)[\s\S]*?contract\{count === 1 \? "" : "s"\}/,
    "active gov contracts rows should render contract totals and counts",
  );
});
