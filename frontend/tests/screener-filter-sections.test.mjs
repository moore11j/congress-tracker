import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const page = fs.readFileSync(path.join(root, "app", "screener", "page.tsx"), "utf8");
const section = fs.readFileSync(path.join(root, "components", "screener", "CollapsibleFilterSection.tsx"), "utf8");
const upgradeOverlay = fs.readFileSync(path.join(root, "components", "screener", "ScreenerUpgradeOverlay.tsx"), "utf8");
const columns = fs.readFileSync(path.join(root, "lib", "screenerColumns.ts"), "utf8");

test("screener filter sections persist user expansion state across runs", () => {
  assert.match(section, /"use client"/);
  assert.match(section, /window\.localStorage\.getItem\(storageKey\)/);
  assert.match(section, /window\.localStorage\.setItem\(storageKey, nextOpen \? "open" : "closed"\)/);
  assert.match(section, /onToggle=\{\(event\) => handleToggle\(event\.currentTarget\.open\)\}/);
  assert.match(page, /storageKey="screener-section-intelligence"/);
  assert.match(page, /storageKey="screener-section-technical"/);
  assert.match(page, /storageKey="screener-section-fundamental"/);
  assert.doesNotMatch(page, /<details className=\{sectionCardClassName\} open=\{defaultOpen\}>/);
});

test("screener active column rules keep default intelligence params inactive", () => {
  assert.match(columns, /cleaned !== "" && cleaned\.toLowerCase\(\) !== "any"/);
  assert.doesNotMatch(columns, /hasActiveIntelligenceFilters[\s\S]*government_contracts_min_amount/);
  assert.doesNotMatch(columns, /hasActiveIntelligenceFilters[\s\S]*options_flow_lookback_days/);
  assert.doesNotMatch(columns, /hasActiveIntelligenceFilters[\s\S]*institutional_activity_lookback_days/);
  assert.match(columns, /if \(hasAnyActiveParam\(params, \["rel_volume_min", "rel_volume_max"\]\)\) columns\.push\("rel_volume"\)/);
  assert.match(columns, /\["trailing_pe", "trailing_pe_min", "trailing_pe_max"\]/);
  assert.match(columns, /\["price_sales", "price_to_sales_min", "price_to_sales_max"\]/);
  assert.match(columns, /\["debt_equity", "debt_to_equity_min", "debt_to_equity_max"\]/);
  assert.match(columns, /if \(hasAnyActiveParam\(params, \[minKey, maxKey\]\)\) columns\.push\(column\)/);
});

test("free screener gates premium filter groups without the top monitoring badge", () => {
  assert.match(page, /title="Technical screener filters"/);
  assert.match(page, /<TechnicalFiltersContent params=\{params\} locked \/>/);
  assert.match(page, /title="Fundamental screener filters"/);
  assert.match(page, /<FundamentalFiltersContent params=\{params\} locked \/>/);
  assert.match(page, /fieldset disabled=\{locked\}/);
  assert.match(page, /badge=\{null\}/);
  assert.match(upgradeOverlay, /badge = "Premium"/);
  assert.match(upgradeOverlay, /badge \? \(/);
  assert.match(page, /<FilterSelect name="congress_activity" label="Congress"/);
  assert.match(page, /<FilterSelect name="government_contracts_active" label="Contracts"/);
  assert.doesNotMatch(page, /congress_activity_locked/);
  assert.doesNotMatch(page, /government_contracts_active_locked/);
  assert.match(page, /title="Confirmation filters"/);
  assert.match(page, /Options flow filters require Pro\./);
  assert.match(page, /Institutional activity filters require Pro\./);
  assert.doesNotMatch(page, /<ScreenerUpgradeOverlay\s+title="Intelligence screener filters"/);
});
