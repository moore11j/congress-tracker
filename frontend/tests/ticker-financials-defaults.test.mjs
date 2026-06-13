import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const financialsPanel = readFileSync(join(root, "components/ticker/TickerFinancialsPanel.tsx"), "utf8");

test("ticker financial trend charts default to annual when annual data exists", () => {
  assert.match(
    financialsPanel,
    /function defaultMode\(annual: TickerFinancialsPoint\[\], quarterly: TickerFinancialsPoint\[\]\): PeriodMode \{\s*return annual\.length > 0 \? "annual" : "quarterly";\s*\}/,
  );
  assert.match(financialsPanel, /<FinancialChart title="Revenue Trend"/);
  assert.match(financialsPanel, /<FinancialChart title="Earnings Trend"/);
  assert.match(financialsPanel, /<ModeToggle mode=\{mode\} annualAvailable=\{annual\.length > 0\} quarterlyAvailable=\{quarterly\.length > 0\} onChange=\{setMode\} \/>/);
});

test("ticker financials keep core data visible when estimates are unavailable", () => {
  assert.match(financialsPanel, /const ESTIMATES_UNAVAILABLE_MESSAGE = "Analyst estimates are not available for this ticker\."/);
  assert.match(financialsPanel, /const estimatesUnavailable = estimatesStatus === "unavailable" && !hasForecastData;/);
  assert.match(financialsPanel, /annual\.length \|\| quarterly\.length \|\| earnings\.length \|\| hasForecastData \|\| hasSummaryData/);
  assert.match(financialsPanel, /<FinancialSection title="Analyst Estimates">/);
  assert.match(financialsPanel, /<FinancialSection title="Balance Sheet Quality">/);
  assert.doesNotMatch(financialsPanel, /\bFMP\b|provider|402|plan|endpoint failure/i);
});
