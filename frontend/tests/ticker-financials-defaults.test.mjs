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
