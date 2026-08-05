import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const nav = read("components/AppTopNav.tsx");
const outcomesPage = read("app/outcomes/page.tsx");
const outcomesClient = read("components/outcomes/OutcomeLedgerClient.tsx");
const adminPage = read("app/admin/outcomes/page.tsx");
const adminClient = read("components/admin/AdminOutcomesDiagnostics.tsx");
const api = read("lib/api.ts");

test("top navigation exposes Outcomes and groups existing tools under Tools", () => {
  assert.match(nav, /label: "Outcomes"/);
  assert.match(nav, /NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0"/);
  assert.match(nav, /aria-haspopup="menu"/);
  assert.match(nav, /aria-expanded=\{toolsOpen\}/);
  assert.match(nav, /document\.addEventListener\("mousedown", closeOnOutsideClick\)/);
  assert.match(nav, /event\.key === "Escape"/);
  assert.match(nav, /Stock Research[\s\S]*\/screener[\s\S]*Screener[\s\S]*\/compare[\s\S]*Compare/);
  assert.match(nav, /Analysis[\s\S]*\/backtesting[\s\S]*Backtesting[\s\S]*\/market-pressure[\s\S]*Market Maps/);
  assert.match(nav, /Find stocks matching specific criteria/);
  assert.match(nav, /Compare companies and Walnut evidence/);
  assert.match(nav, /Test strategies against history/);
  assert.match(nav, /Visualize market pressure/);
  assert.doesNotMatch(nav, /label: "Screener" \},\s*\{ href: "\/leaderboards/);
  assert.match(nav, /Leaderboards[\s\S]*Tools[\s\S]*Pricing/);
});

test("Outcome Ledger page uses real API data and truthful empty states", () => {
  assert.match(outcomesPage, /getOutcomeLedgerStatus/);
  assert.match(outcomesPage, /getOutcomeSnapshots\(\{ limit: 25, calculation_type: "live" \}\)/);
  assert.match(outcomesClient, /Outcome Ledger/);
  assert.match(outcomesClient, /Track what Walnut believed at the time - and what happened next\./);
  assert.match(outcomesClient, /Performance by Score Band/);
  assert.match(outcomesClient, /Event Outcomes/);
  assert.match(outcomesClient, /Confirmation Events/);
  assert.match(outcomesClient, /Event Detail/);
  assert.match(outcomesClient, /Price Path vs SPY/);
  assert.match(outcomesClient, /Walnut is now preserving live confirmation-score judgments/);
  assert.match(outcomesClient, /Public preview: latest/);
  assert.match(outcomesClient, /seen\.has\(key\)/);
  assert.match(outcomesClient, /Bullish and bearish calls measured at 30D/);
  assert.match(outcomesClient, /maturedOutcome\(snapshot\)/);
  assert.doesNotMatch(outcomesClient, /NBIS|INFQ|BMNR|META|AAPL|NVDA/);
});

test("admin Outcomes diagnostics uses admin guard, filters, and internal endpoints", () => {
  assert.match(adminPage, /<VerifiedSessionGuard returnTo="\/admin\/outcomes" requireAdmin>/);
  assert.match(adminClient, /getAdminOutcomeLedgerStatus/);
  assert.match(adminClient, /getAdminOutcomeSnapshots/);
  assert.match(adminClient, /duplicate_attempts_ignored/);
  assert.match(adminClient, /missing_reference_prices/);
  assert.match(adminClient, /missing_security_ids/);
  assert.match(adminClient, /missing_source_contribution_payloads/);
  assert.match(adminClient, /JSON\.stringify\(expanded, null, 2\)/);
});

test("frontend API separates public and admin Outcome Ledger fields", () => {
  assert.match(api, /export type OutcomeLedgerStatus/);
  assert.match(api, /export type AdminOutcomeLedgerStatus = OutcomeLedgerStatus &/);
  assert.match(api, /\/api\/outcomes\/status/);
  assert.match(api, /\/api\/outcomes\/snapshots/);
  assert.match(api, /\/api\/admin\/outcomes\/status/);
  assert.match(api, /\/api\/admin\/outcomes\/snapshots/);
});
