import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const source = fs.readFileSync(path.join(process.cwd(), "components", "admin", "DataSourcesReport.tsx"), "utf8");

test("admin data sources panel splits health and risk from configuration", () => {
  assert.match(source, /<Th help=\{HEADER_HELP\.Health\}>Health<\/Th>/);
  assert.match(source, /<Th help=\{HEADER_HELP\.Risk\}>Risk<\/Th>/);
  assert.match(source, /<HealthBadge domain=\{domain\} issue=\{issue\} \/>/);
  assert.match(source, /<RiskBadges domain=\{domain\} \/>/);
  assert.match(source, /domain\.settings\.is_enabled \? "Enabled" : "Disabled"/);
  assert.match(source, /if \(domain\.last_error\) return "Error";/);
  assert.doesNotMatch(source, /badges\.append\("Active"\)/);
  assert.doesNotMatch(source, /StatusLine label="Configuration"/);
  assert.doesNotMatch(source, /<Th help=\{HEADER_HELP\.Status\}>Status<\/Th>/);
});

test("admin data sources panel explains provider keys, cache mode, and entitlement issues", () => {
  assert.match(source, /walnut_cache: "Local Walnut Cache"/);
  assert.match(source, /sec_edgar: "SEC EDGAR"/);
  assert.match(source, /walnut_official: "Walnut Official Pipeline"/);
  assert.match(source, /<code className="mt-0\.5 block text-\[11px\] text-slate-500">\{provider\}<\/code>/);
  assert.match(source, /Local Walnut Cache means the app reads from Walnut's database\/cache instead of calling an external API during page render\./);
  assert.match(source, /provider_entitlement/);
  assert.match(source, /Provider entitlement/);
});

test("admin data sources panel explains shadow pipelines and structured comparisons", () => {
  assert.match(source, /Shadow mode: staging\/comparison only\. Not powering public feed\./);
  assert.match(source, /View raw comparison/);
  assert.match(source, /function ComparisonBlock/);
  assert.match(source, /\[overflow-wrap:anywhere\]/);
});

test("admin data source map is grouped and responsive", () => {
  assert.match(source, /function DataSourceMap/);
  assert.match(source, /md:grid-cols-2/);
  assert.match(source, /Market Data/);
  assert.match(source, /Alternative Data/);
  assert.match(source, /Internal\/Computed/);
});

test("admin data sources column headers include helper text", () => {
  assert.match(source, /const HEADER_HELP/);
  assert.match(source, /The dataset or product area/);
  assert.match(source, /The currently selected source for this domain/);
  assert.match(source, /Enabled means this domain is configured for use/);
  assert.match(source, /Check the Health column for errors, stale data, or missing data/);
  assert.match(source, /Latest refresh\/check condition for this data domain/);
  assert.match(source, /Provider or licensing\/runtime risk/);
  assert.match(source, /The local Walnut table or cache used by the app/);
});

test("admin data sources mode tooltip explains every mode as a compact list", () => {
  assert.match(source, /const MODE_HELP_ITEMS = \[/);
  assert.match(source, /"Primary", "This provider is the selected production source for this data domain\."/);
  assert.match(source, /"Fallback", "This provider is used only if the primary provider is unavailable or disabled\."/);
  assert.match(source, /"Shadow", "This provider can ingest, stage, or compare data in the background, but it does not power public user-facing pages yet\."/);
  assert.match(source, /"Dry-run", "This mode can run test\/staging jobs without writing to production event tables\."/);
  assert.match(source, /"Disabled", "This data domain is intentionally turned off\."/);
  assert.match(source, /function ModeHelpList/);
  assert.match(source, /<dl className="grid gap-1\.5">/);
});

test("admin data sources folds issue labels into health", () => {
  assert.match(source, /const label = issue \? `\$\{state\} · \$\{issue\.label\}` : state;/);
  assert.match(source, /Raw issue key: \$\{domain\.last_error \?\? "unknown_error"\}/);
  assert.match(source, /provider_entitlement/);
  assert.match(source, /missing_cache/);
  assert.match(source, /stale_cache/);
  assert.match(source, /missing_refresh/);
  assert.match(source, /unknown_error/);
  assert.doesNotMatch(source, /Enabled does not mean healthy/);
  assert.doesNotMatch(source, />Issue<\/span>/);
});

test("admin data sources dropdowns use backend domain metadata", () => {
  assert.match(source, /domain\.allowed_providers \?\? providerOptions/);
  assert.match(source, /domain\.allowed_fallbacks \?\? \["none", \.\.\.providerOptions\]/);
  assert.match(source, /domain\.allowed_modes \?\? modeOptions/);
  assert.match(source, /Invalid saved value:/);
  assert.match(source, /validation_warnings/);
  assert.match(source, /Choose a valid provider, fallback, and mode before making other changes/);
});
