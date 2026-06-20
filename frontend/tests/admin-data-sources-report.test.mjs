import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const source = fs.readFileSync(path.join(process.cwd(), "components", "admin", "DataSourcesReport.tsx"), "utf8");

test("admin data sources panel separates configuration, health, and risk", () => {
  assert.match(source, /StatusLine label="Configuration"/);
  assert.match(source, /StatusLine label="Health"/);
  assert.match(source, /StatusLine label="Risk"/);
  assert.match(source, /domain\.settings\.is_enabled \? "Enabled" : "Disabled"/);
  assert.match(source, /if \(domain\.last_error\) return "Error";/);
  assert.doesNotMatch(source, /badges\.append\("Active"\)/);
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
  assert.match(source, /Latest health\/risk state based on refresh jobs/);
  assert.match(source, /The local Walnut table or cache used by the app/);
});

test("admin data sources dropdowns use backend domain metadata", () => {
  assert.match(source, /domain\.allowed_providers \?\? providerOptions/);
  assert.match(source, /domain\.allowed_fallbacks \?\? \["none", \.\.\.providerOptions\]/);
  assert.match(source, /domain\.allowed_modes \?\? modeOptions/);
  assert.match(source, /Invalid saved value:/);
  assert.match(source, /validation_warnings/);
  assert.match(source, /Choose a valid provider, fallback, and mode before making other changes/);
});
