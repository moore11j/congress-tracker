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

test("admin data sources panel explains shadow readiness and optional history", () => {
  assert.match(source, /Shadow mode: staging\/comparison only\. Not powering public feed\./);
  assert.match(source, /Congress Official Pipeline Readiness/);
  assert.match(source, /SEC Form 4 Pipeline Readiness/);
  assert.match(source, /Provider switching affects future ingest jobs only/);
  assert.match(source, /Existing Walnut records remain stored/);
  assert.match(source, /Provider switch safety/);
  assert.match(source, /Historical coverage comparison \(optional\)/);
  assert.match(source, /This does not need to match before switching providers for future ingests/);
  assert.match(source, /Large historical gaps are expected before a backfill is run/);
  assert.match(source, /function HistoricalComparisonDetails/);
  assert.match(source, /\[overflow-wrap:anywhere\]/);
  assert.doesNotMatch(source, /Congress official vs current/);
  assert.doesNotMatch(source, /SEC Form 4 vs current/);
  assert.doesNotMatch(source, /View raw comparison/);
  assert.doesNotMatch(source, /function ComparisonBlock/);
  assert.doesNotMatch(source, /Missing in official/);
});

test("admin data sources explains official congress source hierarchy", () => {
  assert.match(source, /Aggregate output combining House \+ Senate disclosures into normalized Congress trade events\./);
  assert.match(source, /In shadow mode, this does not power the public feed\./);
  assert.match(source, /Raw official House disclosure discovery and parsing source\. Feeds the Walnut Official Congress Pipeline\./);
  assert.match(source, /Raw official Senate disclosure discovery and parsing source\. Feeds the Walnut Official Congress Pipeline\./);
  assert.match(source, /House disclosures \+ Senate disclosures \\u2192 Walnut Official Pipeline \\u2192 normalized Congress trades\./);
  assert.match(source, /House disclosures and Senate disclosures are raw official source layers/);
  assert.match(source, /Configured, but not production\. This pipeline is not considered ready until filings discovered, filings parsed, and normalized transactions are greater than zero with acceptable duplicate risk\./);
  assert.match(source, /Configured but not populated yet\./);
  assert.match(source, /function isCongressOfficialSourceDomain/);
});

test("admin data sources pipeline overview includes raw, cache, intelligence, and product layers", () => {
  assert.match(source, /Pipeline Overview/);
  assert.match(source, /How raw official sources, licensed data, and Walnut caches flow into product features\./);
  assert.match(source, /Raw Sources/);
  assert.match(source, /Official House Disclosures/);
  assert.match(source, /Official Senate Disclosures/);
  assert.match(source, /SEC Form 4 Filings/);
  assert.match(source, /FMP Market Data/);
  assert.match(source, /FRED Macro Data/);
  assert.match(source, /Walnut Pipelines/);
  assert.match(source, /Walnut Official Congress Pipeline/);
  assert.match(source, /Walnut Official Insider Pipeline/);
  assert.match(source, /Walnut Market Data \/ Cache Layer/);
  assert.match(source, /FRED Macro\/Treasury Cache/);
  assert.match(source, /Normalized \/ Cached Outputs/);
  assert.match(source, /Normalized Congress Trades/);
  assert.match(source, /Normalized Insider Trades/);
  assert.match(source, /Unified Event Layer \/ Local Walnut Cache/);
  assert.match(source, /Insights Snapshots/);
  assert.match(source, /Screener Caches/);
  assert.match(source, /Signal Inputs/);
  assert.match(source, /Gain \/ Loss Enrichment/);
  assert.match(source, /Trade Outcomes/);
  assert.match(source, /Analytics \/ Intelligence Layer/);
  assert.match(source, /Screener, Leaderboards, and Backtesting consume normalized events and cached market\/fundamental data/);
  assert.match(source, /They should not read raw House\/Senate\/SEC filings directly\./);
  assert.match(source, /Portfolio Simulation/);
  assert.match(source, /Product Surfaces/);
  assert.match(source, /Watchlists \/ Monitoring/);
});

test("admin data sources pipeline overview shows required status chips and flows", () => {
  assert.match(source, /Official Source/);
  assert.match(source, /Licensed Provider/);
  assert.match(source, /Shadow/);
  assert.match(source, /Cache/);
  assert.match(source, /Internal Computed/);
  assert.match(source, /Product Surface/);
  assert.match(source, /Official House Disclosures \+ Official Senate Disclosures \\u2192 Walnut Official Congress Pipeline \\u2192 Normalized Congress Trades/);
  assert.match(source, /SEC Form 4 Filings \\u2192 Walnut Official Insider Pipeline \\u2192 Normalized Insider Trades/);
  assert.match(source, /FMP Market Data \\u2192 Walnut Market Data \/ Cache Layer \\u2192 Local Walnut Cache/);
  assert.match(source, /FRED Macro Data \\u2192 FRED Macro\/Treasury Cache \\u2192 Insights Snapshots \\u2192 Insights/);
  assert.match(source, /function PipelineOverview/);
  assert.match(source, /function PipelineLayer/);
  assert.match(source, /function PipelineCard/);
});

test("admin data source map is grouped and responsive", () => {
  assert.match(source, /function DataSourceMap/);
  assert.match(source, /md:grid-cols-2/);
  assert.match(source, /xl:grid-cols-4/);
  assert.match(source, /xl:col-span-2/);
  assert.match(source, /SOURCE_MAP_GROUP_ORDER = \["Market Data", "Official \/ Alternative Data", "Insights", "Internal \/ Computed"\]/);
  assert.match(source, /Market Data/);
  assert.match(source, /Official \/ Alternative Data/);
  assert.match(source, /Internal \/ Computed/);
  assert.match(source, /riskStates\(domain\)\[0\]/);
  assert.match(source, /form4_filings/);
  assert.match(source, /government_contract/);
  assert.match(source, /pnl_enrichment/);
  assert.match(source, /signal_inputs/);
  assert.match(source, /watchlist_alerts/);
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

test("admin data sources readiness panels emphasize dedupe and feed safety", () => {
  assert.match(source, /public_feed_impact: "Public feed impact"/);
  assert.match(source, /existing_data_preserved: "Existing data preserved"/);
  assert.match(source, /duplicate_candidates/);
  assert.match(source, /potential_duplicate_insert_risk: "Potential duplicate insert risk"/);
  assert.match(source, /would_insert_count: "Would insert count"/);
  assert.match(source, /would_skip_duplicate_count: "Would skip duplicate count"/);
  assert.match(source, /readiness_status: "Readiness status"/);
  assert.match(source, /Current production feed count/);
  assert.match(source, /Shadow normalized count/);
  assert.match(source, /Historical gap/);
  assert.match(source, /Missing in shadow/);
});

test("admin data sources dropdowns use backend domain metadata", () => {
  assert.match(source, /domain\.allowed_providers \?\? providerOptions/);
  assert.match(source, /domain\.allowed_fallbacks \?\? \["none", \.\.\.providerOptions\]/);
  assert.match(source, /domain\.allowed_modes \?\? modeOptions/);
  assert.match(source, /Invalid provider:/);
  assert.match(source, /Invalid fallback:/);
  assert.match(source, /Invalid mode:/);
  assert.match(source, /validation_warnings/);
  assert.match(source, /Provider validation warning/);
  assert.match(source, /Choose a valid provider, fallback, and mode before making other changes/);
});

test("admin data sources lets admins save and manually test provider endpoints", () => {
  assert.match(source, /testAdminDataSourceEndpoint/);
  assert.match(source, /primary_endpoint_url/);
  assert.match(source, /fallback_endpoint_url/);
  assert.match(source, /primary_endpoint_contract_json/);
  assert.match(source, /fallback_endpoint_contract_json/);
  assert.match(source, /endpoint_tests/);
  assert.match(source, /Save endpoints/);
  assert.match(source, /Test endpoint/);
  assert.match(source, /function EndpointEditor/);
  assert.match(source, /Use default/);
  assert.match(source, /Use \{"\{symbol\}"\} or \[symbol\]/);
  assert.match(source, /Request\/response contract/);
  assert.match(source, /Intraday chart uses date as YYYY-MM-DD HH:MM:SS and close as price/);
  assert.match(source, /Local Walnut Cache/);
});
