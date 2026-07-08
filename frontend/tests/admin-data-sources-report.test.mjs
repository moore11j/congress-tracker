import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const source = fs.readFileSync(path.join(process.cwd(), "components", "admin", "DataSourcesReport.tsx"), "utf8");
const apiSource = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");
const backendSnapshotSource = fs.readFileSync(path.join(process.cwd(), "..", "backend", "app", "services", "data_architecture.py"), "utf8");

test("admin data sources is now a read-only data architecture dashboard", () => {
  assert.match(source, /Data Architecture/);
  assert.match(source, /Read-only overview of Walnut's data pipelines, provider connectivity, cache health, backend routes, and operational status\./);
  assert.match(source, /Read-only architecture view\. Configuration changes live in Settings or environment secrets\./);
  assert.match(source, /getAdminDataArchitecture/);
  assert.doesNotMatch(source, /getAdminDataSourcesStatus/);
  assert.doesNotMatch(source, /updateAdminDataSourceSetting/);
  assert.doesNotMatch(source, /runAdminDataSource/);
  assert.doesNotMatch(source, /testAdminDataSourceEndpoint/);
});

test("dashboard renders the required sections and summary cards", () => {
  assert.match(source, /Overall Data Health/);
  assert.match(source, /Backend API Health/);
  assert.match(source, /Provider Health/);
  assert.match(source, /Cache \/ DB Health/);
  assert.match(source, /Architecture Map/);
  assert.match(source, /Data Pipelines/);
  assert.match(source, /Provider Endpoints/);
  assert.match(source, /Internal API Health/);
  assert.match(source, /Recent Health Events \/ Errors/);
});

test("architecture map is lightweight SVG and shows the production layers", () => {
  assert.match(source, /<svg className="min-w-\[760px\]"/);
  assert.match(source, /Vercel \/ Next\.js Frontend/);
  assert.match(source, /Fly\.io \/ FastAPI Backend/);
  assert.match(source, /Auth \+ Entitlements/);
  assert.match(source, /Admin Observability/);
  assert.match(source, /Cache Layer/);
  assert.match(source, /Production DB/);
  assert.match(source, /Background Jobs \/ Ingestion/);
  assert.match(source, /Provider Layer/);
  assert.doesNotMatch(source, /ReactFlow|react-flow|@xyflow/);
});

test("provider and internal route tables expose read-only health fields", () => {
  assert.match(source, /Endpoint URL \/ route template/);
  assert.match(source, /Secret name/);
  assert.match(source, /Secret status/);
  assert.match(source, /p95 latency/);
  assert.match(source, /Last checked/);
  assert.match(source, /Last successful refresh/);
  assert.match(source, /Latest error/);
  assert.match(source, /Consumer/);
  assert.match(source, /Error rate/);
  assert.match(source, /Last seen/);
});

test("pipeline copy uses correct institutional language and includes expected flows", () => {
  assert.match(source, /data\.pipelines\.map/);
  assert.match(source, /pipeline\.flow\.map/);
  assert.match(backendSnapshotSource, /Quarterly reported holdings/);
  assert.match(backendSnapshotSource, /Filing date normalization/);
  assert.match(backendSnapshotSource, /Quarter-end holdings/);
  assert.match(backendSnapshotSource, /not live trading/);
  assert.match(backendSnapshotSource, /FMP Congress Latest/);
  assert.match(backendSnapshotSource, /FMP house-latest \/ senate-latest/);
  assert.match(backendSnapshotSource, /FMP Insider Trading Latest/);
  assert.match(backendSnapshotSource, /FMP insider-trading\/latest/);
  assert.match(backendSnapshotSource, /Official House\/Senate disclosure links are retained/);
  assert.match(backendSnapshotSource, /SEC Form 4 filing links are retained/);
  assert.match(backendSnapshotSource, /Government Contracts/);
  assert.match(backendSnapshotSource, /Options Flow/);
});

test("no edit save delete or provider testing controls are present", () => {
  assert.doesNotMatch(source, /<input\b|<select\b|<textarea\b|<button\b/);
  assert.doesNotMatch(source, /Save endpoints|Test endpoint|Use default|Request\/response contract/);
  assert.doesNotMatch(source, /primary_endpoint_url|fallback_endpoint_url|primary_endpoint_contract_json|fallback_endpoint_contract_json/);
  assert.doesNotMatch(source, /ProviderSettingPatch|DataSourceRunPayload|EndpointEditor/);
});

test("secret values are not rendered and only names or status labels are shown", () => {
  assert.match(source, /secret_names/);
  assert.match(source, /Configured/);
  assert.match(source, /Missing/);
  assert.match(source, /Unknown/);
  assert.doesNotMatch(source, /slice\(.*secret|hash|prefix|suffix|apikey=.*\$\{/i);
  assert.doesNotMatch(source, /Authorization/);
});

test("frontend api uses the single data architecture endpoint", () => {
  assert.match(apiSource, /export type AdminDataArchitectureResponse/);
  assert.match(apiSource, /export async function getAdminDataArchitecture/);
  assert.match(apiSource, /buildApiUrl\("\/api\/admin\/data-architecture"\)/);
  assert.match(apiSource, /source: "AdminDataArchitecture"/);
});
