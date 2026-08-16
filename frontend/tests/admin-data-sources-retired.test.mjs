import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const adminPanel = fs.readFileSync(path.join(process.cwd(), "components", "admin", "AdminSettingsPanel.tsx"), "utf8");
const apiClient = fs.readFileSync(path.join(process.cwd(), "lib", "api.ts"), "utf8");

test("admin data architecture dashboard is retired", () => {
  assert.doesNotMatch(adminPanel, /data_sources|Data Sources|DataSourcesReport/);
  assert.doesNotMatch(apiClient, /\/api\/admin\/data-sources|\/api\/admin\/data-architecture|AdminDataArchitecture/);
});
