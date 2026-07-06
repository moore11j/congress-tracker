import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const screenerPage = read("app/screener/page.tsx");
const exportButton = read("components/screener/ScreenerExportButton.tsx");

test("screener CSV export lock copy uses the configured required plan label", () => {
  assert.match(screenerPage, /getPlanConfig\(\)/);
  assert.match(screenerPage, /\.catch\(\(\) => defaultPlanConfig\)/);
  assert.match(screenerPage, /csvExportRequiredPlanLabel\(planConfig\)/);
  assert.match(screenerPage, /lockedReason=\{csvExportLockedReason\}/);
  assert.match(screenerPage, /requiredPlanLabel=\{csvExportPlanLabel\}/);

  assert.match(exportButton, /requiredPlanLabel = "Pro"/);
  assert.match(exportButton, /CSV export is a \$\{planLabel\} feature\./);
  assert.match(exportButton, /`Export CSV - \$\{planLabel\}`/);
  assert.doesNotMatch(exportButton, /CSV export is included with Premium/);
  assert.doesNotMatch(exportButton, /Export CSV .*Premium/);
});
