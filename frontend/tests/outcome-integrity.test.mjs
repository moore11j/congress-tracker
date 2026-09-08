import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");
const outcomes = read("components/outcomes/OutcomeLedgerClient.tsx");
const api = read("lib/api.ts");

test("percentage formatting treats API values as percentage points exactly once", () => {
  assert.match(outcomes, /return `\$\{prefix\}\$\{value\.toFixed\(1\)\}%`/);
  assert.doesNotMatch(outcomes, /value \* 100/);
});

test("Outcome charts consume the exact event price-path endpoint", () => {
  assert.match(api, /\/api\/outcomes\/snapshots\/\$\{encodeURIComponent\(String\(snapshotId\)\)\}\/price-path/);
  assert.match(outcomes, /getOutcomePricePath\(selected\.id, Number\.parseInt\(horizon, 10\), controller\.signal\)/);
  assert.doesNotMatch(outcomes, /getTickerChartBundle\(selected\.ticker, 30/);
});

test("Outcome scatter plots measured or provisional thesis returns without clipping", () => {
  assert.match(outcomes, /const measuredReturn = numericReturn\(matured\?\.directional_return_pct \?\? matured\?\.return_pct\)/);
  assert.match(outcomes, /returnValue: measuredReturn \?\? currentReturn \?\? 0/);
  assert.match(outcomes, /const yExtent = Math\.ceil\(maxAbsoluteReturn \/ 5\) \* 5/);
  assert.doesNotMatch(outcomes, /Math\.max\(-25, Math\.min\(25, point\.returnValue\)\)/);
});

test("Outcome dates use the verified entry session without date-only timezone drift", () => {
  assert.match(outcomes, /snapshot\.entry_session_date \?\? snapshot\.entry_timestamp \?\? snapshot\.reference_price_at/);
  assert.match(outcomes, /dateOnly \? `\$\{value\}T12:00:00Z` : value/);
  assert.match(outcomes, /Awaiting \{horizon\} · provisional thesis return/);
  assert.match(outcomes, /X-axis = official entry date; weekends and market holidays are not shown\. The month appears on the first displayed date, followed by day numbers\. Filled dots = the selected horizon has been measured; the thesis may still be open\./);
  assert.match(outcomes, /openedTradingDays/);
  assert.match(outcomes, /openedTradingDayIndexes/);
  assert.match(outcomes, /x: xForOpened\(time\)/);
  assert.match(outcomes, /const x = xForOpened\(point\.opened\)/);
  assert.doesNotMatch(outcomes, /point\.opened - minOpened/);
  assert.match(outcomes, /label: index === 0 \|\| month !== priorMonth \? `\$\{month\} \$\{day\}` : day/);
  assert.match(outcomes, /openedTradingDays\.length <= 24/);
  assert.match(outcomes, /snapshot\.live_mark\?\.return_pct/);
  assert.match(outcomes, /if \(filter === "Matured"\) return `\$\{horizon\} Measured`/);
  assert.match(outcomes, /if \(filter === "Open"\) return "Thesis Open"/);
});

test("Outcome headline metrics explicitly exclude provisional points but include measured open and closed theses", () => {
  assert.match(outcomes, /Headline accuracy and averages use measured horizons only, across both open and closed theses; provisional outlined points and audit-held events are excluded\./);
  assert.match(outcomes, /measured calls across open and closed theses; provisional excluded/);
  assert.match(outcomes, /Average measured \$\{horizonFilter\} outcome across \$\{outcomeMetrics\.directionalSampleCount\} open and closed theses/);
});

test("Outcome table pagination is usable at every tier and supports bounded rapid navigation", () => {
  assert.match(outcomes, /const totalRows = sortedSnapshots\.length/);
  assert.match(outcomes, /Browse all outcomes · 10 rows per page/);
  assert.match(outcomes, /function movePage\(delta: number\) \{\s*setPage\(\(current\) => Math\.max\(0, Math\.min\(pageCount - 1, current \+ delta\)\)\);/);
  assert.match(outcomes, /aria-label="Outcome table pages"/);
  assert.match(outcomes, />\s*First\s*</);
  assert.match(outcomes, />\s*Previous\s*</);
  assert.match(outcomes, />\s*Next\s*</);
  assert.match(outcomes, />\s*Last\s*</);
  assert.doesNotMatch(outcomes, /if \(!gatePremiumTable\(\)\) return;\s*setPage/);
});

test("Outcome chart refreshes a horizon-balanced 500-event sample", () => {
  assert.match(outcomes, /getOutcomeSnapshots\(\{ limit: 500, horizon: horizonFilter \}\)/);
  assert.match(outcomes, /getOutcomeLedgerSummary\(\{ horizon: horizonFilter \}\)/);
  assert.match(api, /fetchOutcomePublicJson<OutcomeSnapshotsResponse>\(url, \{\s*cache: "no-store"/);
  assert.match(api, /!\[502, 503, 504\]\.includes\(error\.status\)/);
});

test("frontend never synthesizes missing Outcome returns", () => {
  assert.doesNotMatch(outcomes, /demoReturnForSnapshot/);
  assert.doesNotMatch(outcomes, /baseReturns =/);
  assert.match(outcomes, /data_integrity_status === "requires_reconstruction" \? "Audit hold"/);
});

test("null and missing horizons render as missing rather than zero", () => {
  assert.match(outcomes, /typeof outcome\.return_pct === "number"/);
  assert.match(outcomes, /return "-"/);
  assert.doesNotMatch(outcomes, /outcome\.return_pct \|\| 0/);
});
