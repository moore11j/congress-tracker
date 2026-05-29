import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const columnsPath = path.join(process.cwd(), "lib", "screenerColumns.ts");
const source = fs.readFileSync(columnsPath, "utf8");

test("technical columns appear only for active technical filters", () => {
  assert.match(source, /if \(hasAnyActiveParam\(params, \["rel_volume_min", "rel_volume_max"\]\)\) columns\.push\("rel_volume"\);/);
  assert.match(source, /if \(hasAnyActiveParam\(params, \["price_move_min", "price_move_max"\]\)\) columns\.push\("price_move_pct"\);/);
  assert.match(source, /if \(hasAnyActiveParam\(params, \["rsi_min", "rsi_max"\]\)\) columns\.push\("rsi"\);/);
  assert.match(source, /if \(hasActiveParam\(params, "macd_state"\)\) columns\.push\("macd_state"\);/);
  assert.match(source, /if \(hasActiveParam\(params, "trend_state"\)\) columns\.push\("trend_state"\);/);
});

test("empty and Any technical values are inactive", () => {
  assert.match(source, /if \(value === undefined \|\| value === null\) return false;/);
  assert.match(source, /return cleaned !== "" && cleaned\.toLowerCase\(\) !== "any";/);
});
