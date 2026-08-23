import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const dashboard = readFileSync(join(process.cwd(), "components/profiles/EnhancedProfileDashboards.tsx"), "utf8");

test("Congress most-traded stocks table identifies its 12-month trade-count ranking", () => {
  assert.match(dashboard, /Last 12 months · ranked by trade count/);
  assert.match(dashboard, /Number\(right\.trades \?\? 0\) - Number\(left\.trades \?\? 0\)/);
});
