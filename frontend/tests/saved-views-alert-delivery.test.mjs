import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const source = fs.readFileSync(path.join(process.cwd(), "components/saved-views/SavedViewsBar.tsx"), "utf8");

test("saved-screen alert controls are clearly labeled and persist exclusive cadence flags", () => {
  assert.match(source, /<span className=\{`uppercase tracking-wide \$\{dense \? "text-slate-400" : "text-slate-500"\}`\}>Alerts<\/span>/);
  assert.match(source, /daily_digest_enabled: nextDelivery === "daily"/);
  assert.match(source, /intraday_alerts_enabled: nextDelivery === "intraday"/);
});
