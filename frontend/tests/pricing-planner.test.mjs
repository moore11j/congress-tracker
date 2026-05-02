import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const pricingPlannerPath = path.join(process.cwd(), "components", "billing", "PricingPlanner.tsx");
const source = fs.readFileSync(pricingPlannerPath, "utf8");

test("annual pricing badge derives rounded-up months free from configured prices", () => {
  assert.match(source, /Math\.ceil\(\(\(monthlyYear - annualAmount\) \/ monthlyAmount\) \* 2\) \/ 2/);
  assert.match(source, /return `\$\{formattedMonths\} \$\{monthsFree === 1 \? "month" : "months"\} free`;/);
  assert.doesNotMatch(source, /Save \$\{percent\}%/);

  const annualSavingsLabel = (monthlyCents, annualCents) => {
    const monthlyYear = monthlyCents * 12;
    if (monthlyYear <= 0 || monthlyCents <= 0 || annualCents <= 0 || annualCents >= monthlyYear) return null;
    const monthsFree = Math.ceil(((monthlyYear - annualCents) / monthlyCents) * 2) / 2;
    const formattedMonths = Number.isInteger(monthsFree) ? monthsFree.toFixed(0) : monthsFree.toFixed(1);
    return `${formattedMonths} ${monthsFree === 1 ? "month" : "months"} free`;
  };

  assert.equal(annualSavingsLabel(1995, 19995), "2 months free");
  assert.equal(annualSavingsLabel(4995, 49995), "2 months free");
  assert.equal(annualSavingsLabel(2000, 22000), "1 month free");
  assert.equal(annualSavingsLabel(2000, 21000), "1.5 months free");
  assert.equal(annualSavingsLabel(2000, 24000), null);
});

test("included feature cells render as green checks while limits stay numeric", () => {
  assert.match(source, /aria-label="Included"[\s\S]*?text-emerald-300[\s\S]*?✓/);
  assert.match(source, /if \(feature\.kind === "limit"\) return formatLimit\(feature, feature\.limits\[tier\] \?\? 0\);/);
  assert.doesNotMatch(source, /return "Included";/);
});

test("free/core rows lead screener and monitoring pricing categories", () => {
  const screenerOrderStart = source.indexOf('"Screener & signals": {');
  const screenerOrderEnd = source.indexOf("},", screenerOrderStart);
  const screenerOrderSource = source.slice(screenerOrderStart, screenerOrderEnd);
  const screenerMarkers = ["screener:", "screener_results:", "screener_intelligence:", "screener_presets:", "signals:", "leaderboards:"];
  const screenerPositions = screenerMarkers.map((marker) => screenerOrderSource.indexOf(marker));
  screenerPositions.forEach((position, index) => assert.notEqual(position, -1, `missing screener marker ${screenerMarkers[index]}`));
  for (let index = 1; index < screenerPositions.length; index += 1) {
    assert.ok(screenerPositions[index] > screenerPositions[index - 1], `${screenerMarkers[index]} should follow ${screenerMarkers[index - 1]}`);
  }

  const monitoringOrderStart = source.indexOf('"Watchlists & monitoring": {');
  const monitoringOrderEnd = source.indexOf("},", monitoringOrderStart);
  const monitoringOrderSource = source.slice(monitoringOrderStart, monitoringOrderEnd);
  const monitoringMarkers = [
    "inbox_alerts:",
    "inbox_alert_retention:",
    "monitoring_sources:",
    "watchlists:",
    "watchlist_tickers:",
    "saved_views:",
    "screener_saved_screens:",
    "screener_monitoring:",
  ];
  const monitoringPositions = monitoringMarkers.map((marker) => monitoringOrderSource.indexOf(marker));
  monitoringPositions.forEach((position, index) => assert.notEqual(position, -1, `missing monitoring marker ${monitoringMarkers[index]}`));
  for (let index = 1; index < monitoringPositions.length; index += 1) {
    assert.ok(monitoringPositions[index] > monitoringPositions[index - 1], `${monitoringMarkers[index]} should follow ${monitoringMarkers[index - 1]}`);
  }
});
