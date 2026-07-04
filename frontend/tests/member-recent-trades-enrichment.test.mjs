import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const memberPageSource = fs.readFileSync(
  path.join(process.cwd(), "app", "member", "[slug]", "page.tsx"),
  "utf8",
);
const memberAnalyticsSource = fs.readFileSync(
  path.join(process.cwd(), "components", "member", "MemberAnalyticsClient.tsx"),
  "utf8",
);
const feedCardSource = fs.readFileSync(
  path.join(process.cwd(), "components", "feed", "FeedCard.tsx"),
  "utf8",
);
const typesSource = fs.readFileSync(
  path.join(process.cwd(), "lib", "types.ts"),
  "utf8",
);

test("member recent trades pass outcome enrichment through to feed cards", () => {
  assert.match(memberPageSource, /initialTrades=\{initialTrades\}/);
  assert.match(memberAnalyticsSource, /useState<MemberTradesResponse>\(\(\) => initialTrades \?\? tradesFallback/);
  assert.match(memberAnalyticsSource, /trade\.estimated_price != null/);
  assert.match(memberAnalyticsSource, /trade\.current_price != null/);
  assert.match(memberAnalyticsSource, /tone\(trade\.pnl_pct\)/);
  assert.match(memberAnalyticsSource, /trade\.pnl_source/);
});

test("feed cards render compact estimated shares and explicit missing pnl state", () => {
  assert.match(feedCardSource, /const congressEstimatedShares = isCongress/);
  assert.match(feedCardSource, /Est\. Shares: \{formatShares\(congressEstimatedShares\)\}/);
  assert.match(feedCardSource, /const missingPnlLabel =/);
  assert.match(feedCardSource, />\{missingPnlLabel\}<\/div>/);
  assert.match(feedCardSource, /outcomeReasonLabel/);
});

test("member trade types include enriched outcome display fields", () => {
  assert.match(typesSource, /estimated_trade_value\?: number \| null/);
  assert.match(typesSource, /estimated_shares\?: number \| null/);
  assert.match(typesSource, /outcome_methodology\?: string \| null/);
  assert.match(typesSource, /price_basis\?: string \| null/);
});
