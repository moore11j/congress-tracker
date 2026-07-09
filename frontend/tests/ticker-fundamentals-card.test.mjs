import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const tickerPage = read("app/ticker/[symbol]/page.tsx");
const api = read("lib/api.ts");

test("ticker fundamentals card is wired into the upper context row", () => {
  assert.match(api, /export type TickerFundamentalsSummary/);
  assert.match(api, /fundamentals\?: TickerFundamentalsSummary/);
  assert.match(tickerPage, /fundamentalsContext: signalsRes\.fundamentals \?\? null/);
  assert.match(tickerPage, /<FundamentalsCard summary=\{fundamentalsContext\} \/>/);
  assert.match(tickerPage, /<p className="text-\[11px\] font-semibold uppercase tracking-\[0\.16em\] text-slate-400">Price \/ Volume<\/p>[\s\S]*<FundamentalsCard summary=\{fundamentalsContext\} \/>/);
});

test("fundamentals card uses source-specific copy and missing metric dash", () => {
  const fundamentalsSection = tickerPage.slice(tickerPage.indexOf("function FundamentalsCard"));
  assert.match(tickerPage, /Fundamental strength/);
  assert.match(tickerPage, /Mixed fundamental profile/);
  assert.match(tickerPage, /Fundamental pressure/);
  assert.match(tickerPage, /Fundamentals unavailable/);
  assert.match(tickerPage, /Revenue Growth/);
  assert.match(tickerPage, /Net Debt \/ EBITDA/);
  assert.match(fundamentalsSection, /"\\u2014"/);
  assert.doesNotMatch(fundamentalsSection, /Bearish tape confirmation/);
});
