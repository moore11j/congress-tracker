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
  const metricSection = tickerPage.slice(tickerPage.indexOf("const FUNDAMENTALS_METRICS"), tickerPage.indexOf("function fundamentalsToneClass"));
  assert.match(tickerPage, /Fundamental strength/);
  assert.match(tickerPage, /Mixed fundamental profile/);
  assert.match(tickerPage, /Fundamental pressure/);
  assert.match(tickerPage, /Fundamentals unavailable/);
  assert.match(tickerPage, /Revenue Growth/);
  assert.match(tickerPage, /ROE/);
  assert.match(tickerPage, /EV\/EBITDA/);
  assert.match(tickerPage, /Op Margin \\u0394/);
  assert.match(tickerPage, /Net Debt \/ EBITDA/);
  assert.doesNotMatch(metricSection, /fcf_yield|FCF Yield/);
  assert.match(metricSection, /key: "revenue_growth"[\s\S]*key: "return_on_equity"[\s\S]*key: "ev_to_ebitda"[\s\S]*key: "operating_margin_expansion"[\s\S]*key: "net_debt_to_ebitda"/);
  assert.match(fundamentalsSection, /"\\u2014"/);
  assert.doesNotMatch(fundamentalsSection, /Bearish tape confirmation/);
});

test("upper price volume card renders five compact rows including MACD", () => {
  assert.match(tickerPage, /function compactPriceVolumeRows/);
  assert.match(tickerPage, /Latest close \$\{formatUpperCardPrice/);
  assert.match(tickerPage, /1D change \$\{formatUpperCardSignedPercent/);
  assert.match(tickerPage, /Vol vs 30D \$\{formatUpperCardMultiple/);
  assert.doesNotMatch(tickerPage, /Vol vs 20D/);
  assert.match(tickerPage, /RSI \$\{formatUpperCardRsi/);
  assert.match(tickerPage, /MACD bullish/);
  assert.match(tickerPage, /MACD bearish/);
  assert.match(tickerPage, /MACD neutral/);
  assert.match(tickerPage, /MACD \\u2014/);
  assert.match(tickerPage, /const rsi = context\?\.rsi \?\? technicalIndicators\.rsi/);
  assert.match(tickerPage, /const macd = context\?\.macd \?\? technicalIndicators\.macd/);
});

test("ticker overview refreshes signal summary and mentions price volume when active", () => {
  assert.match(tickerPage, /const loadFreshSignalSummary = \(\) => getTickerSignalsSummary/);
  assert.match(tickerPage, /if \(contextBundle\?\.signals_summary\) return contextBundle\.signals_summary/);
  assert.match(tickerPage, /signalSummaryRequest: loadFreshSignalSummary\(\)/);
  assert.match(tickerPage, /Price \/ Volume: bearish tape/);
  assert.match(tickerPage, /Price \/ Volume: bullish tape/);
  assert.match(tickerPage, /Price \/ Volume: mixed tape/);
  assert.match(tickerPage, /Institutional Activity: active \/ reduction/);
  assert.match(tickerPage, /Institutional Activity: active \/ accumulation/);
  assert.match(tickerPage, /Institutional Activity: active \/ mixed/);
  assert.match(tickerPage, /return Array\.from\(bullets\)\.slice\(0, 5\)/);
});
