import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const tickerPage = read("app/ticker/[symbol]/page.tsx");
const chartLoader = read("components/ticker/TickerChartLoader.tsx");
const tickerContextCard = read("components/ticker/TickerContextCard.tsx");
const tickerSignalActivityClient = read("components/ticker/TickerSignalActivityClient.tsx");
const participantLeaderboards = read("components/ticker/TickerParticipantLeaderboards.tsx");
const api = read("lib/api.ts");

test("ticker page keeps confirmation on 30D while chart uses selected URL range", () => {
  assert.match(tickerPage, /const lookback = clampLookback\(one\(sp, "lookback"\)\)/);
  assert.match(tickerPage, /type Lookback = "1" \| "5" \| "30" \| "90" \| "180" \| "365"/);
  assert.match(tickerPage, /v === "1" \|\| v === "5" \|\| v === "30"/);
  assert.match(chartLoader, /\[1, "1D"\], \[5, "5D"\], \[30, "1M"\], \[90, "3M"\], \[180, "6M"\], \[365, "1Y"\]/);
  assert.match(chartLoader, /headerControls=\{rangeControls\}/);
  assert.match(tickerPage, /const SIGNAL_WINDOW_DAYS = 30/);
  assert.match(tickerPage, /const lookbackDays = Number\(lookback\)/);
  assert.match(tickerPage, /recent_days: lookbackDays/);
  assert.match(tickerPage, /getTickerSignalsSummary\(normalizedSymbol,[\s\S]*?lookback_days: lookbackDays/);
  assert.match(tickerPage, /lookbackDays=\{selectedLookbackDays\}/);
  assert.doesNotMatch(tickerSignalActivityClient, /lookbackStartKey/);
  assert.match(api, /congress_recent_days: params\.congress_recent_days/);
  assert.match(api, /insider_recent_days: params\.insider_recent_days/);
  assert.match(tickerPage, /effectiveWindowDays \?\? SIGNAL_WINDOW_DAYS/);
  assert.match(tickerPage, /activityConfirmationScoreBundle \?\? confirmationScoreBundle/);
  assert.match(tickerPage, /const selectedLookbackDays = Number\(lookback\)/);
  assert.match(tickerPage, /normalizeOptionsFlowSummary\(optionsFlowSummary, normalizedSymbol, effectiveLookbackDays\)/);
  assert.match(tickerPage, /optionsFlow = \{ \.\.\.optionsFlow, lookback_days: effectiveLookbackDays \}/);
  assert.match(tickerPage, /<TickerChartLoader symbol=\{normalizedSymbol\} days=\{selectedLookbackDays\} deferLoad=\{deferHeavyTickerLoads\} eager \/>/);
  assert.doesNotMatch(tickerPage, /<TickerChartLoader symbol=\{normalizedSymbol\} days=\{lookbackDays\}/);
  assert.doesNotMatch(tickerPage, /getTickerSignalsSummary\(normalizedSymbol,[\s\S]*?lookback_days: SIGNAL_WINDOW_DAYS/);
});

test("ticker shows the chart before research and removes redundant table filters", () => {
  assert.ok(tickerPage.indexOf("<TickerChartLoader") < tickerPage.indexOf("<TickerContextCard"));
  assert.doesNotMatch(tickerPage, />Activity view<|>Trade side<|>Chart range</);
  assert.match(tickerPage, /const source: SourceFilter = "all"/);
  assert.match(tickerPage, /const side: SideFilter = "all"/);
  assert.match(chartLoader, /aria-label="Chart time range"/);
});

test("ticker participant leaderboards rank the live activity tapes by trades and net flow", () => {
  assert.match(tickerPage, /<TickerParticipantLeaderboards/);
  assert.match(participantLeaderboards, /const RANKING_LIMIT = 100/);
  assert.match(participantLeaderboards, /right\.trades - left\.trades/);
  assert.match(participantLeaderboards, /Math\.abs\(right\.netFlow\) - Math\.abs\(left\.netFlow\)/);
  assert.match(participantLeaderboards, /Ranked by trade count · net flow breaks ties/);
  assert.match(participantLeaderboards, /<th className="px-2 py-2\.5">#<\/th>/);
  assert.match(participantLeaderboards, />Chamber<\/th>/);
  assert.match(participantLeaderboards, />Party<\/th>/);
  assert.match(participantLeaderboards, />Role<\/th>/);
  assert.match(participantLeaderboards, /Net flow<\/th>/);
  assert.doesNotMatch(participantLeaderboards, /<Badge/);
  assert.match(participantLeaderboards, /request\("congress"\), request\("insider"\)/);
});

test("ticker chart helper forwards selected days to chart-bundle", () => {
  assert.match(chartLoader, /getTickerChartBundle\(symbol, days,/);
  assert.match(chartLoader, /\}, \[attempt, days, shouldLoad, symbol\]\)/);
  assert.match(api, /buildApiUrl\(`\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/chart-bundle`, \{ days \}\)/);
});

test("ticker activity requests visible trade prices while heavy tab disclosure rows stay base-only", () => {
  assert.match(tickerPage, /enrich_prices: 0/);
  assert.match(tickerContextCard, /enrich_prices: 0/);
  assert.doesNotMatch(tickerPage, /source: "TickerEvents"/);
  assert.doesNotMatch(tickerPage, /limit: 100/);
  assert.match(tickerPage, /enrich_prices: 1,[\s\S]*?source: "TickerCongressActivity"/);
  assert.match(tickerPage, /enrich_prices: 1,[\s\S]*?source: "TickerInsiderActivity"/);
  assert.match(tickerPage, /ACTIVITY_FETCH_SIZE = ACTIVITY_PAGE_SIZE \+ 1/);
  assert.match(tickerPage, /const boundedEvents = \[/);
});
