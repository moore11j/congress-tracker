import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const card = read("components/ticker/TickerContextCard.tsx");

function effectBlockStartingWith(fragment) {
  const fragmentIndex = card.indexOf(fragment);
  assert.notEqual(fragmentIndex, -1, `missing fragment: ${fragment}`);
  const start = card.lastIndexOf("  useEffect(() => {", fragmentIndex);
  assert.notEqual(start, -1, `missing effect start before: ${fragment}`);
  const end = card.indexOf("  }, [activeTab", fragmentIndex);
  assert.notEqual(end, -1, `missing activeTab effect close after: ${fragment}`);
  return card.slice(start, end);
}

test("ticker context starts on overview and loads heavy tabs only after tab activation", () => {
  assert.match(card, /const \[activeTab, setActiveTab\] = useState<ContextTab>\("overview"\)/);

  const newsEffect = effectBlockStartingWith('if (activeTab !== "news")');
  assert.match(newsEffect, /return;/);
  assert.match(newsEffect, /getTickerNews\(symbol/);
  assert.ok(newsEffect.indexOf('if (activeTab !== "news")') < newsEffect.indexOf("getTickerNews(symbol"));

  const financialsEffect = effectBlockStartingWith('if (activeTab !== "financials")');
  assert.match(financialsEffect, /return;/);
  assert.match(financialsEffect, /getTickerFinancials\(symbol/);
  assert.ok(financialsEffect.indexOf('if (activeTab !== "financials")') < financialsEffect.indexOf("getTickerFinancials(symbol"));

  const ownershipEffect = effectBlockStartingWith('if (activeTab !== "ownership")');
  assert.match(ownershipEffect, /return;/);
  assert.match(ownershipEffect, /getTickerOwnership\(symbol/);
  assert.ok(ownershipEffect.indexOf('if (activeTab !== "ownership")') < ownershipEffect.indexOf("getTickerOwnership(symbol"));
  assert.ok(ownershipEffect.indexOf("if (!canViewOwnership)") < ownershipEffect.indexOf("getTickerOwnership(symbol"));

  const pressEffect = effectBlockStartingWith('getTickerPressReleases(symbol');
  assert.match(pressEffect, /if \(activeTab !== "events"\)/);
  assert.ok(pressEffect.indexOf('if (activeTab !== "events")') < pressEffect.indexOf("getTickerPressReleases(symbol"));

  const filingsEffect = effectBlockStartingWith('getTickerSecFilings(symbol');
  assert.match(filingsEffect, /if \(activeTab !== "events"\)/);
  assert.ok(filingsEffect.indexOf('if (activeTab !== "events")') < filingsEffect.indexOf("getTickerSecFilings(symbol"));

  const disclosureEffect = effectBlockStartingWith("const response = await getEvents");
  assert.match(disclosureEffect, /if \(activeTab !== "events"\)/);
  assert.ok(disclosureEffect.indexOf('if (activeTab !== "events")') < disclosureEffect.indexOf("getEvents({"));
});

test("ticker lazy tab requests have panel-specific attribution", () => {
  assert.match(card, /const TICKER_NEWS_PANEL_SOURCE = "TickerNewsPanel"/);
  assert.match(card, /const TICKER_FINANCIALS_PANEL_SOURCE = "TickerFinancialsPanel"/);
  assert.match(card, /const TICKER_OWNERSHIP_PANEL_SOURCE = "TickerOwnershipPanel"/);
  assert.match(card, /const TICKER_PRESS_PANEL_SOURCE = "TickerPressPanel"/);
  assert.match(card, /const TICKER_FILINGS_PANEL_SOURCE = "TickerFilingsPanel"/);
  assert.match(card, /const TICKER_DISCLOSURE_PANEL_SOURCE = "TickerDisclosurePanel"/);

  assert.match(card, /getTickerNews\(symbol, \{[^}]*source: TICKER_NEWS_PANEL_SOURCE/s);
  assert.match(card, /getTickerFinancials\(symbol, \{[^}]*source: TICKER_FINANCIALS_PANEL_SOURCE/s);
  assert.match(card, /getTickerOwnership\(symbol, \{[^}]*source: TICKER_OWNERSHIP_PANEL_SOURCE/s);
  assert.match(card, /getTickerPressReleases\(symbol, \{[^}]*source: TICKER_PRESS_PANEL_SOURCE/s);
  assert.match(card, /getTickerSecFilings\(symbol, \{[^}]*source: TICKER_FILINGS_PANEL_SOURCE/s);
  assert.match(card, /getEvents\(\{[^}]*requestSource: "client"[^}]*routeFamily: "ticker"[^}]*source: TICKER_DISCLOSURE_PANEL_SOURCE/s);
});

test("ticker bottom activity detail fallback fetches on visibility with attribution", () => {
  const page = read("app/ticker/[symbol]/page.tsx");
  const detailClient = read("components/ticker/TickerActivityDetailClient.tsx");

  assert.match(page, /import \{ TickerActivityDetailClient \}/);
  assert.match(page, /<TickerActivityDetailClient kind="congress" symbol=\{normalizedSymbol\} lookbackDays=\{selectedLookbackDays\} side=\{side\} statusElementId="congress-activity-status" canViewPremiumMetrics=\{canViewPremiumMetrics\} \/>/);
  assert.match(page, /<TickerActivityDetailClient kind="insider" symbol=\{normalizedSymbol\} lookbackDays=\{selectedLookbackDays\} side=\{side\} statusElementId="insider-activity-status" canViewPremiumMetrics=\{canViewPremiumMetrics\} \/>/);
  assert.match(detailClient, /IntersectionObserver/);
  assert.match(detailClient, /requestSource: "visibility"/);
  assert.match(detailClient, /routeFamily: "ticker"/);
  assert.match(detailClient, /source: kind === "congress" \? "congress-detail" : "insider-detail"/);
});

test("ticker tabs use truthful public empty states", () => {
  assert.match(card, /const NEWS_EMPTY_MESSAGE = "No recent headlines found\."/);
  assert.match(card, /const ACTIVITY_EMPTY_MESSAGE = "No disclosure activity found for this ticker\."/);
});

test("ownership tab is a pro lazy tab", () => {
  const page = read("app/ticker/[symbol]/page.tsx");
  const api = read("lib/api.ts");
  const panel = read("components/ticker/TickerOwnershipPanel.tsx");

  assert.match(card, /type ContextTab = "overview" \| "news" \| "financials" \| "ownership" \| "events"/);
  assert.match(card, /canViewOwnership\?: boolean/);
  assert.match(card, /<TickerOwnershipPanel data=\{ownership\} locked=\{!canViewOwnership\} \/>/);
  assert.match(page, /canViewOwnership=\{canViewProTickerContext\}/);
  assert.match(api, /export async function getTickerOwnership/);
  assert.match(api, /\/api\/tickers\/\$\{tickerPathSymbol\(symbol\)\}\/ownership/);
  assert.match(panel, /Ownership breakdown requires Pro\./);
  assert.match(panel, /strokeDasharray="6 5"/);
});
