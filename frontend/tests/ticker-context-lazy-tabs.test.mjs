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
  assert.ok(newsEffect.indexOf('if (activeTab !== "news")') < newsEffect.indexOf("getTickerNews(symbol"), "news fetch should be gated before the request");

  const financialsEffect = effectBlockStartingWith('if (activeTab !== "financials")');
  assert.match(financialsEffect, /return;/);
  assert.match(financialsEffect, /getTickerFinancials\(symbol/);
  assert.ok(
    financialsEffect.indexOf('if (activeTab !== "financials")') < financialsEffect.indexOf("getTickerFinancials(symbol"),
    "financials fetch should be gated before the request",
  );

  const pressEffect = effectBlockStartingWith('if (activeTab !== "events")');
  assert.match(pressEffect, /getTickerPressReleases\(symbol/);
  assert.ok(pressEffect.indexOf('if (activeTab !== "events")') < pressEffect.indexOf("getTickerPressReleases(symbol"), "press fetch should be gated before the request");

  const filingsEffect = effectBlockStartingWith('getTickerSecFilings(symbol');
  assert.match(filingsEffect, /if \(activeTab !== "events"\)/);
  assert.ok(filingsEffect.indexOf('if (activeTab !== "events")') < filingsEffect.indexOf("getTickerSecFilings(symbol"), "filings fetch should be gated before the request");

  const disclosureEffect = effectBlockStartingWith("const response = await getEvents");
  assert.match(disclosureEffect, /if \(activeTab !== "events"\)/);
  assert.ok(disclosureEffect.indexOf('if (activeTab !== "events")') < disclosureEffect.indexOf("getEvents({"), "events fetch should be gated before the request");
});

test("ticker lazy tab requests have panel-specific attribution", () => {
  assert.match(card, /const TICKER_NEWS_PANEL_SOURCE = "TickerNewsPanel"/);
  assert.match(card, /const TICKER_FINANCIALS_PANEL_SOURCE = "TickerFinancialsPanel"/);
  assert.match(card, /const TICKER_PRESS_PANEL_SOURCE = "TickerPressPanel"/);
  assert.match(card, /const TICKER_FILINGS_PANEL_SOURCE = "TickerFilingsPanel"/);
  assert.match(card, /const TICKER_DISCLOSURE_PANEL_SOURCE = "TickerDisclosurePanel"/);

  assert.match(card, /getTickerNews\(symbol, \{[^}]*source: TICKER_NEWS_PANEL_SOURCE/s);
  assert.match(card, /getTickerFinancials\(symbol, \{[^}]*source: TICKER_FINANCIALS_PANEL_SOURCE/s);
  assert.match(card, /getTickerPressReleases\(symbol, \{[^}]*source: TICKER_PRESS_PANEL_SOURCE/s);
  assert.match(card, /getTickerSecFilings\(symbol, \{[^}]*source: TICKER_FILINGS_PANEL_SOURCE/s);
  assert.match(card, /getEvents\(\{[^}]*source: TICKER_DISCLOSURE_PANEL_SOURCE/s);
});

test("ticker news tab uses a truthful empty headline state", () => {
  assert.match(card, /const NEWS_EMPTY_MESSAGE = "No recent headlines found\."/);
});
