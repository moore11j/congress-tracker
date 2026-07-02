import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

test("global search only requests suggestions while focused and aborts stale fetches", () => {
  const search = read("components/GlobalSearch.tsx");
  const hook = read("hooks/useFastSearchSuggest.ts");
  const api = read("lib/api.ts");

  assert.match(search, /const \[searchFocused, setSearchFocused\] = useState\(false\)/);
  assert.match(search, /enabled: searchFocused/);
  assert.match(search, /setSearchFocused\(true\)/);
  assert.match(search, /setSearchFocused\(false\)/);
  assert.doesNotMatch(search, /GlobalSearchPrefetch|prefetchSearchPrefixes|PREFETCH_SESSION_KEY|prefetchedSearchPrefixes/);
  assert.match(hook, /enabled\?: boolean/);
  assert.match(hook, /if \(!enabled \|\| trimmed\.length < minLength\)/);
  assert.match(hook, /abortRef\.current\?\.abort\(\)/);
  assert.match(api, /signal: options\?\.signal/);
  assert.match(api, /searchSuggestCache/);
  assert.match(api, /searchSuggestPromises/);
});

test("events and auth helpers coalesce identical short-lived requests", () => {
  const api = read("lib/api.ts");

  assert.match(api, /if \(mePromise\) return mePromise/);
  assert.match(api, /const eventsCache = new Map/);
  assert.match(api, /const eventsPromises = new Map/);
  assert.match(api, /const EVENTS_CACHE_TTL_MS = 5_000/);
  assert.match(api, /const cacheKey = `events:\$\{url\}`/);
  assert.match(api, /if \(pending\) return raceWithAbort\(pending, requestSignal\)/);
  assert.match(api, /eventsCache\.set\(cacheKey, \{ value: normalized, expiresAt: Date\.now\(\) \+ EVENTS_CACHE_TTL_MS \}\)/);
  assert.match(api, /nextParams\.debug === undefined/);
});

test("ticker reuses server signals summary for source cards and defers chart bundle pressure", () => {
  const tickerPage = read("app/ticker/[symbol]/page.tsx");
  const signalActivity = read("components/ticker/TickerSignalActivityClient.tsx");
  const signalCard = read("components/ticker/TickerSignalsSourceCardClient.tsx");
  const institutionalCard = read("components/ticker/TickerInstitutionalSourceCardClient.tsx");
  const chart = read("components/ticker/TickerChartLoader.tsx");

  assert.match(tickerPage, /signalSummaryResolved: signalsResult\.resolved/);
  assert.match(tickerPage, /const canReuseSignalSummary = signalSummaryResolved && !signalsAuthPending/);
  assert.match(tickerPage, /initialResolved=\{canReuseSignalSummary\}/);
  assert.match(tickerPage, /initialItems=\{null\}/);
  assert.match(signalActivity, /getSignalsAll\(\{/);
  assert.match(signalActivity, /source: "TickerSignalActivity"/);
  assert.match(signalActivity, /if \(hasInitialItems\) \{/);
  assert.match(signalCard, /fallbackSource\.present \|\| initialResolved/);
  assert.match(institutionalCard, /initialResolved \|\| sourceLocked/);
  assert.match(chart, /const CHART_VISIBILITY_FALLBACK_MS = 2200/);
  assert.match(chart, /IntersectionObserver/);
  assert.match(chart, /if \(!shouldLoad\) return/);
  assert.match(chart, /return runHeavyTickerRequest/);
});
