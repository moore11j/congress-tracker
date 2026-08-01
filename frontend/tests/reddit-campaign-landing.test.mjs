import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const redditPage = read("app/reddit/stock-research/page.tsx");
const middleware = read("middleware.ts");
const sitemap = read("public/sitemap.xml");
const comparePage = read("app/compare/[left]/[right]/page.tsx");
const compareCta = read("components/compare/CompareContextualCta.tsx");
const researchCta = read("components/research/ResearchBriefContextualCta.tsx");
const watchlistQuickAdd = read("components/campaign/WatchlistQuickAddButton.tsx");
const generatedBriefPage = read("components/research/GeneratedResearchBriefPage.tsx");
const loginPanel = read("components/auth/LoginRegisterPanel.tsx");

test("reddit stock research landing page is separate, noindex and self-canonical", () => {
  assert.match(redditPage, /const pagePath = "\/reddit\/stock-research"/);
  assert.match(redditPage, /marketingPageMetadata\(pagePath/);
  assert.match(redditPage, /index: false,\s*follow: true/s);
  assert.match(redditPage, /Before you buy any stock, run it through Walnut\./);
  assert.match(redditPage, /Research Your First Stock Free/);
  assert.match(redditPage, /referring_landing_page: pagePath/);
  assert.match(middleware, /"\/reddit\/stock-research"/);
  assert.doesNotMatch(sitemap, /\/reddit\/stock-research/);
});

test("reddit landing page records required campaign events", () => {
  for (const eventName of [
    "reddit_landing_view",
    "reddit_landing_primary_cta_click",
    "reddit_landing_secondary_cta_click",
    "reddit_signup_start",
    "reddit_ticker_search_start",
  ]) {
    assert.match(redditPage + read("components/campaign/RedditTickerSearchForm.tsx"), new RegExp(eventName));
  }
});

test("compare pages render contextual CTA with return path and attribution", () => {
  assert.match(comparePage, /<CompareContextualCta/);
  assert.match(compareCta, /Track this comparison\./);
  assert.match(compareCta, /Create Free Account/);
  assert.match(compareCta, /Add Both to Watchlist/);
  assert.match(compareCta, /compare_contextual_cta_view/);
  assert.match(compareCta, /compare_contextual_cta_click/);
  assert.match(compareCta, /compare_signup_start/);
  assert.match(compareCta, /compare_add_watchlist_click/);
  assert.match(compareCta, /registerHref\(currentPath\)/);
  assert.match(compareCta, /ticker_a: tickerA/);
  assert.match(compareCta, /ticker_b: tickerB/);
});

test("research briefs use reusable CTA and events", () => {
  assert.match(generatedBriefPage, /<ResearchBriefContextualCta/);
  for (const route of ["app/research/mu-dd/page.tsx", "app/research/ai-earnings-dd/page.tsx", "app/research/nbis-vs-crwv-ai-neoclouds/page.tsx"]) {
    assert.match(read(route), /ResearchBriefContextualCta/);
  }
  for (const eventName of [
    "research_brief_contextual_cta_view",
    "research_brief_contextual_cta_click",
    "research_brief_signup_start",
    "research_brief_watchlist_click",
  ]) {
    assert.match(researchCta, new RegExp(eventName));
  }
  assert.match(researchCta, /cta_ticker/);
  assert.match(researchCta, /research_slug/);
});

test("signup redirects and completion events preserve campaign route context", () => {
  assert.match(loginPanel, /destination = resolvedReturnTo \? nextPath : "\/account\/settings\?registered=1"/);
  assert.match(loginPanel, /recordSignupCompleteEvents\(destination\)/);
  assert.match(loginPanel, /reddit_signup_complete/);
  assert.match(loginPanel, /compare_signup_complete/);
  assert.match(loginPanel, /research_brief_signup_complete/);
  assert.match(loginPanel, /campaignParamKeys/);
});

test("contextual watchlist actions avoid duplicate ticker adds", () => {
  assert.match(watchlistQuickAdd, /function watchlistHasSymbol/);
  assert.match(watchlistQuickAdd, /const toAdd = normalizedSymbols\.filter\(\(symbol\) => !watchlistHasSymbol\(target, symbol\)\)/);
  assert.match(watchlistQuickAdd, /for \(const symbol of toAdd\)/);
  assert.match(watchlistQuickAdd, /already saved in \$\{target\.name\}/);
});
