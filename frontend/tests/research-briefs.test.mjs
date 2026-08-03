import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const registry = read("lib/researchBriefs.ts");
const muPage = read("app/research/mu-dd/page.tsx");
const nvdaMuPage = read("app/research/nvda-vs-mu/page.tsx");
const spcxPage = read("app/research/spcx-earnings-preview/page.tsx");
const researchIndexPage = read("app/research/page.tsx");
const insightsPage = read("app/insights/page.tsx");
const researchSection = read("components/insights/ResearchBriefsSection.tsx");
const generatedBriefPage = read("components/research/GeneratedResearchBriefPage.tsx");

function registryEntry(slug) {
  const slugIndex = registry.indexOf(`slug: "${slug}"`);
  assert.notEqual(slugIndex, -1, `Expected registry entry for ${slug}`);
  const start = registry.lastIndexOf("  {", slugIndex);
  const end = registry.indexOf("\n  },", slugIndex);
  assert.notEqual(start, -1, `Expected registry entry start for ${slug}`);
  assert.notEqual(end, -1, `Expected registry entry end for ${slug}`);
  return registry.slice(start, end + "\n  },".length);
}

test("mu dd brief remains a canonical research brief", () => {
  const muEntry = registryEntry("mu-dd");
  assert.match(registry, /slug: "mu-dd"/);
  assert.match(registry, /route: "\/research\/mu-dd"/);
  assert.match(registry, /title: "Is the Micron momentum trade dead\?"/);
  assert.match(registry, /A Walnut preview of the MU momentum setup/);
  assert.match(muEntry, /premium: true/);
  assert.doesNotMatch(muEntry, /judgment: "bullish"/);
  assert.doesNotMatch(registry, /A research-only Micron DD landing page/);
  assert.match(muEntry, /publishedAt: "2026-07-20"/);
  assert.match(muEntry, /featured: true/);
});

test("mu dd route reuses canonical research metadata", () => {
  assert.match(muPage, /getResearchBriefBySlug\("mu-dd"\)/);
  assert.match(muPage, /title: `\$\{brief\?\.title/);
  assert.match(muPage, /description: brief\?\.description/);
  assert.doesNotMatch(muPage, /description:\s*"A research-only Micron DD landing page/);
});

test("spcx earnings preview is published as a static research brief", () => {
  assert.match(registry, /slug: "spcx-earnings-preview"/);
  assert.match(registry, /route: "\/research\/spcx-earnings-preview"/);
  assert.match(registry, /title: "SPCX earnings preview: can growth justify its valuation\?"/);
  assert.match(registry, /tickers: \["SPCX"\]/);
  assert.match(registry, /publishedAt: "2026-08-03"/);
  assert.match(spcxPage, /getResearchBriefBySlug\("spcx-earnings-preview"\)/);
  assert.match(spcxPage, /SpaceX reports its first public-company earnings on August 4, 2026/);
  assert.match(spcxPage, /ResearchBriefContextualCta ticker="SPCX"/);
  assert.match(spcxPage, /view_ticker_spcx_click/);
  assert.match(spcxPage, /Not investment advice/);
});

test("nvda vs mu is published as a premium gated research brief", () => {
  const nvdaMuEntry = registryEntry("nvda-vs-mu");
  assert.match(registry, /slug: "nvda-vs-mu"/);
  assert.match(registry, /route: "\/research\/nvda-vs-mu"/);
  assert.match(registry, /title: "NVDA vs MU: which is the better buy right now\?"/);
  assert.match(registry, /tickers: \["NVDA", "MU"\]/);
  assert.match(nvdaMuEntry, /premium: true/);
  assert.match(nvdaMuPage, /force-dynamic/);
  assert.match(nvdaMuPage, /getResearchBriefBySlug\("nvda-vs-mu"\)/);
  assert.match(nvdaMuPage, /optionalPageAuthToken/);
  assert.match(nvdaMuPage, /canReadFullArticle/);
  assert.match(nvdaMuPage, /PremiumResearchGate/);
  assert.match(nvdaMuPage, /Unlock Walnut's Full NVDA vs MU Conclusion/);
  assert.match(nvdaMuPage, /research_full_article_viewed/);
  assert.match(nvdaMuPage, /canReadFull \? "More upside torque: MU\. Better business plus cleaner data: NVDA\." : "The NVDA versus MU debate turns on quality versus cycle torque\."/);
  assert.match(nvdaMuPage, /ResearchBriefContextualCta ticker="NVDA"/);
  assert.match(nvdaMuPage, /https:\/\/app\.walnutmarkets\.com\/ticker\/NVDA/);
  assert.match(nvdaMuPage, /https:\/\/app\.walnutmarkets\.com\/ticker\/MU/);
  assert.match(nvdaMuPage, /Not investment advice/);
});

test("mu dd brief gates the conclusion behind premium access only", () => {
  assert.match(muPage, /force-dynamic/);
  assert.match(muPage, /optionalPageAuthToken/);
  assert.match(muPage, /canReadFullArticle/);
  assert.match(muPage, /<MuPremiumGate authState=\{authenticated \? "free" : "logged_out"\} entitlement=\{userEntitlement\} returnTo=\{returnTo\} \/>/);
  assert.match(muPage, /canReadFull \? "Cyclical\? Yes\. Broken\? Not on the current data\." : "The MU debate turns on whether the memory cycle is rolling over\."/);
  assert.match(muPage, /research_full_article_viewed/);
  assert.match(muPage, /research_preview_viewed/);
});

test("mu premium gate uses requested copy, CTAs, and analytics events", () => {
  const gate = read("components/research/MuPremiumGate.tsx");
  assert.match(gate, /heading="Unlock Walnut's Full MU Conclusion"/);
  assert.match(gate, /See the confirmation score, directional judgment, supporting evidence, catalysts, risks, and what could change the outlook\./);
  assert.match(gate, /Create an Account to Continue/);
  assert.match(gate, /Unlock with Premium/);
  assert.match(gate, /View Premium Plans/);
  for (const eventName of [
    "research_preview_viewed",
    "research_paywall_viewed",
    "research_paywall_cta_clicked",
    "research_signup_started",
    "research_checkout_started",
  ]) {
    assert.match(gate, new RegExp(eventName));
  }
});

test("insights renders research briefs from the registry", () => {
  assert.match(insightsPage, /<ResearchBriefsSection \/>/);
  assert.match(researchSection, /getPublishedResearchBriefs/);
  assert.match(researchSection, /brief\.route/);
  assert.match(researchSection, /Read brief/);
  assert.match(researchSection, /brief\.premium/);
  assert.match(researchSection, /Premium/);
  assert.match(researchSection, /const BRIEFS_PER_PAGE = 6/);
  assert.match(researchSection, /mode = "preview"/);
  assert.match(researchSection, /href="\/research"/);
  assert.match(researchSection, /Open Research Briefs/);
  assert.match(researchSection, /sortBriefsNewestFirst\(\[\.\.\.staticBriefs, \.\.\.generated\]\)/);
  assert.match(researchSection, /showAll \? briefs : briefs\.slice\(pageIndex \* BRIEFS_PER_PAGE, pageIndex \* BRIEFS_PER_PAGE \+ BRIEFS_PER_PAGE\)/);
  assert.match(researchSection, /briefs\.slice\(pageIndex \* BRIEFS_PER_PAGE, pageIndex \* BRIEFS_PER_PAGE \+ BRIEFS_PER_PAGE\)/);
  assert.match(researchSection, /xl:grid-cols-3/);
  assert.match(researchSection, /Show more/);
  assert.match(researchSection, /setPageIndex\(\(current\) => Math\.min\(totalPages - 1, current \+ 1\)\)/);
  assert.doesNotMatch(researchSection, /NVDA vs MU: Quality vs Cycle Torque/);
  assert.doesNotMatch(researchSection, /View all briefs/);
});

test("research index opens the full research briefs archive", () => {
  assert.match(researchIndexPage, /appPageMetadata\("\/research"/);
  assert.match(researchIndexPage, /Back to Insights/);
  assert.match(researchIndexPage, /All published Walnut research briefs/);
  assert.match(researchIndexPage, /<ResearchBriefsSection mode="archive" \/>/);
});

test("generated research briefs render pipe-delimited data as production tables", () => {
  assert.match(generatedBriefPage, /function cleanInlineText/);
  assert.match(generatedBriefPage, /replace\(\/\\\*\\\*\/g, ""\)/);
  assert.match(generatedBriefPage, /function parsePipeTable/);
  assert.match(generatedBriefPage, /const columnCount = 3/);
  assert.match(generatedBriefPage, /isMarkdownDivider\(cells\.slice\(cursor, cursor \+ columnCount\)\)/);
  assert.match(generatedBriefPage, /<ResearchDataTable key=\{block\.key\} header=\{block\.header\} rows=\{block\.rows\} \/>/);
  assert.match(generatedBriefPage, /<table className="min-w-full border-collapse text-left text-sm">/);
  assert.match(generatedBriefPage, /rowIndex % 2 === 0/);
});

test("generated research briefs render markdown links and bare urls as anchors", () => {
  assert.match(generatedBriefPage, /function inlineMarkdown/);
  assert.match(generatedBriefPage, /markdownLinkPattern/);
  assert.match(generatedBriefPage, /function autoLinkUrls/);
  assert.match(generatedBriefPage, /target=\{href\.startsWith\("http"\) \? "_blank" : undefined\}/);
  assert.match(generatedBriefPage, /<p key=\{block\.key\}>\{inlineMarkdown\(block\.text\)\}<\/p>/);
  assert.match(generatedBriefPage, /\{inlineMarkdown\(row\[cellIndex\] \|\| ""\)\}/);
});

test("generated research briefs expose stored-signal conversion and miss visuals", () => {
  assert.match(generatedBriefPage, /signal_results/);
  assert.match(generatedBriefPage, /price_move_charts/);
  assert.match(generatedBriefPage, /function StoredSignalsHeroGraphic/);
  assert.match(generatedBriefPage, /function StoredSignalResultsTable/);
  assert.match(generatedBriefPage, /META moved against the signal/);
  assert.match(generatedBriefPage, /row\.aligned \? "Aligned" : "Miss"/);
  assert.match(generatedBriefPage, /function TickerLookupCard/);
  assert.match(generatedBriefPage, /placeholder="Enter a ticker"/);
  assert.match(generatedBriefPage, /function generatedResearchJsonLd/);
  assert.match(generatedBriefPage, /walnut-intel-logo-mark\.png/);
});
