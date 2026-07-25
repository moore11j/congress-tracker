import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (relativePath) => fs.readFileSync(path.join(root, relativePath), "utf8");

const registry = read("lib/researchBriefs.ts");
const muPage = read("app/research/mu-dd/page.tsx");
const insightsPage = read("app/insights/page.tsx");
const researchSection = read("components/insights/ResearchBriefsSection.tsx");
const generatedBriefPage = read("components/research/GeneratedResearchBriefPage.tsx");

test("mu dd brief remains a canonical research brief", () => {
  assert.match(registry, /slug: "mu-dd"/);
  assert.match(registry, /route: "\/research\/mu-dd"/);
  assert.match(registry, /title: "Is the MU momentum trade dead\?"/);
  assert.match(registry, /The bear case needs memory demand to roll over/);
  assert.doesNotMatch(registry, /A research-only Micron DD landing page/);
  assert.match(registry, /publishedAt: "2026-07-20"/);
  assert.match(registry, /featured: true/);
});

test("mu dd route reuses canonical research metadata", () => {
  assert.match(muPage, /getResearchBriefBySlug\("mu-dd"\)/);
  assert.match(muPage, /title: `\$\{brief\?\.title/);
  assert.match(muPage, /description: brief\?\.description/);
  assert.doesNotMatch(muPage, /description:\s*"A research-only Micron DD landing page/);
});

test("insights renders research briefs from the registry", () => {
  assert.match(insightsPage, /<ResearchBriefsSection \/>/);
  assert.match(researchSection, /getPublishedResearchBriefs/);
  assert.match(researchSection, /brief\.route/);
  assert.match(researchSection, /Read brief/);
  assert.match(researchSection, /const BRIEFS_PER_PAGE = 6/);
  assert.match(researchSection, /sortBriefsNewestFirst\(\[\.\.\.staticBriefs, \.\.\.generated\]\)/);
  assert.match(researchSection, /briefs\.slice\(pageIndex \* BRIEFS_PER_PAGE, pageIndex \* BRIEFS_PER_PAGE \+ BRIEFS_PER_PAGE\)/);
  assert.match(researchSection, /xl:grid-cols-3/);
  assert.match(researchSection, /Show more/);
  assert.match(researchSection, /setPageIndex\(\(current\) => Math\.min\(totalPages - 1, current \+ 1\)\)/);
  assert.doesNotMatch(researchSection, /NVDA vs MU: Quality vs Cycle Torque/);
  assert.doesNotMatch(researchSection, /View all briefs/);
});

test("generated research briefs render pipe-delimited data as production tables", () => {
  assert.match(generatedBriefPage, /function parsePipeTable/);
  assert.match(generatedBriefPage, /const columnCount = 3/);
  assert.match(generatedBriefPage, /isMarkdownDivider\(cells\.slice\(cursor, cursor \+ columnCount\)\)/);
  assert.match(generatedBriefPage, /<ResearchDataTable key=\{block\.key\} header=\{block\.header\} rows=\{block\.rows\} \/>/);
  assert.match(generatedBriefPage, /<table className="min-w-full border-collapse text-left text-sm">/);
  assert.match(generatedBriefPage, /rowIndex % 2 === 0/);
});
