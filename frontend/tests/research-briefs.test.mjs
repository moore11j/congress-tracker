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

test("mu dd brief is the first canonical research brief", () => {
  const configuredBriefs = registry.slice(registry.indexOf("export const researchBriefs"));
  const firstSlugIndex = configuredBriefs.indexOf('slug: "mu-dd"');
  assert.ok(firstSlugIndex >= 0, "MU DD brief should be configured");
  assert.equal(firstSlugIndex, configuredBriefs.indexOf("slug:"), "MU DD should be the first configured brief");
  assert.match(registry, /route: "\/research\/mu-dd"/);
  assert.match(registry, /title: "Is the MU momentum trade dead\?"/);
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
  assert.doesNotMatch(researchSection, /NVDA vs MU: Quality vs Cycle Torque/);
  assert.doesNotMatch(researchSection, /View all briefs/);
});
