import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");

test("admin research briefs section is admin-only and exposed in admin navigation", () => {
  const page = read("app/admin/research-briefs/page.tsx");
  const panel = read("components/admin/AdminSettingsPanel.tsx");

  assert.match(page, /VerifiedSessionGuard[\s\S]*requireAdmin/);
  assert.match(page, /initialTab="research_briefs"/);
  assert.match(panel, /label: "Research Briefs"/);
  assert.match(panel, /<AdminResearchBriefGeneratorView showToast=\{showToast\} \/>/);
});

test("research brief generator uses Responses-backed admin APIs and defaults to drafts", () => {
  const component = read("components/admin/AdminResearchBriefGeneratorView.tsx");
  const api = read("lib/api.ts");

  assert.match(component, /Generate Draft/);
  assert.match(component, /Save Draft/);
  assert.match(component, /Ready for Review/);
  assert.match(component, /window\.confirm\("Publish this research brief/);
  assert.match(component, /window\.prompt\("Type DELETE/);
  assert.match(component, /include_charts: false/);
  assert.match(api, /\/api\/admin\/research-briefs\/generate/);
  assert.match(api, /publication_default/);
});

test("public research brief integration preserves existing MU brief", () => {
  const registry = read("lib/researchBriefs.ts");
  const section = read("components/insights/ResearchBriefsSection.tsx");
  const generatedPage = read("app/research/[slug]/page.tsx");

  assert.match(registry, /slug: "mu-dd"/);
  assert.match(registry, /route: "\/research\/mu-dd"/);
  assert.match(section, /getPublishedResearchBriefs/);
  assert.match(section, /getGeneratedResearchBriefCards/);
  assert.match(section, /!seen\.has\(brief\.slug\)/);
  assert.match(generatedPage, /GeneratedResearchBriefPage/);
});
