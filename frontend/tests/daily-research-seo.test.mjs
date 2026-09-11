import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const ui = fs.readFileSync("components/admin/DailyResearchSeo.tsx", "utf8");
const api = fs.readFileSync("lib/api.ts", "utf8");
test("daily SEO controls disclose approval and spending limits", () => {
  for (const text of ["Nothing publishes without your approval", "one discovery attempt per day", "No automatic daily discovery retry", "Seven unreviewed drafts", "Save Daily SEO"]) assert.ok(ui.includes(text));
  assert.match(ui, /disabled=\{busy \|\| dirty\}/);
  assert.match(ui, /Preview and edit draft/);
  assert.match(ui, /Review email:/);
  assert.match(ui, /role="alert"/);
});
test("daily SEO uses authenticated admin routes and does not publish from its UI", () => {
  assert.match(api, /\/api\/admin\/research-briefs\/daily-seo\/run/);
  assert.doesNotMatch(ui, /publishNowAdminResearchBriefDraft|approveScheduledAdminResearchBriefDraft/);
  assert.match(ui, /encodeURIComponent\(run.draft_id\)/);
});

test("published briefs have a discoverable, fresh sitemap without draft leakage", () => {
  const route = fs.readFileSync("app/sitemap-research.xml/route.ts", "utf8");
  const middleware = fs.readFileSync("middleware.ts", "utf8");
  assert.match(route, /getGeneratedResearchBriefCards/);
  assert.doesNotMatch(route, /getAdminResearchBriefDrafts/);
  assert.match(route, /status: 503/);
  assert.match(route, /s-maxage=60/);
  assert.match(middleware, /Sitemap: https:\/\/walnutmarkets\.com\/sitemap-research\.xml/);
  assert.match(middleware, /normalized === "\/sitemap-research.xml"/);
});
