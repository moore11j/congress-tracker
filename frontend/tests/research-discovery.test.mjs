import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import vm from "node:vm";
import test from "node:test";
import ts from "typescript";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

const require = createRequire(import.meta.url);
function load(file, dependencies = {}) {
  const output = ts.transpileModule(readFileSync(file, "utf8"), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX, esModuleInterop: true },
  }).outputText;
  const module = { exports: {} };
  vm.runInNewContext(output, { module, exports: module.exports, URL, Intl,
    require: id => id in dependencies ? dependencies[id] : require(id),
  }, { filename: file });
  return module.exports;
}
const metadata = load("lib/marketingMetadata.ts", {
  "@/lib/homepageContent": { homepageContent: { metadata: { title: "Walnut", description: "Research", socialDescription: "Research" } } },
});
const card = (slug, date = "2026-09-18") => ({ slug, route: `/research/${slug}`, title: slug, description: "Evidence", tickers: ["NVDA"], category: "Research", publishedAt: date, readingMinutes: 7 });
function archive(fetcher = async () => ({ items: [] })) {
  return load("lib/researchArchive.ts", {
    react: { cache: fn => fn },
    "@/lib/api": { getGeneratedResearchBriefCards: fetcher },
    "@/lib/researchBriefs": { getPublishedResearchBriefs: () => [card("older-static", "2026-07-20")] },
  });
}
const Link = ({ children, prefetch, ...props }) => React.createElement("a", props, children);
const navigation = { notFound: () => { throw new Error("NOT_FOUND"); }, redirect: url => { throw new Error(`REDIRECT:${url}`); } };
function section(data) {
  return load("components/insights/ResearchBriefsSection.tsx", {
    "next/link": Link, "next/navigation": navigation,
    "@/lib/researchArchive": data, "@/lib/marketingMetadata": metadata,
  }).ResearchBriefsSection;
}

test("archive HTML contains current generated briefs without client effects and preserves premium labels", async () => {
  let requests = 0;
  const data = archive(async () => { requests++; return { items: [{ ...card("latest-brief"), premium: true, requiredPlan: "pro" }] }; });
  const html = renderToStaticMarkup(await section(data)({ mode: "archive" }));
  assert.equal(requests, 1);
  assert.match(html, /href="https:\/\/walnutmarkets.com\/research\/latest-brief"/);
  assert.ok(html.indexOf("latest-brief") < html.indexOf("older-static"));
  assert.match(html, />Pro</);
});

test("archive pages expose working next and previous anchors and distinct briefs", async () => {
  const data = archive(async () => ({ items: Array.from({ length: 12 }, (_, i) => card(`new-${String(i).padStart(2, "0")}`)) }));
  const Section = section(data);
  const first = renderToStaticMarkup(await Section({ mode: "archive", page: 1 }));
  const second = renderToStaticMarkup(await Section({ mode: "archive", page: 2 }));
  assert.match(first, /href="https:\/\/walnutmarkets.com\/research\?page=2" rel="next"/);
  assert.match(second, /href="https:\/\/walnutmarkets.com\/research" rel="prev"/);
  assert.match(second, /href="https:\/\/walnutmarkets.com\/research\?page=3" rel="next"/);
  assert.match(second, /new-06/);
  assert.doesNotMatch(second, /new-00/);
  assert.doesNotMatch(first, /<button/);
  await assert.rejects(Section({ mode: "archive", page: 4 }), /NOT_FOUND/);
});

test("archive failures do not silently return a stale static-only list", async () => {
  const data = archive(async () => { throw new Error("provider unavailable"); });
  await assert.rejects(section(data)({ mode: "archive" }), /provider unavailable/);
});

test("archive metadata self-canonicalizes each page and rejects malformed page values", async () => {
  const data = archive();
  for (const value of ["0", "-1", "2x", "01", "1.5", ["2", "3"], "9007199254740992"]) assert.equal(data.researchArchivePage(value), null);
  assert.equal(data.researchArchivePage(undefined), 1);
  const Page = load("app/research/page.tsx", {
    "next/link": Link, "next/navigation": navigation,
    "@/lib/researchArchive": data, "@/lib/marketingMetadata": metadata,
    "@/components/insights/ResearchBriefsSection": { ResearchBriefsSection: () => null },
  });
  const meta = await Page.generateMetadata({ searchParams: Promise.resolve({ page: "2" }) });
  assert.equal(meta.alternates.canonical, "https://walnutmarkets.com/research?page=2");
  assert.equal(meta.openGraph.url, meta.alternates.canonical);
  assert.equal(meta.robots.index, true);
  await assert.rejects(Page.default({ searchParams: Promise.resolve({ page: "1" }) }), /REDIRECT:\/research/);
  await assert.rejects(Page.default({ searchParams: Promise.resolve({ page: "bad" }) }), /NOT_FOUND/);
});

test("archive deduplicates known static slugs and excludes non-article routes", () => {
  const data = archive();
  const items = data.mergeResearchBriefs([card("static")], [card("static"), { ...card("bad"), route: "/admin/settings" }]);
  assert.equal(items.length, 1);
});

test("legacy research links open actual financials tabs and foreign URLs stay unchanged", () => {
  const { researchLinkHref } = load("lib/researchLinks.ts");
  for (const value of ["/ticker/NVDA/earnings", "https://walnutmarkets.com/ticker/nvda/financials", "https://app.walnutmarkets.com/ticker/NVDA/financials/"]) {
    assert.equal(researchLinkHref(value), "https://app.walnutmarkets.com/ticker/NVDA#financials");
  }
  assert.equal(researchLinkHref("/research/example"), "https://walnutmarkets.com/research/example");
  assert.equal(researchLinkHref("https://example.com/ticker/NVDA/earnings"), "https://example.com/ticker/NVDA/earnings");
  assert.equal(researchLinkHref("javascript:alert(1)"), "");
  assert.equal(researchLinkHref("//evil.example"), "");
});

test("generated brief HTML shows truthful publication details, repaired links and public related titles", () => {
  const component = load("components/research/GeneratedResearchBriefPage.tsx", {
    "next/link": Link, "@/lib/marketingMetadata": metadata,
    "@/lib/researchLinks": load("lib/researchLinks.ts"),
    "@/components/research/ResearchBriefContextualCta": { ResearchBriefContextualCta: () => null },
    "@/components/research/MuPremiumGate": { PremiumResearchGate: () => React.createElement("div", null, "Premium gate") },
    "@/components/research/ResearchBriefTopNav": { ResearchBriefTopNav: () => null },
    "@/components/campaign/CampaignAnalytics": { CampaignEventOnMount: () => null },
  });
  const draft = { primary_ticker: "NVDA", created_at: "2026-09-10T00:00:00Z", published_at: "2026-09-14T00:00:00Z", updated_at: "2026-09-18T00:00:00Z",
    article: { slug: "test", title: "Test brief", summary: "Summary", primary_ticker: "NVDA", sections: [{ key: "summary", heading: "Evidence", body_markdown: "See [earnings](/ticker/NVDA/earnings)." }],
      access: { premium_required: true, full_article_visible: false }, catalysts: [], risks: [], watch_items: [], source_links: [] } };
  const html = renderToStaticMarkup(React.createElement(component.GeneratedResearchBriefPage, { draft, relatedBriefs: [card("related-brief")] }));
  assert.match(html, /Published <time dateTime="2026-09-14T00:00:00Z">Sep 14, 2026<\/time>/);
  assert.match(html, /AI-assisted research/);
  assert.match(html, /href="https:\/\/app.walnutmarkets.com\/ticker\/NVDA#financials"/);
  assert.match(html, /href="https:\/\/walnutmarkets.com\/research\/related-brief"/);
  assert.match(html, /Premium gate/);
  const jsonLd = component.generatedResearchJsonLd(draft, "https://walnutmarkets.com/research/test");
  assert.equal(jsonLd.article.author.name, "Walnut Markets");
  assert.equal(jsonLd.article.datePublished, draft.published_at);
});
