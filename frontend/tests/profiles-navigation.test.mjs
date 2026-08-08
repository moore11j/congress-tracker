import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = process.cwd();
const read = (path) => readFileSync(join(root, path), "utf8");

test("top nav adds Profiles dropdown between Insights and Signals", () => {
  const nav = read("components/AppTopNav.tsx");

  assert.match(nav, /Profiles <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /href: "\/profiles", label: "Overview"/);
  assert.match(nav, /href: "\/members", label: "Congress"/);
  assert.match(nav, /href: "\/insiders", label: "Insiders"/);
  assert.match(nav, /href: "\/institutions", label: "Institutions"/);
  assert.match(nav, /href: "\/departments", label: "Departments"/);
  assert.match(nav, /link\.href !== "\/insights"[\s\S]*<Fragment key="insights-profiles">[\s\S]*\{navLink\}[\s\S]*Profiles/);
});

test("top nav adds Company dropdown after Pricing with matching arrows", () => {
  const nav = read("components/AppTopNav.tsx");

  assert.match(nav, /Tools <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /Company <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /const companyNavItems = \[[\s\S]*href: "\/about", label: "About"[\s\S]*href: "\/faq", label: "FAQ"[\s\S]*href: "\/contact", label: "Contact"[\s\S]*href: "\/terms", label: "Terms"[\s\S]*href: "\/privacy", label: "Privacy"/);
  assert.match(nav, /href="\/pricing"[\s\S]*Pricing[\s\S]*Company <span aria-hidden="true">&#9662;<\/span>/);
  assert.match(nav, /aria-label="Company"/);
  assert.match(nav, /data-company-link/);
});

test("profile landing routes include SEO metadata and shared native components", () => {
  const pages = [
    ["app/profiles/page.tsx", "Investor & Government Profiles"],
    ["app/members/page.tsx", "Congress Stock Trading & Member Portfolios"],
    ["app/insiders/page.tsx", "Insider Trading Activity & Corporate Insider Purchases"],
    ["app/institutions/page.tsx", "Institutional Holdings & 13F Position Changes"],
    ["app/departments/page.tsx", "Government Contracts & Department Spending"],
  ];

  for (const [path, title] of pages) {
    const source = read(path);
    assert.match(source, new RegExp(title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    assert.match(source, /ProfilePageHeader|SummaryCards|MetricGrid|DataPanel/);
    assert.doesNotMatch(source, /sidebar/i);
  }
});

test("profiles overview uses directory overview instead of the duplicate activity feed", () => {
  const source = read("app/profiles/page.tsx");

  assert.match(source, /ProfileDirectoryGrid/);
  assert.doesNotMatch(source, /ActivityFeed/);
  assert.doesNotMatch(source, /searchParams/);
});
