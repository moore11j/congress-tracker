import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const landingPage = fs.readFileSync(path.join(root, "app/landing/page.tsx"), "utf8");
const faqPage = fs.readFileSync(path.join(root, "app/faq/page.tsx"), "utf8");
const legalShell = fs.readFileSync(path.join(root, "components/landing/LegalPageShell.tsx"), "utf8");
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");

test("landing insights link keeps label and arrow on one line", () => {
  assert.match(landingPage, /inline-flex[^"]*whitespace-nowrap[^"]*/);
  assert.match(landingPage, /Open insights/);
  assert.match(landingPage, /aria-hidden="true">→<\/span>/);
});

test("landing Pelosi portfolio card links to canonical member profile", () => {
  assert.match(landingPage, /href=\{`\$\{appUrl\}\/member\/nancy-pelosi`\}/);
  assert.doesNotMatch(landingPage, /\/member\/nancy-pelosi\?portfolio_lb=1095/);
  assert.match(landingPage, /Nancy Pelosi disclosure portfolio/);
});

test("landing Tim Cook insider card links to lightweight canonical profile", () => {
  assert.match(landingPage, /const timCookInsiderUrl = `\$\{appUrl\}\/insider\/tim-cook-0001214156`/);
  assert.match(landingPage, /href=\{timCookInsiderUrl\}/);
  assert.doesNotMatch(landingPage, /tim-cook-0001214156\?issuer=AAPL&chart=stock/);
  assert.match(landingPage, /Tim Cook insider activity profile/);
});

test("landing mobile header uses feed-style login instead of terminal launch", () => {
  assert.match(
    landingPage,
    /href=\{loginUrl\}\s+className="whitespace-nowrap rounded-lg border border-emerald-300\/30 bg-emerald-300\/10 px-3 py-1\.5 text-sm font-medium text-emerald-100 transition hover:bg-emerald-300\/15 md:hidden"[\s\S]*?Login \/ Register/,
  );
  assert.match(
    landingPage,
    /href=\{appUrl\}\s+className="hidden rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950\/30 transition hover:bg-emerald-200 md:inline-flex"[\s\S]*?Launch Terminal/,
  );
});

test("landing SEO labels use insights and stock screener copy", () => {
  assert.match(landingPage, /Explore Insights/);
  assert.doesNotMatch(landingPage, /Explore Signals/);
  assert.match(landingPage, /<SectionEyebrow>Daily Insights<\/SectionEyebrow>/);
  assert.match(landingPage, /<SectionEyebrow>Congress and Insider Trade Profiles<\/SectionEyebrow>/);
  assert.match(landingPage, /\["Stock Screener", "#screener"\]/);
  assert.match(landingPage, /<SectionEyebrow>Stock Screener<\/SectionEyebrow>/);
});

test("landing macro rows resolve Core CPI by label variants", () => {
  assert.match(landingPage, /landingMacroLabelGroups/);
  assert.match(landingPage, /"Core CPI YoY"/);
  assert.match(landingPage, /"core_cpi_yoy"/);
  assert.match(landingPage, /"CPILFESL"/);
  assert.match(landingPage, /const economics = landingMacroRows\(snapshot\.economics \?\? \[\]\)/);
});

test("public legal navigation includes FAQ across landing and legal shell", () => {
  assert.match(landingPage, /href="\/faq"[\s\S]*?FAQ/);
  assert.match(legalShell, /href="\/faq"[\s\S]*?FAQ/);
  assert.match(middleware, /publicStaticPaths = new Set\(\["\/landing", "\/terms", "\/privacy", "\/faq"\]\)/);
  assert.match(middleware, /matcher: \["\/", "\/landing", "\/terms", "\/privacy", "\/faq"/);
  assert.match(faqPage, /title: "Frequently Asked Questions \| Walnut Markets"/);
  assert.match(faqPage, /Answers about data sources, disclosures, billing, privacy, and how Walnut Market Terminal works\./);
  assert.match(faqPage, /Data & Disclosures/);
  assert.match(faqPage, /Subscriptions & Billing/);
  assert.match(faqPage, /No\. Walnut provides informational and research tools only\./);
});
