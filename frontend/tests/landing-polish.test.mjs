import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const landingPage = fs.readFileSync(path.join(root, "app/landing/page.tsx"), "utf8");
const faqPage = fs.readFileSync(path.join(root, "app/faq/page.tsx"), "utf8");
const legalShell = fs.readFileSync(path.join(root, "components/landing/LegalPageShell.tsx"), "utf8");
const legalPageChrome = fs.readFileSync(path.join(root, "lib/legalPageChrome.ts"), "utf8");
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

test("landing quote cards render prices with two decimals", () => {
  assert.match(landingPage, /minimumFractionDigits:\s*2/);
  assert.match(landingPage, /maximumFractionDigits:\s*2/);
  assert.doesNotMatch(landingPage, /maximumFractionDigits:\s*value >= 100 \? 0 : 2/);
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
  assert.match(legalShell, /chrome\?: "public" \| "embedded"/);
  assert.match(legalShell, /if \(chrome === "embedded"\)/);
  assert.match(legalPageChrome, /publicLandingHosts\.has\(host\) \? "public" : "embedded"/);
  assert.match(faqPage, /const chrome = await legalPageChrome\(\)/);
  assert.match(faqPage, /chrome=\{chrome\}/);
  assert.match(middleware, /publicStaticPaths = new Set\(\["\/landing", "\/pricing", "\/terms", "\/privacy", "\/faq"\]\)/);
  assert.match(middleware, /appHost = "app\.walnutmarkets\.com"/);
  assert.match(middleware, /const isMarketingStaticPage = publicStaticPaths\.has\(pathname\) && publicLandingHosts\.has\(host\)/);
  assert.match(middleware, /if \(isMarketingStaticPage \|\| publicAccountPaths\.has\(pathname\)\)/);
  assert.match(middleware, /publicLandingHosts\.has\(host\) && !publicStaticPaths\.has\(pathname\) && !publicAccountPaths\.has\(pathname\)/);
  assert.match(middleware, /appUrl\.host = appHost/);
  assert.match(middleware, /matcher: \["\/", "\/robots\.txt", "\/landing", "\/pricing", "\/terms", "\/privacy", "\/faq", "\/ticker\/:path\*", "\/insider\/:path\*"/);
  assert.match(faqPage, /title: "Frequently Asked Questions \| Walnut Markets"/);
  assert.match(faqPage, /Answers about data sources, disclosures, billing, privacy, and how Walnut Market Terminal works\./);
  assert.match(faqPage, /Data & Disclosures/);
  assert.match(faqPage, /Subscriptions & Billing/);
  assert.match(faqPage, /No\. Walnut provides informational and research tools only\./);
});

test("terminal app routes log request intent and bypass anonymous bot prefetch SSR", () => {
  assert.match(middleware, /const terminalRouteFamilies = \["ticker", "insider", "member", "institution"\] as const/);
  assert.match(middleware, /function isPrefetchRequest\(request: NextRequest\)/);
  assert.match(middleware, /function isBotUserAgent\(userAgent: string\)/);
  assert.match(middleware, /function safeRefererPath\(referer: string, request: NextRequest\)/);
  assert.match(middleware, /console\.info\(\s*"terminal_page_request"/);
  assert.match(middleware, /referer: safeRefererPath\(referer, request\)/);
  assert.match(middleware, /user_agent: userAgent\.slice\(0, 180\)/);
  assert.match(middleware, /authenticated: hasBackendSession \|\| hasAuthHint/);
  assert.match(middleware, /isTerminalRoute\(pathname\) && !hasBackendSession && !hasAuthHint && \(prefetch \|\| bot \|\| !isInteractiveBrowserUserAgent\(userAgent\)\)/);
  assert.match(middleware, /terminalShellResponse\(pathname, host, prefetch \? "prefetch" : bot \? "bot" : "inactive"\)/);
  assert.match(middleware, /reason === "prefetch"\s+\?\s+null/);
  assert.match(middleware, /"x-walnut-terminal-shell": reason/);
  assert.match(middleware, /"x-robots-tag": "noindex, nofollow"/);
});
