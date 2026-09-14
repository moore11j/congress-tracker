import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = process.cwd();
const landingPage = fs.readFileSync(path.join(root, "app/landing/page.tsx"), "utf8");
const workflow = fs.readFileSync(path.join(root, "components/landing/HomepageWorkflow.tsx"), "utf8");
const researchExample = fs.readFileSync(path.join(root, "components/landing/HomepageResearchExample.tsx"), "utf8");
const homepageContent = fs.readFileSync(path.join(root, "lib/homepageContent.ts"), "utf8");
const landingSearch = fs.readFileSync(path.join(root, "components/landing/LandingSearch.tsx"), "utf8");
const marketingMetadata = fs.readFileSync(path.join(root, "lib/marketingMetadata.ts"), "utf8");
const marketingHeader = fs.readFileSync(path.join(root, "components/landing/MarketingHeader.tsx"), "utf8");
const faqPage = fs.readFileSync(path.join(root, "app/faq/page.tsx"), "utf8");
const contactPage = fs.readFileSync(path.join(root, "app/contact/page.tsx"), "utf8");
const contactForm = fs.readFileSync(path.join(root, "components/landing/ContactForm.tsx"), "utf8");
const contactApiRoute = fs.readFileSync(path.join(root, "app/api/contact/route.ts"), "utf8");
const legalShell = fs.readFileSync(path.join(root, "components/landing/LegalPageShell.tsx"), "utf8");
const legalPageChrome = fs.readFileSync(path.join(root, "lib/legalPageChrome.ts"), "utf8");
const middleware = fs.readFileSync(path.join(root, "middleware.ts"), "utf8");

test("landing retains a direct research briefs link in the consolidated workflow", () => {
  assert.match(workflow, /whitespace-nowrap/);
  assert.match(workflow, /Explore research briefs/);
  assert.match(workflow, /insights/);
});

test("landing removes the generic news feed and its fetches", () => {
  assert.doesNotMatch(landingPage, /loadLatestInsights|LatestInsightImage|loadMarketSnapshot/);
  assert.match(landingPage, /HomepageMethodology/);
});

test("landing removes standalone tool promos but preserves access paths", () => {
  assert.doesNotMatch(landingPage, /Nancy Pelosi disclosure portfolio/);
  assert.doesNotMatch(landingPage, /Tim Cook insider activity profile/);
  assert.doesNotMatch(landingPage, /<SectionEyebrow>Congress &amp; Insider Profiles<\/SectionEyebrow>/);
  assert.doesNotMatch(landingPage, /<SectionEyebrow>Stock Comparison Tool<\/SectionEyebrow>/);
  assert.doesNotMatch(landingPage, /<SectionEyebrow>Stock Screener<\/SectionEyebrow>/);
  assert.doesNotMatch(marketingHeader, /\{ label: "Congress", href: `\$\{appUrl\}\/feed\?mode=congress`/);
  assert.doesNotMatch(marketingHeader, /\{ label: "Insiders", href: `\$\{appUrl\}\/feed\?mode=insider`/);
  assert.match(marketingHeader, /\{ label: "Stock Screener", href: `\$\{appUrl\}\/screener`/);
  assert.match(marketingHeader, /\{ label: "Stock Comparisons", href: `\$\{appUrl\}\/compare`/);
});

test("landing logged-out header separates Log in from terminal launch", () => {
  assert.match(
    marketingHeader,
    /href=\{`\$\{appUrl\}\/login`\}\s+className="hidden rounded-lg border border-white\/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white\/25 hover:text-white md:inline-flex"[\s\S]*?Log in/,
  );
  assert.match(
    marketingHeader,
    /href=\{`\$\{appUrl\}\/login`\}\s+className="whitespace-nowrap rounded-lg border border-emerald-200 bg-emerald-300 px-3 py-1\.5 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950\/30 transition hover:bg-emerald-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 md:hidden"[\s\S]*?Log in/,
  );
  assert.match(
    marketingHeader,
    /href=\{appUrl\}\s+className="hidden rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950\/30 transition hover:bg-emerald-200 md:inline-flex"[\s\S]*?Launch Terminal/,
  );
  assert.doesNotMatch(marketingHeader, /Login \/ Register/);
});

test("landing header dropdowns layer above page content", () => {
  assert.match(landingPage, /<MarketingHeader pricingHref=\{pricingUrl\} \/>/);
  assert.doesNotMatch(legalShell, /MarketingHeader/);
  assert.match(marketingHeader, /<header className="sticky top-0 isolate z-\[8000\][\s\S]*style=\{\{ zIndex: 8000 \}\}/);
  assert.match(marketingHeader, /<DesktopMenu label="Profiles" heading="Profiles" items=\{profilesNavLinks\} \/>/);
  assert.match(marketingHeader, /<DesktopMenu label="Tools" heading="Research tools" items=\{toolsNavLinks\} \/>/);
  assert.match(marketingHeader, /<DesktopMenu label="Company" heading="Company" items=\{companyNavLinks\} \/>/);
  assert.match(marketingHeader, /z-\[10000\][\s\S]*bg-\[#030712\]/);
  assert.doesNotMatch(marketingHeader, /bg-slate-950\/96/);
  assert.match(landingSearch, /relative z-\[80\][\s\S]*z-\[1400\]/);
});

test("landing header menus are hover-only and include Strategies in the primary sequence", () => {
  assert.match(
    marketingHeader,
    /\{ label: "Feed"[\s\S]*\{ label: "Insights"[\s\S]*\{ label: "Signals"[\s\S]*\{ label: "Leaderboards"[\s\S]*\{ label: "Strategies"[\s\S]*\{ label: "Outcomes"/,
  );
  assert.match(marketingHeader, /group-hover:pointer-events-auto group-hover:visible group-hover:opacity-100/);
  assert.doesNotMatch(marketingHeader, /<details className="group relative isolate z-\[9000\]"/);
});

test("landing SEO labels use insights and stock screener copy", () => {
  assert.doesNotMatch(landingPage, /Explore Ticker Research/);
  assert.doesNotMatch(landingPage, /View Congress Trades/);
  assert.doesNotMatch(landingPage, /Explore Signals/);
  assert.doesNotMatch(landingPage, /\["Trends", "#signals"\]/);
  assert.match(marketingHeader, /\{ label: "Feed", href: `\$\{appUrl\}\/feed` \}/);
  assert.match(marketingHeader, /\{ label: "Insights", href: `\$\{appUrl\}\/insights` \}/);
  assert.match(marketingHeader, /\{ label: "Signals", href: `\$\{appUrl\}\/signals` \}/);
  assert.match(marketingHeader, /\{ label: "Outcomes", href: `\$\{appUrl\}\/outcomes` \}/);
  assert.match(marketingHeader, /\{ label: "Leaderboards", href: `\$\{appUrl\}\/leaderboards` \}/);
  assert.match(marketingHeader, /<LandingNavLink href=\{pricingHref\} label="Pricing" \/>/);
  assert.match(marketingHeader, /const profilesNavLinks = \[[\s\S]*\{ label: "Overview", href: `\$\{appUrl\}\/profiles`[\s\S]*\{ label: "Congress", href: `\$\{appUrl\}\/members`[\s\S]*\{ label: "Insiders", href: `\$\{appUrl\}\/insiders`[\s\S]*\{ label: "Institutions", href: `\$\{appUrl\}\/institutions`[\s\S]*\{ label: "Departments", href: `\$\{appUrl\}\/departments`/);
  assert.match(marketingHeader, /const companyNavLinks = \[[\s\S]*\{ label: "About", href: `\$\{appUrl\}\/about`[\s\S]*\{ label: "FAQ", href: "https:\/\/walnutmarkets\.com\/faq"[\s\S]*\{ label: "Contact", href: `\$\{appUrl\}\/contact`[\s\S]*\{ label: "Terms", href: `\$\{appUrl\}\/terms`[\s\S]*\{ label: "Privacy", href: `\$\{appUrl\}\/privacy`/);
  assert.match(marketingHeader, /<span>\{label\}<\/span>[\s\S]*&#9662;/);
  assert.match(marketingHeader, /<NavMenuItems items=\{companyNavLinks\} mobile \/>/);
  assert.doesNotMatch(landingPage, /\["Congress", "#congress"\]|\["Insiders", "#insiders"\]|\["Stock Comparisons", "#compare"\]|\["Stock Screener", "#screener"\]/);
  assert.match(marketingHeader, /const toolsNavLinks = \[/);
  assert.match(marketingHeader, /\{ label: "Stock Screener", href: `\$\{appUrl\}\/screener`/);
  assert.match(marketingHeader, /\{ label: "Stock Comparisons", href: `\$\{appUrl\}\/compare`/);
  assert.match(marketingHeader, /\{ label: "Backtesting", href: `\$\{appUrl\}\/backtesting`/);
  assert.doesNotMatch(marketingHeader, /\{ label: "Congress", href: `\$\{appUrl\}\/feed\?mode=congress`/);
  assert.doesNotMatch(marketingHeader, /\{ label: "Insiders", href: `\$\{appUrl\}\/feed\?mode=insider`/);
  assert.match(marketingHeader, /\{ label: "Strategies", href: `\$\{appUrl\}\/strategies`, beta: true/);
  assert.match(landingPage, /<MarketingHeader pricingHref=\{pricingUrl\} \/>/);
  assert.match(workflow, /Explore research briefs/);
  assert.doesNotMatch(landingPage, /<section id="insights"/);
  assert.doesNotMatch(landingPage, /<SectionEyebrow>Feature Depth<\/SectionEyebrow>/);
});

test("landing uses portfolio outcome positioning and preserves discovery actions", () => {
  assert.match(homepageContent, /Build Your Next Winning Portfolio/);
  assert.match(homepageContent, /historically strong market participants/);
  assert.match(landingPage, /homepageContent.hero.title/);
  assert.match(landingPage, /homepageContent.hero.description/);
  assert.match(landingPage, /Open Screener/);
  assert.match(landingPage, /eventName="see_top_performers_click"/);
  assert.match(landingPage, /View Leaderboards/);
  assert.match(landingPage, /Explore Strategies/);
  assert.match(landingPage, /leaderboards#top-stocks/);
  assert.match(landingPage, /eventName="top_stocks_click"/);
  assert.match(landingPage, /Top-Ranked Stocks/);
  assert.match(landingPage, /publicHomepageRanking/);
  assert.match(landingPage, /publicHomepageResearch/);
  assert.match(landingPage, /\/api\/leaderboards\/preview/);
  assert.doesNotMatch(landingPage, /recalculated|correction notice|outcomeSummary/);
});

test("homepage research activation keeps the existing search flow and emits GA4 funnel events", () => {
  assert.match(landingSearch, /getEntitlements/);
  assert.match(landingSearch, /recordGoogleAnalyticsEvent/);
  for (const eventName of [
    "homepage_view",
    "homepage_search_focus",
    "homepage_search_input",
    "homepage_ticker_selected",
    "homepage_research_stock_click",
  ]) {
    assert.match(landingSearch, new RegExp(eventName));
  }
  assert.match(landingSearch, /source_page_type: "homepage"/);
  assert.match(landingSearch, /auth_state: entitlements\.user \? "authenticated" : "anonymous"/);
  assert.match(landingSearch, /isHighConfidenceSearchResult\(bestResult, trimmedQuery\)/);
  assert.match(landingSearch, /window\.location\.href = absoluteAppHref\(appUrl, routeForSearchResult\(bestResult\)\)/);
  assert.match(landingSearch, /window\.location\.href = absoluteAppHref\(appUrl, searchResultsHref\(trimmedQuery\)\)/);
  assert.match(landingPage, /reassuranceCopy="Free to research · No credit card required"/);
  assert.match(landingSearch, /homepageViewTrackedRef/);
  assert.match(landingSearch, /searchFocusTrackedRef/);
  assert.match(landingSearch, /searchInputTrackedRef/);
});

test("landing metadata reflects research and monitoring positioning", () => {
  assert.match(marketingMetadata, /import \{ homepageContent \} from "@\/lib\/homepageContent"/);
  assert.match(marketingMetadata, /WALNUT_MARKETING_TITLE = homepageContent\.metadata\.title/);
  assert.match(marketingMetadata, /WALNUT_MARKETING_DESCRIPTION = homepageContent\.metadata\.description/);
  assert.match(homepageContent, /title: "Stock Analysis, Congress Trades & Insider Data \| Walnut Markets"/);
  assert.match(homepageContent, /Research ranked stocks with fundamentals, technicals, Congress trades, insider data/);
  assert.match(marketingMetadata, /canonical: marketingCanonicalUrl\("\/"\)/);
  assert.match(marketingMetadata, /openGraph:/);
  assert.match(marketingMetadata, /twitter:/);
});

test("landing replaces static proof with public data and withholds performance figures", () => {
  assert.match(landingPage, /HomepageResearchExample/);
  assert.match(landingPage, /PortfolioBlueprint/);
  assert.match(researchExample, /Supporting evidence/);
  assert.match(researchExample, /Risks and conflicting evidence/);
  assert.doesNotMatch(landingPage, /nvdaProductScreenshot|confirmationEvidence|loadPublishedStrategies|performance\\.benchmarkReturnPct|Research Memory/);
  assert.doesNotMatch(landingPage, /outcomes-confirmation-events\\.png|data-outcomes-screenshot/);
});

test("landing includes compact watchlist and alert proof", () => {
  assert.match(landingPage, /Follow what changes/);
  assert.match(landingPage, /\{homepageContent\.monitoring\.title\}/);
  assert.match(landingPage, /After researching a stock, track what happens next\./);
  assert.match(landingPage, /Watchlists · Monitoring · Alerts · Custom logical alerts/);
  assert.match(landingPage, /href=\{`\$\{appUrl\}\/watchlists`\}/);
  assert.match(homepageContent, /Save tickers to watchlists and get alerted when meaningful disclosures, news, press releases, institutional activity/);
});

test("landing consolidates participant and strategy discovery into the portfolio blueprint", () => {
  assert.match(workflow, /Your portfolio blueprint/);
  assert.match(workflow, /Learn from historical performers/);
  assert.match(workflow, /Study backtested strategies/);
  assert.match(workflow, /eventName="insider_profile_click"/);
  assert.match(workflow, /eventName="strategy_click"/);
  assert.doesNotMatch(landingPage, /followActivityCards|5,685 holdings|BlackRock, Inc/);
});

test("landing daily insights does not render old quote cards", () => {
  assert.doesNotMatch(landingPage, /loadTrendingTickers|fallbackTrending|formatTickerPrice|formatPct/);
  assert.doesNotMatch(landingPage, /trendingTickers\.slice/);
  assert.doesNotMatch(landingPage, /<SectionEyebrow>Market brief<\/SectionEyebrow>/);
});

test("landing pricing fetch bypasses the shared landing data cache", () => {
  assert.match(landingPage, /type LandingFetchCacheMode = "revalidate" \| "no-store"/);
  assert.match(landingPage, /cacheMode === "no-store" \? \{ cache: "no-store" as const \} : \{ next: \{ revalidate \} \}/);
  assert.match(landingPage, /landingFetchJson<PlanConfig>\("\/api\/plan-config", undefined, 2500, "no-store"\)/);
});

test("landing omits unused macro widgets while preserving the product navigation", () => {
  assert.doesNotMatch(landingPage, /LandingMarketSnapshot|landingMacroRows/);
  assert.match(landingPage, /Macro Positioning/);
});

test("landing navigation sends app-owned information pages to the app host", () => {
  assert.match(marketingHeader, /\{ label: "FAQ", href: "https:\/\/walnutmarkets\.com\/faq"/);
  assert.match(marketingHeader, /\{ label: "Contact", href: `\$\{appUrl\}\/contact`/);
  assert.match(marketingHeader, /pricingHref = `\$\{appUrl\}\/pricing`/);
  assert.doesNotMatch(legalShell, /MarketingHeader|chrome\?: "public" \| "embedded"/);
  assert.match(contactPage, /<ContactForm \/>/);
  assert.doesNotMatch(contactPage, /mailto:support@walnutmarkets.com/);
  assert.match(contactForm, /fetch\("\/api\/contact"/);
  assert.match(contactForm, /request_type: String\(formData\.get\("request_type"\)/);
  assert.match(contactForm, /"Feedback", "Reporting a bug", "Requesting a new feature", "General inquiry"/);
  assert.match(contactForm, /Your message was successfully sent\. We will try to respond within the next 2-3 business days\./);
  assert.match(contactApiRoute, /\$\{API_BASE\}\/api\/contact/);
  assert.match(contactApiRoute, /fallbackRefererPath: "\/contact"/);
  assert.match(middleware, /const publicStaticPaths = new Set\(\[/);
  for (const route of ["/landing", "/congress-trades", "/insider-trading-tracker"]) {
    assert.match(middleware, new RegExp(`"${route}"`));
  }
  assert.match(middleware, /const appHostedPaths = new Set\(\["\/about", "\/pricing", "\/terms", "\/privacy", "\/contact"\]\)/);
  assert.match(middleware, /\(publicLandingHosts\.has\(host\) \|\| legacyMarketingHosts\.has\(host\)\) && appHostedPaths\.has\(pathname\)/);
  assert.match(middleware, /appUrl\.hostname = appHost/);
  assert.match(middleware, /return NextResponse\.redirect\(appUrl, 308\)/);
  assert.match(middleware, /appHost = "app\.walnutmarkets\.com"/);
  assert.match(middleware, /const localDevHosts = new Set\(\["localhost", "127\.0\.0\.1", "::1"\]\)/);
  assert.match(middleware, /const isMarketingHost = publicLandingHosts\.has\(host\) \|\| localDevHosts\.has\(host\)/);
  assert.match(middleware, /The research archive stays public on walnutmarkets\.com, but uses the/);
  assert.match(middleware, /const isResearchArticle = isPublicResearchRoute\(pathname\) && pathname !== "\/research"/);
  assert.match(middleware, /const isMarketingStaticPage = \(publicStaticPaths\.has\(pathname\) \|\| isResearchArticle \|\| isPublicComparisonRoute\(pathname\)\) && isMarketingHost/);
  assert.match(middleware, /if \(isMarketingStaticPage \|\| publicAccountPaths\.has\(pathname\)\)/);
  assert.match(middleware, /publicLandingHosts\.has\(host\) && !publicStaticPaths\.has\(pathname\) && !isPublicResearchRoute\(pathname\) && !isPublicComparisonRoute\(pathname\) && !publicAccountPaths\.has\(pathname\)/);
  assert.match(middleware, /legacyMarketingHosts = new Set\(\["walnut-intel\.com", "www\.walnut-intel\.com", "www\.walnutmarkets\.com"\]\)/);
  assert.match(middleware, /return NextResponse\.redirect\(canonicalUrl, 301\)/);
  assert.match(middleware, /appUrl\.host = appHost/);
  assert.match(middleware, /"\/\(\(\?!_next\/static\|_next\/image\|favicon\.ico\|apple-icon\.png\|icon\.png\)\.\*\)"/);
  assert.match(faqPage, /title: "Frequently Asked Questions \| Walnut Markets"/);
  assert.match(faqPage, /Answers about data sources, disclosures, billing, privacy, and how Walnut Market Terminal works\./);
  assert.match(faqPage, /Data & Disclosures/);
  assert.match(faqPage, /Research Glossary/);
  assert.match(faqPage, /What are fundamentals\?/);
  assert.match(faqPage, /What are SMA and EMA\?/);
  assert.match(faqPage, /What is Walnut's confirmation score\?/);
  assert.match(faqPage, /What is FCF yield\?/);
  assert.match(faqPage, /institutionals, insiders, and Congress activity/);
  assert.match(faqPage, /Subscriptions & Billing/);
  assert.match(faqPage, /No\. Walnut provides informational and research tools only\./);
});

test("landing footer links Walnut's official Instagram and TikTok profiles", () => {
  assert.match(landingPage, /WALNUT_INSTAGRAM_URL/);
  assert.match(landingPage, /Instagram \/ @walnutmarkets/);
  assert.match(landingPage, /WALNUT_TIKTOK_URL/);
  assert.match(landingPage, /TikTok \/ @walnutmarkets/);
  assert.match(marketingMetadata, /WALNUT_INSTAGRAM_URL = "https:\/\/www\.instagram\.com\/walnutmarkets\/"/);
  assert.match(marketingMetadata, /WALNUT_TIKTOK_URL = "https:\/\/www\.tiktok\.com\/@walnutmarkets"/);
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
  assert.match(middleware, /isTerminalRoute\(pathname\) && !isPublicSeoEntityRoute\(pathname\) && !hasBackendSession && !hasAuthHint && \(prefetch \|\| bot \|\| !isInteractiveBrowserUserAgent\(userAgent\)\)/);
  assert.match(middleware, /terminalShellResponse\(pathname, host, prefetch \? "prefetch" : bot \? "bot" : "inactive"\)/);
  assert.match(middleware, /reason === "prefetch"\s+\?\s+null/);
  assert.match(middleware, /"x-walnut-terminal-shell": reason/);
  assert.match(middleware, /"x-robots-tag": "noindex, follow"/);
});
