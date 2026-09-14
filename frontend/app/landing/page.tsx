import type { Metadata } from "next";
import type { ReactNode } from "react";
import { HomepageResearchExample, homepageDate } from "@/components/landing/HomepageResearchExample";
import { PortfolioBlueprint, HomepageMethodology, HomepageFaq } from "@/components/landing/HomepageWorkflow";
import { publicHomepageRanking, publicHomepageResearch, selectHomepageResearch, type HomepageRanking } from "@/lib/homepagePreview";

import { HomepageCtaLink } from "@/components/landing/HomepageCtaLink";
import { LandingSearch } from "@/components/landing/LandingSearch";
import { MarketingHeader } from "@/components/landing/MarketingHeader";
import { API_BASE, type PlanConfig, type PlanPrice } from "@/lib/api";
import {
  WALNUT_MARKETING_DESCRIPTION,
  WALNUT_MARKETING_URL,
  WALNUT_INSTAGRAM_URL,
  WALNUT_REDDIT_URL,
  WALNUT_SOCIAL_IMAGE_URL,
  WALNUT_SOCIAL_URLS,
  WALNUT_TIKTOK_URL,
  WALNUT_X_HANDLE,
  WALNUT_X_URL,
  walnutMarketingMetadata,
} from "@/lib/marketingMetadata";
import { defaultPlanConfig } from "@/lib/defaultPlanConfig";
import { homepageContent } from "@/lib/homepageContent";

export const dynamic = "force-dynamic";
export const revalidate = 300;

export const metadata: Metadata = walnutMarketingMetadata;

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");
const loginUrl = `${appUrl}/login`;
const pricingUrl = `${appUrl}/pricing`;
const topStocksUrl = `${appUrl}/leaderboards#top-stocks`;


type PlanTier = "free" | "premium" | "pro";
type BillingInterval = "monthly" | "annual";
type LandingPlanPriceDisplay = {
  primary: string;
  secondary?: string;
  savings?: string;
};

const platformFooterLinks = [
  { label: "Compare Walnut", href: "/compare" },
  { label: "Stock Research Software", href: "/stock-research-software" },
  { label: "Stock Analysis Platform", href: "/stock-analysis-platform" },
  { label: "Stock Analysis Tools", href: "/stock-analysis-tools" },
  { label: "Leaderboards", href: topStocksUrl },
  { label: "Stock Screener", href: `${appUrl}/screener` },
  { label: "Compare Stocks", href: `${appUrl}/compare/NVDA/MU` },
  { label: "Research Briefs", href: `${appUrl}/insights` },
  { label: "Pricing", href: pricingUrl },
] as const;

const researchDataFooterLinks = [
  { label: "Congress Trades", href: "/congress-trades" },
  { label: "Insider Trading", href: "/insider-trading-tracker" },
  { label: "Insider Analysis Software", href: "/insider-trading-analysis-software" },
  { label: "Alternative Data", href: "/alternative-data-stock-analysis" },
  { label: "Government Contracts", href: "/government-contracts" },
  { label: "Institutional Filings", href: "/institutional-filings" },
  { label: "Institutional Activity", href: "/institutional-activity-tracker" },
  { label: "Confirmation Score", href: "/stock-confirmation-score" },
  { label: "Macro Positioning", href: `${appUrl}/insights#macro-positioning` },
] as const;

const companyFooterLinks = [
  { label: "About", href: `${appUrl}/about` },
  { label: "FAQ", href: "https://walnutmarkets.com/faq" },
  { label: "Contact", href: `${appUrl}/contact` },
  { label: "Terms", href: `${appUrl}/terms` },
  { label: "Privacy", href: `${appUrl}/privacy` },
] as const;

const heroEvidenceSources = ["Fundamentals", "Technicals", "Congress", "Insiders", "Institutions", "Contracts", "Analysts", "Macro"] as const;
const heroFeaturedTicker = {
  kind: "ticker",
  id: "NVDA",
  symbol: "NVDA",
  label: "NVDA — NVIDIA Corporation",
  subtitle: "Ticker",
  href: "/ticker/NVDA",
} as const;

type LandingFetchCacheMode = "revalidate" | "no-store";

async function landingFetchJson<T>(
  path: string,
  params?: Record<string, string | number | undefined>,
  timeoutMs = 3500,
  cacheMode: LandingFetchCacheMode = "revalidate",
): Promise<T> {
  const url = new URL(path, API_BASE);
  Object.entries(params ?? {}).forEach(([key, value]) => {
    if (value !== undefined) url.searchParams.set(key, String(value));
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const cacheOptions = cacheMode === "no-store" ? { cache: "no-store" as const } : { next: { revalidate } };
  try {
    const response = await fetch(url, {
      headers: {
        "X-Walnut-Route": "/landing",
        "X-Walnut-Component": "LandingPage",
      },
      ...cacheOptions,
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Landing fetch failed: ${response.status}`);
    return (await response.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

async function loadPlanConfig(): Promise<PlanConfig | null> {
  try {
    const config = await landingFetchJson<PlanConfig>("/api/plan-config", undefined, 2500, "no-store");
    return config.plan_prices?.length ? config : null;
  } catch {
    return null;
  }
}

async function loadTopStocks(): Promise<HomepageRanking> {
  try {
    // No viewer cookies: reuse the product's public, score-redacted top-three teaser.
    return publicHomepageRanking(await landingFetchJson<unknown>("/api/leaderboards/preview", undefined, 2500));
  } catch {
    return {items: [], generatedAt: null};
  }
}

async function loadResearchExample(ranking: HomepageRanking) {
  const candidates = await Promise.all(ranking.items.map(async stock => {
    try {
      const response = await landingFetchJson<unknown>(`/api/tickers/${encodeURIComponent(stock.symbol)}/context-bundle`, undefined, 4500);
      return publicHomepageResearch(stock, response);
    } catch {
      return null;
    }
  }));
  return selectHomepageResearch(candidates);
}

function planPriceFor(config: PlanConfig | null, tier: PlanTier, interval: BillingInterval): PlanPrice | undefined {
  return config?.plan_prices.find((price) => price.tier === tier && price.billing_interval === interval);
}

function formatPlanMoney(price: PlanPrice): string {
  const amount = (price.amount_cents ?? 0) / 100;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: price.currency || "USD",
    minimumFractionDigits: amount % 1 === 0 ? 0 : 2,
    maximumFractionDigits: 2,
  }).format(amount);
}

function landingPlanPriceDisplay(config: PlanConfig | null, tier: PlanTier): LandingPlanPriceDisplay {
  if (tier === "free") return { primary: "Free" };
  const monthly = planPriceFor(config, tier, "monthly");
  if (!monthly) return { primary: "See pricing page" };
  const annual = planPriceFor(config, tier, "annual");
  const annualSavings = annual ? monthly.amount_cents * 12 - annual.amount_cents : 0;
  return {
    primary: `${formatPlanMoney(monthly)}/mo`,
    secondary: annual ? `${formatPlanMoney(annual)}/yr` : undefined,
    savings: annualSavings > 0 ? `Save ${formatPlanMoney({ ...annual!, amount_cents: annualSavings })}/year with annual` : undefined,
  };
}

function planOffer(price: PlanPrice, name: string) {
  return {
    "@type": "Offer",
    name,
    url: pricingUrl,
    price: Number((price.amount_cents / 100).toFixed(2)),
    priceCurrency: price.currency || "USD",
    availability: "https://schema.org/InStock",
  };
}

function landingPlanOffers(config: PlanConfig | null) {
  const effectiveConfig = config ?? defaultPlanConfig;
  const offers: Array<Record<string, unknown>> = [
    {
      "@type": "Offer",
      name: "Free plan",
      url: pricingUrl,
      price: 0,
      priceCurrency: "USD",
      availability: "https://schema.org/InStock",
    },
  ];

  for (const tier of ["premium", "pro"] as const) {
    const label = tier === "premium" ? "Premium" : "Pro";
    const monthly = planPriceFor(effectiveConfig, tier, "monthly");
    const annual = planPriceFor(effectiveConfig, tier, "annual");
    if (monthly) offers.push(planOffer(monthly, `${label} monthly plan`));
    if (annual) offers.push(planOffer(annual, `${label} annual plan`));
  }

  return offers;
}

function landingJsonLd(config: PlanConfig | null) {
  const organization = {
    "@context": "https://schema.org",
    "@type": "Organization",
    "@id": `${WALNUT_MARKETING_URL}/#organization`,
    name: "Walnut Markets",
    legalName: "Walnut Intelligence Inc.",
    alternateName: "Walnut Markets",
    url: WALNUT_MARKETING_URL,
    logo: `${WALNUT_MARKETING_URL}/walnut-intel-logo-mark.png`,
    description: WALNUT_MARKETING_DESCRIPTION,
    sameAs: WALNUT_SOCIAL_URLS,
  };

  const website = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    "@id": `${WALNUT_MARKETING_URL}/#website`,
    name: "Walnut Markets",
    url: `${WALNUT_MARKETING_URL}/`,
    description: WALNUT_MARKETING_DESCRIPTION,
    publisher: {
      "@id": `${WALNUT_MARKETING_URL}/#organization`,
    },
    potentialAction: {
      "@type": "SearchAction",
      target: `${appUrl}/search?q={search_term_string}`,
      "query-input": "required name=search_term_string",
    },
  };

  const application = {
    "@context": "https://schema.org",
    "@type": "SoftwareApplication",
    "@id": `${WALNUT_MARKETING_URL}/#application`,
    name: "Walnut Market Terminal",
    brand: {
      "@type": "Brand",
      name: "Walnut Markets",
    },
    applicationCategory: "FinanceApplication",
    operatingSystem: "Web",
    url: WALNUT_MARKETING_URL,
    image: WALNUT_SOCIAL_IMAGE_URL,
    description: WALNUT_MARKETING_DESCRIPTION,
    publisher: {
      "@id": `${WALNUT_MARKETING_URL}/#organization`,
    },
    offers: landingPlanOffers(config),
  };

  return [organization, website, application];
}

function LandingPlanPrice({ display }: { display: LandingPlanPriceDisplay }) {
  return (
    <div className="mt-4 min-h-10">
      <p className="flex flex-wrap items-baseline gap-x-2 gap-y-1 text-white">
        <span className="text-3xl font-semibold tracking-normal">{display.primary}</span>
        {display.secondary ? <span className="text-sm font-semibold text-slate-400">/ {display.secondary}</span> : null}
      </p>
      {display.savings ? <p className="mt-1 text-xs font-semibold text-emerald-200">{display.savings}</p> : null}
    </div>
  );
}

function SectionEyebrow({ children }: { children: ReactNode }) {
  return <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{children}</p>;
}

export default async function LandingPage() {
  const [planConfig, topStocks] = await Promise.all([loadPlanConfig(), loadTopStocks()]);
  const researchExample = await loadResearchExample(topStocks);
  const freePrice = landingPlanPriceDisplay(planConfig, "free");
  const premiumPrice = landingPlanPriceDisplay(planConfig, "premium");
  const proPrice = landingPlanPriceDisplay(planConfig, "pro");
  const structuredData = landingJsonLd(planConfig);
  return (
    <main className="min-h-screen overflow-hidden bg-[#030712] text-slate-100">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData).replace(/</g, "\\u003c") }} />
      <MarketingHeader pricingHref={pricingUrl} />
      <section data-walnut-homepage className="relative border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8 lg:py-20">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-4xl">
            <SectionEyebrow>{homepageContent.hero.eyebrow}</SectionEyebrow>
            <h1 className="mt-4 max-w-5xl text-balance text-[2.35rem] font-semibold leading-[1.04] text-white sm:text-5xl lg:text-6xl">
              {homepageContent.hero.title}
            </h1>
            <p className="mt-5 max-w-3xl text-base leading-7 text-slate-300 sm:text-xl sm:leading-8">
              {homepageContent.hero.description}
            </p>
            <div className="mt-8 flex flex-col gap-3 sm:flex-row sm:flex-wrap">
              <HomepageCtaLink href={`${appUrl}/screener`} eventName="open_screener_click" className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">Open Screener</HomepageCtaLink>
              <HomepageCtaLink href={topStocksUrl} eventName="see_top_performers_click" className="inline-flex items-center justify-center px-5 py-3 text-sm font-semibold text-emerald-200 underline underline-offset-4 hover:text-emerald-100">
                View Leaderboards
              </HomepageCtaLink>
              <a href={`${appUrl}/strategies`} className="inline-flex items-center justify-center px-5 py-3 text-sm font-semibold text-emerald-200 underline underline-offset-4 hover:text-emerald-100">
                Explore Strategies
              </a>
            </div>
            <div id="analyze-a-stock" className="scroll-mt-28">
              <LandingSearch appUrl={appUrl} buttonLabel="Analyze a Stock" buttonOutside subduedButton placeholder="Search tickers, companies, Congress members, insiders, institutions, departments..." reassuranceCopy="Free to research · No credit card required" className="mt-6 max-w-3xl" featuredSuggestion={heroFeaturedTicker} submitEventName="analyze_stock_click" />
            </div>
            <p className="mt-4 flex max-w-4xl flex-wrap gap-x-2 gap-y-1 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
              {heroEvidenceSources.map((source, index) => (
                <span key={source}>
                  {index > 0 ? <span aria-hidden="true" className="mr-2 text-slate-600">&middot;</span> : null}
                  {source}
                </span>
              ))}
            </p>
            <p className="mt-5 max-w-2xl text-xs leading-5 text-slate-400">
              A research blueprint for investors who make their own decisions. Rankings and backtests do not guarantee investment performance.
            </p>
          </div>
        </div>
      </section>


      <section id="top-stock-opportunities" className="scroll-mt-24 border-b border-white/10 px-4 py-12 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>Live product preview</SectionEyebrow>
          <div className="mt-3 flex flex-col justify-between gap-4 sm:flex-row sm:items-end">
            <div><h2 className="text-3xl font-semibold text-white sm:text-4xl">Top-Ranked Stocks</h2><p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">Start with three companies from Walnut&apos;s stored ranking, built from the Bullish Confirmation screener. Then investigate the evidence.</p></div>
            <HomepageCtaLink href={topStocksUrl} eventName="top_stocks_click" className="shrink-0 text-sm font-semibold text-emerald-200 underline underline-offset-4">View Full Rankings →</HomepageCtaLink>
          </div>
          <p className="mt-3 text-xs text-slate-500">Ranking snapshot: {homepageDate(topStocks.generatedAt)} (UTC)</p>
          <div className="mt-6 grid gap-4 md:grid-cols-3">
            {topStocks.items.map(stock => <article data-homepage-ranked-stock={stock.symbol} key={stock.symbol} className="min-w-0 rounded-lg border border-white/10 bg-slate-950/85 p-5">
              <p className="font-mono text-sm font-semibold text-emerald-300">#{stock.rank}</p>
              <h3 className="mt-3 font-mono text-2xl font-semibold text-white">{stock.symbol}</h3>
              <p className="mt-1 min-h-12 break-words text-sm leading-6 text-slate-400">{stock.companyName}</p>
              <p className="mt-4 text-xs leading-5 text-slate-400">Activity in snapshot: {stock.drivers.join(" · ") || "See the ranked company in Walnut"}</p>
              <p className="mt-3 text-xs text-slate-500">Confirmation Score · Premium</p>
              <HomepageCtaLink href={`${appUrl}/ticker/${encodeURIComponent(stock.symbol)}`} eventName="analyze_stock_click" className="mt-4 inline-flex text-sm font-semibold text-emerald-200 hover:text-emerald-100">View Analysis →</HomepageCtaLink>
            </article>)}
          </div>
          {!topStocks.items.length && <p className="mt-5 text-sm text-slate-400">The ranked preview is unavailable right now. Open the screener to continue your research.</p>}
          <p className="mt-4 text-xs leading-5 text-slate-500">Guests and Free accounts see up to three ranked stocks. Full rankings, Confirmation Scores and protected datasets retain their existing plan access.</p>
        </div>
      </section>

      <HomepageResearchExample example={researchExample} appUrl={appUrl} rankingAt={topStocks.generatedAt} />
      <PortfolioBlueprint appUrl={appUrl} />

      <section id="monitoring" className="border-b border-white/10 px-4 py-12 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl rounded-lg border border-emerald-300/20 bg-emerald-300/[0.045] p-6 sm:flex sm:items-start sm:justify-between sm:gap-8">
          <div>
            <SectionEyebrow>Follow what changes</SectionEyebrow>
            <h2 className="mt-3 text-2xl font-semibold text-white sm:text-3xl">{homepageContent.monitoring.title}</h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-300">After researching a stock, track what happens next. Save it to a watchlist and get alerted when disclosures, the Confirmation Score or other monitored data changes.</p>
            <p className="mt-3 text-xs leading-5 text-slate-400">Watchlists · Monitoring · Alerts · Custom logical alerts</p>
          </div>
          <a href={`${appUrl}/watchlists`} className="mt-5 inline-flex shrink-0 items-center gap-1 text-sm font-semibold text-emerald-200 hover:text-emerald-100 sm:mt-1"><span>Explore watchlists</span><span aria-hidden="true">&rarr;</span></a>
        </div>
      </section>

      <HomepageMethodology appUrl={appUrl} />

      <section id="pricing" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>Pricing</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">{homepageContent.pricing.title}</h2>
          <p className="mt-4 max-w-3xl text-sm leading-6 text-slate-400">
            <span className="font-semibold text-emerald-200">Free tier available.</span> Explore core ticker research, Congress disclosures, insider activity, government contracts, and price/volume context before upgrading.
          </p>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">Premium is $24.95/month—about $0.82 a day for a deeper, source-aware research workflow.</p>
          <div className="mt-8 grid gap-4 lg:grid-cols-3">
            <article className="rounded-lg border border-white/10 bg-white/[0.035] p-6">
              <h3 className="text-xl font-semibold text-white">Free</h3>
              <LandingPlanPrice display={freePrice} />
              <p className="mt-3 text-sm leading-6 text-slate-400">Explore core ticker research, Congress disclosures, insider activity, government contracts, and price/volume context.</p>
            </article>
            <article className="rounded-lg border border-emerald-300/25 bg-emerald-300/[0.04] p-6">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-xl font-semibold text-white">Premium</h3>
                <span className="rounded border border-emerald-300/35 bg-emerald-300/10 px-2 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-emerald-100">
                  Popular
                </span>
              </div>
              <LandingPlanPrice display={premiumPrice} />
              <p className="mt-3 text-sm leading-6 text-slate-400">
                Elevate your stock research with Walnut premium and start evaluating the fundamentals, technicals, Congress trades, insider trades, catalysts, risks, and Walnut&apos;s proprietary confirmation score all in one place.
              </p>
            </article>
            <article className="rounded-lg border border-cyan-300/25 bg-cyan-300/[0.035] p-6">
              <div className="flex items-center justify-between gap-3">
                <h3 className="text-xl font-semibold text-white">Pro</h3>
                <span className="rounded border border-cyan-300/35 bg-cyan-300/10 px-2 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-cyan-100">
                  Highest limits
                </span>
              </div>
              <LandingPlanPrice display={proPrice} />
              <p className="mt-3 text-sm leading-6 text-slate-400">
                See the data most investors miss with Walnut Pro, including institutional activity, options flow, and macro positioning that can show whether buying interest is building or fading.
              </p>
            </article>
          </div>
          <div className="mt-8 flex flex-col gap-3 sm:flex-row">
            <a
              href={pricingUrl}
              className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
            >
              Compare Plans
            </a>
            <a
              href={loginUrl}
              className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]"
            >
              Login / Register
            </a>
          </div>
        </div>
      </section>

      <HomepageFaq />

      <footer className="px-4 py-10 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 text-sm text-slate-400 lg:grid-cols-[1.1fr_2fr]">
          <div>
            <p className="font-semibold text-white">Walnut Markets</p>
            <p className="mt-3 max-w-2xl text-xs leading-5 text-slate-400">
              Walnut is a stock research and analysis platform operated by Walnut Intelligence Inc. It is provided for research and informational purposes only and does not provide investment advice.
            </p>
          </div>
          <div className="grid gap-6 sm:grid-cols-3">
            <nav className="grid content-start gap-2" aria-label="Platform footer">
              <p className="font-semibold text-slate-300">Platform</p>
              {platformFooterLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
            </nav>
            <nav className="grid content-start gap-2" aria-label="Research data footer">
              <p className="font-semibold text-slate-300">Research data</p>
              {researchDataFooterLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
            </nav>
            <nav className="grid content-start gap-2" aria-label="Company footer">
              <p className="font-semibold text-slate-300">Company</p>
              {companyFooterLinks.map((link) => (
                <a key={link.href} href={link.href} className="hover:text-white">
                  {link.label}
                </a>
              ))}
              <a href={WALNUT_X_URL} target="_blank" rel="noreferrer" className="hover:text-white">
                X / {WALNUT_X_HANDLE}
              </a>
              <a href={WALNUT_REDDIT_URL} target="_blank" rel="noreferrer" className="hover:text-white">
                Reddit / r/walnutmarkets
              </a>
              <a href={WALNUT_INSTAGRAM_URL} target="_blank" rel="noreferrer" className="hover:text-white">
                Instagram / @walnutmarkets
              </a>
              <a href={WALNUT_TIKTOK_URL} target="_blank" rel="noreferrer" className="hover:text-white">
                TikTok / @walnutmarkets
              </a>
            </nav>
          </div>
        </div>
      </footer>
    </main>
  );
}
