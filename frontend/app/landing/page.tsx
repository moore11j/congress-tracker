import type { Metadata } from "next";
import type { ReactNode } from "react";
import { LatestInsightImage } from "@/components/landing/LatestInsightImage";
import { LandingSearch } from "@/components/landing/LandingSearch";
import { MarketingHeader } from "@/components/landing/MarketingHeader";
import { API_BASE, type PlanConfig, type PlanPrice } from "@/lib/api";
import {
  WALNUT_MARKETING_DESCRIPTION,
  WALNUT_MARKETING_URL,
  WALNUT_REDDIT_URL,
  WALNUT_SOCIAL_IMAGE_URL,
  WALNUT_SOCIAL_URLS,
  WALNUT_X_HANDLE,
  WALNUT_X_URL,
  walnutMarketingMetadata,
} from "@/lib/marketingMetadata";
import { defaultPlanConfig } from "@/lib/defaultPlanConfig";
import type { InsightsNewsResponse, MacroSnapshotIndex, MacroSnapshotPoint, MacroSnapshotResponse, NewsItem } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 300;

export const metadata: Metadata = walnutMarketingMetadata;

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");
const loginUrl = `${appUrl}/login`;
const pricingUrl = `${appUrl}/pricing`;
const publicPricingUrl = `${WALNUT_MARKETING_URL}/pricing`;
const researchStartUrl = `${appUrl}/feed`;
const nvdaProductScreenshot = "/landing/nvda-ticker-intelligence.png";
const outcomesProductScreenshot = "/landing/outcomes-confirmation-events.png";

type PlanTier = "free" | "premium" | "pro";
type BillingInterval = "monthly" | "annual";
type LandingPlanPriceDisplay = {
  primary: string;
  secondary?: string;
};

type MarketInstrument = {
  label: string;
  symbol?: string | null;
  value?: number | string | null;
  changePct?: number | null;
  timeframeLabel?: string | null;
};

const platformFooterLinks = [
  { label: "Compare Walnut", href: "/compare" },
  { label: "Stock Research Software", href: "/stock-research-software" },
  { label: "Stock Analysis Platform", href: "/stock-analysis-platform" },
  { label: "Stock Analysis Tools", href: "/stock-analysis-tools" },
  { label: "Stock Screener", href: `${appUrl}/screener` },
  { label: "Compare Stocks", href: `${appUrl}/compare/NVDA/MU` },
  { label: "Research Briefs", href: `${appUrl}/insights` },
  { label: "Pricing", href: publicPricingUrl },
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
  { label: "About", href: "/about" },
  { label: "FAQ", href: "/faq" },
  { label: "Contact", href: "/contact" },
  { label: "Terms", href: "/terms" },
  { label: "Privacy", href: "/privacy" },
] as const;

const evidenceSources = ["Fundamentals", "Technicals", "Insiders", "Congress", "Institutions", "Contracts", "Analysts"] as const;
const interpretedOutputs = ["Confirmation Score", "What Changed", "Catalysts", "Risks", "What to Watch Next"] as const;
const heroEvidenceSources = ["Fundamentals", "Technicals", "Congress", "Insiders", "Institutions", "Contracts", "Analysts", "Macro"] as const;
const heroFeaturedTicker = {
  kind: "ticker",
  id: "NVDA",
  symbol: "NVDA",
  label: "NVDA — NVIDIA Corporation",
  subtitle: "Ticker",
  href: "/ticker/NVDA",
} as const;

const confirmationEvidence = [
  ["Fundamental strength", "Supportive"],
  ["Bullish analyst consensus", "Supportive"],
  ["Bullish tape confirmation", "Supportive"],
  ["Supportive macro positioning", "Supportive"],
  ["Mixed Congress activity", "Mixed"],
  ["No notable insider activity", "Inactive"],
] as const;

const confirmationPrinciples = [
  ["Higher confirmation", "More of the available evidence is reinforcing the same directional view."],
  ["Lower confirmation", "The evidence is weaker, mixed, or conflicting."],
  ["Explainable", "Every confirmation score is accompanied by the underlying evidence so you can see what is data behind the judgment."],
] as const;

const workflowStages = [
  ["01", "What does the evidence say?", "Walnut interprets multiple data sources together rather than leaving each one isolated."],
  ["02", "What changed?", "See which parts of the underlying data have strengthened, weakened, or reversed the investment thesis."],
  ["03", "Does that change the thesis?", "Understand whether new evidence reinforces or challenges the current directional view."],
  ["04", "What should I watch next?", "Track the catalysts, risks, and developments most likely to matter."],
] as const;

const whySources = ["Fundamentals", "Technicals", "Congress", "Insiders", "Institutions", "Contracts", "Options", "Analysts"] as const;
const whySteps = ["Raw data", "Interpretation", "Judgment", "Ongoing research"] as const;

const followActivityCards = [
  {
    title: "Congress Members",
    body: "See disclosed trades, transaction history, portfolio activity, and the companies individual members are trading.",
    cta: "Explore Congress",
    href: `${appUrl}/feed?mode=congress`,
    eyebrow: "Congress feed",
    primary: "Member activity",
    metric: "Disclosed trades",
    rows: [
      ["Cleo Fields", "NVDA purchase", "Filed"],
      ["House disclosure", "MSFT sale", "New"],
      ["Senate disclosure", "LMT purchase", "Active"],
    ],
  },
  {
    title: "Corporate Insiders",
    body: "Research executives and directors through their Form 4 transaction history and activity across companies.",
    cta: "Explore Insiders",
    href: `${appUrl}/feed?mode=insider`,
    eyebrow: "Form 4 activity",
    primary: "Insider tape",
    metric: "Open-market buys",
    rows: [
      ["Director", "AAPL acquisition", "Form 4"],
      ["CEO", "PLTR purchase", "Filed"],
      ["10% owner", "AMD buy", "Recent"],
    ],
  },
  {
    title: "Institutions",
    body: "See reported holdings, position changes, top positions, and which companies institutions are accumulating or reducing.",
    cta: "Explore Institutions",
    href: `${appUrl}/feed?mode=institutional`,
    eyebrow: "Institutional activity",
    primary: "BlackRock, Inc.",
    metric: "5,685 holdings",
    rows: [
      ["NVDA", "5.87%", "$336B"],
      ["AAPL", "5.08%", "$291B"],
      ["MSFT", "3.84%", "$220B"],
    ],
  },
  {
    title: "Government Departments",
    body: "Follow government contract activity by department and see which public companies are receiving awards.",
    cta: "Explore Government Contracts",
    href: `${appUrl}/feed?mode=government_contracts`,
    eyebrow: "Contract awards",
    primary: "Department activity",
    metric: "Award recipients",
    rows: [
      ["Defense", "LMT award", "$587M"],
      ["Energy", "PLTR contract", "$172M"],
      ["NASA", "NVDA supplier", "New"],
    ],
  },
] as const;

const featureDepthItems = [
  ["Stock Research", "Ticker-level research workflow"],
  ["Confirmation Score", "Point-in-time evidence alignment"],
  ["Screener", "Filter by data and market context"],
  ["Compare", "Ticker-to-ticker research comparison"],
  ["Backtesting", "Historical strategy testing"],
  ["Activity Feeds", "Congress, insiders, institutions, and contracts"],
  ["Macro Positioning", "Market and macro context"],
  ["Options Flow", "Options context where available"],
  ["Analyst Consensus", "Analyst view and coverage context"],
  ["Research Briefs", "Published and generated research"],
  ["Outcomes", "Historical accountability layer"],
  ["Research Memory", "Coming Soon"],
  ["Walnut Strategies", "Live Beta — explore published strategies with transparent methodology and performance"],
] as const;

const strategyConcepts = [
  ["Cleo Fields Portfolio", "Congress Strategies", "58.9%", "CAGR"],
  ["Insider Open-Market Buys", "Insider Strategies", "37.7%", "CAGR"],
  ["Insider + Institutional Accumulation", "Hybrid Strategies", "52.1%", "CAGR"],
] as const;

const dailyInsightTickers = [
  ["NVDA", "NVIDIA Corp", "$223.96", "+2.27%"],
  ["AAPL", "Apple Inc", "$313.33", "+0.29%"],
  ["LMT", "Lockheed Martin", "$587.95", "+0.88%"],
  ["PLTR", "Palantir Technologies", "$172.01", "+10.32%"],
] as const;

const fallbackInsights: NewsItem[] = [
  {
    title: "Congressional disclosures, insider trades, and ticker context update as the data changes.",
    url: `${appUrl}/insights`,
    source: "walnut_landing",
    site: "Walnut",
  },
  {
    title: "Government contracts, political exposure, and issuer-level data are available in the live app.",
    url: `${appUrl}/feed?mode=government_contracts`,
    source: "walnut_landing",
    site: "Walnut",
  },
];

const fallbackMarketSnapshot: MacroSnapshotResponse = {
  indexes: [
    { label: "S&P 500", symbol: "SPY", timeframe_label: "1D change" },
    { label: "NASDAQ", symbol: "^IXIC", timeframe_label: "1D change" },
    { label: "Dow", symbol: "^DJI", timeframe_label: "1D change" },
  ],
  treasury: [
    { label: "2Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
    { label: "10Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  ],
  economics: [
    { label: "Fed Overnight Rate", value: null, value_format: "percent", change_format: "bps" },
    { label: "Core CPI", value: null, value_format: "percent", change_format: "percentage_points" },
    { label: "Unemployment", value: null, value_format: "percent", change_format: "percentage_points" },
  ],
  sector_performance: [],
  status: "unavailable",
  generated_at: "1970-01-01T00:00:00.000Z",
};

const curatedMarketSnapshotFallback = [
  {
    title: "US Macro",
    subtitle: "Rates, inflation, labor",
    rows: [
      ["Fed policy", "Rate context"],
      ["Inflation trend", "CPI lens"],
      ["Labor market", "Jobs trend"],
    ],
  },
  {
    title: "US Indexes",
    subtitle: "Market breadth",
    rows: [
      ["S&P 500", "Index context"],
      ["NASDAQ", "Growth tape"],
      ["Dow", "Blue-chip tape"],
    ],
  },
  {
    title: "Treasury",
    subtitle: "Yield curve",
    rows: [
      ["2Y Treasury", "Front-end rates"],
      ["10Y Treasury", "Long-rate trend"],
      ["Curve pressure", "Macro context"],
    ],
  },
] as const;

const landingMacroLabelGroups = [
  ["Fed Overnight Rate", "Federal Funds Rate", "Effective Federal Funds Rate", "federalFunds"],
  ["Core CPI", "Core CPI YoY", "Core CPI Year over Year", "core_cpi", "coreCpi", "core_cpi_yoy", "coreCpiYoY", "cpi_core", "CPILFESL", "CPIAUCSL"],
  ["Unemployment", "Unemployment Rate", "unemploymentRate"],
] as const;

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

async function loadLatestInsights(): Promise<NewsItem[]> {
  try {
    const response = await landingFetchJson<InsightsNewsResponse>("/api/insights/news", { limit: 6, page: 0 });
    return response.items?.filter((item) => item.title && item.url).slice(0, 6) ?? fallbackInsights;
  } catch {
    return fallbackInsights;
  }
}

async function loadMarketSnapshot(): Promise<MacroSnapshotResponse> {
  try {
    return await landingFetchJson<MacroSnapshotResponse>("/api/insights/snapshot", undefined, 1800);
  } catch {
    return fallbackMarketSnapshot;
  }
}

function formatMarketValue(value: number | string | null | undefined, digits = 2): string {
  if (typeof value === "string") return value || "Unavailable";
  if (typeof value !== "number" || !Number.isFinite(value)) return "Unavailable";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(value);
}

function formatMacroValue(item: MacroSnapshotPoint): string {
  if (typeof item.value !== "number" || !Number.isFinite(item.value)) return "Unavailable";
  const valueFormat = item.value_format ?? (item.unit_label === "yield" ? "percent" : "number");
  if (valueFormat === "percent") return `${formatMarketValue(item.value)}%`;
  if (valueFormat === "bps") return `${formatMarketValue(item.value, 0)} bps`;
  if (valueFormat === "currency") {
    return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact", maximumFractionDigits: 1 }).format(item.value);
  }
  return formatMarketValue(item.value);
}

function formatMarketChange(value: number | null | undefined, suffix = "%"): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "Latest available";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}${suffix}`;
}

function publicSnapshotMetaLabel(...values: Array<string | null | undefined>): string {
  for (const value of values) {
    const text = value?.trim();
    if (!text) continue;
    const lowered = text.toLowerCase();
    if (
      lowered.includes("fred") ||
      lowered.includes("cache") ||
      lowered.includes("proxy") ||
      lowered.includes("provider") ||
      lowered.includes("backend")
    ) {
      continue;
    }
    if (lowered === "latest available") return "Latest";
    if (lowered === "1d change" || lowered === "eod change" || lowered === "daily change") return "1D";
    if (lowered === "macro data") return "Latest";
    return text;
  }
  return "Latest";
}

function publicUsIndexLabel(item: MacroSnapshotIndex): string {
  const symbol = item.symbol?.trim().toUpperCase();
  const label = item.label?.trim() ?? "";
  const identity = `${label} ${symbol ?? ""}`.toLowerCase();
  if (symbol === "SPY" || identity.includes("s&p 500")) return "S&P 500";
  if (symbol === "QQQ" || symbol === "^IXIC" || identity.includes("nasdaq")) return "NASDAQ";
  if (symbol === "DIA" || symbol === "^DJI" || identity.includes("dow")) return "Dow";
  return label.replace(/\s*ETF\s+proxy\s*/gi, " ").replace(/\s+/g, " ").trim() || "Index";
}

function formatMacroChange(item: MacroSnapshotPoint): string {
  const value = item.change_value ?? item.change;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return publicSnapshotMetaLabel(item.change_label, item.timeframe_label, item.context_label);
  }
  const format = item.change_format ?? item.change_unit;
  const sign = value > 0 ? "+" : "";
  if (format === "bps") return `${sign}${value.toFixed(0)} bps`;
  if (format === "percentage_points") return `${sign}${value.toFixed(2)} pp`;
  if (format === "percent") return `${sign}${value.toFixed(2)}%`;
  return `${sign}${value.toFixed(2)}`;
}

function deltaClassName(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "text-slate-400";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-400";
}

function insightImageUrl(item: NewsItem): string | null {
  const record = item as NewsItem & Record<string, unknown>;
  const candidate = [record.image_url, record.image, record.thumbnail, record.urlToImage]
    .find((value): value is string => typeof value === "string" && value.trim().length > 0)
    ?.trim();
  return candidate?.startsWith("http") || candidate?.startsWith("/") ? candidate : null;
}

function indexToInstrument(item: MacroSnapshotIndex): MarketInstrument {
  return {
    label: publicUsIndexLabel(item),
    symbol: item.symbol,
    value: item.value,
    changePct: item.change_pct,
    timeframeLabel: publicSnapshotMetaLabel(item.timeframe_label),
  };
}

function insightHref(item: NewsItem): string {
  if (item.url.startsWith("http")) return item.url;
  return `${appUrl}${item.url.startsWith("/") ? item.url : `/${item.url}`}`;
}

function normalizedMacroLabel(value: string | null | undefined): string {
  return (value ?? "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function hasUsableMacroValue(item: MacroSnapshotPoint | undefined): boolean {
  return typeof item?.value === "number" && Number.isFinite(item.value);
}

function findMacroPoint(items: MacroSnapshotPoint[], labels: readonly string[], fallback: MacroSnapshotPoint): MacroSnapshotPoint {
  const aliases = new Set(labels.map(normalizedMacroLabel));
  const matches = items.filter((item) => aliases.has(normalizedMacroLabel(item.label)));
  return matches.find(hasUsableMacroValue) ?? matches[0] ?? fallback;
}

function landingMacroRows(items: MacroSnapshotPoint[]): MacroSnapshotPoint[] {
  const source = items.length ? items : fallbackMarketSnapshot.economics;
  return landingMacroLabelGroups.map((labels, index) => findMacroPoint(source, labels, fallbackMarketSnapshot.economics[index]));
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
  return {
    primary: `${formatPlanMoney(monthly)}/mo`,
    secondary: annual ? `${formatPlanMoney(annual)}/yr` : undefined,
  };
}

function planOffer(price: PlanPrice, name: string) {
  return {
    "@type": "Offer",
    name,
    url: publicPricingUrl,
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
      url: publicPricingUrl,
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
    name: "Walnut Markets",
    legalName: "Walnut Intelligence Inc.",
    alternateName: "Walnut Markets",
    url: WALNUT_MARKETING_URL,
    logo: `${WALNUT_MARKETING_URL}/walnut-intel-logo-mark.png`,
    description: "Stock research and stock analysis software for technicals, fundamentals, public disclosures, alternative data, and confirmation-score context.",
    sameAs: WALNUT_SOCIAL_URLS,
  };

  const website = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: "Walnut Markets",
    url: appUrl,
    description: "Stock research and stock analysis software for technicals, fundamentals, public disclosures, alternative data, and confirmation-score context.",
    publisher: {
      "@type": "Organization",
      name: "Walnut Intelligence Inc.",
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
      "@type": "Organization",
      name: "Walnut Intelligence Inc.",
    },
    offers: landingPlanOffers(config),
  };

  return [organization, website, application];
}

function MarketDataCard({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">{subtitle}</p>
      <h3 className="mt-2 text-lg font-semibold text-white">{title}</h3>
      <div className="mt-4 space-y-3">{children}</div>
    </div>
  );
}

function InstrumentRows({ items }: { items: MarketInstrument[] }) {
  return (
    <>
      {items.map((item) => (
        <div key={`${item.label}-${item.symbol ?? "na"}`} className="flex items-center justify-between gap-3">
          <div className="min-w-0">
            <p className="truncate text-sm font-semibold text-slate-100">{item.label}</p>
            <p className="mt-1 truncate font-mono text-xs text-slate-400">{publicSnapshotMetaLabel(item.timeframeLabel)}</p>
          </div>
          <div className="shrink-0 text-right">
            <p className="font-mono text-sm font-semibold text-white">{formatMarketValue(item.value)}</p>
            <p className={`mt-1 text-xs ${deltaClassName(item.changePct)}`}>{formatMarketChange(item.changePct)}</p>
          </div>
        </div>
      ))}
    </>
  );
}

function MacroRows({ items }: { items: MacroSnapshotPoint[] }) {
  return (
    <>
      {items.map((item) => {
        const changeValue = item.change_value ?? item.change;
        return (
          <div key={`${item.label}-${item.date ?? "na"}`} className="flex items-center justify-between gap-3">
            <div className="min-w-0">
              <p className="truncate text-sm font-semibold text-slate-100">{item.label}</p>
              <p className="mt-1 truncate text-xs text-slate-400">{publicSnapshotMetaLabel(item.change_label, item.timeframe_label, item.context_label)}</p>
            </div>
            <div className="shrink-0 text-right">
              <p className="font-mono text-sm font-semibold text-white">{formatMacroValue(item)}</p>
              <p className={`mt-1 text-xs ${deltaClassName(changeValue)}`}>{formatMacroChange(item)}</p>
            </div>
          </div>
        );
      })}
    </>
  );
}

function LandingMarketSnapshot({ snapshot }: { snapshot: MacroSnapshotResponse }) {
  const hasUsableSnapshot =
    snapshot.status === "ok" ||
    snapshot.status === "partial" ||
    (snapshot.indexes ?? []).some((item) => typeof item.value === "number" && Number.isFinite(item.value)) ||
    (snapshot.economics ?? []).some((item) => typeof item.value === "number" && Number.isFinite(item.value)) ||
    (snapshot.treasury ?? []).some((item) => typeof item.value === "number" && Number.isFinite(item.value));

  if (!hasUsableSnapshot) {
    return (
      <div className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Market snapshot examples</p>
            <p className="mt-2 text-sm leading-6 text-slate-400">A preview of the market context Walnut surfaces inside the terminal.</p>
          </div>
          <span className="shrink-0 rounded border border-cyan-300/30 bg-cyan-300/10 px-2 py-1 text-xs font-semibold text-cyan-100">Preparing</span>
        </div>
        <div className="mt-5 grid gap-3 xl:grid-cols-3">
          {curatedMarketSnapshotFallback.map((card) => (
            <MarketDataCard key={card.title} title={card.title} subtitle={card.subtitle}>
              {card.rows.map(([label, value]) => (
                <div key={label} className="flex items-center justify-between gap-3">
                  <p className="truncate text-sm font-semibold text-slate-100">{label}</p>
                  <p className="shrink-0 text-right text-xs font-semibold text-emerald-200">{value}</p>
                </div>
              ))}
            </MarketDataCard>
          ))}
        </div>
      </div>
    );
  }

  const usIndexes = (snapshot.indexes?.length ? snapshot.indexes : fallbackMarketSnapshot.indexes).slice(0, 3).map(indexToInstrument);
  const economics = landingMacroRows(snapshot.economics ?? []);
  const treasury = (snapshot.treasury?.length ? snapshot.treasury : fallbackMarketSnapshot.treasury).slice(0, 2);
  const statusLabel = snapshot.status === "ok" || snapshot.status === "partial" ? "Market snapshot" : "Market snapshot examples";

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{statusLabel}</p>
          <p className="mt-2 text-sm leading-6 text-slate-400">US macro, rates, and index context surfaced inside the terminal.</p>
        </div>
        <span className="shrink-0 rounded border border-cyan-300/30 bg-cyan-300/10 px-2 py-1 text-xs font-semibold text-cyan-100">Terminal data</span>
      </div>
      <div className="mt-5 grid gap-3 xl:grid-cols-3">
        <MarketDataCard title="US Macro" subtitle="Latest available">
          <MacroRows items={economics} />
        </MarketDataCard>
        <MarketDataCard title="US Indexes" subtitle="1D change">
          <InstrumentRows items={usIndexes} />
        </MarketDataCard>
        <MarketDataCard title="Treasury" subtitle="Yield and change">
          <MacroRows items={treasury} />
        </MarketDataCard>
      </div>
    </div>
  );
}

function LandingPlanPrice({ display }: { display: LandingPlanPriceDisplay }) {
  return (
    <p className="mt-4 flex min-h-10 flex-wrap items-baseline gap-x-2 gap-y-1 text-white">
      <span className="text-3xl font-semibold tracking-normal">{display.primary}</span>
      {display.secondary ? <span className="text-sm font-semibold text-slate-400">/ {display.secondary}</span> : null}
    </p>
  );
}

function SectionEyebrow({ children }: { children: ReactNode }) {
  return <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{children}</p>;
}

export default async function LandingPage() {
  const [latestInsights, planConfig] = await Promise.all([loadLatestInsights(), loadPlanConfig()]);
  const heroInsight = latestInsights[0] ?? fallbackInsights[0];
  const heroImageInsight = insightImageUrl(heroInsight) ? heroInsight : latestInsights.find((item) => insightImageUrl(item)) ?? heroInsight;
  const heroInsightImage = insightImageUrl(heroImageInsight);
  const freePrice = landingPlanPriceDisplay(planConfig, "free");
  const premiumPrice = landingPlanPriceDisplay(planConfig, "premium");
  const proPrice = landingPlanPriceDisplay(planConfig, "pro");
  const structuredData = landingJsonLd(planConfig);

  return (
    <main className="min-h-screen overflow-hidden bg-[#030712] text-slate-100">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData).replace(/</g, "\\u003c") }} />
      <div className="absolute inset-0 -z-10 bg-[linear-gradient(90deg,rgba(148,163,184,0.05)_1px,transparent_1px),linear-gradient(180deg,rgba(148,163,184,0.04)_1px,transparent_1px)] bg-[size:56px_56px]" />
      <MarketingHeader pricingHref="#pricing" />

      <section className="relative border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8 lg:py-20">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-4xl">
            <SectionEyebrow>Stock Research &amp; Market Intelligence</SectionEyebrow>
            <h1 className="mt-4 max-w-5xl text-[2.35rem] font-semibold leading-[1.04] text-white sm:text-5xl lg:text-6xl">
              Before You Buy Your Next Stock, Run It Through Walnut.
            </h1>
            <p className="mt-5 max-w-3xl text-base leading-7 text-slate-300 sm:text-xl sm:leading-8">
              Research fundamentals, technicals, insider trades, Congress trades, institutional holdings, government contracts, analyst consensus, macro positioning, and more&mdash;see whether the evidence confirms or challenges your investment thesis.
            </p>
            <LandingSearch appUrl={appUrl} buttonLabel="Run Walnut" buttonOutside placeholder="Search tickers, companies, Congress members, insiders, institutions, departments..." className="mt-8 max-w-3xl" featuredSuggestion={heroFeaturedTicker} />
            <a href="#how-it-works" className="mt-4 inline-flex items-center gap-1 text-sm font-semibold text-emerald-200 hover:text-emerald-100">
              <span>See How It Works</span>
              <span aria-hidden="true">&darr;</span>
            </a>
            <p className="mt-4 flex max-w-4xl flex-wrap gap-x-2 gap-y-1 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
              {heroEvidenceSources.map((source, index) => (
                <span key={source}>
                  {index > 0 ? <span aria-hidden="true" className="mr-2 text-slate-600">&middot;</span> : null}
                  {source}
                </span>
              ))}
            </p>
            <p className="mt-5 max-w-2xl text-xs leading-5 text-slate-400">
              Research and informational tools only. Walnut does not provide personalized financial advice or make investment decisions for users.
            </p>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-10 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <figure className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/90 p-2 shadow-2xl shadow-black/40">
            <div className="overflow-x-auto [scrollbar-width:thin]">
              <img
                src={nvdaProductScreenshot}
                alt="Walnut Markets NVDA ticker intelligence page showing a 65 out of 100 Strong Bullish confirmation score with What Changed, Catalysts, Risks, What to Watch Next, price volume, fundamentals, insiders, Congress, analysts, macro positioning, and valuation."
                width={1511}
                height={773}
                className="h-auto min-w-[920px] rounded-md border border-white/10 lg:min-w-0 lg:w-full"
              />
            </div>
            <figcaption className="flex flex-col gap-1 px-2 py-3 text-xs leading-5 text-slate-400 sm:flex-row sm:items-center sm:justify-between">
              <span>Real Walnut ticker research interface. NVDA example shown.</span>
              <span>Confirmation Score, catalysts, risks, and source context remain visible.</span>
            </figcaption>
          </figure>
        </div>
      </section>

      <section id="insights" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
            <div>
              <SectionEyebrow>Daily Insights</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Keep informed with the news that moves the market.</h2>
            </div>
            <a href={`${appUrl}/insights`} className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap text-sm font-semibold text-emerald-200 hover:text-emerald-100 md:ml-4">
              <span>Open insights</span>
              <span aria-hidden="true">-&gt;</span>
            </a>
          </div>
          <div className="mt-8 grid gap-5 lg:grid-cols-[1.05fr_0.95fr]" data-nosnippet>
            <div className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
              <div className="divide-y divide-white/10">
                {latestInsights.slice(0, 5).map((item) => (
                  <a key={`${item.title}-${item.url}`} href={insightHref(item)} target={item.url.startsWith("http") ? "_blank" : undefined} rel="noreferrer" className="block py-4 first:pt-0 last:pb-0">
                    <p className="text-sm font-semibold leading-6 text-white hover:text-emerald-100">{item.title}</p>
                    <p className="mt-1 text-xs text-slate-400">{item.site || item.source || "Market news"}</p>
                  </a>
                ))}
              </div>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                {dailyInsightTickers.map(([symbol, company, price, change]) => (
                  <a key={symbol} href={`${appUrl}/ticker/${symbol}`} className="rounded-lg border border-white/10 bg-white/[0.035] p-4 transition hover:border-emerald-300/35">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="font-mono text-base font-semibold text-emerald-200">{symbol}</p>
                        <p className="mt-2 overflow-hidden text-ellipsis whitespace-nowrap text-xs text-slate-400">{company}</p>
                      </div>
                      <div className="shrink-0 text-right">
                        <p className="font-mono text-sm font-semibold text-white">{price}</p>
                        <p className="mt-1 text-xs font-semibold text-emerald-300">{change}</p>
                      </div>
                    </div>
                  </a>
                ))}
              </div>
            </div>
            <article className="rounded-lg border border-white/10 bg-slate-950/90 p-5 shadow-2xl shadow-black/30">
              <div className="mb-3 flex items-center justify-between gap-3">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">Market Brief</p>
                <span className="rounded border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">Updated</span>
              </div>
              <a href={insightHref(heroInsight)} className="group block" target={heroInsight.url.startsWith("http") ? "_blank" : undefined} rel="noreferrer">
                <LatestInsightImage src={heroInsightImage} alt={heroImageInsight.title} />
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">{heroInsight.site || heroInsight.source || "Walnut"}</p>
                <h3 className="mt-3 text-2xl font-semibold leading-tight text-white group-hover:text-emerald-100">{heroInsight.title}</h3>
                {heroInsight.summary ? (
                  <p className="mt-3 overflow-hidden text-sm leading-6 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:3]">
                    {heroInsight.summary}
                  </p>
                ) : null}
              </a>
            </article>
          </div>
        </div>
      </section>

      <section id="how-it-works" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>What Walnut Actually Does</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">One research workflow. Multiple sources of evidence.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Walnut brings together fundamental analysis, technical analysis, insider trades, Congress trades, institutional holdings, government contracts, analyst consensus, macro positioning, and other market data so you can clearly see which sources reinforce or contradict your investment thesis.
            </p>
          </div>
          <div className="mt-10 grid gap-4 lg:grid-cols-[1fr_auto_1.2fr_auto_1fr] lg:items-center">
            <div className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Evidence sources</p>
              <div className="mt-4 flex flex-wrap gap-2">
                {evidenceSources.map((source) => (
                  <span key={source} className="rounded border border-white/10 bg-slate-950/70 px-3 py-2 text-sm font-semibold text-slate-100">
                    {source}
                  </span>
                ))}
              </div>
            </div>
            <div className="hidden text-2xl font-semibold text-emerald-300 lg:block">-&gt;</div>
            <div className="rounded-lg border border-emerald-300/25 bg-emerald-300/[0.045] p-6 text-center">
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-200">Interpretation</p>
              <h3 className="mt-3 text-2xl font-semibold text-white">Walnut interprets the evidence together</h3>
              <p className="mt-3 text-sm leading-6 text-slate-300">Walnut reveals alignment, conflict, and change across different investment data sets rather than leaving each one isolated.</p>
            </div>
            <div className="hidden text-2xl font-semibold text-emerald-300 lg:block">-&gt;</div>
            <div className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Research outputs</p>
              <div className="mt-4 grid gap-2">
                {interpretedOutputs.map((item) => (
                  <span key={item} className="rounded border border-white/10 bg-slate-950/70 px-3 py-2 text-sm font-semibold text-slate-100">
                    {item}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="confirmation-score" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 lg:grid-cols-[0.82fr_1.18fr] lg:items-center">
          <div>
            <SectionEyebrow>Confirmation Score</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">See whether the evidence agrees.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              The Walnut Confirmation Score is a proprietary 0-100 measure of how strongly the available evidence supports a directional view at a specific point in time.
            </p>
            <p className="mt-5 max-w-2xl text-xs leading-5 text-slate-400">
              It is not a probability of future returns and not a prediction score. Its purpose is to summarize the strength and alignment of available evidence.
            </p>
          </div>
          <div className="rounded-lg border border-white/10 bg-slate-950/85 p-5 shadow-2xl shadow-black/30">
            <div className="grid gap-5 md:grid-cols-[0.45fr_0.55fr] md:items-center">
              <div className="rounded-lg border border-emerald-300/25 bg-emerald-300/[0.045] p-6 text-center">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-400">NVIDIA Corp</p>
                <p className="mt-4 font-mono text-6xl font-semibold text-emerald-300">65</p>
                <p className="mt-1 font-mono text-xl font-semibold text-slate-500">/ 100</p>
                <p className="mt-4 text-2xl font-semibold text-emerald-200">Strong Bullish</p>
              </div>
              <div className="grid gap-2">
                {confirmationEvidence.map(([label, status]) => (
                  <div key={label} className="flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-white/[0.035] px-4 py-3">
                    <span className="text-sm font-semibold text-slate-100">{label}</span>
                    <span className="shrink-0 rounded border border-white/10 bg-slate-950/70 px-2 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-300">{status}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="mt-5 grid gap-3 md:grid-cols-3">
              {confirmationPrinciples.map(([title, body]) => (
                <article key={title} className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
                  <h3 className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">{title}</h3>
                  <p className="mt-3 text-sm leading-6 text-slate-400">{body}</p>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>The Walnut Research Workflow</SectionEyebrow>
          <div className="mt-3 flex flex-col justify-between gap-4 lg:flex-row lg:items-end">
            <h2 className="max-w-3xl text-3xl font-semibold text-white sm:text-4xl">From evidence to ongoing research.</h2>
            <p className="max-w-2xl text-sm leading-6 text-slate-400">Walnut is built around the research process every serious investor repeats before, during, and after an investment thesis changes.</p>
          </div>
          <div className="mt-9 grid gap-3 lg:grid-cols-4">
            {workflowStages.map(([number, title, body], index) => (
              <article key={number} className="relative rounded-lg border border-white/10 bg-white/[0.035] p-5">
                {index < workflowStages.length - 1 ? <div className="absolute -right-2 top-8 hidden h-px w-4 bg-emerald-300/40 lg:block" /> : null}
                <p className="font-mono text-sm font-semibold text-emerald-300">{number}</p>
                <h3 className="mt-4 text-base font-semibold uppercase tracking-[0.12em] text-white">{title}</h3>
                <p className="mt-4 text-sm leading-6 text-slate-400">{body}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 lg:grid-cols-[0.86fr_1.14fr] lg:items-center">
          <div>
            <SectionEyebrow>Why Walnut</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Data is easy to find. Context is harder.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Most stock research platforms only specialize in one part of the market data and leave you to connect the dots yourself. Walnut evaluates multiple data sources together so you can see where the evidence agrees, where it conflicts, and what has changed.
            </p>
          </div>
          <div className="rounded-lg border border-white/10 bg-slate-950/85 p-5">
            <div className="mb-5 flex flex-wrap gap-2">
              {whySources.map((source) => (
                <span key={source} className="rounded border border-white/10 bg-white/[0.035] px-2.5 py-1.5 text-xs font-semibold text-slate-300">
                  {source}
                </span>
              ))}
            </div>
            <div className="grid gap-3 md:grid-cols-4">
              {whySteps.map((step, index) => (
                <div key={step} className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
                  <p className="font-mono text-xs font-semibold text-slate-500">0{index + 1}</p>
                  <p className="mt-3 text-sm font-semibold uppercase tracking-[0.12em] text-white">{step}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-4xl">
            <SectionEyebrow>Follow the Activity</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Research the people and institutions behind the market.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Ticker research is only one side of Walnut. Explore the trading, holdings, and contract activity behind members of Congress, corporate insiders, institutions, and government departments.
            </p>
          </div>
          <div className="mt-9 grid gap-4 lg:grid-cols-4">
            {followActivityCards.map((card) => (
              <article key={card.title} className="group flex min-h-full flex-col overflow-hidden rounded-lg border border-white/10 bg-slate-950/85 shadow-2xl shadow-black/20 transition hover:border-emerald-300/30">
                <div className="border-b border-white/10 bg-white/[0.025] p-4">
                  <div className="flex items-center justify-between gap-3">
                    <p className="text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-300">{card.eyebrow}</p>
                    <span className="h-2 w-2 rounded-full bg-emerald-300 shadow-[0_0_14px_rgba(110,231,183,0.7)]" />
                  </div>
                  <div className="mt-4 flex items-end justify-between gap-4">
                    <div className="min-w-0">
                      <p className="truncate text-sm font-semibold text-white">{card.primary}</p>
                      <p className="mt-1 text-xs text-slate-500">{card.metric}</p>
                    </div>
                    <div className="flex h-12 w-12 shrink-0 items-end gap-1">
                      <span className="h-5 flex-1 rounded-t bg-emerald-300/35" />
                      <span className="h-9 flex-1 rounded-t bg-cyan-300/35" />
                      <span className="h-7 flex-1 rounded-t bg-violet-300/35" />
                    </div>
                  </div>
                  <div className="mt-4 space-y-0 divide-y divide-white/10">
                    {card.rows.map(([name, activity, value]) => (
                      <div key={`${card.title}-${name}-${activity}`} className="grid grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)_auto] items-center gap-2 py-2 text-[11px]">
                        <span className="truncate font-semibold text-slate-200">{name}</span>
                        <span className="truncate text-slate-400">{activity}</span>
                        <span className="font-mono text-emerald-200">{value}</span>
                      </div>
                    ))}
                  </div>
                </div>
                <div className="flex flex-1 flex-col p-5">
                  <h3 className="text-lg font-semibold text-white">{card.title}</h3>
                  <p className="mt-3 flex-1 text-sm leading-6 text-slate-400">{card.body}</p>
                  <a href={card.href} className="mt-5 inline-flex items-center gap-1 text-sm font-semibold text-emerald-200 hover:text-emerald-100">
                    <span>{card.cta}</span>
                    <span aria-hidden="true">&rarr;</span>
                  </a>
                </div>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="outcomes" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 lg:grid-cols-[0.8fr_1.2fr] lg:items-center">
          <div>
            <SectionEyebrow>Outcomes</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Research that can be evaluated after the fact.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Walnut preserves confirmation judgments at the time they are made and tracks what happens afterward, creating a historical record of how different levels and types of confirmation performed.
            </p>
          </div>
          <figure className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/90 p-2 shadow-2xl shadow-black/40" data-outcomes-screenshot="confirmation-events">
            <div className="overflow-x-auto [scrollbar-width:thin]">
              <img
                src={outcomesProductScreenshot}
                alt="Walnut Markets Outcomes confirmation events table showing preserved confirmation events with opened date, opened score, direction, entry price, 7 day, 30 day, 90 day, 180 day, 365 day returns, and status."
                width={1806}
                height={871}
                className="h-auto min-w-[980px] rounded-md border border-white/10 lg:min-w-0 lg:w-full"
              />
            </div>
            <figcaption className="px-2 py-3 text-xs leading-5 text-slate-400">
              Real Outcomes view showing confirmation judgments preserved at opening and measured after the fact. Scores are research context, not predictions of future performance.
            </figcaption>
          </figure>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 lg:grid-cols-[0.82fr_1.18fr] lg:items-center">
          <div>
            <SectionEyebrow>Research Memory - Coming Soon</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Your thesis shouldn't disappear when you close the tab.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Capture why you're interested in a particular stock, the catalysts you expect, the risks you're watching, and what would invalidate your investment thesis. As the evidence changes, Walnut helps you recall the original reasoning behind the investment decision.
            </p>
          </div>
          <div className="rounded-lg border border-white/10 bg-slate-950/85 p-5 shadow-2xl shadow-black/30">
            <div className="flex items-start justify-between gap-4 border-b border-white/10 pb-4">
              <div>
                <p className="font-mono text-lg font-semibold text-emerald-300">NVDA</p>
                <p className="text-sm font-semibold text-white">Research Thesis</p>
              </div>
              <span className="rounded border border-amber-300/25 bg-amber-300/10 px-2 py-1 text-xs font-semibold uppercase tracking-[0.14em] text-amber-100">Coming Soon</span>
            </div>
            <div className="mt-5 grid gap-3 sm:grid-cols-2">
              {[
                ["Thesis", "AI infrastructure spending and continued accelerator demand support durable earnings growth."],
                ["Expected catalysts", "Next earnings report; Blackwell demand; data center revenue growth."],
                ["Risks", "Valuation compression; hyperscaler CapEx slowdown; competitive pressure."],
                ["Thesis invalidation", "Material deterioration in AI infrastructure demand or sustained margin compression."],
                ["Time horizon", "12-24 months"],
                ["Status", "Thesis intact"],
                ["What changed", "Example placeholder research update."],
              ].map(([label, body]) => (
                <div key={label} className="rounded-lg border border-white/10 bg-white/[0.035] p-4">
                  <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
                  <p className="mt-2 text-sm leading-6 text-slate-200">{body}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 lg:grid-cols-[0.82fr_1.18fr] lg:items-center">
          <div>
            <SectionEyebrow>Walnut Strategies - Live Beta</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Why settle for average market returns?</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Explore published Walnut strategies with transparent methodology, stored performance records, and the data behind each approach.
            </p>
            <p className="mt-4 text-sm leading-6 text-slate-500">Live beta: strategy research built from Walnut data and historical outcomes, not generic AI-generated picks.</p>
          </div>
          <div className="grid gap-4 sm:grid-cols-3">
            {strategyConcepts.map(([title, type, value, status]) => (
              <article key={title} className="rounded-lg border border-white/10 bg-slate-950/85 p-5">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">{type}</p>
                <h3 className="mt-3 text-lg font-semibold text-white">{title}</h3>
                <div className="mt-5 rounded-lg border border-emerald-300/15 bg-emerald-300/[0.045] p-4">
                  <p className="font-mono text-3xl font-semibold text-emerald-200">{value}</p>
                  <p className="mt-2 text-xs leading-5 text-slate-400">{status}</p>
                </div>
              </article>
            ))}
          </div>
        </div>
      </section>


      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Feature Depth</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">The broader Walnut research surface.</h2>
            <p className="mt-4 text-sm leading-6 text-slate-400">Walnut gives the everyday investors access to the tools and data sets they need to make better, more informed, investment decisions.</p>
          </div>
          <div className="mt-8 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            {featureDepthItems.map(([title, body]) => (
              <article key={title} className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
                <h3 className="text-sm font-semibold text-white">{title}</h3>
                <p className={`mt-2 text-xs leading-5 ${body === "Coming Soon" ? "text-amber-200" : "text-slate-400"}`}>{body}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="pricing" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>Pricing</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Start free. Upgrade to trade like a pro. Cancel anytime.</h2>
          <p className="mt-4 max-w-3xl text-sm leading-6 text-slate-400">
            <span className="font-semibold text-emerald-200">Free tier available.</span> Explore core ticker research, Congress disclosures, insider activity, government contracts, and price/volume context before upgrading.
          </p>
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
                See the data most retail investors miss, including institutional activity, options flow, and deeper positioning that can show whether buying interest is building or fading.
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

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl rounded-lg border border-emerald-300/20 bg-emerald-300/[0.045] p-6 sm:p-8">
          <div className="flex flex-col justify-between gap-6 lg:flex-row lg:items-center">
            <div>
              <SectionEyebrow>Start Research</SectionEyebrow>
              <h2 className="mt-3 max-w-3xl text-3xl font-semibold text-white sm:text-4xl">Before the next trade, check whether the evidence agrees.</h2>
            </div>
            <a
              href={researchStartUrl}
              className="inline-flex shrink-0 items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
            >
              Run a Stock Through Walnut
            </a>
          </div>
        </div>
      </section>

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
            </nav>
          </div>
        </div>
      </footer>
    </main>
  );
}
