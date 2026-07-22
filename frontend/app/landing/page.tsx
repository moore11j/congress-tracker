import type { Metadata } from "next";
import type { ReactNode } from "react";
import { LandingSearch } from "@/components/landing/LandingSearch";
import { LatestInsightImage } from "@/components/landing/LatestInsightImage";
import { WalnutBrandMark } from "@/components/WalnutBrandMark";
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
import { publicResearchTools } from "@/lib/publicResearchTools";
import type { InsightsNewsResponse, MacroSnapshotIndex, MacroSnapshotPoint, MacroSnapshotResponse, NewsItem } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 300;

export const metadata: Metadata = walnutMarketingMetadata;

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnutmarkets.com").replace(/\/+$/, "");
const loginUrl = `${appUrl}/login`;
const pricingUrl = `${appUrl}/pricing`;
const publicPricingUrl = `${WALNUT_MARKETING_URL}/pricing`;
const timCookInsiderUrl = `${appUrl}/insider/tim-cook-0001214156`;

type TrendingTicker = {
  symbol: string;
  companyName: string;
  price: number | null;
  dayChangePct: number | null;
};

type MarketQuoteItem = {
  symbol: string;
  company_name?: string | null;
  current_price?: number | null;
  day_change_pct?: number | null;
  as_of?: string | null;
};

type MarketQuotesResponse = {
  items?: MarketQuoteItem[];
  status?: "ok" | "partial" | "unavailable" | string;
};

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

const navLinks = [
  ["Trends", "#signals"],
  ["Congress", "#congress"],
  ["Insiders", "#insiders"],
  ["Stock Screener", "#screener"],
  ["About", "/about"],
  ["Pricing", "#pricing"],
] as const;

const signalCards = [
  {
    title: "Congress Activity",
    body: "Monitor House and Senate activity with ticker, filing, party, chamber, and trade context.",
    label: "Public disclosures",
  },
  {
    title: "Insider Activity",
    body: "Track executive and director purchases, sales, ownership changes, and role-weighted activity.",
    label: "Insider filings",
  },
  {
    title: "Institutional Activity",
    body: "See whether newly filed institutional positioning is accumulating, reducing, or staying quiet.",
    label: "Pro layer",
  },
  {
    title: "Options Flow",
    body: "Options activity will add another confirmation layer when flow supports or contradicts the setup.",
    label: "Coming soon",
  },
  {
    title: "Macro Positioning",
    body: "Understand whether institutional futures positioning supports or contradicts your investment thesis.",
    label: "Pro layer",
    href: `${appUrl}/insights#macro-positioning`,
  },
  {
    title: "Government Contracts",
    body: "Connect contract awards and modifications to ticker-level confirmation when the exposure is material.",
    label: "Public awards",
  },
  {
    title: "Technical indicator filters",
    body: "Screen for RSI, relative volume, price momentum, MACD state, trend state, beta, and liquidity conditions.",
    label: "Technicals",
  },
  {
    title: "Fundamental indicator filters",
    body: "Filter by valuation, margins, growth, leverage, cash flow, earnings yield, ROE, ROIC, and balance-sheet quality.",
    label: "Fundamentals",
  },
] as const;

const whyWalnut = [
  {
    title: "Bullish trends",
    icon: "trendUp",
    cardClassName: "border-emerald-300/20 bg-emerald-300/[0.045]",
    iconClassName: "border-emerald-300/25 bg-emerald-300/10 text-emerald-200",
    glowClassName: "bg-emerald-300/15",
  },
  {
    title: "Bearish trends",
    icon: "trendDown",
    cardClassName: "border-rose-300/20 bg-rose-300/[0.04]",
    iconClassName: "border-rose-300/25 bg-rose-300/10 text-rose-200",
    glowClassName: "bg-rose-300/15",
  },
  {
    title: "Trend confirmation",
    icon: "confirmedTrend",
    cardClassName: "border-lime-300/20 bg-lime-300/[0.04]",
    iconClassName: "border-lime-300/25 bg-lime-300/10 text-lime-200",
    glowClassName: "bg-lime-300/15",
  },
  {
    title: "Contradicting data",
    icon: "splitData",
    cardClassName: "border-cyan-300/20 bg-cyan-300/[0.04]",
    iconClassName: "border-cyan-300/25 bg-cyan-300/10 text-cyan-200",
    glowClassName: "bg-cyan-300/15",
  },
  {
    title: "Risk factors",
    icon: "warning",
    cardClassName: "border-amber-300/20 bg-amber-300/[0.04]",
    iconClassName: "border-amber-300/25 bg-amber-300/10 text-amber-200",
    glowClassName: "bg-amber-300/15",
  },
  {
    title: "Alerts & watchlists",
    icon: "alarm",
    cardClassName: "border-violet-300/20 bg-violet-300/[0.04]",
    iconClassName: "border-violet-300/25 bg-violet-300/10 text-violet-200",
    glowClassName: "bg-violet-300/15",
  },
] as const;

type WhyWalnutIconKind = (typeof whyWalnut)[number]["icon"];

const marketToolCategories = [
  {
    name: "Market snapshots",
    body: "Fast scans across price, valuation, liquidity, and basic fundamentals.",
  },
  {
    name: "Charting workspaces",
    body: "Technical charts, indicators, alerts, and visual analysis.",
  },
  {
    name: "Public data trackers",
    body: "Disclosure feeds, alternative datasets, and event monitoring.",
  },
  {
    name: "Flow and activity feeds",
    body: "Options flow, volume spikes, dark-pool prints, and unusual activity.",
  },
] as const;

const availableNowColumns = [
  ["Congress trades", "Insider trades", "Ticker intelligence", "Confirmation score"],
  ["Government contracts", "Watchlists", "Screener", "Member/insider performance"],
  ["Institutional Activity", "Macro Positioning", "Market pressure"],
  ["Portfolio backtesting", "Congress leaderboards", "Earnings and event calendar overlays"],
] as const;

const comingSoon = [
  "Options Flow",
  "AI analyst briefs",
  "Social Sentiment",
  "API and webhooks",
] as const;

const fallbackTrending: TrendingTicker[] = [
  { symbol: "NVDA", companyName: "NVIDIA Corp", price: null, dayChangePct: null },
  { symbol: "AAPL", companyName: "Apple Inc", price: null, dayChangePct: null },
  { symbol: "LMT", companyName: "Lockheed Martin", price: null, dayChangePct: null },
  { symbol: "PLTR", companyName: "Palantir Technologies", price: null, dayChangePct: null },
  { symbol: "NOW", companyName: "ServiceNow Inc", price: null, dayChangePct: null },
  { symbol: "TSLA", companyName: "Tesla Inc", price: null, dayChangePct: null },
];

const fallbackInsights: NewsItem[] = [
  {
    title: "Congressional disclosures, insider trades, and ticker context update throughout the terminal.",
    url: `${appUrl}/insights`,
    source: "walnut_landing",
    site: "Walnut",
  },
  {
    title: "Government contracts, political exposure, and issuer-level intelligence are available in the live app.",
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

async function loadTrendingTickers(): Promise<TrendingTicker[]> {
  try {
    const symbols = fallbackTrending.map((ticker) => ticker.symbol).join(",");
    const response = await landingFetchJson<MarketQuotesResponse>("/api/market/quotes", { symbols }, 1800);
    const quotesBySymbol = new Map((response.items ?? []).map((item) => [item.symbol?.toUpperCase(), item]));
    return fallbackTrending.map((ticker) => {
      const quote = quotesBySymbol.get(ticker.symbol);
      return {
        ...ticker,
        companyName: quote?.company_name || ticker.companyName,
        price: typeof quote?.current_price === "number" && Number.isFinite(quote.current_price) ? quote.current_price : null,
        dayChangePct: typeof quote?.day_change_pct === "number" && Number.isFinite(quote.day_change_pct) ? quote.day_change_pct : null,
      };
    });
  } catch {
    return fallbackTrending;
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
  const candidate = [record.image_url, record.image, record.thumbnail, record.urlToImage].find((value): value is string => typeof value === "string" && value.trim().length > 0);
  return candidate?.startsWith("http") ? candidate : null;
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

function formatTickerPrice(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Open app";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
}

function formatPct(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Explore trend";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
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
    const monthly = planPriceFor(config, tier, "monthly");
    const annual = planPriceFor(config, tier, "annual");
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
    description: WALNUT_MARKETING_DESCRIPTION,
    sameAs: WALNUT_SOCIAL_URLS,
  };

  const website = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: "Walnut Markets",
    url: WALNUT_MARKETING_URL,
    description: WALNUT_MARKETING_DESCRIPTION,
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

function WhyWalnutIcon({ kind }: { kind: WhyWalnutIconKind }) {
  const commonProps = {
    viewBox: "0 0 96 96",
    fill: "none",
    className: "h-14 w-14",
    "aria-hidden": true,
  };

  if (kind === "trendUp") {
    return (
      <svg {...commonProps}>
        <path d="M17 75h62" stroke="currentColor" strokeWidth="4" strokeLinecap="round" opacity="0.7" />
        <path d="M22 66l15-17 13 9 26-32" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M61 26h15v15" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M26 75V63M45 75V58M65 75V42" stroke="currentColor" strokeWidth="3" strokeLinecap="round" opacity="0.55" />
      </svg>
    );
  }

  if (kind === "trendDown") {
    return (
      <svg {...commonProps}>
        <path d="M17 75h62" stroke="currentColor" strokeWidth="4" strokeLinecap="round" opacity="0.7" />
        <path d="M22 30l15 17 13-9 26 32" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M61 70h15V55" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M26 75V43M45 75V54M65 75V66" stroke="currentColor" strokeWidth="3" strokeLinecap="round" opacity="0.55" />
      </svg>
    );
  }

  if (kind === "confirmedTrend") {
    return (
      <svg {...commonProps}>
        <path d="M18 72h62" stroke="currentColor" strokeWidth="4" strokeLinecap="round" />
        <path d="M22 64l14-16 12 9 22-28" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M60 28h12v12" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M24 33l8 8 17-19" stroke="currentColor" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M25 72V58M45 72V60M66 72V42" stroke="currentColor" strokeWidth="3" strokeLinecap="round" opacity="0.6" />
      </svg>
    );
  }

  if (kind === "splitData") {
    return (
      <svg {...commonProps}>
        <path d="M20 48h21" stroke="currentColor" strokeWidth="5" strokeLinecap="round" />
        <path d="M41 48c13 0 17-20 31-20h7" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M41 48c13 0 17 20 31 20h7" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M72 20l8 8-8 8M72 60l8 8-8 8" stroke="currentColor" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M18 76h19M18 20h19" stroke="currentColor" strokeWidth="3" strokeLinecap="round" opacity="0.55" />
      </svg>
    );
  }

  if (kind === "warning") {
    return (
      <svg {...commonProps}>
        <path d="M48 17 83 78H13L48 17Z" stroke="currentColor" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M48 39v17M48 67h.1" stroke="currentColor" strokeWidth="6" strokeLinecap="round" />
        <path d="M31 78h34" stroke="currentColor" strokeWidth="3" strokeLinecap="round" opacity="0.65" />
      </svg>
    );
  }

  return (
    <svg {...commonProps}>
      <path d="M31 21 20 31M65 21l11 10M48 29a25 25 0 1 1 0 50 25 25 0 0 1 0-50Z" stroke="currentColor" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M48 44v15l11 7M34 83l-5 6M62 83l5 6" stroke="currentColor" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M35 14c-6 0-11 5-11 11M61 14c6 0 11 5 11 11" stroke="currentColor" strokeWidth="3" strokeLinecap="round" opacity="0.75" />
    </svg>
  );
}

export default async function LandingPage() {
  const [latestInsights, planConfig, marketSnapshot, trendingTickers] = await Promise.all([loadLatestInsights(), loadPlanConfig(), loadMarketSnapshot(), loadTrendingTickers()]);
  const heroInsight = latestInsights[0] ?? fallbackInsights[0];
  const heroInsightImage = insightImageUrl(heroInsight);
  const freePrice = landingPlanPriceDisplay(planConfig, "free");
  const premiumPrice = landingPlanPriceDisplay(planConfig, "premium");
  const proPrice = landingPlanPriceDisplay(planConfig, "pro");
  const structuredData = landingJsonLd(planConfig);

  return (
    <main className="min-h-screen overflow-hidden bg-[#030712] text-slate-100">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify(structuredData).replace(/</g, "\\u003c") }} />
      <div className="absolute inset-0 -z-10 bg-[linear-gradient(90deg,rgba(148,163,184,0.05)_1px,transparent_1px),linear-gradient(180deg,rgba(148,163,184,0.04)_1px,transparent_1px)] bg-[size:56px_56px]" />
      <header className="sticky top-0 z-40 border-b border-white/10 bg-slate-950/88 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut home">
            <WalnutBrandMark
              className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]"
              svgClassName="h-6 w-6 overflow-visible"
            />
            <span className="leading-none">
              <span className="block whitespace-nowrap text-base font-semibold text-white">Walnut</span>
              <span className="mt-1 block whitespace-nowrap text-[11px] font-medium text-slate-400">Market Terminal</span>
            </span>
          </a>
          <nav className="hidden items-center gap-5 text-sm font-medium text-slate-300 lg:flex">
            {navLinks.map(([label, href]) => (
              <a key={label} href={href} className="transition hover:text-white">
                {label}
              </a>
            ))}
          </nav>
          <div className="flex shrink-0 items-center gap-2">
            <a
              href={loginUrl}
              className="hidden rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/25 hover:text-white md:inline-flex"
            >
              Login / Register
            </a>
            <a
              href={loginUrl}
              className="whitespace-nowrap rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-1.5 text-sm font-medium text-emerald-100 transition hover:bg-emerald-300/15 md:hidden"
            >
              Login / Register
            </a>
            <a
              href={appUrl}
              className="hidden rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200 md:inline-flex"
            >
              Launch Terminal
            </a>
          </div>
        </div>
      </header>

      <section className="relative border-b border-white/10">
        <div className="mx-auto grid min-h-[calc(100vh-73px)] max-w-7xl items-center gap-10 px-4 py-16 sm:px-6 lg:grid-cols-[1.02fr_0.98fr] lg:px-8 lg:py-20">
          <div className="max-w-3xl">
            <SectionEyebrow>Walnut Market Terminal</SectionEyebrow>
            <h1 className="mt-5 max-w-4xl text-4xl font-semibold leading-[1.04] text-white sm:text-5xl lg:text-6xl">
              The market has tells. Walnut finds them.
            </h1>
            <p className="mt-6 max-w-2xl text-lg font-semibold leading-7 text-emerald-100 sm:text-xl">Identify bullish and bearish trends with the data that confirms the move.</p>
            <p className="mt-6 max-w-2xl text-base leading-7 text-slate-300 sm:text-lg">
              Walnut helps investors identify and confirm bullish and bearish trends across technicals, fundamentals, Congress trades, insider activity, government contracts, reported institutional activity, options flow, news, and filings in one market intelligence terminal.
            </p>
            <LandingSearch appUrl={appUrl} />
            <div className="mt-7 flex flex-col gap-3 sm:flex-row sm:flex-wrap">
              <a
                href={appUrl}
                className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
              >
                Launch Terminal
              </a>
            </div>
            <p className="mt-5 max-w-2xl text-xs leading-5 text-slate-400">
              Free users can explore core ticker research, price/volume context, Congress disclosures, insider activity, and government contract data. Paid tiers unlock heavier research features such as trend confirmation, our proprietary confirmation score, reported institutional activity, and options flow. Built for research. Not investment advice.
            </p>
          </div>

          <div className="relative">
            <div className="rounded-lg border border-white/10 bg-slate-950/90 shadow-2xl shadow-black/40">
              <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Market brief</p>
                  <p className="mt-1 text-sm font-semibold text-white">A snapshot of what Walnut is watching now.</p>
                </div>
                <span className="rounded border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">Updated</span>
              </div>
              <div className="border-b border-white/10 p-5">
                <a href={insightHref(heroInsight)} className="group block" target={heroInsight.url.startsWith("http") ? "_blank" : undefined} rel="noreferrer">
                  <LatestInsightImage src={heroInsightImage} alt={heroInsight.title} />
                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">{heroInsight.site || heroInsight.source || "Walnut"}</p>
                  <h2 className="mt-3 text-2xl font-semibold leading-tight text-white group-hover:text-emerald-100">{heroInsight.title}</h2>
                  {heroInsight.summary ? (
                    <p className="mt-3 overflow-hidden text-sm leading-6 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:3]">
                      {heroInsight.summary}
                    </p>
                  ) : null}
                </a>
              </div>
              <div className="grid gap-3 p-4 sm:grid-cols-2">
                {trendingTickers.slice(0, 4).map((ticker) => {
                  const changeTone =
                    ticker.dayChangePct === null
                      ? "text-slate-400"
                      : ticker.dayChangePct >= 0
                        ? "text-emerald-300"
                        : "text-rose-300";
                  return (
                    <a key={ticker.symbol} href={`${appUrl}/ticker/${ticker.symbol}`} className="rounded-lg border border-white/10 bg-white/[0.035] p-4 transition hover:border-emerald-300/35">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="font-mono text-lg font-semibold text-emerald-200">{ticker.symbol}</p>
                          <p className="mt-1 overflow-hidden text-ellipsis whitespace-nowrap text-xs text-slate-400">{ticker.companyName}</p>
                        </div>
                        <div className="shrink-0 text-right">
                          <p className="font-mono text-sm font-semibold text-white">{formatTickerPrice(ticker.price)}</p>
                          <p className={`mt-1 text-xs ${changeTone}`}>{formatPct(ticker.dayChangePct)}</p>
                        </div>
                      </div>
                    </a>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="signals" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Ticker Research Data</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Track the data that can confirm the next trend.</h2>
            <p className="mt-4 text-base leading-7 text-slate-400">
              Walnut brings market data together so investors can see whether a ticker&apos;s trend is supported, contradicted, or still unclear.
            </p>
          </div>
          <div className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            {signalCards.map((card) => {
              const content = (
                <>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">{card.label}</p>
                  <h3 className="mt-4 text-lg font-semibold text-white">{card.title}</h3>
                  <p className="mt-3 text-sm leading-6 text-slate-400">{card.body}</p>
                </>
              );
              return "href" in card ? (
                <a key={card.title} href={card.href} className="rounded-lg border border-white/10 bg-white/[0.035] p-5 transition hover:border-emerald-300/35 hover:bg-white/[0.055]">
                  {content}
                </a>
              ) : (
                <article key={card.title} className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
                  {content}
                </article>
              );
            })}
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
            <div>
              <SectionEyebrow>Daily Insights</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Access the latest insights and market data available inside the terminal.</h2>
            </div>
            <a href={`${appUrl}/insights`} className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap text-sm font-semibold text-emerald-200 hover:text-emerald-100 md:ml-4">
              <span>Open insights</span>
              <span aria-hidden="true">→</span>
            </a>
          </div>
          <div className="mt-8 grid gap-5 lg:grid-cols-[1.05fr_0.95fr]">
            <div className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
              <div className="mt-5 divide-y divide-white/10">
                {latestInsights.slice(0, 5).map((item) => (
                  <a key={`${item.title}-${item.url}`} href={insightHref(item)} target={item.url.startsWith("http") ? "_blank" : undefined} rel="noreferrer" className="block py-4 first:pt-0 last:pb-0">
                    <p className="text-sm font-semibold leading-6 text-white hover:text-emerald-100">{item.title}</p>
                    <p className="mt-1 text-xs text-slate-400">{item.site || item.source || "Market news"}</p>
                  </a>
                ))}
              </div>
            </div>
            <LandingMarketSnapshot snapshot={marketSnapshot} />
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Congress and Insider Trade Profiles</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Go straight into the real research pages.</h2>
            <p className="mt-4 text-sm leading-6 text-slate-400">Portfolio simulations, insider profiles, ticker charts, and transaction tables live inside the app.</p>
          </div>

          <div className="mt-8 grid gap-5 lg:grid-cols-2">
            <a id="congress" href={`${appUrl}/member/nancy-pelosi`} className="rounded-lg border border-white/10 bg-slate-950/85 p-6 shadow-2xl shadow-black/25 transition hover:border-emerald-300/35">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Congress portfolio simulation</p>
              <h3 className="mt-3 text-2xl font-semibold text-white">Nancy Pelosi disclosure portfolio</h3>
              <p className="mt-3 text-sm leading-6 text-slate-400">
                Open the member profile with simulated holdings, benchmark comparison, recent disclosures, and trade outcome context.
              </p>
              <span className="mt-5 inline-flex rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100">Open portfolio -&gt;</span>
            </a>

            <a id="insiders" href={timCookInsiderUrl} className="rounded-lg border border-white/10 bg-slate-950/85 p-6 shadow-2xl shadow-black/25 transition hover:border-cyan-300/35">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-cyan-300">Insider profile with ticker chart</p>
              <h3 className="mt-3 text-2xl font-semibold text-white">Tim Cook insider activity profile</h3>
              <p className="mt-3 text-sm leading-6 text-slate-400">
                Open the insider profile with Apple ticker chart context, transaction history, issuer details, and performance readouts.
              </p>
              <span className="mt-5 inline-flex rounded-lg border border-cyan-300/30 bg-cyan-300/10 px-3 py-2 text-sm font-semibold text-cyan-100">Open insider profile -&gt;</span>
            </a>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-10 lg:grid-cols-[0.85fr_1.15fr]">
          <div>
            <SectionEyebrow>Why Walnut</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Better research starts with better trend confirmation.</h2>
            <p className="mt-5 text-base leading-7 text-slate-400">
              Walnut brings technical analysis, fundamentals, Congress trades, insider activity, government contracts, reported institutional activity, options flow, news, filings, and our proprietary confirmation score into one research workflow.
            </p>
            <p className="mt-4 text-base leading-7 text-slate-400">
              We help investors identify bullish trends, bearish trends, confirming data, contradicting data, risk factors, and the next data points to watch.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {whyWalnut.map((item) => (
              <div key={item.title} className={`relative min-h-[150px] overflow-hidden rounded-lg border p-5 ${item.cardClassName}`}>
                <div className={`pointer-events-none absolute -bottom-8 -right-8 h-28 w-28 rounded-full blur-2xl ${item.glowClassName}`} />
                <div className="relative flex h-full min-h-[110px] flex-col justify-between gap-5">
                  <div className="flex items-start justify-between gap-4">
                    <h3 className="max-w-[9rem] text-sm font-semibold leading-6 text-slate-100">{item.title}</h3>
                    <div className={`flex h-16 w-16 shrink-0 items-center justify-center rounded-lg border ${item.iconClassName}`}>
                      <WhyWalnutIcon kind={item.icon} />
                    </div>
                  </div>
                  <div className="h-px w-full bg-gradient-to-r from-white/15 via-white/5 to-transparent" />
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Differentiation</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">How Walnut is different</h2>
            <p className="mt-5 text-base leading-7 text-slate-400">
              Market research usually starts with charts, screeners, data feeds, and alerts.
            </p>
            <p className="mt-4 text-lg font-semibold leading-7 text-emerald-100">Walnut turns raw market data into trend-driven ticker research: bullish trends, bearish trends, confirmation, risk, and what to watch next.</p>
          </div>
          <div className="mt-8 grid gap-4 lg:grid-cols-[1fr_1fr] xl:grid-cols-[1fr_1.05fr]">
            <div className="grid gap-4 sm:grid-cols-2">
              {marketToolCategories.map((card) => (
                <article key={card.name} className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
                  <h3 className="text-lg font-semibold text-white">{card.name}</h3>
                  <p className="mt-3 text-sm leading-6 text-slate-400">{card.body}</p>
                </article>
              ))}
            </div>
            <article className="rounded-lg border border-emerald-300/30 bg-emerald-300/[0.06] p-6 shadow-2xl shadow-emerald-950/15">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-200">Walnut</p>
              <h3 className="mt-4 text-2xl font-semibold text-white">Intelligent investment decisions.</h3>
              <p className="mt-4 text-sm leading-6 text-slate-300">
                We connect technical analysis, fundamentals, Congress trades, insider activity, government contracts, reported institutional activity, options flow, news, filings, and our proprietary confirmation score into a clearer read on whether the data points bullish, bearish, or mixed.
              </p>
              <div className="mt-6 grid gap-3 sm:grid-cols-2">
                {["Market take", "Recent changes", "Supporting data", "Risks", "What to watch next", "Research workflow"].map((item) => (
                  <div key={item} className="rounded-lg border border-emerald-300/20 bg-slate-950/55 px-4 py-3 text-sm font-semibold text-emerald-50">
                    {item}
                  </div>
                ))}
              </div>
            </article>
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>The Walnut data</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Available now, with new market intelligence datasets coming next.</h2>
          <div className="mt-8 grid gap-4">
            <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.04] p-6">
              <h3 className="text-lg font-semibold text-white">Available Now</h3>
              <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {availableNowColumns.map((column, columnIndex) => (
                  <div key={`available-column-${columnIndex}`} className="grid content-start gap-3">
                    {column.map((item) => (
                      <div key={item} className="rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-medium text-slate-200">
                        {item}
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
            <div className="rounded-lg border border-cyan-300/20 bg-cyan-300/[0.035] p-6">
              <h3 className="text-lg font-semibold text-white">Coming Soon</h3>
              <div className="mt-5 grid gap-3 md:grid-cols-3">
                {comingSoon.map((item) => (
                  <div key={item} className="flex items-center justify-between gap-3 rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-medium text-slate-200">
                    <span>{item}</span>
                    <span className="shrink-0 rounded border border-cyan-300/30 bg-cyan-300/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-cyan-100">
                      Coming Soon
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="screener" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl rounded-lg border border-white/10 bg-white/[0.035] p-6 sm:p-8">
          <div className="grid gap-8 lg:grid-cols-[0.75fr_1.25fr] lg:items-center">
            <div>
              <SectionEyebrow>Stock Screener</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white">An advanced stock screener built for trend confirmation.</h2>
              <p className="mt-5 text-sm leading-6 text-slate-400">
                Screen across disclosure activity, government contracts, technical indicators, fundamentals, liquidity, valuation, trend, quality, and confirmation data from the same terminal experience.
              </p>
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Filter market data by</p>
              <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                {[
                  "Congressional activity",
                  "Insider activity",
                  "Government contracts",
                  "Confirmation score",
                  "RSI and relative volume",
                  "MACD and trend state",
                  "Valuation multiples",
                  "Margins and growth",
                  "ROE, ROIC, cash flow",
                ].map((item) => (
                  <div key={item} className="rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-semibold text-white">
                    {item}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="pricing" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>Pricing</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Start free. Upgrade to Premium or Pro when you need deeper insights.</h2>
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
                Unlock advanced screening, saved views, monitoring, alerts, exports, and deeper trend-confirmation workflows.
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
                Higher limits and power-user capacity for serious research, watchlists, and multi-trend monitoring.
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

      <footer className="px-4 py-10 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-8 text-sm text-slate-400 lg:grid-cols-[1fr_1fr_1fr]">
          <div>
            <p className="font-semibold text-white">Walnut Market Terminal</p>
            <p className="mt-1">Walnut Market Terminal is operated by Walnut Intelligence Inc.</p>
            <p className="mt-3 max-w-2xl text-xs leading-5 text-slate-400">
              Walnut is a market intelligence terminal for research and informational purposes only. Walnut does not provide investment advice.
            </p>
          </div>
          <nav className="grid gap-2" aria-label="Research tools footer">
            <p className="font-semibold text-slate-300">Research tools</p>
            {publicResearchTools.map((tool) => (
              <a key={tool.href} href={tool.href} className="hover:text-white">
                {tool.label}
              </a>
            ))}
          </nav>
          <nav className="flex flex-wrap gap-4 lg:justify-end" aria-label="Company footer">
            <a href={appUrl} className="hover:text-white">
              App
            </a>
            <a href={pricingUrl} className="hover:text-white">
              Pricing
            </a>
            <a href={loginUrl} className="hover:text-white">
              Login / Register
            </a>
            <a href="/about" className="hover:text-white">
              About
            </a>
            <a href="/faq" className="hover:text-white">
              FAQ
            </a>
            <a href="/terms" className="hover:text-white">
              Terms
            </a>
            <a href="/privacy" className="hover:text-white">
              Privacy
            </a>
            <a href="mailto:support@walnutmarkets.com" className="hover:text-white">
              Contact / support@walnutmarkets.com
            </a>
            <a href={WALNUT_X_URL} target="_blank" rel="noreferrer" className="hover:text-white">
              X / {WALNUT_X_HANDLE}
            </a>
            <a href={WALNUT_REDDIT_URL} target="_blank" rel="noreferrer" className="hover:text-white">
              Reddit / r/walnutmarkets
            </a>
          </nav>
        </div>
      </footer>
    </main>
  );
}
