import type { Metadata } from "next";
import type { ReactNode } from "react";
import { LandingSearch } from "@/components/landing/LandingSearch";
import { LatestInsightImage } from "@/components/landing/LatestInsightImage";
import { API_BASE, type PlanConfig, type PlanPrice } from "@/lib/api";
import type { InsightsNewsResponse, MacroSnapshotIndex, MacroSnapshotPoint, MacroSnapshotResponse, NewsItem } from "@/lib/types";

export const revalidate = 300;

export const metadata: Metadata = {
  metadataBase: new URL("https://walnut-intel.com"),
  title: "Walnut | Market Terminal",
  description: "Professional-grade market intelligence from public signals: Congress trades, insider activity, government contracts, ticker intelligence, and cross-source confirmation.",
  alternates: {
    canonical: "/",
  },
};

const appUrl = (process.env.NEXT_PUBLIC_APP_URL ?? "https://app.walnut-intel.com").replace(/\/+$/, "");
const loginUrl = `${appUrl}/login`;
const pricingUrl = `${appUrl}/pricing`;

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
  ["Signals", "#signals"],
  ["Congress Trades", "#congress"],
  ["Insider Trades", "#insiders"],
  ["Screener", "#screener"],
  ["Pricing", "#pricing"],
] as const;

const signalCards = [
  {
    title: "Congressional disclosures",
    body: "Monitor House and Senate activity with ticker, filing, party, chamber, and trade context.",
    label: "Public disclosures",
  },
  {
    title: "Insider transactions",
    body: "Track executive and director purchases, sales, ownership changes, and role-weighted activity.",
    label: "SEC Form 4",
  },
  {
    title: "Ticker intelligence",
    body: "Unify political, insider, financial, and event-level context around a single public-market name.",
    label: "Ticker lens",
  },
  {
    title: "Signal Conviction Score",
    body: "Rank names by cross-source confirmation instead of treating each disclosure as an isolated datapoint.",
    label: "Confirmation",
  },
  {
    title: "Watchlists and alerts",
    body: "Keep priority tickers close and prepare for premium monitoring workflows as new signals land.",
    label: "Monitoring",
  },
  {
    title: "Screener and saved views",
    body: "Turn recurring research patterns into repeatable screens across market and intelligence filters.",
    label: "Research ops",
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
  "Professional-grade signal stack: Congress, insiders, contracts, filings, fundamentals, technicals, and market context.",
  "Transparent conviction: see why a ticker scores high before you trust the score.",
  "Built for speed: move from market event to ticker page to evidence trail in seconds.",
  "Public-data edge: identify patterns hidden in plain sight.",
] as const;

const availableNow = [
  "Congress trades",
  "Insider trades",
  "Ticker intelligence",
  "Signal scores",
  "Government contracts",
  "Watchlists",
  "Screener",
  "Member/insider performance",
] as const;

const comingSoon = [
  "AI analyst briefs",
  "Options Flow",
  "Institutional Activity",
  "Earnings and event calendar overlays",
  "Social Sentiment",
  "Advanced alerts/exports",
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
    { label: "S&P 500", symbol: "^GSPC", timeframe_label: "1D change" },
    { label: "Nasdaq", symbol: "^IXIC", timeframe_label: "1D change" },
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

const landingMacroLabelGroups = [
  ["Fed Overnight Rate", "Federal Funds Rate", "Effective Federal Funds Rate", "federalFunds"],
  ["Core CPI", "Core CPI YoY", "Core CPI Year over Year", "core_cpi", "coreCpi", "core_cpi_yoy", "coreCpiYoY", "cpi_core", "CPILFESL", "CPIAUCSL"],
  ["Unemployment", "Unemployment Rate", "unemploymentRate"],
] as const;

async function landingFetchJson<T>(path: string, params?: Record<string, string | number | undefined>, timeoutMs = 3500): Promise<T> {
  const url = new URL(path, API_BASE);
  Object.entries(params ?? {}).forEach(([key, value]) => {
    if (value !== undefined) url.searchParams.set(key, String(value));
  });

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      headers: {
        "X-Walnut-Route": "/landing",
        "X-Walnut-Component": "LandingPage",
      },
      next: { revalidate },
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
    const config = await landingFetchJson<PlanConfig>("/api/plan-config", undefined, 2500);
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
    return await landingFetchJson<MacroSnapshotResponse>("/api/insights/macro-snapshot", undefined, 1800);
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

function formatMacroChange(item: MacroSnapshotPoint): string {
  const value = item.change_value ?? item.change;
  if (typeof value !== "number" || !Number.isFinite(value)) return item.change_label ?? "Latest available";
  const format = item.change_format ?? item.change_unit;
  const sign = value > 0 ? "+" : "";
  if (format === "bps") return `${sign}${value.toFixed(0)} bps`;
  if (format === "percentage_points") return `${sign}${value.toFixed(2)} pp`;
  if (format === "percent") return `${sign}${value.toFixed(2)}%`;
  return `${sign}${value.toFixed(2)}`;
}

function deltaClassName(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "text-slate-500";
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
    label: item.label,
    symbol: item.symbol,
    value: item.value,
    changePct: item.change_pct,
    timeframeLabel: item.timeframe_label,
  };
}

function formatTickerPrice(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Open app";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: value >= 100 ? 0 : 2 }).format(value);
}

function formatPct(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return "Quote unavailable";
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
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{subtitle}</p>
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
            <p className="mt-1 truncate font-mono text-xs text-slate-500">{item.symbol ?? item.timeframeLabel ?? "Latest available"}</p>
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
              <p className="mt-1 truncate text-xs text-slate-500">{item.change_label ?? item.context_label ?? "Latest available"}</p>
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
  const usIndexes = (snapshot.indexes?.length ? snapshot.indexes : fallbackMarketSnapshot.indexes).slice(0, 3).map(indexToInstrument);
  const economics = landingMacroRows(snapshot.economics ?? []);
  const treasury = (snapshot.treasury?.length ? snapshot.treasury : fallbackMarketSnapshot.treasury).slice(0, 2);
  const statusLabel = snapshot.status === "ok" || snapshot.status === "partial" ? "Market snapshot" : "Market snapshot examples";

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">{statusLabel}</p>
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

function WalnutMark() {
  return (
    <span className="flex h-9 w-9 items-center justify-center rounded-lg border border-emerald-300/35 bg-slate-950 shadow-[0_0_28px_rgba(16,185,129,0.18)]">
      <svg viewBox="0 0 48 48" aria-hidden="true" className="h-6 w-6">
        <path
          d="M24 7c-4.5 0-7.8 3.2-8.1 7.5-4.2.5-7.3 3.9-7.3 8.1 0 1.6.4 3 1.2 4.3-2 1.6-3.1 3.9-3.1 6.5 0 4.7 3.8 8.6 8.5 8.6 2.6 0 4.8-1.1 6.4-2.9.7.2 1.5.3 2.4.3s1.7-.1 2.4-.3c1.6 1.8 3.8 2.9 6.4 2.9 4.7 0 8.5-3.9 8.5-8.6 0-2.6-1.1-4.9-3.1-6.5.8-1.3 1.2-2.7 1.2-4.3 0-4.2-3.1-7.6-7.3-8.1C31.8 10.2 28.5 7 24 7Z"
          fill="#020617"
          stroke="#34d399"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="3"
        />
        <path
          d="M24 8.5v30M16 16c3.2 2.4 5.4 5.5 6.4 9M32 16c-3.2 2.4-5.4 5.5-6.4 9M10.5 27c4.1 1.5 7.1 3.9 9.1 7.4M37.5 27c-4.1 1.5-7.1 3.9-9.1 7.4"
          fill="none"
          stroke="#ccfbf1"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth="2.4"
        />
      </svg>
    </span>
  );
}

function SectionEyebrow({ children }: { children: ReactNode }) {
  return <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-300">{children}</p>;
}

export default async function LandingPage() {
  const [latestInsights, planConfig, marketSnapshot, trendingTickers] = await Promise.all([loadLatestInsights(), loadPlanConfig(), loadMarketSnapshot(), loadTrendingTickers()]);
  const heroInsight = latestInsights[0] ?? fallbackInsights[0];
  const heroInsightImage = insightImageUrl(heroInsight);
  const freePrice = landingPlanPriceDisplay(planConfig, "free");
  const premiumPrice = landingPlanPriceDisplay(planConfig, "premium");
  const proPrice = landingPlanPriceDisplay(planConfig, "pro");

  return (
    <main className="min-h-screen overflow-hidden bg-[#030712] text-slate-100">
      <div className="absolute inset-0 -z-10 bg-[linear-gradient(90deg,rgba(148,163,184,0.05)_1px,transparent_1px),linear-gradient(180deg,rgba(148,163,184,0.04)_1px,transparent_1px)] bg-[size:56px_56px]" />
      <header className="sticky top-0 z-40 border-b border-white/10 bg-slate-950/88 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4 sm:px-6 lg:px-8">
          <a href="/" className="flex min-w-0 items-center gap-3" aria-label="Walnut home">
            <WalnutMark />
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
              href={appUrl}
              className="rounded-lg bg-emerald-300 px-3 py-2 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
            >
              Launch Terminal
            </a>
          </div>
        </div>
      </header>

      <section className="relative border-b border-white/10">
        <div className="mx-auto grid min-h-[calc(100vh-73px)] max-w-7xl items-center gap-10 px-4 py-16 sm:px-6 lg:grid-cols-[1.02fr_0.98fr] lg:px-8 lg:py-20">
          <div className="max-w-3xl">
            <SectionEyebrow>Market Terminal</SectionEyebrow>
            <h1 className="mt-5 max-w-4xl text-4xl font-semibold leading-[1.04] text-white sm:text-5xl lg:text-6xl">
              Crack the market.
            </h1>
            <p className="mt-6 max-w-2xl text-lg font-semibold leading-7 text-emerald-100 sm:text-xl">The market has tells. Walnut finds them.</p>
            <p className="mt-6 max-w-2xl text-base leading-7 text-slate-300 sm:text-lg">
              Walnut turns scattered public data into a signal stack investors can actually use — Congress trades, insider activity, government contracts, ticker intelligence, and cross-source confirmation in one market terminal.
            </p>
            <LandingSearch appUrl={appUrl} />
            <div className="mt-7 flex flex-col gap-3 sm:flex-row">
              <a
                href={appUrl}
                className="inline-flex items-center justify-center rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
              >
                Launch Terminal
              </a>
              <a
                href="#signals"
                className="inline-flex items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-5 py-3 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:bg-white/[0.06]"
              >
                Explore Signals
              </a>
            </div>
            <p className="mt-5 text-xs leading-5 text-slate-500">Built for research. Not investment advice.</p>
          </div>

          <div className="relative">
            <div className="rounded-lg border border-white/10 bg-slate-950/90 shadow-2xl shadow-black/40">
              <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Live signal board</p>
                  <p className="mt-1 text-sm font-semibold text-white">A snapshot of what Walnut is watching now.</p>
                </div>
                <span className="rounded border border-emerald-300/30 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">Updated</span>
              </div>
              <div className="border-b border-white/10 p-5">
                <a href={insightHref(heroInsight)} className="group block" target={heroInsight.url.startsWith("http") ? "_blank" : undefined} rel="noreferrer">
                  <LatestInsightImage src={heroInsightImage} alt="" />
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
                      ? "text-slate-500"
                      : ticker.dayChangePct >= 0
                        ? "text-emerald-300"
                        : "text-rose-300";
                  return (
                    <a key={ticker.symbol} href={`${appUrl}/ticker/${ticker.symbol}`} className="rounded-lg border border-white/10 bg-white/[0.035] p-4 transition hover:border-emerald-300/35">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="font-mono text-lg font-semibold text-emerald-200">{ticker.symbol}</p>
                          <p className="mt-1 overflow-hidden text-ellipsis whitespace-nowrap text-xs text-slate-500">{ticker.companyName}</p>
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

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="max-w-3xl">
            <SectionEyebrow>Signal stack</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">One signal is noise. A stack is intelligence.</h2>
          </div>
          <div className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            {signalCards.map((card) => (
              <article key={card.title} className="rounded-lg border border-white/10 bg-white/[0.035] p-5">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">{card.label}</p>
                <h3 className="mt-4 text-lg font-semibold text-white">{card.title}</h3>
                <p className="mt-3 text-sm leading-6 text-slate-400">{card.body}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section id="signals" className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-end">
            <div>
              <SectionEyebrow>Live data</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Access the latest insights and market data available inside the terminal.</h2>
            </div>
            <a href={`${appUrl}/insights`} className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap text-sm font-semibold text-emerald-200 hover:text-emerald-100 md:ml-4">
              <span>Open insights</span>
              <span aria-hidden="true">→</span>
            </a>
          </div>
          <div className="mt-8 grid gap-5 lg:grid-cols-[1.05fr_0.95fr]">
            <div className="rounded-lg border border-white/10 bg-slate-950/80 p-5">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Live signal board</p>
              <div className="mt-5 divide-y divide-white/10">
                {latestInsights.slice(0, 5).map((item) => (
                  <a key={`${item.title}-${item.url}`} href={insightHref(item)} target={item.url.startsWith("http") ? "_blank" : undefined} rel="noreferrer" className="block py-4 first:pt-0 last:pb-0">
                    <p className="text-sm font-semibold leading-6 text-white hover:text-emerald-100">{item.title}</p>
                    <p className="mt-1 text-xs text-slate-500">{item.site || item.source || "Market news"}</p>
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
            <SectionEyebrow>Terminal surfaces</SectionEyebrow>
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Go straight into the real research pages.</h2>
            <p className="mt-4 text-sm leading-6 text-slate-500">Portfolio simulations, insider profiles, ticker charts, and transaction tables live inside the app.</p>
          </div>

          <div className="mt-8 grid gap-5 lg:grid-cols-2">
            <a id="congress" href={`${appUrl}/member/nancy-pelosi?portfolio_lb=1095`} className="rounded-lg border border-white/10 bg-slate-950/85 p-6 shadow-2xl shadow-black/25 transition hover:border-emerald-300/35">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-emerald-300">Congress portfolio simulation</p>
              <h3 className="mt-3 text-2xl font-semibold text-white">Nancy Pelosi disclosure portfolio</h3>
              <p className="mt-3 text-sm leading-6 text-slate-400">
                Open the member profile with simulated holdings, benchmark comparison, recent disclosures, and trade outcome context.
              </p>
              <span className="mt-5 inline-flex rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100">Open portfolio -&gt;</span>
            </a>

            <a id="insiders" href={`${appUrl}/insider/tim-cook-0001214156?issuer=AAPL&chart=stock`} className="rounded-lg border border-white/10 bg-slate-950/85 p-6 shadow-2xl shadow-black/25 transition hover:border-cyan-300/35">
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
            <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Built for investors who want the evidence, not just a rating.</h2>
            <p className="mt-5 text-base leading-7 text-slate-400">
              Walnut brings political disclosures, insider activity, government contracts, ticker context, and signal confirmation into one compact research workflow.
            </p>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
            {whyWalnut.map((item) => (
              <div key={item} className="rounded-lg border border-white/10 bg-white/[0.035] p-5 text-sm leading-6 text-slate-300">
                {item}
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="border-b border-white/10 px-4 py-16 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-7xl">
          <SectionEyebrow>The Walnut signal stack</SectionEyebrow>
          <h2 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Available now, with new market-intelligence datasets coming next.</h2>
          <div className="mt-8 grid gap-4 lg:grid-cols-2">
            <div className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.04] p-6">
              <h3 className="text-lg font-semibold text-white">Available Now</h3>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                {availableNow.map((item) => (
                  <div key={item} className="rounded-lg border border-white/10 bg-slate-950/70 px-4 py-3 text-sm font-medium text-slate-200">
                    {item}
                  </div>
                ))}
              </div>
            </div>
            <div className="rounded-lg border border-cyan-300/20 bg-cyan-300/[0.035] p-6">
              <h3 className="text-lg font-semibold text-white">Coming Soon</h3>
              <div className="mt-5 grid gap-3 sm:grid-cols-2">
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
              <SectionEyebrow>Screener</SectionEyebrow>
              <h2 className="mt-3 text-3xl font-semibold text-white">An advanced stock screener built for signal confirmation.</h2>
              <p className="mt-5 text-sm leading-6 text-slate-400">
                Screen across disclosure activity, government contracts, technical indicators, fundamentals, liquidity, valuation, trend, quality, and confirmation signals from the same terminal experience.
              </p>
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Filter market data by</p>
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
          <div className="mt-8 grid gap-4 lg:grid-cols-3">
            <article className="rounded-lg border border-white/10 bg-white/[0.035] p-6">
              <h3 className="text-xl font-semibold text-white">Free</h3>
              <LandingPlanPrice display={freePrice} />
              <p className="mt-3 text-sm leading-6 text-slate-400">Start with public signal discovery, ticker pages, and core disclosure research.</p>
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
                Unlock advanced screening, saved views, monitoring, alerts, exports, and deeper signal workflows.
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
                Higher limits and power-user capacity for serious research, watchlists, and multi-signal monitoring.
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
        <div className="mx-auto flex max-w-7xl flex-col gap-6 text-sm text-slate-400 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="font-semibold text-white">Walnut</p>
            <p className="mt-1">by Walnut Intelligence Inc.</p>
            <p className="mt-3 max-w-2xl text-xs leading-5 text-slate-500">
              Walnut is a market intelligence terminal for research and informational purposes only. Walnut does not provide investment advice.
            </p>
          </div>
          <nav className="flex flex-wrap gap-4">
            <a href={appUrl} className="hover:text-white">
              App
            </a>
            <a href={pricingUrl} className="hover:text-white">
              Pricing
            </a>
            <a href={loginUrl} className="hover:text-white">
              Login / Register
            </a>
            <a href="mailto:support@walnut-intel.com" className="hover:text-white">
              Contact
            </a>
            <a href="/terms" className="hover:text-white">
              Terms
            </a>
            <a href="/privacy" className="hover:text-white">
              Privacy
            </a>
          </nav>
        </div>
      </footer>
    </main>
  );
}
