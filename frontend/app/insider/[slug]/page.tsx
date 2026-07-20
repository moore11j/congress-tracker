import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { getInsiderAlphaSummary, getInsiderSummary, getInsiderTrades } from "@/lib/api";
import { Badge } from "@/components/Badge";
import { InsiderAnalyticsClient } from "@/components/insider/InsiderAnalyticsClient";
import { InsiderProfileHeaderClient } from "@/components/insider/InsiderProfileHeaderClient";
import { ShareLinks } from "@/components/member/ShareLinks";
import {
  getInsiderDisplayName,
  insiderDisplayNameFromSlug,
  insiderSlug,
  reportingCikFromInsiderSlug,
  shouldRedirectToCanonicalInsiderSlug,
} from "@/lib/insider";
import { resolveWikipediaHeadshot } from "@/lib/wikipediaHeadshot";

export const dynamic = "force-dynamic";
export const revalidate = 0;

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "180" | "365" | "1095";
type InsiderSummaryData = Awaited<ReturnType<typeof getInsiderSummary>>;
type InsiderTradesData = Awaited<ReturnType<typeof getInsiderTrades>>;
const DEFAULT_SITE_URL = "https://congress-tracker-two.vercel.app";

const LOOKBACK_OPTIONS = [
  { label: "30D", value: "30" },
  { label: "90D", value: "90" },
  { label: "180D", value: "180" },
  { label: "1Y", value: "365" },
  { label: "3Y", value: "1095" },
] as const satisfies readonly { label: string; value: Lookback }[];

type OptionalSectionResult<T> = {
  data: T;
  unavailable: boolean;
};

type InsiderSectionContext = {
  reportingCik: string;
  lookbackDays: number;
  issuer?: string;
  section: string;
};

function errorForLog(error: unknown) {
  if (error instanceof Error) {
    return { name: error.name, message: error.message };
  }
  return { message: String(error) };
}

async function loadInsiderSection<T>(
  context: InsiderSectionContext,
  load: () => Promise<T>,
  fallback: T,
): Promise<OptionalSectionResult<T>> {
  const startedAt = Date.now();
  try {
    return { data: await load(), unavailable: false };
  } catch (error) {
    console.error("[insider-profile] section unavailable", {
      route: "/insider/[slug]",
      reporting_cik: context.reportingCik,
      lookback_days: context.lookbackDays,
      issuer: context.issuer ?? null,
      section: context.section,
      duration_ms: Date.now() - startedAt,
      error: errorForLog(error),
    });
    return { data: fallback, unavailable: true };
  }
}

function fallbackInsiderSummary(reportingCik: string, lookbackDays: number, issuer: string | undefined, slug: string): InsiderSummaryData {
  return {
    reporting_cik: reportingCik,
    insider_name: insiderDisplayNameFromSlug(slug),
    primary_company_name: null,
    primary_role: null,
    primary_symbol: issuer ?? null,
    role_contexts: [],
    lookback_days: lookbackDays,
    total_trades: 0,
    buy_count: 0,
    sell_count: 0,
    unique_tickers: 0,
    gross_buy_value: 0,
    gross_sell_value: 0,
    net_flow: 0,
    latest_filing_date: null,
    latest_transaction_date: null,
  };
}

function firstText(...values: Array<string | null | undefined>): string | null {
  for (const value of values) {
    const trimmed = typeof value === "string" ? value.trim() : "";
    if (trimmed) return trimmed;
  }
  return null;
}

function fallbackInsiderTrades(reportingCik: string, lookbackDays: number): InsiderTradesData {
  return {
    reporting_cik: reportingCik,
    lookback_days: lookbackDays,
    total: 0,
    page: 0,
    limit: 5,
    has_next: false,
    items: [],
  };
}

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function clampLookback(v: string): Lookback {
  return LOOKBACK_OPTIONS.some((option) => option.value === v) ? (v as Lookback) : "90";
}

function clampPage(v: string): number {
  const parsed = Number(v);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
}

function buildInsiderBacktestHref(reportingCik: string, lookbackDays: number) {
  const query = new URLSearchParams({
    strategy: "insider",
    scope: "insider",
    insider_cik: reportingCik,
    lookback_days: String(lookbackDays),
    hold_days: "90",
    benchmark: "SPY",
  });
  return `/backtesting?${query.toString()}`;
}

function getSiteUrl() {
  return process.env.NEXT_PUBLIC_SITE_URL ?? DEFAULT_SITE_URL;
}

function buildInsiderSharePath(
  canonicalSlug: string,
  lookback: Lookback,
  issuer: string,
  chartSymbol: string,
  recentTradesPage: number,
) {
  const query = new URLSearchParams();
  if (lookback !== "90") query.set("lookback", lookback);
  if (issuer) query.set("issuer", issuer);
  if (chartSymbol) query.set("symbol", chartSymbol);
  if (recentTradesPage > 0) query.set("recent_trades_page", String(recentTradesPage));
  const suffix = query.toString();
  return `/insider/${encodeURIComponent(canonicalSlug)}${suffix ? `?${suffix}` : ""}`;
}

function initialsForName(name: string) {
  const parts = name.split(/\s+/).filter(Boolean);
  const first = parts[0]?.[0] ?? "I";
  const last = parts.length > 1 ? parts[parts.length - 1]?.[0] : parts[0]?.[1];
  return `${first}${last ?? ""}`.toUpperCase();
}

function VerifiedBadge() {
  return (
    <span className="grid h-4 w-4 place-items-center rounded-full bg-sky-500 text-white shadow-[0_0_12px_rgba(14,165,233,0.35)]">
      <svg viewBox="0 0 12 12" aria-hidden="true" className="h-2.5 w-2.5" fill="none">
        <path d="M3 6.2 5 8l4-4.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8" />
      </svg>
    </span>
  );
}

export default async function InsiderPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const reportingCik = reportingCikFromInsiderSlug(slug);
  if (!reportingCik) notFound();
  const sp = (await searchParams) ?? {};
  const lookback = clampLookback(one(sp, "lookback"));
  const issuer = one(sp, "issuer").trim().toUpperCase();
  const chartSymbol = one(sp, "symbol").trim().toUpperCase();
  const recentTradesPage = clampPage(one(sp, "recent_trades_page"));

  const lookbackDays = Number(lookback);
  const normalizedIssuer = issuer || undefined;
  const summaryResult = await loadInsiderSection(
    { reportingCik, lookbackDays, issuer: normalizedIssuer, section: "summary" },
    () => getInsiderSummary(reportingCik, lookbackDays, normalizedIssuer, { source: "InsiderSummary" }),
    fallbackInsiderSummary(reportingCik, lookbackDays, normalizedIssuer, slug),
  );
  const summary = summaryResult.data;
  const resolvedInsiderName = getInsiderDisplayName(summary.insider_name);
  const fallbackSlugName = insiderDisplayNameFromSlug(slug);
  const insiderName = getInsiderDisplayName(resolvedInsiderName, fallbackSlugName) ?? "Unknown Insider";
  const canonicalSlug = insiderSlug(resolvedInsiderName, reportingCik) ?? reportingCik;
  const canonicalInsiderPath = buildInsiderSharePath(canonicalSlug, lookback, issuer, chartSymbol, recentTradesPage);
  const canonicalInsiderUrl = new URL(canonicalInsiderPath, getSiteUrl()).toString();

  if (shouldRedirectToCanonicalInsiderSlug(slug, canonicalSlug)) {
    const query = new URLSearchParams();
    if (lookback !== "90") query.set("lookback", lookback);
    query.set("chart", "stock");
    if (issuer) query.set("issuer", issuer);
    if (chartSymbol) query.set("symbol", chartSymbol);
    if (recentTradesPage > 0) query.set("recent_trades_page", String(recentTradesPage));
    const suffix = query.toString();
    redirect(`/insider/${encodeURIComponent(canonicalSlug)}${suffix ? `?${suffix}` : ""}`);
  }

  const stockSymbol = chartSymbol || issuer || summary.primary_symbol || undefined;
  const needsHeaderFallback = !summary.primary_company_name || !summary.primary_role;
  const headerTradesResult = needsHeaderFallback
    ? await loadInsiderSection(
        { reportingCik, lookbackDays, issuer: normalizedIssuer, section: "header-trades" },
        () => getInsiderTrades(reportingCik, lookbackDays, 5, normalizedIssuer, { source: "InsiderHeaderTrades" }),
        fallbackInsiderTrades(reportingCik, lookbackDays),
      )
    : null;
  const headerTrade = headerTradesResult?.data.items.find(
    (item) => firstText(item.company_name, item.companyName, item.security_name, item.securityName) || firstText(item.role),
  );
  const roleText = firstText(summary.primary_role, headerTrade?.role) ?? "Role unavailable";
  const companyText =
    firstText(summary.primary_company_name, headerTrade?.company_name, headerTrade?.companyName, headerTrade?.security_name, headerTrade?.securityName) ??
    "Company unavailable";
  const [initialAlphaSummaryResult, initialTradesResult, headshotResult] = await Promise.allSettled([
    getInsiderAlphaSummary(reportingCik, {
      lookback_days: lookbackDays,
      issuer: normalizedIssuer,
      source: "InsiderProfileInitialAlpha",
    }),
    getInsiderTrades(reportingCik, lookbackDays, 20, normalizedIssuer, {
      page: recentTradesPage,
      source: "InsiderProfileInitialTrades",
    }),
    resolveWikipediaHeadshot(insiderName, {
      kind: "insider",
      company: companyText,
      role: roleText,
      symbol: stockSymbol,
    }),
  ]);
  const initialAlphaSummary =
    initialAlphaSummaryResult.status === "fulfilled" ? initialAlphaSummaryResult.value : undefined;
  const initialTrades =
    initialTradesResult.status === "fulfilled" ? initialTradesResult.value : undefined;
  const headshot = headshotResult.status === "fulfilled" ? headshotResult.value : null;
  const initialBuyCount = initialTrades?.items.filter((trade) => {
    const value = (trade.trade_type ?? trade.tradeType ?? "").toLowerCase();
    return value === "p" || value.includes("buy") || value.includes("purchase") || value.includes("acquire");
  }).length ?? summary.buy_count;
  const initialSellCount = initialTrades?.items.filter((trade) => {
    const value = (trade.trade_type ?? trade.tradeType ?? "").toLowerCase();
    return value === "s" || value.includes("sale") || value.includes("sell") || value.includes("dispose");
  }).length ?? summary.sell_count;
  const ownershipContext =
    initialSellCount > initialBuyCount
      ? "Net seller"
      : initialBuyCount > initialSellCount
        ? "Net buyer"
        : summary.total_trades > 0
          ? "Insider activity"
          : "Ownership context";
  const actionClassName =
    "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-white/10 bg-slate-950/20 px-4 text-xs font-semibold text-slate-100 transition hover:border-white/25 hover:bg-white/[0.04] sm:text-sm";
  const primaryActionClassName =
    "inline-flex h-9 min-w-0 items-center justify-center rounded-lg border border-emerald-400/35 bg-emerald-500/10 px-4 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-500/18 sm:text-sm";

  return (
    <div className="space-y-3">
      <section className="relative overflow-hidden rounded-lg border border-white/10 bg-[linear-gradient(135deg,rgba(9,20,35,0.98),rgba(4,10,20,0.98))] px-4 pt-3 shadow-[0_18px_48px_rgba(0,0,0,0.32)] sm:px-5">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <Link href="/?mode=insider" className="text-[10px] font-semibold uppercase tracking-[0.22em] text-emerald-300/80 hover:text-emerald-200">
            Insider profile
          </Link>
          <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:justify-end lg:absolute lg:right-5 lg:top-3">
            <Link href="/?mode=insider" className={actionClassName}>
              Follow Insider
            </Link>
            <ShareLinks canonicalUrl={canonicalInsiderUrl} showCopyButton={false} buttonClassName={actionClassName} />
            <Link href={buildInsiderBacktestHref(reportingCik, lookbackDays)} prefetch={false} className={primaryActionClassName}>
              Create Research
            </Link>
          </div>
        </div>
        <div className="mt-3 flex min-w-0 gap-4 pb-2 lg:pr-[28rem]">
            {headshot ? (
              <img
                src={headshot.src}
                alt={`${insiderName} headshot from Wikipedia`}
                className="h-20 w-20 shrink-0 rounded-full border border-white/15 bg-slate-950/70 object-cover shadow-inner"
                referrerPolicy="no-referrer"
              />
            ) : (
              <div className="grid h-20 w-20 shrink-0 place-items-center rounded-full border border-white/15 bg-slate-950/70 text-2xl font-semibold text-emerald-100 shadow-inner">
                {initialsForName(insiderName)}
              </div>
            )}
            <div className="min-w-0 pt-0.5">
              <div className="mt-1.5 flex flex-wrap items-center gap-2">
                <h1 className="truncate text-2xl font-semibold leading-tight text-white sm:text-3xl">{insiderName}</h1>
                <VerifiedBadge />
              </div>
              <InsiderProfileHeaderClient
                reportingCik={reportingCik}
                lookback={lookback}
                lookbackDays={lookbackDays}
                issuer={normalizedIssuer}
                stockSymbol={stockSymbol}
                canonicalSlug={canonicalSlug}
                recentTradesPage={recentTradesPage}
                initialSummary={summary}
                initialRoleText={roleText}
                initialCompanyText={companyText}
                initialOwnershipContext={ownershipContext}
              />
            </div>
        </div>
        {summaryResult.unavailable ? (
          <p className="mt-4 rounded-lg border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            Insider profile details are loading from the latest available disclosures.
          </p>
        ) : null}
        <nav className="flex gap-7 overflow-x-auto border-t border-white/10 pt-2 text-sm font-medium text-slate-400">
          {["Overview", "Transactions", "Ownership", "Performance", "Filings", "About"].map((item) => (
            <a
              key={item}
              href={item === "Overview" ? "#overview" : item === "Filings" || item === "Transactions" ? "#recent-filings" : "#insider-performance"}
              className={`shrink-0 border-b-2 pb-2 ${item === "Overview" ? "border-amber-300 text-amber-200" : "border-transparent hover:text-white"}`}
            >
              {item}
            </a>
          ))}
        </nav>
      </section>

      <div id="overview">
        <InsiderAnalyticsClient
          reportingCik={reportingCik}
          insiderName={insiderName}
          lookback={lookback}
          lookbackDays={lookbackDays}
          issuer={normalizedIssuer}
          stockSymbol={stockSymbol}
          recentTradesPage={recentTradesPage}
          summary={summary}
          initialAlphaSummary={initialAlphaSummary}
          initialTrades={initialTrades}
        />
      </div>
    </div>
  );
}
