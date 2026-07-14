import Link from "next/link";
import { notFound, redirect } from "next/navigation";
import { getInsiderSummary, getInsiderTrades } from "@/lib/api";
import { Badge } from "@/components/Badge";
import { InsiderAnalyticsClient } from "@/components/insider/InsiderAnalyticsClient";
import { ShareLinks } from "@/components/member/ShareLinks";
import { cardClassName, ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import {
  getInsiderDisplayName,
  insiderDisplayNameFromSlug,
  insiderSlug,
  reportingCikFromInsiderSlug,
  shouldRedirectToCanonicalInsiderSlug,
} from "@/lib/insider";

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

  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insider profile</p>
            <h1 className="mt-1 text-3xl font-semibold text-white">
              {issuer && companyText !== "Company unavailable" ? `${insiderName} / ${companyText}` : insiderName}
            </h1>
            <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
              <span className="rounded-full border border-white/10 bg-slate-900/60 px-2.5 py-1">{companyText}</span>
              <Badge tone="neutral">{roleText}</Badge>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Link href={buildInsiderBacktestHref(reportingCik, lookbackDays)} prefetch={false} className={subtlePrimaryButtonClassName}>
              Backtest following this insider
            </Link>
            <ShareLinks canonicalUrl={canonicalInsiderUrl} />
            <Link href="/" className={ghostButtonClassName}>Back to feed</Link>
          </div>
        </div>
        {summaryResult.unavailable ? (
          <p className="mt-4 rounded-xl border border-amber-300/25 bg-amber-400/10 px-3 py-2 text-sm text-amber-100">
            Insider profile details are loading from the latest available disclosures.
          </p>
        ) : null}
      </section>

      <InsiderAnalyticsClient
        reportingCik={reportingCik}
        insiderName={insiderName}
        lookback={lookback}
        lookbackDays={lookbackDays}
        issuer={normalizedIssuer}
        stockSymbol={stockSymbol}
        recentTradesPage={recentTradesPage}
      />
    </div>
  );
}
