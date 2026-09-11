import Link from "next/link";
import { headers } from "next/headers";
import { notFound, redirect } from "next/navigation";
import type { Metadata } from "next";
import { Suspense } from "react";
import { getInsiderAlphaSummary, getInsiderTrades } from "@/lib/api";
import { Badge } from "@/components/Badge";
import { InsiderAnalyticsClient } from "@/components/insider/InsiderAnalyticsClient";
import { InsiderProfileHeaderClient } from "@/components/insider/InsiderProfileHeaderClient";
import { ShareLinks } from "@/components/member/ShareLinks";
import { AddWatchlistTarget } from "@/components/watchlists/AddWatchlistTarget";
import {
  reportingCikFromInsiderSlug,
} from "@/lib/insider";
import { resolveWikipediaHeadshot } from "@/lib/wikipediaHeadshot";
import { optionalPageAuthState, requestMayHavePageAuthState } from "@/lib/serverAuth";
import { WALNUT_APP_URL } from "@/lib/marketingMetadata";
import { insiderCanonicalSlug, insiderProfileMetadata, loadPublicInsiderProfile, resolvedInsiderName as publicInsiderName } from "@/lib/insiderSeo";

export const dynamic = "force-dynamic";
export const revalidate = 0;

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type Lookback = "30" | "90" | "180" | "365" | "1095";
type InsiderTradesData = Awaited<ReturnType<typeof getInsiderTrades>>;

const LOOKBACK_OPTIONS = [
  { label: "30D", value: "30" },
  { label: "90D", value: "90" },
  { label: "180D", value: "180" },
  { label: "1Y", value: "365" },
  { label: "3Y", value: "1095" },
] as const satisfies readonly { label: string; value: Lookback }[];
const INSIDER_NAV_ITEMS = [
  { label: "Overview", href: "#overview" },
  { label: "Transactions", href: "#recent-filings" },
  { label: "Ownership", href: "#insider-ownership" },
  { label: "Performance", href: "#insider-performance" },
  { label: "Filings", href: "#recent-filings" },
] as const;

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
  return process.env.NEXT_PUBLIC_SITE_URL ?? WALNUT_APP_URL;
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

function InsiderHeadshotFallback({ insiderName }: { insiderName: string }) {
  return (
    <div className="grid h-20 w-20 shrink-0 place-items-center rounded-full border border-white/15 bg-slate-950/70 text-2xl font-semibold text-emerald-100 shadow-inner">
      {initialsForName(insiderName)}
    </div>
  );
}

async function StreamedInsiderHeadshot({
  insiderName,
  headshotPromise,
}: {
  insiderName: string;
  headshotPromise: ReturnType<typeof resolveWikipediaHeadshot>;
}) {
  const headshot = await headshotPromise;
  if (!headshot) return <InsiderHeadshotFallback insiderName={insiderName} />;
  return (
    <img
      src={headshot.src}
      alt={`${insiderName} headshot from Wikipedia`}
      className="h-20 w-20 shrink-0 rounded-full border border-white/15 bg-slate-950/70 object-cover shadow-inner"
      referrerPolicy="no-referrer"
    />
  );
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

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const reportingCik = reportingCikFromInsiderSlug(slug);
  if (!reportingCik) notFound();

  const profile = await loadPublicInsiderProfile(
    reportingCik, Number(clampLookback(one(sp, "lookback"))),
    one(sp, "issuer").trim().toUpperCase() || undefined,
    clampPage(one(sp, "recent_trades_page")), true,
  );
  if (profile.status === "missing") notFound();
  return insiderProfileMetadata(slug, sp, profile);
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
  const requestHeaders = await headers();
  const authState = requestMayHavePageAuthState(requestHeaders)
    ? await optionalPageAuthState()
    : { token: null, hasAuthHint: false, entitlementHint: null };
  const publicStalePageCache = !authState.token && !authState.hasAuthHint;

  const lookbackDays = Number(lookback);
  const normalizedIssuer = issuer || undefined;
  const publicProfile = await loadPublicInsiderProfile(reportingCik, lookbackDays, normalizedIssuer, recentTradesPage, publicStalePageCache);
  if (publicProfile.status === "missing") notFound();
  const summary = publicProfile.summary;
  const resolvedInsiderName = publicInsiderName(summary);
  if (publicProfile.status !== "ready" || !summary || !resolvedInsiderName) {
    return (
      <div className="space-y-4 py-6">
        <h1 className="text-2xl font-semibold text-white">Insider profile unavailable</h1>
        <p className="text-sm text-slate-300">Public identity and disclosure details could not be resolved for CIK {reportingCik}. Try again later or search for another insider.</p>
        <Link href="/insiders" className="inline-flex text-sm font-semibold text-emerald-300">Explore insiders</Link>
      </div>
    );
  }
  const insiderName = resolvedInsiderName;
  const canonicalSlug = insiderCanonicalSlug(slug, publicProfile);
  const shareInsiderPath = buildInsiderSharePath(canonicalSlug, lookback, issuer, chartSymbol, recentTradesPage);
  const shareInsiderUrl = new URL(shareInsiderPath, getSiteUrl()).toString();

  if (slug !== canonicalSlug) {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(sp)) {
      for (const entry of Array.isArray(value) ? value : value === undefined ? [] : [value]) query.append(key, entry);
    }
    const suffix = query.toString();
    redirect(`/insider/${encodeURIComponent(canonicalSlug)}${suffix ? `?${suffix}` : ""}`);
  }

  const stockSymbol = chartSymbol || issuer || summary.primary_symbol || undefined;
  const needsHeaderFallback = !summary.primary_company_name || !summary.primary_role;
  const headerTradesResult = needsHeaderFallback
    ? await loadInsiderSection(
        { reportingCik, lookbackDays, issuer: normalizedIssuer, section: "header-trades" },
        () => getInsiderTrades(reportingCik, lookbackDays, 5, normalizedIssuer, { source: "InsiderHeaderTrades", stalePageCache: publicStalePageCache }),
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
  const headshotPromise = resolveWikipediaHeadshot(insiderName, {
    kind: "insider",
    company: companyText,
    role: roleText,
    symbol: stockSymbol,
  });
  const initialAlphaSummary = await getInsiderAlphaSummary(reportingCik, {
    lookback_days: lookbackDays,
    issuer: normalizedIssuer,
    source: "InsiderProfileInitialAlpha",
    stalePageCache: publicStalePageCache,
  }).catch(() => undefined);
  const initialTrades = publicProfile.trades ?? undefined;
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
          <nav aria-label="Breadcrumb" className="flex min-w-0 flex-wrap items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">
            <Link href="/" className="text-slate-400 hover:text-slate-200">Home</Link>
            <span>/</span>
            <Link href="/insiders" className="text-emerald-300/80 hover:text-emerald-200">Insiders</Link>
            <span>/</span>
            <span className="truncate text-slate-300">{insiderName}</span>
          </nav>
          <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:justify-end lg:absolute lg:right-5 lg:top-3">
            <AddWatchlistTarget targetType="insider" targetValue={reportingCik} targetLabel={insiderName} buttonLabel="Follow Insider" className={actionClassName} />
            <ShareLinks canonicalUrl={shareInsiderUrl} showCopyButton={false} buttonClassName={actionClassName} />
            <Link href={buildInsiderBacktestHref(reportingCik, lookbackDays)} prefetch={false} className={primaryActionClassName}>
              Backtest this Insider
            </Link>
          </div>
        </div>
        <div className="mt-3 flex min-w-0 gap-4 pb-2 lg:pr-[28rem]">
            <Suspense fallback={<InsiderHeadshotFallback insiderName={insiderName} />}>
              <StreamedInsiderHeadshot insiderName={insiderName} headshotPromise={headshotPromise} />
            </Suspense>
            <div className="min-w-0 pt-0.5">
              <div className="mt-1.5 flex flex-wrap items-center gap-2">
                <h1 className="truncate text-2xl font-semibold leading-tight text-white sm:text-3xl">{insiderName} Insider Activity</h1>
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
        <nav className="flex gap-7 overflow-x-auto border-t border-white/10 pt-2 text-sm font-medium text-slate-400">
          {INSIDER_NAV_ITEMS.map((item) => (
            <a
              key={item.label}
              href={item.href}
              className={`shrink-0 border-b-2 pb-2 ${item.label === "Overview" ? "border-amber-300 text-amber-200" : "border-transparent hover:text-white"}`}
            >
              {item.label}
            </a>
          ))}
        </nav>
      </section>

      <div id="overview">
        <InsiderAnalyticsClient
          reportingCik={reportingCik}
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
