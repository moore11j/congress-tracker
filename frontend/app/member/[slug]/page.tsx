import Link from "next/link";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { Suspense } from "react";
import { Badge } from "@/components/Badge";
import { ShareLinks } from "@/components/member/ShareLinks";
import { FeedCard } from "@/components/feed/FeedCard";
import { TickerPill } from "@/components/ui/TickerPill";
import { PerformanceChart } from "@/components/member/PerformanceChart";
import {
  getMemberAlphaSummary,
  getMemberProfile,
  getMemberProfileBySlug,
  getMemberPortfolioPerformance,
  getMemberTrades,
} from "@/lib/api";
import {
  cardClassName,
  compactInteractiveSurfaceClassName,
  ghostButtonClassName,
  pillClassName,
  subtlePrimaryButtonClassName,
  tickerLinkClassName,
} from "@/lib/styles";
import { chamberBadge, formatDateShort, partyBadge } from "@/lib/format";
import { nameToSlug } from "@/lib/memberSlug";
import type { FeedItem } from "@/lib/types";
import { tickerHref } from "@/lib/ticker";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { resolveSmartSignalValue } from "@/lib/smartSignal";
import {
  DEFAULT_PORTFOLIO_LOOKBACK_DAYS,
  PORTFOLIO_LOOKBACK_OPTIONS,
  PORTFOLIO_MODE,
  isPortfolioLookbackDays,
  normalizeMemberPortfolioChartData,
  normalizeMemberPortfolioEventMarkers,
} from "@/lib/portfolioPerformance.mjs";

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type SignalOverlayItem = {
  event_id: number;
  smart_score: number;
  smart_band: string;
};

type SignalOverlay = { score: number; band: string };
type SignalOverlayMap = Record<string, SignalOverlay>;

const DEFAULT_SITE_URL = "https://congress-tracker-two.vercel.app";

function getSiteUrl() {
  return process.env.NEXT_PUBLIC_SITE_URL ?? DEFAULT_SITE_URL;
}

function getLookbackParam(sp: Record<string, string | string[] | undefined>) {
  const lb = getParam(sp, "lb");
  if (["90", "180", "365"].includes(lb)) return lb;
  return "";
}

function getChartMetricParam(sp: Record<string, string | string[] | undefined>) {
  const metric = getParam(sp, "am");
  if (metric === "alpha" || metric === "return") return metric;
  return "return";
}

function getPortfolioLookbackParam(sp: Record<string, string | string[] | undefined>) {
  const raw = Number(getParam(sp, "portfolio_lb"));
  return isPortfolioLookbackDays(raw) ? raw : DEFAULT_PORTFOLIO_LOOKBACK_DAYS;
}

function buildMemberPath(
  prettySlug: string,
  lbParam: string,
  chartMetric?: "return" | "alpha",
  portfolioLookbackDays?: number,
) {
  const path = `/member/${prettySlug}`;
  const query = new URLSearchParams();
  if (lbParam) query.set("lb", lbParam);
  if (chartMetric && chartMetric !== "return") query.set("am", chartMetric);
  if (portfolioLookbackDays && portfolioLookbackDays !== DEFAULT_PORTFOLIO_LOOKBACK_DAYS) {
    query.set("portfolio_lb", String(portfolioLookbackDays));
  }
  const qs = query.toString();
  return qs ? `${path}?${qs}` : path;
}

function buildMemberBacktestHref(memberId: string, lookbackDays: number) {
  const query = new URLSearchParams({
    strategy: "congress",
    scope: "member",
    member_id: memberId,
    lookback_days: String(lookbackDays),
    hold_days: "90",
    benchmark: "^GSPC",
  });
  return `/backtesting?${query.toString()}`;
}

export async function generateMetadata({
  params,
  searchParams,
}: Props): Promise<Metadata> {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbParam = getLookbackParam(sp);
  const siteUrl = getSiteUrl();
  const fallbackName = slug.replace(/-/g, " ");
  const prettySlug = slug;
  const chartMetric = getChartMetricParam(sp);
  const portfolioLookbackDays = getPortfolioLookbackParam(sp);
  const canonicalPath = buildMemberPath(prettySlug, lbParam, chartMetric, portfolioLookbackDays);
  const canonicalUrl = new URL(canonicalPath, siteUrl).toString();
  const title = `${fallbackName || "Member"} — Member Profile`;

  return {
    metadataBase: new URL(siteUrl),
    title,
    alternates: {
      canonical: canonicalPath,
    },
    openGraph: {
      title,
      type: "website",
      url: canonicalUrl,
    },
    twitter: {
      card: "summary",
      title,
    },
  };
}

function getParam(
  sp: Record<string, string | string[] | undefined>,
  key: string,
) {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function toQueryString(sp: Record<string, string | string[] | undefined>) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(sp)) {
    if (typeof value === "string") {
      query.set(key, value);
      continue;
    }
    if (Array.isArray(value)) {
      value.forEach((entry) => query.append(key, entry));
    }
  }
  return query.toString();
}

function pct(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}%`;
}

function pct0(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function decimal(n: number | null | undefined, digits = 2) {
  if (n == null || !Number.isFinite(n)) return "â€”";
  return n.toFixed(digits);
}

function numberOrDash(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n)}`;
}

function asDate(v: string | null | undefined) {
  if (!v) return "—";
  const d = new Date(v);
  if (!Number.isFinite(d.getTime())) return v;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function parseNum(value: unknown): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const n = Number(value.replace(/[$,% ,]/g, "").trim());
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function tone(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "text-white/85";
  if (n > 0) return "text-emerald-300";
  if (n < 0) return "text-rose-300";
  return "text-white/70";
}

function compactUSD(n: number) {
  const abs = Math.abs(n);
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return `${Math.round(n)}`;
}

function renderTradePanelRows(
  panelTitle: string,
  rows: NonNullable<Awaited<ReturnType<typeof getMemberAlphaSummary>>["best_trades"]>,
  alphaSummaryError: boolean,
) {
  if (rows.length === 0) {
    return (
      <p className="mt-3 text-sm text-slate-400">
        {alphaSummaryError
          ? "Unable to load trade-level alpha rows right now."
          : "No scored trades for this lookback window."}
      </p>
    );
  }

  return (
    <div className="mt-3 space-y-2">
      {rows.map((trade) => (
        <div
          key={`${panelTitle}-${trade.event_id}-${trade.symbol}`}
          className="grid grid-cols-[1fr_auto_auto] items-center gap-3 rounded-xl border border-white/10 px-3 py-2"
        >
          <div className="min-w-0">
            {tickerHref(trade.symbol) ? (
              <Link href={tickerHref(trade.symbol)!} prefetch={false} className={`${tickerLinkClassName} truncate`}>
                {trade.symbol}
              </Link>
            ) : (
              <p className="truncate text-sm font-medium text-white">{trade.symbol}</p>
            )}
            <p className="truncate text-xs text-white/45">
              {asDate(trade.asof_date)}{trade.trade_type ? ` · ${trade.trade_type}` : ""}
            </p>
          </div>
          <div className="text-right">
            <p className={`text-sm font-semibold tabular-nums ${tone(trade.return_pct)}`}>{pct(trade.return_pct)}</p>
            <p className="text-[11px] text-white/40">Return</p>
          </div>
          <div className="text-right">
            <p className={`text-sm font-semibold tabular-nums ${tone(trade.alpha_pct)}`}>{pct(trade.alpha_pct)}</p>
            <p className="text-[11px] text-white/40">Alpha</p>
          </div>
        </div>
      ))}
    </div>
  );
}

function DeferredMemberPortfolioSectionSkeleton() {
  return (
    <section className={`${cardClassName} p-4 sm:p-6`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <SkeletonBlock className="h-5 w-56" />
          <SkeletonBlock className="mt-2 h-3 w-80 max-w-full" />
        </div>
        <SkeletonBlock className="h-8 w-20 rounded-full" />
      </div>
      <SkeletonBlock className="mt-4 h-64 w-full" />
    </section>
  );
}

function DeferredMemberAnalyticsStatsSkeleton() {
  return (
    <>
      <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {Array.from({ length: 5 }).map((_, idx) => (
          <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
            <SkeletonBlock className="h-3 w-24" />
            <SkeletonBlock className="mt-3 h-7 w-20" />
          </div>
        ))}
      </div>
      <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-white/45">
        <span>Loading analytics metrics…</span>
      </div>
    </>
  );
}

async function DeferredMemberAnalyticsStats({
  alphaSummaryPromise,
}: {
  alphaSummaryPromise: Promise<Awaited<ReturnType<typeof getMemberAlphaSummary>> | null>;
}) {
  const alphaSummary = await alphaSummaryPromise;
  const alphaSummaryError = alphaSummary == null;
  const analyticsStats = [
    {
      label: "Trades Analyzed",
      value: String(alphaSummary?.trades_analyzed ?? 0),
      valueClass: "text-white",
    },
    {
      label: "Avg Trade Return",
      value: pct(alphaSummary?.avg_return_pct),
      valueClass: tone(alphaSummary?.avg_return_pct),
    },
    {
      label: "Avg Trade Alpha",
      value: pct(alphaSummary?.avg_alpha_pct),
      valueClass: tone(alphaSummary?.avg_alpha_pct),
    },
    {
      label: "Win Rate",
      value: pct0(alphaSummary?.win_rate),
      valueClass: "text-white/90",
    },
    {
      label: "Avg Holding Days",
      value: numberOrDash(alphaSummary?.avg_holding_days),
      valueClass: "text-white/90",
    },
  ];

  return (
    <>
      <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {analyticsStats.map((stat) => (
          <div
            key={stat.label}
            className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3"
          >
            <p className="text-[11px] uppercase tracking-[0.18em] text-white/45">{stat.label}</p>
            <p className={`mt-2 text-xl font-semibold tabular-nums ${stat.valueClass}`}>
              {stat.value}
            </p>
          </div>
        ))}
      </div>

      <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-white/45">
        <span>
          Scored trades: {alphaSummary?.trades_analyzed ?? 0}
        </span>
        {alphaSummaryError && <span>Alpha summary unavailable.</span>}
      </div>
    </>
  );
}

async function DeferredMemberPortfolioSection({
  portfolioPromise,
  selectedLookbackDays,
  lookbackLinks,
}: {
  portfolioPromise: Promise<Awaited<ReturnType<typeof getMemberPortfolioPerformance>> | null>;
  selectedLookbackDays: number;
  lookbackLinks: Array<{ label: string; value: number; href: string }>;
}) {
  const portfolio = await portfolioPromise;
  const summary = portfolio?.summary ?? null;
  const { memberSeries: portfolioSeries, benchmarkSeries } = normalizeMemberPortfolioChartData(portfolio);
  const portfolioEvents = normalizeMemberPortfolioEventMarkers(portfolio);
  const hasPersistedRun =
    portfolio?.persisted_only === true &&
    portfolio.status === "ok" &&
    summary != null;
  const hasChartData = portfolioSeries.length >= 2 && benchmarkSeries.length >= 2;
  const positionsCount = summary?.positions_count ?? 0;
  const curveQualityStatus = portfolio?.curve_quality_status ?? "good";
  const showNoActiveHoldings = hasPersistedRun && (portfolio?.no_active_holdings === true || positionsCount === 0);
  const showLimitedPriceHistory =
    hasPersistedRun && positionsCount > 0 && (curveQualityStatus === "warning" || curveQualityStatus === "poor");
  const showEffectiveWindowNote =
    hasPersistedRun &&
    portfolio?.no_active_holdings !== true &&
    portfolio?.requested_start_date != null &&
    portfolio?.effective_start_date != null &&
    portfolio.effective_start_date > portfolio.requested_start_date;
  const emptyMessage =
    portfolio == null
      ? "Portfolio simulation could not be loaded."
      : "Portfolio simulation is not available for this lookback yet.";
  const skipDiagnostics = summary?.skip_diagnostics ?? portfolio?.skip_diagnostics ?? {};
  const openingPositionsCount =
    portfolio?.opening_positions_count ??
    portfolio?.warmup_diagnostics?.opening_positions_count ??
    null;
  const estimatedOpeningPositionsCount =
    portfolio?.estimated_opening_positions_count ??
    portfolio?.warmup_diagnostics?.estimated_opening_positions_count ??
    0;
  const skipBreakdown = [
    { label: "Non-simulatable assets", value: skipDiagnostics.non_equity_asset ?? 0 },
    { label: "Missing prices", value: skipDiagnostics.missing_execution_price ?? 0 },
    { label: "Unresolved symbols", value: skipDiagnostics.unresolved_symbol ?? 0 },
    { label: "Unmatched sales", value: skipDiagnostics.sale_without_position ?? 0 },
  ].filter((item) => item.value > 0);

  const metrics = summary ? [
    { label: "Total Return", value: pct(summary.total_return_pct), tone: tone(summary.total_return_pct) },
    { label: "CAGR", value: pct(summary.cagr_pct), tone: tone(summary.cagr_pct) },
    { label: "Alpha", value: pct(summary.alpha_pct), tone: tone(summary.alpha_pct) },
    { label: "S&P Return", value: pct(summary.benchmark_return_pct), tone: tone(summary.benchmark_return_pct) },
    { label: "Max Drawdown", value: pct(summary.max_drawdown_pct), tone: tone(summary.max_drawdown_pct == null ? null : -Math.abs(summary.max_drawdown_pct)) },
    { label: "Sharpe", value: decimal(summary.sharpe_ratio, 2), tone: "text-white/90" },
    { label: "Win Rate", value: pct(summary.win_rate_pct), tone: "text-white/90" },
    { label: "Positions", value: numberOrDash(summary.positions_count), tone: "text-white/90" },
    { label: "Opening Holdings", value: numberOrDash(openingPositionsCount), tone: "text-white/90" },
    { label: "Estimated Opening Holdings", value: numberOrDash(estimatedOpeningPositionsCount), tone: "text-white/90" },
    { label: "Excluded", value: numberOrDash(summary.skipped_events_count), tone: "text-white/90" },
  ] : [];

  return (
    <section className={`${cardClassName} p-4 sm:p-6`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Portfolio Performance</h2>
          <p className="mt-1 text-xs uppercase tracking-[0.2em] text-emerald-300">Disclosure-lag realistic portfolio</p>
          <p className="mt-2 max-w-3xl text-sm text-white/45">
            Trades are simulated after public disclosure, not transaction date. Open positions are carried forward through the selected window.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 sm:justify-end">
          {lookbackLinks.map((option) => (
            <Link
              key={option.value}
              href={option.href}
              prefetch={false}
              className={`relative rounded-full border px-3 py-1.5 text-xs transition-colors ${
                option.value === selectedLookbackDays
                  ? "border-emerald-300/50 bg-emerald-300/10 font-medium text-emerald-100"
                  : "border-white/10 bg-slate-950/30 text-white/60 hover:border-emerald-300/30 hover:text-white/85"
              }`}
            >
              {option.value === selectedLookbackDays && (
                <span className="absolute left-2 right-2 -top-[2px] h-[2px] rounded-full bg-emerald-300/75" />
              )}
              {option.label}
            </Link>
          ))}
        </div>
      </div>

      {!hasPersistedRun ? (
        <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
          {emptyMessage}
        </p>
      ) : (
        <>
          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            {metrics.map((metric) => (
              <div key={metric.label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                <p className="text-[11px] uppercase tracking-[0.18em] text-white/45">{metric.label}</p>
                <p className={`mt-2 text-xl font-semibold tabular-nums ${metric.tone}`}>{metric.value}</p>
              </div>
            ))}
          </div>

          {skipBreakdown.length > 0 ? (
            <div className="mt-3 rounded-2xl border border-white/10 bg-white/[0.025] px-4 py-3">
              <div className="flex flex-wrap gap-2 text-xs text-slate-300">
                {skipBreakdown.map((item) => (
                  <span key={item.label} className="rounded-full border border-white/10 bg-slate-950/40 px-2.5 py-1 tabular-nums">
                    {item.label}: {numberOrDash(item.value)}
                  </span>
                ))}
              </div>
              {(skipDiagnostics.non_equity_asset ?? 0) > 0 ? (
                <p className="mt-2 text-xs text-slate-500">
                  Options, bonds, and other non-equity assets are excluded from the equity portfolio simulation.
                </p>
              ) : null}
            </div>
          ) : null}

          {estimatedOpeningPositionsCount > 0 ? (
            <p className="mt-3 rounded-2xl border border-sky-300/15 bg-sky-300/[0.05] px-4 py-3 text-sm text-sky-100/80">
              Sales with no prior purchase in available disclosures are matched to estimated opening holdings at the start of the selected window.
            </p>
          ) : null}

          {showNoActiveHoldings ? (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              No simulated holdings were active in this window.
            </p>
          ) : showEffectiveWindowNote ? (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              Simulation starts on {formatDateShort(portfolio.effective_start_date ?? null)}, when this member first had active holdings in the selected window.
            </p>
          ) : showLimitedPriceHistory ? (
            <p className="mt-4 rounded-2xl border border-amber-300/15 bg-amber-300/[0.06] px-4 py-3 text-sm text-amber-100/80">
              Some holdings have limited price history, so parts of the simulated curve may use stale or incomplete pricing.
            </p>
          ) : null}

          {hasChartData ? (
            <PerformanceChart
              memberSeries={portfolioSeries}
              benchmarkSeries={benchmarkSeries}
              metric="return"
              benchmarkLabel="S&P 500"
              subjectLabel="Portfolio"
              chartLabel="Portfolio Return"
              events={portfolioEvents}
            />
          ) : (
            <p className="mt-4 rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3 text-sm text-slate-400">
              Portfolio simulation is not available for this lookback yet.
            </p>
          )}
        </>
      )}
    </section>
  );
}


function resolveSmartSignal(
  trade: Awaited<ReturnType<typeof getMemberTrades>>["items"][number],
): { score: number | null; band: string | null } {
  return resolveSmartSignalValue(trade as Record<string, unknown>);
}

function tradeDirection(tradeType: string): "buy" | "sell" | null {
  const normalized = tradeType.trim().toLowerCase();
  if (!normalized) return null;
  if (normalized === "s" || normalized === "s-sale") return "sell";
  if (normalized === "p" || normalized === "p-purchase") return "buy";
  if (["sale", "sell", "disposition", "dispose"].some((token) => normalized.includes(token))) {
    return "sell";
  }
  if (["buy", "purchase", "acquire", "acquisition"].some((token) => normalized.includes(token))) {
    return "buy";
  }
  return null;
}

export default async function MemberPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getLookbackParam(sp);
  const chartMetric = getChartMetricParam(sp);
  const portfolioLookbackDays = getPortfolioLookbackParam(sp);
  const lb = lbRaw === "90" || lbRaw === "180" ? Number(lbRaw) : 365;

  const upperSlug = slug.toUpperCase();
  if (upperSlug.startsWith("FMP_")) {
    const legacyData = await getMemberProfile(slug);
    const cleanSlug = nameToSlug(legacyData.member.name);
    const query = toQueryString(sp);
    redirect(`/member/${cleanSlug}${query ? `?${query}` : ""}`);
  }

  const data = await getMemberProfileBySlug(slug, { include_trades: false });
  const canonicalSlug = nameToSlug(data.member.name);
  const canonicalPath = buildMemberPath(canonicalSlug, lbRaw, chartMetric, portfolioLookbackDays);
  const canonicalUrl = new URL(canonicalPath, getSiteUrl()).toString();
  const canonicalMemberId = data.member.bioguide_id;
  const alphaSummaryPromise = getMemberAlphaSummary(canonicalMemberId, { lookback_days: lb }).catch(() => null);
  const portfolioPromise = getMemberPortfolioPerformance(canonicalMemberId, {
    lookback_days: portfolioLookbackDays,
    mode: PORTFOLIO_MODE,
  }).catch(() => null);
  const memberTrades = await getMemberTrades(canonicalMemberId, { lookback_days: lb, limit: 100 });
  const portfolioLookbackLinks = PORTFOLIO_LOOKBACK_OPTIONS.map((option) => ({
    ...option,
    href: buildMemberPath(canonicalSlug, lbRaw, chartMetric, option.value),
  }));
  const recentFeedItems = memberTrades.items.map((trade) => {
    const signal = resolveSmartSignal(trade);
    const feedId = trade.event_id ?? trade.id;
    const assetClass = (trade.asset_class ?? "Security").toLowerCase();
    const kind =
      assetClass === "treasury"
        ? "congress_treasury_trade"
        : assetClass === "crypto"
          ? "congress_crypto_trade"
          : "congress_trade";
    return {
      id: feedId,
      member: {
        bioguide_id: data.member.bioguide_id,
        member_id: data.member.member_id,
        name: data.member.name,
        chamber: data.member.chamber,
        party: data.member.party,
        state: data.member.state,
      },
      security: {
        symbol: trade.symbol,
        name: trade.security_name,
        asset_class: trade.asset_class ?? "Security",
      },
      transaction_type: trade.transaction_type,
      owner_type: "Unknown",
      trade_date: trade.trade_date,
      report_date: trade.report_date,
      amount_range_min: trade.amount_range_min,
      amount_range_max: trade.amount_range_max,
      pnl_pct: trade.pnl_pct ?? null,
      pnl_source: (trade.pnl_source as "filing" | "eod" | "none" | null) ?? null,
      smart_score: signal.score,
      smart_band: signal.band,
      kind,
      payload: {
        asset_class: trade.asset_class ?? null,
        instrument_type: trade.instrument_type ?? null,
        maturity_date: trade.maturity_date ?? null,
        duration_days: trade.duration_days ?? null,
        duration_label: trade.duration_label ?? null,
        coupon_rate: trade.coupon_rate ?? null,
        cusip: trade.cusip ?? null,
        symbol: assetClass === "crypto" ? trade.symbol : null,
      },
    } satisfies FeedItem;
  });
  const overlaySignals: SignalOverlayMap = memberTrades.items.reduce<SignalOverlayMap>((acc, trade) => {
    if (typeof trade.event_id !== "number") return acc;
    const signal = resolveSmartSignal(trade);
    if (signal.score == null || !signal.band) return acc;
    acc[String(trade.event_id)] = { score: signal.score, band: signal.band };
    return acc;
  }, {});
  let net = 0;
  const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  for (const trade of memberTrades.items) {
    const tradeDate = new Date(trade.trade_date ?? "");
    if (!Number.isFinite(tradeDate.getTime()) || tradeDate < cutoff) continue;
    const amountMin = trade.amount_range_min;
    const amountMax = trade.amount_range_max;
    const amount =
      amountMin != null && amountMax != null
        ? (amountMin + amountMax) / 2
        : (amountMax ?? amountMin);
    if (amount == null || !Number.isFinite(amount)) continue;
    const direction = tradeDirection(trade.transaction_type ?? "");
    if (direction === "buy") net += amount;
    if (direction === "sell") net -= amount;
  }
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">
            Member profile
          </p>
          <h1 className="text-3xl font-semibold text-white">
            {data.member.name}
          </h1>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-400">
            <Badge tone={party.tone}>{party.label}</Badge>
            <Badge tone={chamber.tone}>{chamber.label}</Badge>
            <span className={pillClassName}>
              {(data.member.state ?? "").split("-")[0] || "—"}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Link href={buildMemberBacktestHref(canonicalMemberId, lb)} prefetch={false} className={subtlePrimaryButtonClassName}>
            Backtest following this member
          </Link>
          <ShareLinks canonicalUrl={canonicalUrl} />
          <Link href="/?mode=all" className={ghostButtonClassName}>
            Back to feed
          </Link>
        </div>
      </div>

      <Suspense fallback={<DeferredMemberPortfolioSectionSkeleton />}>
        <DeferredMemberPortfolioSection
          portfolioPromise={portfolioPromise}
          selectedLookbackDays={portfolioLookbackDays}
          lookbackLinks={portfolioLookbackLinks}
        />
      </Suspense>

      <section className={`${cardClassName} p-4 sm:p-6`}>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Trade Outcome Analytics</h2>
            <p className="mt-1 text-xs uppercase tracking-[0.2em] text-white/45">
              Benchmark: S&P 500 · Net flow 30D {net < 0 ? `-$${compactUSD(Math.abs(net))}` : `$${compactUSD(net)}`}
            </p>
            <p className="mt-2 max-w-2xl text-sm text-white/45">
              Compact metrics from individually scored disclosures.
            </p>
          </div>
        </div>

        <Suspense fallback={<DeferredMemberAnalyticsStatsSkeleton />}>
          <DeferredMemberAnalyticsStats
            alphaSummaryPromise={alphaSummaryPromise}
          />
        </Suspense>
      </section>

      <div className="grid items-start gap-6 lg:grid-cols-[max-content_1fr]">
        <div className="w-fit">
          <div className={`${cardClassName} w-fit max-w-[240px]`}>
            <h2 className="text-lg font-semibold text-white">Top tickers</h2>
            <div className="mt-4 space-y-2">
              {data.top_tickers.length === 0 ? (
                <p className="text-sm text-slate-400">
                  No ticker concentration yet.
                </p>
              ) : (
                data.top_tickers.map((ticker) => (
                  <div
                    key={ticker.symbol}
                    className={`${compactInteractiveSurfaceClassName} flex items-center justify-between gap-4 whitespace-nowrap px-3 py-2 text-sm`}
                  >
                    <div className="flex items-center gap-2">
                      <TickerPill symbol={ticker.symbol} href={tickerHref(ticker.symbol)} />
                    </div>
                    <span className="whitespace-nowrap text-xs text-white/50 tabular-nums">
                      {ticker.trades} trades
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        <div className={`${cardClassName} w-full min-w-0`}>
          <h2 className="text-lg font-semibold text-white">Recent trades</h2>
          <div className="mt-4 space-y-2">
            {recentFeedItems.length === 0 ? (
              <p className="text-sm text-slate-400">
                No recent trades for this member.
              </p>
            ) : (
              recentFeedItems.map((item) => (
                <FeedCard
                  key={item.id}
                  item={item}
                  context="member"
                  gridPreset="member"
                  density="compact"
                  signalOverlay={overlaySignals[String(item.id)] ?? null}
                />
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
