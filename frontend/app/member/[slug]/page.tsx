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
  getMemberPerformance,
  getMemberProfile,
  getMemberProfileBySlug,
  getMemberTrades,
} from "@/lib/api";
import {
  cardClassName,
  compactInteractiveSurfaceClassName,
  ghostButtonClassName,
  pillClassName,
  tickerLinkClassName,
} from "@/lib/styles";
import { chamberBadge, partyBadge } from "@/lib/format";
import { nameToSlug } from "@/lib/memberSlug";
import type { FeedItem } from "@/lib/types";
import { tickerHref } from "@/lib/ticker";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

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

function buildMemberPath(prettySlug: string, lbParam: string, chartMetric?: "return" | "alpha") {
  const path = `/member/${prettySlug}`;
  const query = new URLSearchParams();
  if (lbParam) query.set("lb", lbParam);
  if (chartMetric && chartMetric !== "return") query.set("am", chartMetric);
  const qs = query.toString();
  return qs ? `${path}?${qs}` : path;
}

async function resolvePrettySlug(slug: string) {
  const upperSlug = slug.toUpperCase();

  try {
    if (upperSlug.startsWith("FMP_")) {
      const legacyData = await getMemberProfile(slug);
      return {
        prettySlug: nameToSlug(legacyData.member.name),
        memberName: legacyData.member.name,
      };
    }

    const data = await getMemberProfileBySlug(slug, { include_trades: false });
    return {
      prettySlug: nameToSlug(data.member.name),
      memberName: data.member.name,
    };
  } catch {
    return {
      prettySlug: slug,
      memberName: null,
    };
  }
}

export async function generateMetadata({
  params,
  searchParams,
}: Props): Promise<Metadata> {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbParam = getLookbackParam(sp);
  const siteUrl = getSiteUrl();

  const { prettySlug, memberName } = await resolvePrettySlug(slug);
  const chartMetric = getChartMetricParam(sp);
  const canonicalPath = buildMemberPath(prettySlug, lbParam, chartMetric);
  const canonicalUrl = new URL(canonicalPath, siteUrl).toString();
  const title = `${memberName ?? "Member"} — Member Profile`;

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

function DeferredMemberAlphaSectionSkeleton() {
  return (
    <div className="mt-4 space-y-4">
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
        <SkeletonBlock className="h-4 w-44" />
        <SkeletonBlock className="mt-2 h-3 w-56" />
        <SkeletonBlock className="mt-4 h-56 w-full" />
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        {Array.from({ length: 2 }).map((_, idx) => (
          <div key={idx} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <SkeletonBlock className="h-4 w-32" />
            <SkeletonBlock className="mt-3 h-20 w-full" />
          </div>
        ))}
      </div>
    </div>
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
  perf,
}: {
  alphaSummaryPromise: Promise<Awaited<ReturnType<typeof getMemberAlphaSummary>> | null>;
  perf: Awaited<ReturnType<typeof getMemberPerformance>>;
}) {
  const alphaSummary = await alphaSummaryPromise;
  const alphaSummaryError = alphaSummary == null;
  const analyticsStats = [
    {
      label: "Trades Analyzed",
      value: String(alphaSummary?.trades_analyzed ?? perf.trade_count_scored ?? perf.trade_count_total ?? 0),
      valueClass: "text-white",
    },
    {
      label: "Avg Return",
      value: pct(alphaSummary?.avg_return_pct ?? perf.avg_return),
      valueClass: tone(alphaSummary?.avg_return_pct ?? perf.avg_return),
    },
    {
      label: "Avg Alpha",
      value: pct(alphaSummary?.avg_alpha_pct ?? perf.avg_alpha),
      valueClass: tone(alphaSummary?.avg_alpha_pct ?? perf.avg_alpha),
    },
    {
      label: "Win Rate",
      value: pct0(alphaSummary?.win_rate ?? perf.win_rate),
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
          Scored trades: {perf.trade_count_scored ?? 0}/{perf.trade_count_total ?? 0}
        </span>
        {perf.pnl_status === "unavailable" && <span>Quotes limited</span>}
        {alphaSummaryError && <span>Alpha summary unavailable; showing performance fallback metrics.</span>}
      </div>
    </>
  );
}

async function DeferredMemberAlphaSection({
  alphaSummaryPromise,
  alphaSummaryErrorPromise,
  chartMetric,
  canonicalSlug,
  lb,
}: {
  alphaSummaryPromise: Promise<Awaited<ReturnType<typeof getMemberAlphaSummary>> | null>;
  alphaSummaryErrorPromise: Promise<boolean>;
  chartMetric: "alpha" | "return";
  canonicalSlug: string;
  lb: number;
}) {
  const [alphaSummary, alphaSummaryError] = await Promise.all([
    alphaSummaryPromise,
    alphaSummaryErrorPromise,
  ]);
  const memberSeries = alphaSummary?.member_series ?? alphaSummary?.performance_series ?? [];
  const benchmarkSeries = alphaSummary?.benchmark_series ?? [];
  const validChartPointCount = memberSeries.filter((point) => {
    const value = chartMetric === "alpha" ? point.cumulative_alpha_pct : point.cumulative_return_pct;
    return typeof value === "number" && Number.isFinite(value);
  }).length;
  const chartHasEnoughTrades = validChartPointCount >= 2;

  return (
    <div className="mt-4 space-y-4">
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-white/70">Performance Curve</h3>
            <p className="mt-1 text-[11px] text-white/40">Member trade outcomes vs dense S&P 500 market history.</p>
          </div>
          <div className="flex items-center gap-2 text-xs">
            <Link
              href={buildMemberPath(canonicalSlug, String(lb), "return")}
              className={`rounded-full border px-2.5 py-1 ${
                chartMetric === "return"
                  ? "border-white/30 bg-white/[0.07] text-white"
                  : "border-white/10 text-white/55 hover:text-white/80"
              }`}
            >
              Return
            </Link>
            <Link
              href={buildMemberPath(canonicalSlug, String(lb), "alpha")}
              className={`rounded-full border px-2.5 py-1 ${
                chartMetric === "alpha"
                  ? "border-white/30 bg-white/[0.07] text-white"
                  : "border-white/10 text-white/55 hover:text-white/80"
              }`}
            >
              Alpha
            </Link>
          </div>
        </div>

        {alphaSummaryError ? (
          <p className="mt-3 text-sm text-slate-400">Chart data unavailable right now.</p>
        ) : !chartHasEnoughTrades ? (
          <p className="mt-3 text-sm text-slate-400">Not enough scored trades to render a performance chart.</p>
        ) : (
          <PerformanceChart
            memberSeries={memberSeries}
            benchmarkSeries={benchmarkSeries}
            metric={chartMetric}
            benchmarkLabel="S&P 500"
          />
        )}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {[
          { title: "Best Trades", rows: alphaSummary?.best_trades ?? [] },
          { title: "Worst Trades", rows: alphaSummary?.worst_trades ?? [] },
        ].map((panel) => (
          <div key={panel.title} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-white/70">{panel.title}</h3>
            {renderTradePanelRows(panel.title, panel.rows, alphaSummaryError)}
          </div>
        ))}
      </div>
    </div>
  );
}


function resolveSmartSignal(
  trade: Awaited<ReturnType<typeof getMemberTrades>>["items"][number],
): { score: number | null; band: string | null } {
  const tradeRecord = trade as Record<string, unknown>;
  const rawScore = tradeRecord.smart_score ?? tradeRecord.smartScore;
  const score = parseNum(rawScore);
  const rawBand = tradeRecord.smart_band ?? tradeRecord.smartBand;
  const band = typeof rawBand === "string" && rawBand.trim() ? rawBand.trim().toLowerCase() : null;
  return { score, band };
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
  const canonicalPath = buildMemberPath(canonicalSlug, lbRaw, chartMetric);
  const canonicalUrl = new URL(canonicalPath, getSiteUrl()).toString();
  const canonicalMemberId = data.member.bioguide_id;
  const [perf, memberTrades] = await Promise.all([
    getMemberPerformance(canonicalMemberId, { lookback_days: lb }),
    getMemberTrades(canonicalMemberId, { lookback_days: lb, limit: 100 }),
  ]);
  const alphaSummaryPromise = getMemberAlphaSummary(canonicalMemberId, { lookback_days: lb }).catch(() => null);
  const alphaSummaryErrorPromise = alphaSummaryPromise.then((summary) => summary == null);
  const recentFeedItems = memberTrades.items.map((trade) => {
    const signal = resolveSmartSignal(trade);
    const feedId = trade.event_id ?? trade.id;
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
        asset_class: "Security",
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
      kind: "congress_trade",
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
  const options = [
    { label: "90D", value: 90 },
    { label: "180D", value: 180 },
    { label: "365D", value: 365 },
  ];

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
          <ShareLinks canonicalUrl={canonicalUrl} />
          <Link href="/?mode=all" className={ghostButtonClassName}>
            Back to feed
          </Link>
        </div>
      </div>

      <section className={`${cardClassName} p-4 sm:p-6`}>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Member Alpha Analytics</h2>
            <p className="mt-1 text-xs uppercase tracking-[0.2em] text-white/45">
              Benchmark: S&P 500 · Net flow 30D {net < 0 ? `-$${compactUSD(Math.abs(net))}` : `$${compactUSD(net)}`}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2 sm:justify-end">
            {options.map((o) => (
              <Link
                key={o.value}
                href={buildMemberPath(canonicalSlug, String(o.value), chartMetric)}
                className={`relative rounded-full border px-3 py-1.5 text-xs transition-colors ${
                  o.value === lb
                    ? "border-white/30 bg-white/[0.06] font-medium text-white"
                    : "border-white/10 text-white/60 hover:border-white/20 hover:text-white/80"
                }`}
              >
                {o.value === lb && (
                  <span className="absolute left-2 right-2 -top-[2px] h-[2px] rounded-full bg-white/60" />
                )}
                {o.label}
              </Link>
            ))}
          </div>
        </div>

        <Suspense fallback={<DeferredMemberAnalyticsStatsSkeleton />}>
          <DeferredMemberAnalyticsStats
            alphaSummaryPromise={alphaSummaryPromise}
            perf={perf}
          />
        </Suspense>
        <Suspense fallback={<DeferredMemberAlphaSectionSkeleton />}>
          <DeferredMemberAlphaSection
            alphaSummaryPromise={alphaSummaryPromise}
            alphaSummaryErrorPromise={alphaSummaryErrorPromise}
            chartMetric={chartMetric}
            canonicalSlug={canonicalSlug}
            lb={lb}
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
