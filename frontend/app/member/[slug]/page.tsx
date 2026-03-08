import Link from "next/link";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { Badge } from "@/components/Badge";
import { ShareLinks } from "@/components/member/ShareLinks";
import { FeedCard } from "@/components/feed/FeedCard";
import { TickerPill } from "@/components/ui/TickerPill";
import {
  API_BASE,
  getEvents,
  getMemberAlphaSummary,
  getMemberPerformance,
  getMemberProfile,
  getMemberProfileBySlug,
} from "@/lib/api";
import {
  cardClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import { chamberBadge, partyBadge } from "@/lib/format";
import { nameToSlug } from "@/lib/memberSlug";
import type { EventItem } from "@/lib/api";
import type { FeedItem } from "@/lib/types";

type Props = {
  params: Promise<{ slug: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

type SignalOverlayItem = {
  event_id: number;
  smart_score?: number;
  smart_band?: string;
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

function buildMemberPath(prettySlug: string, lbParam: string) {
  const path = `/member/${prettySlug}`;
  return lbParam ? `${path}?lb=${lbParam}` : path;
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

    const data = await getMemberProfileBySlug(slug);
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
  const canonicalPath = buildMemberPath(prettySlug, lbParam);
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

function asTrimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && !Number.isNaN(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value.trim());
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function parsePayload(payload: unknown): any {
  if (typeof payload === "string") {
    try {
      return JSON.parse(payload);
    } catch {
      return {};
    }
  }
  return payload && typeof payload === "object" ? payload : {};
}

function amountMid(ev: EventItem): number | null {
  const payload = parsePayload(ev.payload);
  const amountMin =
    asNumber(payload.amount_min) ??
    asNumber(payload.amount_range_min) ??
    asNumber(ev.payload?.amount_min) ??
    asNumber(ev.payload?.amount_range_min);
  const amountMax =
    asNumber(payload.amount_max) ??
    asNumber(payload.amount_range_max) ??
    asNumber(ev.payload?.amount_max) ??
    asNumber(ev.payload?.amount_range_max);

  if (
    amountMin != null &&
    Number.isFinite(amountMin) &&
    amountMax != null &&
    Number.isFinite(amountMax)
  ) {
    return (amountMin + amountMax) / 2;
  }
  if (amountMax != null && Number.isFinite(amountMax)) return amountMax;
  if (amountMin != null && Number.isFinite(amountMin)) return amountMin;
  return null;
}

async function getSignalsOverlay(): Promise<SignalOverlayItem[]> {
  const url = new URL("/api/signals/unusual", API_BASE);
  url.searchParams.set("preset", "balanced");
  url.searchParams.set("recent_days", "14");
  url.searchParams.set("min_smart_score", "75");
  url.searchParams.set("sort", "smart");
  url.searchParams.set("limit", "50");
  try {
    const res = await fetch(url.toString(), { next: { revalidate: 60 } });
    if (!res.ok) return [];
    const data = await res.json();
    return Array.isArray(data) ? data : (data.items ?? []);
  } catch {
    return [];
  }
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

function mapEventToFeedItem(event: EventItem): FeedItem | null {
  if (event.event_type !== "congress_trade") return null;
  const payload = parsePayload(event.payload);
  const memberPayload = payload.member ?? {};

  return {
    id: event.id,
    kind: "congress_trade",
    member: {
      bioguide_id: asTrimmedString(memberPayload.bioguide_id) ?? "event",
      name:
        asTrimmedString(memberPayload.name) ??
        asTrimmedString(event.member_name) ??
        "Congressional Trade",
      chamber:
        asTrimmedString(memberPayload.chamber) ??
        asTrimmedString(event.chamber) ??
        "House",
      party:
        asTrimmedString(memberPayload.party) ?? asTrimmedString(event.party),
      state: asTrimmedString(memberPayload.state),
      district: asTrimmedString(memberPayload.district),
    },
    security: {
      symbol: asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker),
      name:
        asTrimmedString(payload.security_name) ??
        asTrimmedString(event.headline) ??
        "Security",
      asset_class: asTrimmedString(payload.asset_class) ?? "Security",
      sector: asTrimmedString(payload.sector),
    },
    transaction_type:
      asTrimmedString(payload.transaction_type) ??
      asTrimmedString(event.trade_type) ??
      "",
    owner_type: asTrimmedString(payload.owner_type) ?? "Unknown",
    trade_date: asTrimmedString(payload.trade_date) ?? event.ts,
    report_date: asTrimmedString(payload.report_date) ?? event.ts,
    amount_range_min: asNumber(payload.amount_range_min),
    amount_range_max: asNumber(payload.amount_range_max),
    estimated_price: event.estimated_price ?? asNumber(payload.estimated_price),
    current_price: event.current_price ?? asNumber(payload.current_price),
    smart_score: (event as any).smart_score ?? null,
    smart_band: (event as any).smart_band ?? null,
    pnl_pct: event.pnl_pct ?? asNumber(payload.pnl_pct),
    pnl_source: (event as any).pnl_source ?? null,
    quote_is_stale: (event as any).quote_is_stale ?? null,
    quote_asof_ts: (event as any).quote_asof_ts ?? null,
    member_net_30d: event.member_net_30d ?? asNumber(payload.member_net_30d),
    symbol_net_30d: event.symbol_net_30d ?? asNumber(payload.symbol_net_30d),
  };
}

export default async function MemberPage({ params, searchParams }: Props) {
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getLookbackParam(sp);
  const lb = lbRaw === "90" || lbRaw === "180" ? Number(lbRaw) : 365;

  const upperSlug = slug.toUpperCase();
  if (upperSlug.startsWith("FMP_")) {
    const legacyData = await getMemberProfile(slug);
    const cleanSlug = nameToSlug(legacyData.member.name);
    const query = toQueryString(sp);
    redirect(`/member/${cleanSlug}${query ? `?${query}` : ""}`);
  }

  const data = await getMemberProfileBySlug(slug);
  const canonicalSlug = nameToSlug(data.member.name);
  const canonicalPath = buildMemberPath(canonicalSlug, lbRaw);
  const canonicalUrl = new URL(canonicalPath, getSiteUrl()).toString();
  const canonicalMemberId = data.member.bioguide_id;
  const perf = await getMemberPerformance(canonicalMemberId, { lookback_days: lb });
  let alphaSummary = null;
  let alphaSummaryError = false;
  try {
    alphaSummary = await getMemberAlphaSummary(canonicalMemberId, { lookback_days: lb });
  } catch {
    alphaSummaryError = true;
  }
  const events = await getEvents({
    tape: "congress",
    member: data.member.name,
    limit: 10,
    offset: 0,
  });
  const recentFeedItems = events.items
    .map((ev) => mapEventToFeedItem(ev))
    .filter(Boolean) as FeedItem[];
  const signals = await getSignalsOverlay();
  const overlaySignals: SignalOverlayMap = {};
  for (const s of signals) {
    if (typeof s.event_id !== "number") continue;
    if (typeof s.smart_score !== "number") continue;
    if (typeof s.smart_band !== "string") continue;
    overlaySignals[String(s.event_id)] = {
      score: s.smart_score,
      band: s.smart_band,
    };
  }
  const memberNet30d = events.items.find(
    (ev) =>
      ev.event_type === "congress_trade" &&
      typeof ev.member_net_30d === "number" &&
      Number.isFinite(ev.member_net_30d),
  )?.member_net_30d;
  let net = memberNet30d ?? 0;
  if (memberNet30d == null) {
    const cutoff = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    for (const ev of events.items) {
      if (ev.event_type !== "congress_trade") continue;
      const eventTs = new Date(ev.ts);
      if (!Number.isFinite(eventTs.getTime()) || eventTs < cutoff) continue;
      const payload = parsePayload(ev.payload);
      const tradeType =
        asTrimmedString(payload.transaction_type) ??
        asTrimmedString(ev.trade_type) ??
        "";
      const amount = amountMid(ev);
      if (amount == null || !Number.isFinite(amount)) continue;
      const direction = tradeDirection(tradeType);
      if (direction === "buy") net += amount;
      if (direction === "sell") net -= amount;
    }
  }
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);
  const options = [
    { label: "90D", value: 90 },
    { label: "180D", value: 180 },
    { label: "365D", value: 365 },
  ];

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
              Benchmark: {alphaSummary?.benchmark_symbol ?? perf.benchmark_symbol ?? "^GSPC"} · Net flow 30D {net < 0 ? `-$${compactUSD(Math.abs(net))}` : `$${compactUSD(net)}`}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2 sm:justify-end">
            {options.map((o) => (
              <Link
                key={o.value}
                href={`/member/${canonicalSlug}?lb=${o.value}`}
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

        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          {[
            { title: "Best Trades", rows: alphaSummary?.best_trades ?? [] },
            { title: "Worst Trades", rows: alphaSummary?.worst_trades ?? [] },
          ].map((panel) => (
            <div key={panel.title} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <h3 className="text-sm font-semibold uppercase tracking-[0.14em] text-white/70">{panel.title}</h3>
              {panel.rows.length === 0 ? (
                <p className="mt-3 text-sm text-slate-400">
                  {alphaSummaryError
                    ? "Unable to load trade-level alpha rows right now."
                    : "No scored trades for this lookback window."}
                </p>
              ) : (
                <div className="mt-3 space-y-2">
                  {panel.rows.map((trade) => (
                    <div
                      key={`${panel.title}-${trade.event_id}-${trade.symbol}`}
                      className="grid grid-cols-[1fr_auto_auto] items-center gap-3 rounded-xl border border-white/10 px-3 py-2"
                    >
                      <div className="min-w-0">
                        <p className="truncate text-sm font-medium text-white">{trade.symbol}</p>
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
              )}
            </div>
          ))}
        </div>
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
                  <Link
                    key={ticker.symbol}
                    href={`/ticker/${ticker.symbol}`}
                    className="flex items-center justify-between gap-4 whitespace-nowrap rounded-xl border border-white/10 bg-white/[0.03] px-3 py-2 text-sm text-slate-200 hover:border-emerald-400/40"
                  >
                    <div className="flex items-center gap-2">
                      <TickerPill symbol={ticker.symbol} />
                    </div>
                    <span className="whitespace-nowrap text-xs text-white/50 tabular-nums">
                      {ticker.trades} trades
                    </span>
                  </Link>
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
