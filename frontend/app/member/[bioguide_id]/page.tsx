import Link from "next/link";
import { Badge } from "@/components/Badge";
import { FeedCard } from "@/components/feed/FeedCard";
import { getEvents, getMemberPerformance, getMemberProfile } from "@/lib/api";
import {
  cardClassName,
  ghostButtonClassName,
  pillClassName,
} from "@/lib/styles";
import { chamberBadge, formatStateDistrict, partyBadge } from "@/lib/format";
import type { EventItem } from "@/lib/api";
import type { FeedItem } from "@/lib/types";

type Props = {
  params: Promise<{ bioguide_id: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

function getParam(
  sp: Record<string, string | string[] | undefined>,
  key: string,
) {
  const v = sp[key];
  return typeof v === "string" ? v : "";
}

function pct(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${n.toFixed(1)}%`;
}

function pct0(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—";
  return `${Math.round(n * 100)}%`;
}

function tone(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "text-white/85";
  if (n > 0) return "text-emerald-300";
  if (n < 0) return "text-rose-300";
  return "text-white/70";
}

function formatCompactCurrency(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(n);
}

function netTone(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n) || n === 0) return "text-slate-300";
  if (n > 0) return "text-emerald-400";
  return "text-rose-400";
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
    pnl_pct: event.pnl_pct ?? asNumber(payload.pnl_pct),
    member_net_30d: event.member_net_30d ?? asNumber(payload.member_net_30d),
    symbol_net_30d: event.symbol_net_30d ?? asNumber(payload.symbol_net_30d),
  };
}

export default async function MemberPage({ params, searchParams }: Props) {
  const { bioguide_id } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getParam(sp, "lb");
  const metricRaw = getParam(sp, "metric");
  const lb =
    lbRaw === "90" || lbRaw === "180" || lbRaw === "3650" ? Number(lbRaw) : 365;
  const metric =
    metricRaw === "net30" ||
    metricRaw === "avg" ||
    metricRaw === "med" ||
    metricRaw === "win" ||
    metricRaw === "n" ||
    metricRaw === "alpha"
      ? metricRaw
      : "avg";

  const data = await getMemberProfile(bioguide_id);
  const perf = await getMemberPerformance(bioguide_id, lb);
  const events = await getEvents({
    tape: "congress",
    member: data.member.name,
    limit: 10,
    offset: 0,
  });
  const recentFeedItems = events.items
    .map((ev) => mapEventToFeedItem(ev))
    .filter(Boolean) as FeedItem[];
  const memberNet30dFromApi = asNumber((data as any)?.member?.net_30d ?? (data as any)?.member?.net30);
  const now = Date.now();
  const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
  const computedNet30d = events.items.reduce((sum, ev) => {
    if (ev.event_type !== "congress_trade") return sum;
    const eventTs = Date.parse(ev.ts);
    if (!Number.isFinite(eventTs) || now - eventTs > THIRTY_DAYS_MS) return sum;
    const payload = parsePayload(ev.payload);
    const amount =
      asNumber(payload.amount_range_max) ??
      asNumber(ev.payload?.amount_range_max) ??
      asNumber(payload.amount_range_min) ??
      asNumber(ev.payload?.amount_range_min);
    if (amount == null) return sum;
    const tradeType = (
      asTrimmedString(payload.transaction_type) ??
      asTrimmedString(ev.trade_type) ??
      ""
    ).toLowerCase();
    const signed = tradeType.includes("sale") ? -amount : amount;
    return sum + signed;
  }, 0);
  const net30d = memberNet30dFromApi ?? computedNet30d;
  const chamber = chamberBadge(data.member.chamber);
  const party = partyBadge(data.member.party);
  const options = [
    { label: "90D", value: 90 },
    { label: "180D", value: 180 },
    { label: "1Y", value: 365 },
    { label: "All", value: 3650 },
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
                {formatStateDistrict(data.member.state, data.member.district)}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {options.map((o) => (
              <Link
                key={o.value}
                href={`/member/${bioguide_id}?lb=${o.value}&metric=${metric}`}
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
            <Link href="/" className={ghostButtonClassName}>
              Back to feed
            </Link>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-4 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm lg:text-base">
          <span className="text-white/50">Lookback:</span>
          <span className="tabular-nums font-medium text-white/85">
            {lb === 3650 ? "All" : `${lb}D`}
          </span>

          <span className="text-white/20">|</span>

          {[
            {
              key: "net30",
              label: "Net 30D",
              value: formatCompactCurrency(net30d),
              valueClass: netTone(net30d),
            },
            {
              key: "avg",
              label: "Avg",
              value: pct(perf.avg_return),
              valueClass: tone(perf.avg_return),
            },
            {
              key: "med",
              label: "Med",
              value: pct(perf.median_return),
              valueClass: tone(perf.median_return),
            },
            {
              key: "win",
              label: "Win",
              value: pct0(perf.win_rate),
              valueClass: "text-white/85",
            },
            {
              key: "n",
              label: "n",
              value: String(perf.trade_count ?? 0),
              valueClass: "text-white/85",
            },
            {
              key: "alpha",
              label: "α S&P",
              value: perf.avg_alpha == null ? "—" : pct(perf.avg_alpha),
              valueClass: tone(perf.avg_alpha),
            },
          ].map((stat) => {
            const isActive = metric === stat.key;
            return (
              <Link
                key={stat.key}
                href={`/member/${bioguide_id}?lb=${lb}&metric=${stat.key}`}
                className={`relative inline-flex items-center gap-2 rounded-sm pt-1 transition-colors ${
                  isActive
                    ? "text-white/90 font-medium"
                    : "text-white/60 hover:text-white/80"
                }`}
              >
                {isActive ? (
                  <span className="absolute left-0 right-0 -top-[2px] h-[2px] rounded-full bg-white/60" />
                ) : null}
                <span className="text-white/50">{stat.label}:</span>
                <span className={`tabular-nums font-medium ${stat.valueClass}`}>
                  {stat.value}
                </span>
              </Link>
            );
          })}
        </div>

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
                      <span className="font-medium text-white/85">{ticker.symbol}</span>
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
                  />
                ))
              )}
            </div>
          </div>
        </div>
    </div>
  );
}
