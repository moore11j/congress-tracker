import Link from "next/link";
import { redirect } from "next/navigation";
import type { Metadata } from "next";
import { Badge } from "@/components/Badge";
import { ShareLinks } from "@/components/member/ShareLinks";
import { FeedCard } from "@/components/feed/FeedCard";
import { TickerPill } from "@/components/ui/TickerPill";
import { getEvents, getMemberPerformance, getMemberProfile, getMemberProfileBySlug } from "@/lib/api";
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

const DEFAULT_SITE_URL = "https://congress-tracker-two.vercel.app";

function getSiteUrl() {
  return process.env.NEXT_PUBLIC_SITE_URL ?? DEFAULT_SITE_URL;
}

function getLookbackParam(sp: Record<string, string | string[] | undefined>) {
  const lb = getParam(sp, "lb");
  if (["90", "180", "365", "3650"].includes(lb)) return lb;
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

export async function generateMetadata({ params, searchParams }: Props): Promise<Metadata> {
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

  if (amountMin != null && Number.isFinite(amountMin) && amountMax != null && Number.isFinite(amountMax)) {
    return (amountMin + amountMax) / 2;
  }
  if (amountMax != null && Number.isFinite(amountMax)) return amountMax;
  if (amountMin != null && Number.isFinite(amountMin)) return amountMin;
  return null;
}

function isBuy(tradeType: string): boolean {
  const normalized = tradeType.trim().toLowerCase();
  return ["buy", "purchase", "acquire"].includes(normalized);
}

function isSell(tradeType: string): boolean {
  const normalized = tradeType.trim().toLowerCase();
  return ["sell", "sale", "dispose"].includes(normalized);
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
  const { slug } = await params;
  const sp = (await searchParams) ?? {};
  const lbRaw = getLookbackParam(sp);
  const lb =
    lbRaw === "90" || lbRaw === "180" || lbRaw === "3650" ? Number(lbRaw) : 365;

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
  const perf = await getMemberPerformance(canonicalMemberId, lb);
  const events = await getEvents({
    tape: "congress",
    member: data.member.name,
    limit: 10,
    offset: 0,
  });
  const recentFeedItems = events.items
    .map((ev) => mapEventToFeedItem(ev))
    .filter(Boolean) as FeedItem[];
  const cutoff = new Date(Date.now() - lb * 24 * 60 * 60 * 1000);
  let net = 0;
  for (const ev of events.items) {
    if (ev.event_type !== "congress_trade") continue;
    const eventTs = new Date(ev.ts);
    if (lb !== 3650 && (!Number.isFinite(eventTs.getTime()) || eventTs < cutoff)) continue;
    const payload = parsePayload(ev.payload);
    const tradeType =
      asTrimmedString(payload.transaction_type) ??
      asTrimmedString(ev.trade_type) ??
      "";
    const amount = amountMid(ev);
    if (amount == null || !Number.isFinite(amount)) continue;
    if (isBuy(tradeType)) net += amount;
    if (isSell(tradeType)) net -= amount;
  }
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
                {(data.member.state ?? "").split("-")[0] || "—"}
              </span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <ShareLinks canonicalUrl={canonicalUrl} />
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
              label: "Net",
              value: net < 0 ? `-$${compactUSD(Math.abs(net))}` : `$${compactUSD(net)}`,
              valueClass:
                net > 0
                  ? "text-emerald-400"
                  : net < 0
                    ? "text-rose-400"
                    : "text-white/80",
            },
            {
              label: "Avg",
              value: pct(perf.avg_return),
              valueClass: tone(perf.avg_return),
            },
            {
              label: "Med",
              value: pct(perf.median_return),
              valueClass: tone(perf.median_return),
            },
            {
              label: "Win",
              value: pct0(perf.win_rate),
              valueClass: "text-white/85",
            },
            {
              label: "n",
              value: String(perf.trade_count_total ?? 0),
              valueClass: "text-white/85",
            },
            {
              label: "α S&P",
              value: perf.avg_alpha == null ? "—" : pct(perf.avg_alpha),
              valueClass: tone(perf.avg_alpha),
            },
          ].map((stat) => (
              <span
                key={stat.label}
                className="inline-flex items-center gap-2 rounded-sm pt-1"
              >
                <span className="text-white/50">{stat.label}:</span>
                <span className={`tabular-nums font-medium ${stat.valueClass}`}>
                  {stat.value}
                </span>
              </span>
            ))}
          <span className="inline-flex items-center gap-2 rounded-sm pt-1 text-white/50">
            <span>PnL:</span>
            <span className="tabular-nums">{perf.trade_count_scored ?? 0}/{perf.trade_count_total ?? 0}</span>
          </span>
          {perf.pnl_status === "unavailable" && (
            <span className="inline-flex items-center rounded-sm pt-1 text-xs text-white/40">
              Quotes limited
            </span>
          )}
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
                  />
                ))
              )}
            </div>
          </div>
        </div>
    </div>
  );
}
