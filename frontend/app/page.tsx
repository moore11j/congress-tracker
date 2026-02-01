import Link from "next/link";
import { FeedFilters } from "@/components/feed/FeedFilters";
import { FeedList } from "@/components/feed/FeedList";
import { getFeed } from "@/lib/api";
import type { EventsResponse } from "@/lib/api";
import { primaryButtonClassName } from "@/lib/styles";
import type { FeedItem } from "@/lib/types";

// PR summary: Home feed is now backed by /api/events. The unified tape currently shows only seeded demo events; production
// trades require backfill/dual-write from the legacy trade store.
function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function asTrimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && !Number.isNaN(value)) return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function mapEventToFeedItem(event: {
  id: number;
  event_type: string;
  ts: string;
  ticker?: string | null;
  source?: string | null;
  headline?: string | null;
  summary?: string | null;
  url?: string | null;
  payload?: any;
}): FeedItem {
  if (event.event_type === "congress_trade") {
    const payload = event.payload ?? {};
    const memberPayload = payload.member ?? {};
    const memberBioguide =
      asTrimmedString(memberPayload.bioguide_id) ??
      (typeof memberPayload.bioguide_id === "number" ? String(memberPayload.bioguide_id) : null) ??
      event.source ??
      "event";
    const memberName =
      asTrimmedString(memberPayload.name) ?? asTrimmedString(payload.member_name) ?? event.source ?? "Congressional Trade";
    const memberChamber = asTrimmedString(memberPayload.chamber) ?? event.source ?? event.event_type;
    const memberParty = asTrimmedString(memberPayload.party);
    const memberState = asTrimmedString(memberPayload.state);
    const symbol = asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker);
    const securityName = asTrimmedString(payload.security_name) ?? event.headline ?? event.summary ?? event.event_type;
    const assetClass = asTrimmedString(payload.asset_class) ?? "Security";
    const sector = asTrimmedString(payload.sector);
    const transactionType = asTrimmedString(payload.transaction_type) ?? event.event_type;
    const ownerType = asTrimmedString(payload.owner_type) ?? "Unknown";
    const tradeDate = asTrimmedString(payload.trade_date) ?? event.ts ?? null;
    const reportDate = asTrimmedString(payload.report_date) ?? event.ts ?? null;
    const amountMin = asNumber(payload.amount_range_min);
    const amountMax = asNumber(payload.amount_range_max);
    const documentUrl = asTrimmedString(payload.document_url) ?? event.url ?? null;

    return {
      id: event.id,
      member: {
        bioguide_id: memberBioguide,
        name: memberName,
        chamber: memberChamber,
        party: memberParty,
        state: memberState,
      },
      security: {
        symbol,
        name: securityName,
        asset_class: assetClass,
        sector,
      },
      transaction_type: transactionType,
      owner_type: ownerType,
      trade_date: tradeDate,
      report_date: reportDate,
      amount_range_min: amountMin,
      amount_range_max: amountMax,
    };
  }

  return {
    id: event.id,
    member: {
      bioguide_id: event.source ?? "event",
      name: event.source ?? "Congressional Event",
      chamber: event.event_type ?? "event",
    },
    security: {
      symbol: event.ticker ?? null,
      name: event.headline ?? event.summary ?? event.event_type,
      asset_class: event.event_type,
    },
    transaction_type: event.event_type,
    owner_type: "event",
    trade_date: event.ts,
    report_date: event.ts,
    amount_range_min: null,
    amount_range_max: null,
  };
}

export default async function FeedPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const sp = (await searchParams) ?? {};

  const symbol = getParam(sp, "tickers") || getParam(sp, "ticker") || getParam(sp, "symbol");
  const member = getParam(sp, "member");
  const chamber = getParam(sp, "chamber");
  const party = getParam(sp, "party");
  const tradeType = getParam(sp, "trade_type");
  const minAmount = getParam(sp, "min_amount");
  const recentDays = getParam(sp, "recent_days");
  const cursor = getParam(sp, "cursor");
  const limit = getParam(sp, "limit") || "50";

  let events: EventsResponse = { items: [], next_cursor: null };

  try {
    events = await getFeed({
      symbol: symbol || undefined,
      member: member || undefined,
      chamber: chamber || undefined,
      party: party || undefined,
      trade_type: tradeType || undefined,
      min_amount: minAmount || undefined,
      recent_days: recentDays || undefined,
      cursor: cursor || undefined,
      limit,
    });
  } catch (error) {
    console.error("Failed to load events feed", error);
  }

  const items = events.items.map((event) => {
    const feedItem = mapEventToFeedItem(event);
    const payload = event.payload ?? {};
    const tradeTicker = asTrimmedString(payload.symbol) ?? event.ticker ?? null;
    const tradeUrl = asTrimmedString(payload.document_url) ?? event.url ?? null;
    return {
      ...feedItem,
      title: event.headline ?? event.summary ?? event.event_type,
      ticker: tradeTicker,
      timestamp: event.ts,
      source: event.source ?? null,
      url: tradeUrl,
    };
  }) satisfies FeedItem[];

  const nextParams = new URLSearchParams();
  if (symbol) nextParams.set("tickers", symbol);
  if (member) nextParams.set("member", member);
  if (chamber) nextParams.set("chamber", chamber);
  if (party) nextParams.set("party", party);
  if (tradeType) nextParams.set("trade_type", tradeType);
  if (minAmount) nextParams.set("min_amount", minAmount);
  if (recentDays) nextParams.set("recent_days", recentDays);
  nextParams.set("limit", limit);
  if (events.next_cursor) nextParams.set("cursor", events.next_cursor);

  return (
    <div className="space-y-8">
      <section className="flex flex-col gap-6">
        <div className="flex flex-col gap-2">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Live Capitol Flow</p>
          <h1 className="text-4xl font-semibold text-white sm:text-5xl">Congressional trade intelligence.</h1>
          <p className="max-w-2xl text-sm text-slate-400">
            Screen trades in real time, spotlight large transactions, and track lawmakers or tickers with a premium
            market-style dashboard.
          </p>
        </div>

        <FeedFilters events={events.items} resultsCount={items.length} />
      </section>

      <section className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-xl font-semibold text-white">Latest events</h2>
            <p className="text-sm text-slate-400">Showing {items.length} events.</p>
          </div>
        </div>
        <FeedList items={items} />
        <div className="flex items-center justify-between gap-4">
          <span className="text-xs text-slate-500">Cursor-based pagination ensures real-time freshness.</span>
          {events.next_cursor ? (
            <Link href={`/?${nextParams.toString()}`} className={primaryButtonClassName}>
              Load more
            </Link>
          ) : (
            <span className="text-sm text-slate-500">No more results.</span>
          )}
        </div>
      </section>
    </div>
  );
}
