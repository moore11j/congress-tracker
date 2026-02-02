import Link from "next/link";
import { FeedFilters } from "@/components/feed/FeedFilters";
import { FeedList } from "@/components/feed/FeedList";
import { API_BASE, getFeed } from "@/lib/api";
import type { EventsResponse } from "@/lib/api";
import { primaryButtonClassName } from "@/lib/styles";
import type { FeedItem } from "@/lib/types";

// PR summary: Home feed is now backed by /api/events. The unified tape currently shows only seeded demo events; production
// trades require backfill/dual-write from the legacy trade store.
function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

const feedParamKeys = ["symbol", "member", "chamber", "party", "trade_type", "min_amount", "recent_days", "cursor"] as const;

type FeedParamKey = (typeof feedParamKeys)[number];

function buildEventsUrl(params: Record<FeedParamKey, string>) {
  const url = new URL("/api/events", API_BASE);
  feedParamKeys.forEach((key) => {
    const value = params[key];
    const trimmed = value.trim();
    if (trimmed) {
      url.searchParams.set(key, trimmed);
    }
  });
  return url.toString();
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

  const activeParams: Record<FeedParamKey, string> = {
    symbol: getParam(sp, "symbol"),
    member: getParam(sp, "member"),
    chamber: getParam(sp, "chamber"),
    party: getParam(sp, "party"),
    trade_type: getParam(sp, "trade_type"),
    min_amount: getParam(sp, "min_amount"),
    recent_days: getParam(sp, "recent_days"),
    cursor: getParam(sp, "cursor"),
  };
  const requestUrl = buildEventsUrl(activeParams);

  let events: EventsResponse = { items: [], next_cursor: null };

  try {
    events = await getFeed(activeParams);
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
  feedParamKeys.forEach((key) => {
    if (key === "cursor") return;
    const value = activeParams[key];
    if (value.trim()) {
      nextParams.set(key, value.trim());
    }
  });
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

        <div>
          <Link href="/signals" className={primaryButtonClassName}>
            View Signals
          </Link>
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
        {process.env.NODE_ENV !== "production" ? (
          <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-4 text-xs text-slate-300">
            <div className="font-semibold text-slate-100">Debug feed request</div>
            <div className="mt-2 break-all font-mono text-[11px] text-slate-400">{requestUrl}</div>
            <div className="mt-2 text-slate-400">Events returned: {events.items.length}</div>
            <div className="mt-3 space-y-2">
              {events.items.slice(0, 3).map((event) => {
                const payload = event.payload ?? {};
                const memberPayload = payload.member ?? {};
                const symbol = asTrimmedString(payload.symbol) ?? asTrimmedString(event.ticker) ?? "—";
                const memberName =
                  asTrimmedString(memberPayload.name) ??
                  asTrimmedString(payload.member_name) ??
                  asTrimmedString(event.source) ??
                  "—";
                const tradeType =
                  asTrimmedString(payload.transaction_type) ?? asTrimmedString(event.event_type) ?? "—";
                const amountMin =
                  asNumber(payload.amount_range_min) ?? asNumber(payload.amount_min) ?? asNumber(payload.amount) ?? null;
                const amountMax = asNumber(payload.amount_range_max) ?? asNumber(payload.amount_max) ?? null;
                return (
                  <div key={event.id} className="rounded-lg border border-slate-800/60 bg-slate-900/40 p-3">
                    <div className="text-slate-200">
                      <span className="font-semibold">Symbol:</span> {symbol}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Member:</span> {memberName}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Trade type:</span> {tradeType}
                    </div>
                    <div className="text-slate-400">
                      <span className="font-semibold text-slate-200">Amount:</span>{" "}
                      {amountMin !== null ? amountMin : "—"} / {amountMax !== null ? amountMax : "—"}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : null}
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
