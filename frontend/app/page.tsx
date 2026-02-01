import Link from "next/link";
import { FeedList } from "@/components/feed/FeedList";
import { getEventsWithMeta, getResolvedApiBaseUrl } from "@/lib/api";
import { cardClassName, ghostButtonClassName, inputClassName, primaryButtonClassName, selectClassName } from "@/lib/styles";
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

  const symbol = getParam(sp, "symbol");
  const member = getParam(sp, "member");
  const chamber = getParam(sp, "chamber");
  const minAmount = getParam(sp, "min_amount");
  const whale = getParam(sp, "whale");
  const recentDays = getParam(sp, "recent_days");
  const cursor = getParam(sp, "cursor");
  const limit = getParam(sp, "limit") || "50";
  const debug = getParam(sp, "debug") === "1";

  let events = { items: [], next_cursor: null };
  let requestUrl = "";
  let errorMessage: string | null = null;

  try {
    const result = await getEventsWithMeta({
      tickers: symbol || undefined,
      cursor: cursor || undefined,
      limit,
    });
    events = result.data;
    requestUrl = result.requestUrl;
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : String(error);
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
  if (symbol) nextParams.set("symbol", symbol);
  if (member) nextParams.set("member", member);
  if (chamber) nextParams.set("chamber", chamber);
  if (minAmount) nextParams.set("min_amount", minAmount);
  if (whale) nextParams.set("whale", whale);
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

        <div className={cardClassName}>
          <form method="get" className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
              <input name="symbol" defaultValue={symbol} placeholder="NVDA" className={inputClassName} />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
              <input name="member" defaultValue={member} placeholder="Pelosi" className={inputClassName} />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
              <select name="chamber" defaultValue={chamber} className={selectClassName}>
                <option value="">All chambers</option>
                <option value="house">House</option>
                <option value="senate">Senate</option>
              </select>
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
              <input name="min_amount" defaultValue={minAmount} placeholder="250000" className={inputClassName} />
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
              <select name="recent_days" defaultValue={recentDays} className={selectClassName}>
                <option value="">Anytime</option>
                <option value="7">Last 7 days</option>
                <option value="30">Last 30 days</option>
                <option value="90">Last 90 days</option>
              </select>
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Per page</label>
              <select name="limit" defaultValue={limit} className={selectClassName}>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
              </select>
            </div>
            <div className="flex items-center gap-2">
              <input
                id="whale"
                name="whale"
                type="checkbox"
                value="1"
                defaultChecked={whale === "1"}
                className="h-4 w-4 rounded border-white/30 bg-slate-900 text-emerald-300 focus:ring-emerald-400"
              />
              <label htmlFor="whale" className="text-sm text-slate-300">
                Whale trades only (&gt;$250k)
              </label>
            </div>
            <input type="hidden" name="cursor" value="" />
            <div className="flex flex-wrap items-center gap-3 md:col-span-2 xl:col-span-3">
              <button type="submit" className={primaryButtonClassName}>
                Apply filters
              </button>
              <Link href="/" className={ghostButtonClassName}>
                Clear
              </Link>
            </div>
          </form>
        </div>
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
        {(debug || process.env.NODE_ENV !== "production") && (
          <footer className="rounded-xl border border-slate-800 bg-slate-950/60 p-4 text-xs text-slate-400">
            <p className="text-[11px] font-semibold uppercase tracking-[0.3em] text-slate-500">Feed debug</p>
            <div className="mt-3 grid gap-1">
              <div>
                <span className="text-slate-500">API base URL:</span>{" "}
                {getResolvedApiBaseUrl() || "unset (set NEXT_PUBLIC_API_BASE_URL)"}
              </div>
              <div>
                <span className="text-slate-500">Request URL:</span> {requestUrl || "unavailable"}
              </div>
              <div>
                <span className="text-slate-500">Items returned:</span> {events.items.length}
              </div>
              <div>
                <span className="text-slate-500">Error:</span> {errorMessage ?? "none"}
              </div>
            </div>
          </footer>
        )}
      </section>
    </div>
  );
}
