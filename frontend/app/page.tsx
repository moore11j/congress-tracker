import Link from "next/link";
import { FeedList } from "@/components/feed/FeedList";
import { getEvents } from "@/lib/api";
import { cardClassName, ghostButtonClassName, inputClassName, primaryButtonClassName, selectClassName } from "@/lib/styles";
import type { FeedItem } from "@/lib/types";

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
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

  let events;

  try {
    events = await getEvents({
      ticker: symbol || undefined,
      cursor: cursor || undefined,
      limit,
    });
  } catch (error) {
    if (!symbol) {
      throw error;
    }

    events = await getEvents({
      symbol,
      cursor: cursor || undefined,
      limit,
    });
  }

  const items = events.items.map((event) => ({
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
    title: event.headline ?? event.summary ?? event.event_type,
    ticker: event.ticker ?? null,
    timestamp: event.ts,
    source: event.source ?? null,
    url: event.url ?? null,
  })) satisfies FeedItem[];

  const items = events.items.map((event) => ({
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
    title: event.headline ?? event.summary ?? event.event_type,
    ticker: event.ticker ?? null,
    timestamp: event.ts,
    source: event.source ?? null,
    url: event.url ?? null,
  })) satisfies FeedItem[];

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
      </section>
    </div>
  );
}
