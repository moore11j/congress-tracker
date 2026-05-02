import Link from "next/link";
import { NotificationPreferences } from "@/components/notifications/NotificationPreferences";
import { ConfirmationMonitoringRefreshButton } from "@/components/watchlists/ConfirmationMonitoringRefreshButton";
import { WatchlistRecentActivity } from "@/components/watchlists/WatchlistRecentActivity";
import { WatchlistSeenMarker } from "@/components/watchlists/WatchlistSeenMarker";
import { WatchlistTickerManager } from "@/components/watchlists/WatchlistTickerManager";
import { getWatchlist, getWatchlistConfirmationEvents, getWatchlistEvents, getWatchlistSignals, type EventItem, type SignalItem } from "@/lib/api";
import { formatCompanyName } from "@/lib/companyName";
import { buildReturnTo, requirePageAuth } from "@/lib/serverAuth";
import type { ConfirmationMonitoringEvent, FeedItem } from "@/lib/types";
import { cardClassName, ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

type ActivityMode = "all" | "congress" | "insider" | "signals";

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function parseMode(value: string): ActivityMode {
  return value === "congress" || value === "insider" || value === "signals" ? value : "all";
}

function recentDaysToSince(value: string): string | undefined {
  const days = Number(value);
  if (!Number.isFinite(days) || days < 1) return undefined;
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}

function payloadText(payload: any, keys: string[]): string | null {
  for (const key of keys) {
    const value = payload?.[key] ?? payload?.raw?.[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
}

function eventToFeedItem(event: EventItem): FeedItem {
  const payload = event.payload ?? {};
  const isInsider = event.event_type === "insider_trade";
  const symbol = event.symbol ?? event.ticker ?? payloadText(payload, ["symbol", "ticker"]);
  const insiderName = payloadText(payload, ["insider_name", "insiderName"]) ?? event.member_name ?? "Unknown insider";
  const securityName =
    payloadText(payload, ["company_name", "companyName", "security_name", "securityName"]) ??
    symbol ??
    "Unknown";

  return {
    id: event.id,
    kind: event.event_type as FeedItem["kind"],
    member: {
      bioguide_id: event.member_bioguide_id ?? "",
      name: isInsider ? insiderName : event.member_name ?? "Unknown",
      chamber: event.chamber ?? "",
      party: event.party ?? null,
      state: null,
    },
    security: {
      symbol,
      name: formatCompanyName(securityName) || securityName,
      asset_class: payloadText(payload, ["asset_class", "securityName"]) ?? "stock",
      sector: payloadText(payload, ["sector"]),
    },
    transaction_type: event.trade_type ?? "",
    owner_type: payloadText(payload, ["owner_type", "ownership"]) ?? (isInsider ? "insider" : ""),
    trade_date: payloadText(payload, ["transaction_date", "transactionDate", "trade_date", "tradeDate"]),
    report_date: payloadText(payload, ["filing_date", "filingDate", "report_date", "reportDate"]) ?? event.ts,
    amount_range_min: event.amount_min ?? null,
    amount_range_max: event.amount_max ?? null,
    estimated_price: event.estimated_price ?? null,
    current_price: event.current_price ?? null,
    display_price: event.display_price ?? null,
    reported_price: event.reported_price ?? null,
    reported_price_currency: event.reported_price_currency ?? null,
    pnl_pct: event.pnl_pct ?? null,
    smart_score: event.smart_score ?? null,
    smart_band: event.smart_band ?? null,
    member_net_30d: event.member_net_30d ?? null,
    symbol_net_30d: event.symbol_net_30d ?? null,
    confirmation_30d: event.confirmation_30d ?? null,
    insider: isInsider
      ? {
          name: insiderName,
          ownership: payloadText(payload, ["owner_type", "ownership"]),
          filing_date: payloadText(payload, ["filing_date", "filingDate"]),
          transaction_date: payloadText(payload, ["transaction_date", "transactionDate"]),
          price: typeof payload.price === "number" ? payload.price : null,
          display_price: typeof payload.display_price === "number" ? payload.display_price : null,
          reported_price: typeof payload.reported_price === "number" ? payload.reported_price : null,
          reported_price_currency: payloadText(payload, ["reported_price_currency", "reportedPriceCurrency"]),
          role: payloadText(payload, ["role", "position", "officerTitle", "typeOfOwner"]),
          reporting_cik: payloadText(payload, ["reporting_cik", "reportingCik"]),
        }
      : undefined,
  };
}

function signalToFeedItem(signal: SignalItem): FeedItem {
  const isInsider = signal.kind === "insider";
  const eventType = isInsider ? "insider_trade" : "congress_trade";
  const name = signal.who ?? (isInsider ? "Unknown insider" : "Unknown member");

  return {
    id: signal.event_id,
    kind: eventType,
    member: {
      bioguide_id: signal.member_bioguide_id ?? "",
      name,
      chamber: signal.chamber ?? "",
      party: signal.party ?? null,
      state: null,
    },
    security: {
      symbol: signal.symbol ?? null,
      name: signal.symbol ?? "Unknown",
      asset_class: "stock",
      sector: null,
    },
    transaction_type: signal.trade_type ?? "",
    owner_type: isInsider ? "insider" : "",
    trade_date: signal.ts,
    report_date: signal.ts,
    amount_range_min: signal.amount_min ?? null,
    amount_range_max: signal.amount_max ?? null,
    smart_score: signal.smart_score ?? null,
    smart_band: signal.smart_band ?? null,
    confirmation_30d: signal.confirmation_30d ?? null,
    insider: isInsider
      ? {
          name,
          role: signal.position ?? null,
          reporting_cik: signal.reporting_cik ?? null,
        }
      : undefined,
  };
}

function eventScoreDelta(event: ConfirmationMonitoringEvent) {
  if (typeof event.score_before !== "number" || typeof event.score_after !== "number") return null;
  const delta = event.score_after - event.score_before;
  if (delta === 0) return null;
  return `${delta > 0 ? "+" : ""}${delta}`;
}

function compactDate(value: string) {
  const ts = new Date(value);
  if (Number.isNaN(ts.getTime())) return "";
  return ts.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

type Props = {
  params: Promise<{ id: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function WatchlistDetailPage({ params, searchParams }: Props) {
  const { id } = await params;
  const watchlistId = Number(id);
  const sp = (await searchParams) ?? {};
  const authToken = await requirePageAuth(buildReturnTo(`/watchlists/${id}`, sp));

  const mode = parseMode(getParam(sp, "mode"));
  const recentDays = getParam(sp, "recent_days") || "30";
  const cursor = getParam(sp, "cursor");
  const offset = Number(getParam(sp, "offset") || "0");
  const limit = getParam(sp, "limit") || "25";
  const numericLimit = Math.min(Math.max(Number(limit) || 25, 1), 100);

  const watchlist = await getWatchlist(watchlistId, authToken);
  const confirmationEventsResponse = await getWatchlistConfirmationEvents(watchlistId, { limit: 5, authToken });
  const confirmationEvents = confirmationEventsResponse.items ?? [];
  const onlyNew = getParam(sp, "only_new") === "1" && mode !== "signals";
  const newSince = onlyNew ? getParam(sp, "new_since") || watchlist.unseen_since || "" : "";
  const unseenCount = Math.max(Number(watchlist.unseen_count) || 0, 0);
  const activity =
    mode === "signals"
      ? await getWatchlistSignals(watchlistId, {
          mode: "all",
          sort: "smart",
          limit: numericLimit,
          offset: Number.isFinite(offset) ? offset : 0,
          authToken,
        })
      : onlyNew && !newSince
      ? { items: [], next_cursor: null }
      : await getWatchlistEvents(watchlistId, {
          mode,
          since: onlyNew ? newSince : recentDaysToSince(recentDays),
          cursor: cursor || undefined,
          limit: numericLimit,
          authToken,
        });

  const items =
    mode === "signals"
      ? (activity.items as SignalItem[]).map(signalToFeedItem)
      : (activity.items as EventItem[]).map(eventToFeedItem);

  return (
    <div className="space-y-6">
      <WatchlistSeenMarker watchlistId={watchlist.watchlist_id} />
      <div className="grid w-full min-w-0 items-center gap-6 lg:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlist</p>
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="text-3xl font-semibold text-white">{watchlist.name ?? `Watchlist #${watchlist.watchlist_id}`}</h1>
            {unseenCount > 0 ? (
              <span className="rounded-lg border border-emerald-300/30 bg-emerald-300/15 px-2.5 py-1 text-xs font-semibold text-emerald-100">
                {unseenCount} new
              </span>
            ) : null}
          </div>
          <p className="text-sm text-slate-400">Monitor filings, insider trades, and unusual signals across saved tickers.</p>
        </div>
        <div className="flex w-full min-w-0 gap-2 lg:justify-end">
          <Link href={`/backtesting?strategy=watchlist&watchlist_id=${watchlist.watchlist_id}`} className={subtlePrimaryButtonClassName} prefetch={false}>
            Backtest this watchlist
          </Link>
          <Link href="/watchlists" className={ghostButtonClassName}>
            Back to watchlists
          </Link>
        </div>
      </div>

      <div className="grid w-full min-w-0 gap-6 lg:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <WatchlistTickerManager watchlistId={watchlist.watchlist_id} tickers={watchlist.tickers} />

        <section className={`${cardClassName} min-w-0 space-y-4`}>
          <NotificationPreferences
            sourceType="watchlist"
            sourceId={String(watchlist.watchlist_id)}
            sourceName={watchlist.name ?? `Watchlist #${watchlist.watchlist_id}`}
            useAccountEmailDestination={true}
            sourcePayload={{
              unseen_since: watchlist.unseen_since,
              last_seen_at: watchlist.last_seen_at,
            }}
          />

          <div className="border-y border-white/10 py-4">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 className="text-lg font-semibold text-white">Confirmation monitor</h2>
                <p className="text-sm text-slate-400">Material confirmation changes for saved tickers. Auto-refreshes after scheduled ingest.</p>
              </div>
              <ConfirmationMonitoringRefreshButton watchlistId={watchlist.watchlist_id} />
            </div>

            <div className="mt-4 divide-y divide-white/10">
              {confirmationEvents.length === 0 ? (
                <div className="py-3 text-sm text-slate-400">No confirmation changes recorded yet.</div>
              ) : (
                confirmationEvents.map((event) => {
                  const delta = eventScoreDelta(event);
                  return (
                    <Link
                      key={event.id}
                      href={`/ticker/${encodeURIComponent(event.ticker)}`}
                      prefetch={false}
                      className="grid gap-2 py-3 transition hover:bg-white/[0.03] sm:grid-cols-[4.25rem_minmax(0,1fr)_8.75rem] sm:items-center sm:gap-x-2"
                    >
                      <span className="font-mono text-sm font-semibold text-emerald-200">{event.ticker}</span>
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-semibold text-white">{event.title}</span>
                        {event.body ? <span className="block truncate text-xs text-slate-500">{event.body}</span> : null}
                      </span>
                      <span className="flex min-w-[8.75rem] shrink-0 flex-nowrap items-center gap-1.5 whitespace-nowrap text-xs text-slate-500 sm:justify-end">
                        {delta ? (
                          <span className={`rounded-lg border px-2 py-0.5 font-semibold ${delta.startsWith("+") ? "border-emerald-300/25 text-emerald-100" : "border-rose-300/25 text-rose-100"}`}>
                            {delta}
                          </span>
                        ) : null}
                        <span>{compactDate(event.created_at)}</span>
                      </span>
                    </Link>
                  );
                })
              )}
            </div>
          </div>

          <WatchlistRecentActivity
            watchlistId={watchlist.watchlist_id}
            tickerCount={watchlist.tickers.length}
            unseenCount={unseenCount}
            unseenSince={watchlist.unseen_since ?? ""}
            initialState={{
              mode,
              recentDays,
              limit: numericLimit,
              onlyNew,
              newSince,
            }}
            initialData={{
              items,
              nextCursor: "next_cursor" in activity ? activity.next_cursor ?? null : null,
              offset: mode === "signals" ? (Number.isFinite(offset) ? offset : 0) + items.length : 0,
              hasMore: mode === "signals" ? items.length === numericLimit : Boolean("next_cursor" in activity && activity.next_cursor),
            }}
          />
        </section>
      </div>
    </div>
  );
}
