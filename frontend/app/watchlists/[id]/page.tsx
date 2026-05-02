import Link from "next/link";
import { FeedCard } from "@/components/feed/FeedCard";
import { NotificationPreferences } from "@/components/notifications/NotificationPreferences";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { ConfirmationMonitoringRefreshButton } from "@/components/watchlists/ConfirmationMonitoringRefreshButton";
import { WatchlistSeenMarker } from "@/components/watchlists/WatchlistSeenMarker";
import { WatchlistTickerManager } from "@/components/watchlists/WatchlistTickerManager";
import { getWatchlist, getWatchlistConfirmationEvents, getWatchlistEvents, getWatchlistSignals, type EventItem, type SignalItem } from "@/lib/api";
import { formatCompanyName } from "@/lib/companyName";
import { buildReturnTo, requirePageAuth } from "@/lib/serverAuth";
import type { ConfirmationMonitoringEvent, FeedItem } from "@/lib/types";
import { cardClassName, ghostButtonClassName, pillClassName, primaryButtonClassName, selectClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

type ActivityMode = "all" | "congress" | "insider" | "signals";

const modeOptions: { value: ActivityMode; label: string }[] = [
  { value: "all", label: "All" },
  { value: "congress", label: "Congress" },
  { value: "insider", label: "Insiders" },
  { value: "signals", label: "Signals" },
];

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

function tabHref(
  watchlistId: number,
  mode: ActivityMode,
  recentDays: string,
  limit: string,
  onlyNew: boolean,
  newSince: string,
) {
  const params = new URLSearchParams();
  if (mode !== "all") params.set("mode", mode);
  if (recentDays) params.set("recent_days", recentDays);
  if (limit) params.set("limit", limit);
  if (onlyNew && mode !== "signals" && newSince) {
    params.set("only_new", "1");
    params.set("new_since", newSince);
  }
  const qs = params.toString();
  return `/watchlists/${watchlistId}${qs ? `?${qs}` : ""}`;
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
  const newFilterHref = tabHref(watchlistId, mode, recentDays, String(numericLimit), true, watchlist.unseen_since ?? "");
  const allActivityHref = tabHref(watchlistId, mode, recentDays, String(numericLimit), false, "");
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

  const nextParams = new URLSearchParams();
  if (mode !== "all") nextParams.set("mode", mode);
  if (recentDays) nextParams.set("recent_days", recentDays);
  nextParams.set("limit", String(numericLimit));
  if (onlyNew && newSince) {
    nextParams.set("only_new", "1");
    nextParams.set("new_since", newSince);
  }
  if (mode === "signals") {
    nextParams.set("offset", String((Number.isFinite(offset) ? offset : 0) + numericLimit));
  } else if ("next_cursor" in activity && activity.next_cursor) {
    nextParams.set("cursor", activity.next_cursor);
  }

  const canLoadMore = mode === "signals" ? items.length === numericLimit : Boolean("next_cursor" in activity && activity.next_cursor);

  return (
    <div className="space-y-6">
      <WatchlistSeenMarker watchlistId={watchlist.watchlist_id} />
      <div className="grid w-full items-center gap-6 lg:grid-cols-[0.9fr_1.6fr]">
        <div>
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
        <div className="flex w-full gap-2 lg:justify-end">
          <Link href={`/backtesting?strategy=watchlist&watchlist_id=${watchlist.watchlist_id}`} className={subtlePrimaryButtonClassName} prefetch={false}>
            Backtest this watchlist
          </Link>
          <Link href="/watchlists" className={ghostButtonClassName}>
            Back to watchlists
          </Link>
        </div>
      </div>

      <div className="grid w-full gap-6 lg:grid-cols-[0.9fr_1.6fr]">
        <WatchlistTickerManager watchlistId={watchlist.watchlist_id} tickers={watchlist.tickers} />

        <section className={`${cardClassName} space-y-4`}>
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

          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <h2 className="text-lg font-semibold text-white">Recent activity</h2>
              <p className="text-sm text-slate-400">
                {watchlist.tickers.length
                  ? onlyNew
                    ? `${items.length} new items across ${watchlist.tickers.length} saved tickers.`
                    : `${items.length} items across ${watchlist.tickers.length} saved tickers.`
                  : "Add tickers to turn this into a monitoring feed."}
              </p>
            </div>
            <form method="get" className="flex flex-wrap items-end gap-3">
              <input type="hidden" name="mode" value={mode === "all" ? "" : mode} />
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
                Window
                <select name="recent_days" defaultValue={recentDays} className={`${selectClassName} min-w-[140px] rounded-lg py-1.5`}>
                  <option value="7">Last 7 days</option>
                  <option value="30">Last 30 days</option>
                  <option value="90">Last 90 days</option>
                  <option value="180">Last 180 days</option>
                </select>
              </label>
              <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
                Rows
                <select name="limit" defaultValue={String(numericLimit)} className={`${selectClassName} min-w-[96px] rounded-lg py-1.5`}>
                  <option value="25">25</option>
                  <option value="50">50</option>
                  <option value="100">100</option>
                </select>
              </label>
              <button type="submit" className={subtlePrimaryButtonClassName}>
                Apply
              </button>
            </form>
          </div>

          <div className="mt-5 flex flex-wrap gap-2">
            {modeOptions.map((option) => {
              const active = option.value === mode;
              return (
                <Link
                  key={option.value}
                  href={tabHref(
                    watchlistId,
                    option.value,
                    recentDays,
                    String(numericLimit),
                    onlyNew,
                    newSince || watchlist.unseen_since || "",
                  )}
                  className={`rounded-lg border px-3 py-1.5 text-sm font-semibold transition ${
                    active
                      ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100"
                      : "border-white/10 text-slate-300 hover:border-white/20 hover:text-white"
                  }`}
                >
                  {option.label}
                </Link>
              );
            })}
            {mode !== "signals" ? (
              <Link
                href={onlyNew ? allActivityHref : newFilterHref}
                className={`rounded-lg border px-3 py-1.5 text-sm font-semibold transition ${
                  onlyNew
                    ? "border-sky-300/40 bg-sky-300/15 text-sky-100"
                    : unseenCount > 0
                    ? "border-white/10 text-slate-300 hover:border-sky-300/40 hover:text-white"
                    : "pointer-events-none border-white/10 text-slate-600"
                }`}
                aria-disabled={!onlyNew && unseenCount === 0}
              >
                {onlyNew ? "Showing new" : unseenCount > 0 ? `New only (${unseenCount})` : "No new"}
              </Link>
            ) : null}
          </div>

          <div className="mt-4">
            <SavedViewsBar
              surface="watchlist"
              scopeKey={String(watchlist.watchlist_id)}
              restoreOnLoad={true}
              defaultParams={{ mode: "all", recent_days: "30", limit: "25" }}
              paramKeys={["mode", "recent_days", "limit"]}
              rightSlot={
                <>
                  <span className={pillClassName}>
                    mode <span className="text-white">{mode}</span>
                  </span>
                  <span className={pillClassName}>
                    window <span className="text-white">{recentDays}d</span>
                  </span>
                  <span className={pillClassName}>
                    rows <span className="text-white">{numericLimit}</span>
                  </span>
                  {mode !== "signals" ? (
                    <span className={pillClassName}>
                      new <span className="text-white">{unseenCount}</span>
                    </span>
                  ) : null}
                </>
              }
            />
          </div>

          <div className="mt-5 space-y-4">
            {items.length === 0 ? (
              <div className="rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-6">
                <h3 className="font-semibold text-white">{onlyNew ? "No new activity" : "No recent activity yet"}</h3>
                <p className="mt-1 text-sm text-slate-400">
                  {onlyNew
                    ? "Everything in this watchlist has already been checked."
                    : "Add liquid tickers or widen the window to catch congressional filings, insider Form 4s, and unusual activity."}
                </p>
              </div>
            ) : (
              items.map((item) => <FeedCard key={`${item.kind}-${item.id}`} item={item} density="compact" />)
            )}
          </div>

          <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
            <span className="text-xs text-slate-500">
              Activity is filtered to symbols saved in this watchlist via the unified events and signals APIs.
            </span>
            {canLoadMore ? (
              <Link href={`/watchlists/${watchlistId}?${nextParams.toString()}`} className={primaryButtonClassName}>
                Load more
              </Link>
            ) : (
              <span className="text-sm text-slate-500">No more results.</span>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
