"use client";

import { useEffect, useRef, useState } from "react";
import {
  type EventItem,
  getWatchlistEvents,
  getWatchlistSignals,
  type EventsResponse,
  type SignalItem,
} from "@/lib/api";
import type { FeedItem } from "@/lib/types";
import {
  eventToFeedItem,
  resolveWatchlistEventSince,
  signalToFeedItem,
  type ActivityMode,
  type WatchlistActivityState,
} from "@/lib/watchlistActivity";

type RecentActivityData = {
  items: FeedItem[];
  nextCursor: string | null;
  offset: number;
  hasMore: boolean;
};

const modeOptions: { value: ActivityMode; label: string }[] = [
  { value: "all", label: "All" },
  { value: "congress", label: "Congress" },
  { value: "insider", label: "Insiders" },
  { value: "institutional", label: "Institutional" },
  { value: "government_contracts", label: "Contracts" },
  { value: "news", label: "News" },
  { value: "press", label: "Press" },
];

function buildActivityUrl(watchlistId: number, state: WatchlistActivityState, cursor?: string | null, offset?: number) {
  const params = new URLSearchParams();
  if (state.mode !== "all") params.set("mode", state.mode);
  params.set("recent_days", state.recentDays || "30");
  params.set("limit", String(state.limit));
  if (state.onlyNew && state.mode !== "signals" && state.newSince) {
    params.set("only_new", "1");
    params.set("new_since", state.newSince);
  }
  if (state.mode === "signals" && offset) params.set("offset", String(offset));
  if (state.mode !== "signals" && cursor) params.set("cursor", cursor);
  const qs = params.toString();
  return `/watchlists/${watchlistId}${qs ? `?${qs}` : ""}`;
}

function displaySymbol(raw?: string | null): string {
  const symbol = raw?.trim();
  if (!symbol) return "";
  return (symbol.includes(":") ? symbol.split(":", 2)[1] || symbol : symbol).toUpperCase();
}

function formatActivityTime(value?: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "—";
  return parsed.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", timeZone: "UTC" });
}

function readableTrade(value?: string | null) {
  const normalized = (value || "").replaceAll("_", " ").trim();
  return normalized ? normalized.replace(/\b\w/g, (letter) => letter.toUpperCase()) : "Activity detected";
}

function activityCopy(item: FeedItem) {
  const kind = String(item.kind || "").toLowerCase();
  const person = item.insider?.name || item.member?.name || "";
  const hasPerson = person && !/^unknown/i.test(person);

  if (kind.includes("congress")) return { category: "Congress", detail: `Change in trade: ${readableTrade(item.transaction_type)}` };
  if (kind.includes("insider")) return { category: "Insiders", detail: hasPerson ? `${person}: ${readableTrade(item.transaction_type)}` : readableTrade(item.transaction_type) };
  if (kind.includes("institution")) return { category: "Institutional activity", detail: readableTrade(item.transaction_type) };
  if (kind.includes("contract")) return { category: "Large trade / contract", detail: readableTrade(item.transaction_type) };
  if (kind.includes("press")) return { category: "Press releases", detail: item.transaction_type || item.security?.name || "New company release" };
  if (kind.includes("news")) return { category: "News", detail: item.transaction_type || item.security?.name || "New market news" };
  return { category: "Watchlist activity", detail: item.transaction_type || item.security?.name || "New monitoring activity" };
}

function RecentActivitySkeleton() {
  return (
    <div className="divide-y divide-white/10 overflow-hidden rounded-xl border border-white/10" aria-hidden="true">
      {Array.from({ length: 5 }).map((_, index) => <div key={index} className="h-12 animate-pulse bg-slate-900/55" />)}
    </div>
  );
}

export function WatchlistRecentActivity({
  watchlistId,
  tickerCount,
  unseenCount,
  unseenSince,
  initialState,
  initialData,
}: {
  watchlistId: number;
  tickerCount: number;
  unseenCount: number;
  unseenSince: string;
  initialState: WatchlistActivityState;
  initialData: RecentActivityData;
  canViewPremiumMetrics: boolean;
}) {
  const [state, setState] = useState(initialState);
  const [data, setData] = useState(initialData);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const hasRequestedInitialData = useRef(initialData.items.length > 0 || initialState.onlyNew);

  async function fetchActivity(nextState: WatchlistActivityState, append = false) {
    setIsLoading(true);
    setError(null);
    try {
      let nextItems: FeedItem[] = [];
      let nextCursor: string | null = null;
      let nextOffset = 0;
      if (nextState.mode === "signals") {
        const offset = append ? data.offset : 0;
        const response = await getWatchlistSignals(watchlistId, { mode: "all", sort: "smart", limit: nextState.limit, offset });
        nextItems = (response.items as SignalItem[]).map(signalToFeedItem);
        nextOffset = offset + nextItems.length;
      } else if (!(nextState.onlyNew && !nextState.newSince)) {
        const response = await getWatchlistEvents(watchlistId, {
          mode: nextState.mode,
          recent_days: Number(nextState.recentDays),
          since: resolveWatchlistEventSince(nextState),
          unread_only: nextState.onlyNew ? 1 : undefined,
          cursor: append ? data.nextCursor || undefined : undefined,
          limit: nextState.limit,
          source: "WatchlistPage",
        }) as EventsResponse;
        nextItems = (response.items as EventItem[]).map(eventToFeedItem);
        nextCursor = response.next_cursor ?? null;
      }
      setState(nextState);
      setData((current) => ({
        items: append ? [...current.items, ...nextItems] : nextItems,
        nextCursor,
        offset: nextOffset,
        hasMore: nextState.mode === "signals" ? nextItems.length === nextState.limit : Boolean(nextCursor),
      }));
      window.history.replaceState(null, "", buildActivityUrl(watchlistId, nextState, append ? nextCursor : null, append ? nextOffset : 0));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load recent activity.");
    } finally {
      setIsLoading(false);
    }
  }

  function changeMode(mode: ActivityMode) {
    fetchActivity({ ...state, mode, onlyNew: mode === "signals" ? false : state.onlyNew });
  }

  useEffect(() => {
    if (hasRequestedInitialData.current) return;
    hasRequestedInitialData.current = true;
    void fetchActivity(initialState);
    // The current watchlist route is the only intentional initial request.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [watchlistId]);

  return (
    <section className="min-w-0 rounded-2xl border border-white/10 bg-slate-900/70 p-4 shadow-card">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Recent activity</h2>
          <p className="text-sm text-slate-400">{data.items.length} events across {tickerCount} saved tickers</p>
        </div>
        <button type="button" className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 text-slate-300" aria-label="Activity filters" title="Activity filters">
          ≡
        </button>
      </div>

      <div className="mt-3 flex flex-wrap gap-1.5">
        {modeOptions.map((option) => {
          const active = option.value === state.mode;
          return <button key={option.value} type="button" onClick={() => changeMode(option.value)} disabled={isLoading} className={`rounded-md border px-2.5 py-1 text-xs font-semibold transition ${active ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100" : "border-white/10 text-slate-300 hover:border-white/20 hover:text-white"}`}>{option.label}</button>;
        })}
        {unseenCount > 0 && state.mode !== "signals" ? <button type="button" onClick={() => fetchActivity({ ...state, onlyNew: !state.onlyNew, newSince: !state.onlyNew ? unseenSince : "" })} disabled={isLoading} className={`rounded-md border px-2.5 py-1 text-xs font-semibold ${state.onlyNew ? "border-sky-300/40 bg-sky-300/15 text-sky-100" : "border-white/10 text-slate-300"}`}>{state.onlyNew ? "New only" : `New (${unseenCount})`}</button> : null}
      </div>
      {state.onlyNew ? <p className="mt-2 text-xs text-sky-100">Showing new activity since the latest checkpoint. Switch to All to see every item inside the selected {state.recentDays}-day window.</p> : null}

      <div className="mt-3" aria-busy={isLoading}>
        {error ? <div className="rounded-lg border border-rose-300/20 bg-rose-500/10 p-3 text-sm text-rose-100">{error}</div> : null}
        {isLoading && data.items.length === 0 ? <RecentActivitySkeleton /> : data.items.length === 0 ? (
          <div className="rounded-xl border border-dashed border-white/15 px-3 py-5 text-sm text-slate-400">No recent activity for this view.</div>
        ) : (
          <div className="divide-y divide-white/10 overflow-hidden rounded-xl border border-white/10">
            {data.items.map((item) => {
              const activity = activityCopy(item);
              const symbol = displaySymbol(item.security?.symbol);
              return (
                <div key={`${item.kind}-${item.id}`} className="grid min-w-0 grid-cols-[minmax(7.5rem,.85fr)_minmax(0,1fr)_auto_auto] items-center gap-3 bg-white/[0.02] px-3 py-2.5 text-sm">
                  <div className="min-w-0 truncate font-semibold text-slate-100">{activity.category}</div>
                  <div className="min-w-0 truncate text-slate-400">{activity.detail}</div>
                  {symbol ? <span className="rounded border border-emerald-300/25 bg-emerald-300/10 px-1.5 py-0.5 font-mono text-xs font-semibold text-emerald-200">{symbol}</span> : <span />}
                  <time className="whitespace-nowrap text-xs text-slate-500">{formatActivityTime(item.report_date || item.trade_date)}</time>
                </div>
              );
            })}
          </div>
        )}
      </div>
      <div className="mt-3 flex items-center justify-between gap-3">
        <span className="text-xs text-slate-500">Last {state.recentDays} days</span>
        {data.hasMore ? <button type="button" onClick={() => fetchActivity(state, true)} disabled={isLoading} className="text-sm font-semibold text-sky-300 hover:text-sky-200">View all activity →</button> : null}
      </div>
    </section>
  );
}
