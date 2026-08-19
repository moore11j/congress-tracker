"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
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
import { insiderHref } from "@/lib/insider";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";

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

type ActivitySort = "newest" | "type" | "ticker";

const activitySortOptions: { value: ActivitySort; label: string }[] = [
  { value: "newest", label: "Newest first" },
  { value: "type", label: "Activity type" },
  { value: "ticker", label: "Ticker A–Z" },
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

function formatActivityDate(value?: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "—";
  return parsed.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

function readableTrade(value?: string | null) {
  const normalized = (value || "").replaceAll("_", " ").trim();
  return normalized ? normalized.replace(/\b\w/g, (letter) => letter.toUpperCase()) : "Activity detected";
}

function activityCopy(item: FeedItem) {
  const kind = String(item.kind || "").toLowerCase();

  if (kind.includes("congress")) return { category: "Congress", detail: `Change in trade: ${readableTrade(item.transaction_type)}` };
  if (kind.includes("insider")) return { category: "Insiders", detail: readableTrade(item.transaction_type) };
  if (kind.includes("institution")) return { category: "Institutional activity", detail: readableTrade(item.transaction_type) };
  if (kind.includes("contract")) return { category: "Large trade / contract", detail: readableTrade(item.transaction_type) };
  if (kind.includes("press")) return { category: "Press releases", detail: item.transaction_type || item.security?.name || "New company release" };
  if (kind.includes("news")) return { category: "News", detail: item.transaction_type || item.security?.name || "New market news" };
  return { category: "Watchlist activity", detail: item.transaction_type || item.security?.name || "New monitoring activity" };
}

function activityTimestamp(item: FeedItem) {
  const value = item.report_date || item.trade_date;
  const timestamp = value ? new Date(value).getTime() : 0;
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function activityIcon(category: string) {
  const className = "h-4 w-4 shrink-0 fill-none stroke-current stroke-[1.8]";
  if (category === "Congress") return <svg viewBox="0 0 24 24" className={className} aria-hidden="true"><path d="M3 10h18M5 10v8m4-8v8m6-8v8m4-8v8M3 21h18M12 3l9 5H3l9-5Z" /></svg>;
  if (category === "Insiders") return <svg viewBox="0 0 24 24" className={className} aria-hidden="true"><circle cx="12" cy="8" r="3" /><path d="M5 21a7 7 0 0 1 14 0M3 13h2m14 0h2" /></svg>;
  if (category === "Institutional activity") return <svg viewBox="0 0 24 24" className={className} aria-hidden="true"><path d="M4 21h16M6 21V8l6-4 6 4v13M9 11h2m2 0h2m-6 4h2m2 0h2" /></svg>;
  if (category === "Large trade / contract") return <svg viewBox="0 0 24 24" className={className} aria-hidden="true"><path d="M4 7h11m0 0-3-3m3 3-3 3M20 17H9m0 0 3-3m-3 3 3 3" /></svg>;
  if (category === "Press releases") return <svg viewBox="0 0 24 24" className={className} aria-hidden="true"><path d="M4 5h12v14H4zM8 9h4m-4 4h4m8-5v8m0 0-3-3m3 3 3-3" /></svg>;
  return <svg viewBox="0 0 24 24" className={className} aria-hidden="true"><path d="M5 4h14v16H5zM8 8h8M8 12h8M8 16h5" /></svg>;
}

function safeExternalHref(value?: string | null) {
  return value && /^https?:\/\//i.test(value) ? value : null;
}

function ActivityDetail({ item, category, detail }: { item: FeedItem; category: string; detail: string }) {
  const memberName = item.member?.name?.trim();
  const insiderName = item.insider?.name?.trim();
  const personName = category === "Insiders" ? insiderName : memberName;
  const personHref = category === "Congress"
    ? memberHref({ name: memberName, memberId: item.member?.bioguide_id })
    : category === "Insiders"
      ? insiderHref(insiderName, item.insider?.reporting_cik)
      : null;
  const articleHref = safeExternalHref(item.url);

  if (articleHref && (category === "News" || category === "Press releases")) {
    return <a href={articleHref} target="_blank" rel="noreferrer" className="block truncate text-sky-300 transition hover:text-sky-100 hover:underline" title={detail}>{detail}</a>;
  }
  if (personName && personHref && (category === "Congress" || category === "Insiders")) {
    return <span className="block truncate text-slate-400"><Link href={personHref} prefetch={false} className="font-medium text-slate-200 hover:text-emerald-200 hover:underline">{personName}</Link>{" · "}{detail}</span>;
  }
  return <span className="block truncate text-slate-400" title={detail}>{detail}</span>;
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
  const [pageIndex, setPageIndex] = useState(0);
  const [pageCursors, setPageCursors] = useState<Array<string | null>>([null]);
  const [sort, setSort] = useState<ActivitySort>("newest");
  const [sortMenuOpen, setSortMenuOpen] = useState(false);
  const hasRequestedInitialData = useRef(initialData.items.length > 0 || initialState.onlyNew);

  const sortedItems = useMemo(() => [...data.items].sort((left, right) => {
    if (sort === "type") {
      const category = activityCopy(left).category.localeCompare(activityCopy(right).category);
      return category || activityTimestamp(right) - activityTimestamp(left);
    }
    if (sort === "ticker") {
      const ticker = displaySymbol(left.security?.symbol).localeCompare(displaySymbol(right.security?.symbol));
      return ticker || activityTimestamp(right) - activityTimestamp(left);
    }
    return activityTimestamp(right) - activityTimestamp(left);
  }), [data.items, sort]);

  async function fetchActivity(nextState: WatchlistActivityState, cursor: string | null = null, nextPageIndex = 0) {
    setIsLoading(true);
    setError(null);
    try {
      let nextItems: FeedItem[] = [];
      let nextCursor: string | null = null;
      let nextOffset = 0;
      if (nextState.mode === "signals") {
        const offset = nextPageIndex * nextState.limit;
        const response = await getWatchlistSignals(watchlistId, { mode: "all", sort: "smart", limit: nextState.limit, offset });
        nextItems = (response.items as SignalItem[]).map(signalToFeedItem);
        nextOffset = offset + nextItems.length;
      } else if (!(nextState.onlyNew && !nextState.newSince)) {
        const response = await getWatchlistEvents(watchlistId, {
          mode: nextState.mode,
          recent_days: Number(nextState.recentDays),
          since: resolveWatchlistEventSince(nextState),
          unread_only: nextState.onlyNew ? 1 : undefined,
          cursor: cursor || undefined,
          limit: nextState.limit,
          source: "WatchlistPage",
        }) as EventsResponse;
        nextItems = (response.items as EventItem[]).map(eventToFeedItem);
        nextCursor = response.next_cursor ?? null;
      }
      setState(nextState);
      setPageIndex(nextPageIndex);
      setData({
        items: nextItems,
        nextCursor,
        offset: nextOffset,
        hasMore: nextState.mode === "signals" ? nextItems.length === nextState.limit : Boolean(nextCursor),
      });
      window.history.replaceState(null, "", buildActivityUrl(watchlistId, nextState, nextState.mode === "signals" ? null : cursor, nextState.mode === "signals" ? nextPageIndex * nextState.limit : 0));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load recent activity.");
    } finally {
      setIsLoading(false);
    }
  }

  function changeMode(mode: ActivityMode) {
    setPageCursors([null]);
    fetchActivity({ ...state, mode, onlyNew: mode === "signals" ? false : state.onlyNew });
  }

  function goToNextPage() {
    if (!data.hasMore || isLoading) return;
    const nextIndex = pageIndex + 1;
    const cursor = state.mode === "signals" ? null : data.nextCursor;
    if (state.mode !== "signals" && !cursor) return;
    setPageCursors((current) => {
      const next = [...current];
      next[nextIndex] = cursor;
      return next;
    });
    void fetchActivity(state, cursor, nextIndex);
  }

  function goToPreviousPage() {
    if (pageIndex === 0 || isLoading) return;
    const previousIndex = pageIndex - 1;
    void fetchActivity(state, state.mode === "signals" ? null : pageCursors[previousIndex] ?? null, previousIndex);
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
        <div className="relative">
          <button
            type="button"
            onClick={() => setSortMenuOpen((open) => !open)}
            className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 text-slate-300 transition hover:border-emerald-300/40 hover:text-emerald-100"
            aria-label="Sort recent activity"
            aria-haspopup="menu"
            aria-expanded={sortMenuOpen}
            title="Sort recent activity"
          >
            <svg viewBox="0 0 24 24" className="h-4 w-4 fill-none stroke-current stroke-2" aria-hidden="true"><path d="M4 7h16M7 12h10m-7 5h4" strokeLinecap="round" /></svg>
          </button>
          {sortMenuOpen ? (
            <div role="menu" aria-label="Sort recent activity" className="absolute right-0 top-[calc(100%+0.5rem)] z-20 w-40 rounded-lg border border-white/10 bg-slate-950 p-1 shadow-xl">
              {activitySortOptions.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="menuitemradio"
                  aria-checked={sort === option.value}
                  onClick={() => { setSort(option.value); setSortMenuOpen(false); }}
                  className={`flex w-full items-center justify-between rounded-md px-2.5 py-2 text-left text-xs font-semibold transition ${sort === option.value ? "bg-emerald-300/15 text-emerald-100" : "text-slate-300 hover:bg-white/[0.06] hover:text-white"}`}
                >
                  {option.label}
                  {sort === option.value ? <span aria-hidden="true">✓</span> : null}
                </button>
              ))}
            </div>
          ) : null}
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-1.5">
        {modeOptions.map((option) => {
          const active = option.value === state.mode;
          return <button key={option.value} type="button" onClick={() => changeMode(option.value)} disabled={isLoading} className={`rounded-md border px-2.5 py-1 text-xs font-semibold transition ${active ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100" : "border-white/10 text-slate-300 hover:border-white/20 hover:text-white"}`}>{option.label}</button>;
        })}
        {unseenCount > 0 && state.mode !== "signals" ? <button type="button" onClick={() => { setPageCursors([null]); void fetchActivity({ ...state, onlyNew: !state.onlyNew, newSince: !state.onlyNew ? unseenSince : "" }); }} disabled={isLoading} className={`rounded-md border px-2.5 py-1 text-xs font-semibold ${state.onlyNew ? "border-sky-300/40 bg-sky-300/15 text-sky-100" : "border-white/10 text-slate-300"}`}>{state.onlyNew ? "New only" : `New (${unseenCount})`}</button> : null}
      </div>
      {state.onlyNew ? <p className="mt-2 text-xs text-sky-100">Showing new activity since the latest checkpoint. Switch to All to see every item inside the selected {state.recentDays}-day window.</p> : null}

      <div className="mt-3" aria-busy={isLoading}>
        {error ? <div className="rounded-lg border border-rose-300/20 bg-rose-500/10 p-3 text-sm text-rose-100">{error}</div> : null}
        {isLoading && data.items.length === 0 ? <RecentActivitySkeleton /> : sortedItems.length === 0 ? (
          <div className="rounded-xl border border-dashed border-white/15 px-3 py-5 text-sm text-slate-400">No recent activity for this view.</div>
        ) : (
          <div className="divide-y divide-white/10 overflow-hidden rounded-xl border border-white/10">
            {sortedItems.map((item) => {
              const activity = activityCopy(item);
              const symbol = displaySymbol(item.security?.symbol);
              const symbolHref = tickerHref(symbol);
              return (
                <div key={`${item.kind}-${item.id}`} className="grid min-w-0 grid-cols-[auto_auto_minmax(0,1fr)_auto_auto] items-center gap-x-2.5 gap-y-1 bg-white/[0.02] px-3 py-2.5 text-sm">
                  <span className="text-slate-300" title={activity.category}>{activityIcon(activity.category)}</span>
                  <div className="min-w-0 whitespace-nowrap font-semibold text-slate-100">{activity.category}</div>
                  <div className="min-w-0"><ActivityDetail item={item} category={activity.category} detail={activity.detail} /></div>
                  {symbolHref ? <Link href={symbolHref} prefetch={false} className="rounded border border-emerald-300/25 bg-emerald-300/10 px-1.5 py-0.5 font-mono text-xs font-semibold text-emerald-200 transition hover:border-emerald-200/60 hover:text-emerald-100">{symbol}</Link> : symbol ? <span className="rounded border border-emerald-300/25 bg-emerald-300/10 px-1.5 py-0.5 font-mono text-xs font-semibold text-emerald-200">{symbol}</span> : <span />}
                  <time className="whitespace-nowrap text-xs text-slate-500">{formatActivityDate(item.report_date || item.trade_date)} · {formatActivityTime(item.report_date || item.trade_date)}</time>
                </div>
              );
            })}
          </div>
        )}
      </div>
      <div className="mt-3 flex items-center justify-between gap-3">
        <span className="text-xs text-slate-500">Last {state.recentDays} days · 20 per page</span>
        <div className="flex items-center gap-2 text-xs">
          <button type="button" onClick={goToPreviousPage} disabled={pageIndex === 0 || isLoading} className="rounded-md border border-white/10 px-2 py-1 font-semibold text-slate-300 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:opacity-40">Previous</button>
          <span className="min-w-12 text-center text-slate-400">Page {pageIndex + 1}</span>
          <button type="button" onClick={goToNextPage} disabled={!data.hasMore || isLoading} className="rounded-md border border-white/10 px-2 py-1 font-semibold text-sky-300 transition hover:border-sky-300/35 hover:text-sky-100 disabled:cursor-not-allowed disabled:opacity-40">Next</button>
        </div>
      </div>
    </section>
  );
}
