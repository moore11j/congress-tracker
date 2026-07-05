"use client";

import { useMemo, useState, type FormEvent } from "react";
import { FeedCard } from "@/components/feed/FeedCard";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import {
  type EventItem,
  getWatchlistEvents,
  getWatchlistSignals,
  removeFromWatchlist,
  type EventsResponse,
  type SignalItem,
} from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { pillClassName, primaryButtonClassName, selectClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
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
  { value: "government_contracts", label: "Contracts" },
  { value: "signals", label: "Signals" },
];

function buildActivityUrl(watchlistId: number, state: WatchlistActivityState, cursor?: string | null, offset?: number) {
  const params = new URLSearchParams();
  if (state.mode !== "all") params.set("mode", state.mode);
  if (state.recentDays) params.set("recent_days", state.recentDays);
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
  if (!symbol.includes(":")) return symbol.toUpperCase();
  return (symbol.split(":", 2)[1]?.trim() || symbol).toUpperCase();
}

function activityDescription(items: number, tickerCount: number, onlyNew: boolean) {
  if (!tickerCount) return "Add tickers to turn this into a monitoring feed.";
  return onlyNew ? `${items} new items across ${tickerCount} saved tickers.` : `${items} items across ${tickerCount} saved tickers.`;
}

function formatSinceLabel(value: string) {
  return value ? formatDateShort(value) : "the latest checkpoint";
}

function RecentActivitySkeleton() {
  return (
    <div className="space-y-3" aria-hidden="true">
      {Array.from({ length: 3 }).map((_, index) => (
        <div key={index} className="rounded-3xl border border-white/5 bg-slate-900/60 p-5">
          <div className="grid gap-4 md:grid-cols-[minmax(140px,0.9fr)_minmax(170px,1fr)_minmax(125px,0.7fr)_minmax(105px,0.55fr)_minmax(180px,220px)]">
            <SkeletonBlock className="h-5" />
            <SkeletonBlock className="h-5" />
            <SkeletonBlock className="h-5" />
            <SkeletonBlock className="h-5" />
            <div className="space-y-2">
              <SkeletonBlock className="ml-auto h-5 w-28" />
              <SkeletonBlock className="ml-auto h-4 w-20" />
            </div>
          </div>
        </div>
      ))}
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
  canViewPremiumMetrics,
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
  const [draftRecentDays, setDraftRecentDays] = useState(initialState.recentDays);
  const [draftLimit, setDraftLimit] = useState(String(initialState.limit));
  const [data, setData] = useState(initialData);
  const [isLoading, setIsLoading] = useState(false);
  const [removingSymbol, setRemovingSymbol] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const canLoadMore = data.hasMore;
  const nextOffset = state.mode === "signals" ? data.offset : 0;

  const activeUrl = useMemo(() => buildActivityUrl(watchlistId, state), [state, watchlistId]);

  async function fetchActivity(nextState: WatchlistActivityState, append = false) {
    setIsLoading(true);
    setError(null);
    try {
      let nextItems: FeedItem[] = [];
      let nextCursor: string | null = null;
      let nextOffsetValue = 0;

      if (nextState.mode === "signals") {
        const offset = append ? nextOffset : 0;
        const response = await getWatchlistSignals(watchlistId, {
          mode: "all",
          sort: "smart",
          limit: nextState.limit,
          offset,
        });
        nextItems = (response.items as SignalItem[]).map(signalToFeedItem);
        nextOffsetValue = offset + nextItems.length;
      } else if (nextState.onlyNew && !nextState.newSince) {
        nextItems = [];
      } else {
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
        offset: nextOffsetValue,
        hasMore: nextState.mode === "signals" ? nextItems.length === nextState.limit : Boolean(nextCursor),
      }));
      window.history.replaceState(null, "", buildActivityUrl(watchlistId, nextState, append ? nextCursor : null, append ? nextOffsetValue : 0));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load recent activity.");
    } finally {
      setIsLoading(false);
    }
  }

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const numericLimit = Math.min(Math.max(Number(draftLimit) || 25, 1), 100);
    fetchActivity({
      mode: state.mode,
      recentDays: draftRecentDays || "30",
      limit: numericLimit,
      onlyNew: state.onlyNew && state.mode !== "signals",
      newSince: state.newSince,
    });
  }

  function changeMode(mode: ActivityMode) {
    fetchActivity({
      ...state,
      mode,
      onlyNew: mode === "signals" ? false : state.onlyNew,
    });
  }

  function toggleNewOnly() {
    fetchActivity({
      ...state,
      onlyNew: !state.onlyNew,
      newSince: !state.onlyNew ? unseenSince : "",
    });
  }

  async function removeTicker(symbol: string) {
    const normalized = displaySymbol(symbol);
    if (!normalized) return;
    setRemovingSymbol(normalized);
    setError(null);
    try {
      await removeFromWatchlist(watchlistId, normalized);
      setData((current) => ({
        ...current,
        items: current.items.filter((item) => displaySymbol(item.security?.symbol) !== normalized),
      }));
      window.dispatchEvent(new CustomEvent("watchlist:ticker-removed", { detail: { watchlistId, symbol: normalized } }));
    } catch (err) {
      setError(err instanceof Error ? err.message : `Unable to remove ${normalized}.`);
    } finally {
      setRemovingSymbol(null);
    }
  }

  return (
    <div className="max-w-full min-w-0">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-white">Recent activity</h2>
          <p className="text-sm text-slate-400">{activityDescription(data.items.length, tickerCount, state.onlyNew)}</p>
        </div>
        <form onSubmit={applyFilters} className="flex flex-wrap items-end gap-3">
          <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
            Window
            <select value={draftRecentDays} onChange={(event) => setDraftRecentDays(event.target.value)} className={`${selectClassName} min-w-[140px] rounded-lg py-1.5`}>
              <option value="7">Last 7 days</option>
              <option value="30">Last 30 days</option>
              <option value="90">Last 90 days</option>
              <option value="180">Last 180 days</option>
            </select>
          </label>
          <label className="grid gap-1 text-xs font-semibold uppercase tracking-wide text-slate-400">
            Rows
            <select value={draftLimit} onChange={(event) => setDraftLimit(event.target.value)} className={`${selectClassName} min-w-[96px] rounded-lg py-1.5`}>
              <option value="25">25</option>
              <option value="50">50</option>
              <option value="100">100</option>
            </select>
          </label>
          <button type="submit" className={subtlePrimaryButtonClassName} disabled={isLoading}>
            {isLoading ? "Loading..." : "Apply"}
          </button>
        </form>
      </div>

      <div className="mt-5 flex flex-wrap gap-2">
        {modeOptions.map((option) => {
          const active = option.value === state.mode;
          return (
            <button
              key={option.value}
              type="button"
              onClick={() => changeMode(option.value)}
              className={`rounded-lg border px-3 py-1.5 text-sm font-semibold transition ${
                active
                  ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100"
                  : "border-white/10 text-slate-300 hover:border-white/20 hover:text-white"
              }`}
              disabled={isLoading}
            >
              {option.label}
            </button>
          );
        })}
        {state.mode !== "signals" ? (
          <button
            type="button"
            onClick={toggleNewOnly}
            className={`rounded-lg border px-3 py-1.5 text-sm font-semibold transition ${
              state.onlyNew
                ? "border-sky-300/40 bg-sky-300/15 text-sky-100"
                : unseenCount > 0
                  ? "border-white/10 text-slate-300 hover:border-sky-300/40 hover:text-white"
                  : "pointer-events-none border-white/10 text-slate-600"
            }`}
            aria-disabled={!state.onlyNew && unseenCount === 0}
            disabled={isLoading || (!state.onlyNew && unseenCount === 0)}
          >
            {state.onlyNew ? "Showing new" : unseenCount > 0 ? `New only (${unseenCount})` : "No new"}
          </button>
        ) : null}
      </div>
      {state.mode !== "signals" && state.onlyNew ? (
        <div className="mt-3 rounded-xl border border-sky-300/20 bg-sky-300/10 px-3 py-2 text-xs text-sky-100">
          Showing new activity since {formatSinceLabel(state.newSince || unseenSince)}. Switch to All to see every item inside the selected {state.recentDays}-day window.
        </div>
      ) : null}

      <div className="mt-4">
        <SavedViewsBar
          surface="watchlist"
          scopeKey={String(watchlistId)}
          restoreOnLoad={true}
          defaultParams={{ mode: "all", recent_days: "30", limit: "25" }}
          paramKeys={["mode", "recent_days", "limit"]}
          rightSlot={
            <>
              <span className={pillClassName}>
                mode <span className="text-white">{state.mode}</span>
              </span>
              <span className={pillClassName}>
                window <span className="text-white">{state.recentDays}d</span>
              </span>
              <span className={pillClassName}>
                rows <span className="text-white">{state.limit}</span>
              </span>
              {state.mode !== "signals" ? (
                <span className={pillClassName}>
                  new <span className="text-white">{unseenCount}</span>
                </span>
              ) : null}
            </>
          }
        />
      </div>

      <div className="mt-5 w-full min-w-0 max-w-full space-y-4 overflow-x-hidden" aria-busy={isLoading}>
        {error ? (
          <div className="rounded-lg border border-rose-300/20 bg-rose-500/10 p-4 text-sm text-rose-100">{error}</div>
        ) : null}
        {isLoading ? (
          <RecentActivitySkeleton />
        ) : data.items.length === 0 ? (
          <div className="w-full min-w-0 max-w-full rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-6">
            <h3 className="font-semibold text-white">{state.onlyNew ? "No new activity" : "No recent activity yet"}</h3>
            <p className="mt-1 text-sm text-slate-400">
              {state.onlyNew
                ? "Everything in this watchlist has already been checked."
                : "Add liquid tickers or widen the window to catch congressional filings, insider Form 4s, and unusual activity."}
            </p>
          </div>
        ) : (
          data.items.map((item) => {
            const symbol = displaySymbol(item.security?.symbol);
            return (
              <FeedCard
                key={`${item.kind}-${item.id}`}
                item={item}
                density="compact"
                gridPreset="watchlist"
                tickerAction={
                  symbol ? (
                    <button
                      type="button"
                      data-row-action="true"
                      onClick={(event) => {
                        event.preventDefault();
                        event.stopPropagation();
                        removeTicker(symbol);
                      }}
                      className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-white/10 bg-slate-950/50 text-lg font-semibold leading-none text-slate-300 shadow-sm transition hover:border-rose-300/40 hover:bg-rose-300/10 hover:text-rose-100 disabled:opacity-50"
                      aria-label={`Remove ${symbol} from watchlist`}
                      title={`Remove ${symbol} from watchlist`}
                      disabled={removingSymbol === symbol}
                    >
                      -
                    </button>
                  ) : null
                }
                canViewPremiumMetrics={canViewPremiumMetrics}
              />
            );
          })
        )}
      </div>

      <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
        <span className="text-xs text-slate-500">
          Activity is filtered to symbols saved in this watchlist via the unified events and signals workflow.
        </span>
        {canLoadMore ? (
          <button type="button" onClick={() => fetchActivity(state, true)} className={primaryButtonClassName} disabled={isLoading}>
            {isLoading ? "Loading..." : "Load more"}
          </button>
        ) : (
          <span className="text-sm text-slate-500">No more results.</span>
        )}
      </div>
      <span className="sr-only" aria-live="polite">
        {isLoading ? "Recent activity loading" : `Recent activity loaded at ${activeUrl}`}
      </span>
    </div>
  );
}
