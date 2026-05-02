"use client";

import { useMemo, useState, type FormEvent } from "react";
import { FeedCard } from "@/components/feed/FeedCard";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import {
  getWatchlistEvents,
  getWatchlistSignals,
  removeFromWatchlist,
  type EventItem,
  type EventsResponse,
  type SignalItem,
} from "@/lib/api";
import { formatCompanyName } from "@/lib/companyName";
import { pillClassName, primaryButtonClassName, selectClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import type { FeedItem } from "@/lib/types";

type ActivityMode = "all" | "congress" | "insider" | "government_contracts" | "signals";

type RecentActivityState = {
  mode: ActivityMode;
  recentDays: string;
  limit: number;
  onlyNew: boolean;
  newSince: string;
};

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

function buildActivityUrl(watchlistId: number, state: RecentActivityState, cursor?: string | null, offset?: number) {
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
}: {
  watchlistId: number;
  tickerCount: number;
  unseenCount: number;
  unseenSince: string;
  initialState: RecentActivityState;
  initialData: RecentActivityData;
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

  async function fetchActivity(nextState: RecentActivityState, append = false) {
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
          since: nextState.onlyNew ? nextState.newSince : recentDaysToSince(nextState.recentDays),
          cursor: append ? data.nextCursor || undefined : undefined,
          limit: nextState.limit,
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
              />
            );
          })
        )}
      </div>

      <div className="mt-5 flex flex-wrap items-center justify-between gap-3">
        <span className="text-xs text-slate-500">
          Activity is filtered to symbols saved in this watchlist via the unified events and signals APIs.
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
