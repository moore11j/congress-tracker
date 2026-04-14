"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import {
  getEntitlements,
  getEvents,
  getSignalsAll,
  getWatchlistEvents,
  type EventItem,
  type SignalItem,
  type SignalMode,
  type SignalSort,
} from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import type { WatchlistSummary } from "@/lib/types";
import { compactInteractiveSurfaceClassName, compactInteractiveTitleClassName } from "@/lib/styles";
import {
  markSavedViewSeen,
  parseSavedViewsStore,
  saveSavedViewsStore,
  savedViewHref,
  savedViewsStorageKey,
  type SavedView,
  type SavedViewsStore,
} from "@/lib/savedViews";

const secondaryActionClassName =
  "inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white";
const primaryActionClassName =
  "inline-flex h-10 items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20";

type SourceStatus = "idle" | "loading" | "ready";

type SavedViewStatus = {
  view: SavedView;
  unseenCount: number;
  latest: MonitoredEvent[];
  status: SourceStatus;
  error?: string | null;
};

type MonitoredEvent = {
  id: string;
  ts: string;
  symbol?: string | null;
  title: string;
  sourceName: string;
  sourceHref: string;
  sourceType: "watchlist" | "saved-view";
  savedViewId?: string;
  smartScore?: number | null;
};

type MonitoringDashboardProps = {
  initialWatchlists: WatchlistSummary[];
};

function isNewer(ts: string | null | undefined, checkpoint: string | null | undefined) {
  if (!ts || !checkpoint) return false;
  return new Date(ts).getTime() > new Date(checkpoint).getTime();
}

function sourceHrefForWatchlist(watchlist: WatchlistSummary) {
  const params = new URLSearchParams({ mode: "all", recent_days: "30", limit: "25" });
  if ((watchlist.unseen_count ?? 0) > 0 && watchlist.unseen_since) {
    params.set("only_new", "1");
    params.set("new_since", watchlist.unseen_since);
  }
  return `/watchlists/${watchlist.id}?${params.toString()}`;
}

function eventTitle(event: EventItem) {
  const payload = event.payload ?? {};
  const symbol = event.symbol ?? event.ticker ?? payload.symbol ?? payload.ticker;
  const name =
    event.member_name ??
    payload.member?.name ??
    payload.insider_name ??
    payload.insiderName ??
    event.source ??
    event.event_type;
  const tradeType = event.trade_type ?? payload.transaction_type ?? payload.transactionType ?? payload.raw?.transactionType;
  return [symbol, name, tradeType].filter(Boolean).join(" · ") || event.headline || event.summary || event.event_type;
}

function signalTitle(signal: SignalItem) {
  return [signal.symbol, signal.who, signal.trade_type].filter(Boolean).join(" · ") || "Unusual signal";
}

function eventToMonitoredEvent(
  event: EventItem,
  sourceName: string,
  sourceHref: string,
  sourceType: MonitoredEvent["sourceType"],
  savedViewId?: string,
): MonitoredEvent {
  return {
    id: `${sourceType}:event:${sourceName}:${event.id}`,
    ts: event.ts,
    symbol: event.symbol ?? event.ticker ?? null,
    title: eventTitle(event),
    sourceName,
    sourceHref,
    sourceType,
    savedViewId,
    smartScore: event.smart_score ?? null,
  };
}

function signalToMonitoredEvent(signal: SignalItem, sourceName: string, sourceHref: string, savedViewId?: string): MonitoredEvent {
  return {
    id: `saved-view:signal:${sourceName}:${signal.event_id}`,
    ts: signal.ts,
    symbol: signal.symbol ?? null,
    title: signalTitle(signal),
    sourceName,
    sourceHref,
    sourceType: "saved-view",
    savedViewId,
    smartScore: signal.smart_score ?? null,
  };
}

function validSignalMode(value: string | undefined): SignalMode {
  return value === "congress" || value === "insider" ? value : "all";
}

function validSignalSort(value: string | undefined): SignalSort {
  return value === "multiple" || value === "recent" || value === "amount" ? value : "smart";
}

function useSavedViews() {
  const [store, setStore] = useState<SavedViewsStore | null>(null);

  useEffect(() => {
    setStore(parseSavedViewsStore(window.localStorage.getItem(savedViewsStorageKey)));
  }, []);

  const markSeen = (viewId: string) => {
    setStore((current) => {
      const next = markSavedViewSeen(current ?? parseSavedViewsStore(window.localStorage.getItem(savedViewsStorageKey)), viewId);
      saveSavedViewsStore(next);
      return next;
    });
  };

  return { store, markSeen };
}

export function MonitoringDashboard({ initialWatchlists }: MonitoringDashboardProps) {
  const { store, markSeen } = useSavedViews();
  const [watchlistLatest, setWatchlistLatest] = useState<MonitoredEvent[]>([]);
  const [savedStatuses, setSavedStatuses] = useState<SavedViewStatus[]>([]);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);

  const savedViews = useMemo(() => store?.views ?? [], [store]);
  const canUseMonitoringSources = hasEntitlement(entitlements, "monitoring_sources");
  const monitoringLimit = canUseMonitoringSources ? limitFor(entitlements, "monitoring_sources") : 0;
  const visibleWatchlists = useMemo(() => initialWatchlists.slice(0, monitoringLimit), [initialWatchlists, monitoringLimit]);
  const remainingSourceSlots = Math.max(monitoringLimit - visibleWatchlists.length, 0);
  const visibleSavedViews = useMemo(() => savedViews.slice(0, remainingSourceSlots), [remainingSourceSlots, savedViews]);
  const hiddenSourceCount = Math.max(initialWatchlists.length + savedViews.length - monitoringLimit, 0);
  const totalWatchlistNew = visibleWatchlists.reduce((sum, item) => sum + Math.max(item.unseen_count ?? 0, 0), 0);
  const totalSavedViewNew = savedStatuses.reduce((sum, item) => sum + item.unseenCount, 0);

  useEffect(() => {
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadWatchlists() {
      const active = visibleWatchlists.filter((watchlist) => (watchlist.unseen_count ?? 0) > 0 && watchlist.unseen_since);
      const chunks = await Promise.all(
        active.slice(0, 6).map(async (watchlist) => {
          try {
            const href = sourceHrefForWatchlist(watchlist);
            const data = await getWatchlistEvents(watchlist.id, { since: watchlist.unseen_since ?? undefined, limit: 3 });
            return data.items.map((event) => eventToMonitoredEvent(event, watchlist.name, href, "watchlist"));
          } catch {
            return [];
          }
        }),
      );
      if (!cancelled) setWatchlistLatest(chunks.flat());
    }

    loadWatchlists();
    return () => {
      cancelled = true;
    };
  }, [visibleWatchlists]);

  useEffect(() => {
    let cancelled = false;
    setSavedStatuses(
      visibleSavedViews.map((view) => ({
        view,
        unseenCount: 0,
        latest: [],
        status: view.lastSeenAt ? "loading" : "ready",
        error: null,
      })),
    );

    async function loadSavedViews() {
      const statuses = await Promise.all(
        visibleSavedViews.map(async (view): Promise<SavedViewStatus> => {
          const href = savedViewHref(view);
          if (!view.lastSeenAt) {
            return { view, unseenCount: 0, latest: [], status: "ready" };
          }

          try {
            if (view.surface === "signals") {
              const data = await getSignalsAll({
                mode: validSignalMode(view.params.mode),
                side: view.params.side,
                sort: validSignalSort(view.params.sort),
                symbol: view.params.symbol,
                limit: 100,
              });
              const newItems = data.items.filter((item) => isNewer(item.ts, view.lastSeenAt));
              return {
                view,
                unseenCount: newItems.length,
                latest: newItems.slice(0, 3).map((item) => signalToMonitoredEvent(item, view.name, href, view.id)),
                status: "ready",
              };
            }

            if (view.surface === "watchlist" && view.scopeKey) {
              const id = Number(view.scopeKey);
              if (!Number.isFinite(id)) return { view, unseenCount: 0, latest: [], status: "ready" };
              const data = await getWatchlistEvents(id, {
                mode: view.params.mode,
                since: view.lastSeenAt,
                limit: 100,
              });
              return {
                view,
                unseenCount: typeof data.total === "number" ? data.total : data.items.length,
                latest: data.items.slice(0, 3).map((event) => eventToMonitoredEvent(event, view.name, href, "saved-view", view.id)),
                status: "ready",
              };
            }

            const feedMode = view.params.mode === "congress" || view.params.mode === "insider" ? view.params.mode : "all";
            const params = { ...view.params, mode: undefined, tape: feedMode, since: view.lastSeenAt, limit: 100 };
            const data = await getEvents(params);
            return {
              view,
              unseenCount: typeof data.total === "number" ? data.total : data.items.length,
              latest: data.items.slice(0, 3).map((event) => eventToMonitoredEvent(event, view.name, href, "saved-view", view.id)),
              status: "ready",
            };
          } catch (error) {
            return {
              view,
              unseenCount: 0,
              latest: [],
              status: "ready",
              error: error instanceof Error ? error.message : "Unable to load saved view.",
            };
          }
        }),
      );

      if (!cancelled) setSavedStatuses(statuses);
    }

    loadSavedViews();
    return () => {
      cancelled = true;
    };
  }, [visibleSavedViews]);

  const latestImportant = [...watchlistLatest, ...savedStatuses.flatMap((status) => status.latest)]
    .sort((a, b) => {
      const scoreDelta = (b.smartScore ?? 0) - (a.smartScore ?? 0);
      if (scoreDelta !== 0) return scoreDelta;
      return new Date(b.ts).getTime() - new Date(a.ts).getTime();
    })
    .slice(0, 8);

  return (
    <div className="space-y-6">
      <section className="grid gap-3 sm:grid-cols-3">
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Watchlists</div>
          <div className="mt-2 text-3xl font-semibold text-white">{totalWatchlistNew}</div>
          <p className="mt-1 text-sm text-slate-400">new items</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Saved views</div>
          <div className="mt-2 text-3xl font-semibold text-white">{totalSavedViewNew}</div>
          <p className="mt-1 text-sm text-slate-400">new items</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Sources</div>
          <div className="mt-2 text-3xl font-semibold text-white">{visibleWatchlists.length + visibleSavedViews.length}</div>
          <p className="mt-1 text-sm text-slate-400">monitored</p>
        </div>
      </section>

      {hiddenSourceCount > 0 ? (
        <UpgradePrompt
          title="Monitor every source with Premium"
          body={`Free monitors ${monitoringLimit} sources in the inbox. ${hiddenSourceCount} saved source${hiddenSourceCount === 1 ? " is" : "s are"} waiting behind the Premium limit.`}
        />
      ) : null}

      <section className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-white">Monitored sources</h2>
              <p className="text-sm text-slate-400">Open a row to clear its current checkpoint.</p>
            </div>
            <Link href="/watchlists" prefetch={false} className={secondaryActionClassName}>
              Manage watchlists
            </Link>
          </div>

          <div className="mt-4 divide-y divide-white/10">
            {visibleWatchlists.map((watchlist) => {
              const count = Math.max(watchlist.unseen_count ?? 0, 0);
              return (
                <Link
                  key={`watchlist-${watchlist.id}`}
                  href={sourceHrefForWatchlist(watchlist)}
                  prefetch={false}
                  className={`${compactInteractiveSurfaceClassName} grid gap-2 rounded-2xl px-4 py-3 text-sm sm:grid-cols-[1fr_auto_auto] sm:items-center`}
                >
                  <div>
                    <div className={`font-medium ${compactInteractiveTitleClassName}`}>{watchlist.name}</div>
                    <div className="text-xs text-slate-500">Watchlist #{watchlist.id}</div>
                  </div>
                  <span className={`w-fit rounded-lg border px-2.5 py-1 text-xs font-semibold ${count > 0 ? "border-emerald-300/30 bg-emerald-300/15 text-emerald-100" : "border-white/10 text-slate-400"}`}>
                    {count > 0 ? `${count} new` : "0 new"}
                  </span>
                  <span className="text-sm text-slate-400">Open</span>
                </Link>
              );
            })}
            {savedStatuses.map((status) => {
              const href = savedViewHref(status.view);
              return (
                <Link
                  key={`saved-${status.view.id}`}
                  href={href}
                  prefetch={false}
                  onClick={() => markSeen(status.view.id)}
                  className="grid gap-2 py-3 transition hover:bg-white/[0.03] sm:grid-cols-[1fr_auto_auto] sm:items-center"
                >
                  <div>
                    <div className="font-medium text-white">{status.view.name}</div>
                    <div className="text-xs text-slate-500">
                      Saved view · {status.view.surface}
                      {status.error ? " · refresh failed" : ""}
                    </div>
                  </div>
                  <span className={`w-fit rounded-lg border px-2.5 py-1 text-xs font-semibold ${status.unseenCount > 0 ? "border-sky-300/30 bg-sky-300/15 text-sky-100" : "border-white/10 text-slate-400"}`}>
                    {status.status === "loading" ? "checking" : `${status.unseenCount} new`}
                  </span>
                  <span className="text-sm text-slate-400">Open</span>
                </Link>
              );
            })}
            {visibleWatchlists.length === 0 && visibleSavedViews.length === 0 ? (
              <div className="py-8 text-sm text-slate-400">Create a watchlist or save a view to start monitoring from here.</div>
            ) : null}
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-white">Latest important</h2>
              <p className="text-sm text-slate-400">New items ranked by smart score, then recency.</p>
            </div>
            <Link href="/signals" className={primaryActionClassName}>
              Signals
            </Link>
          </div>

          <div className="mt-4 space-y-3">
            {latestImportant.length === 0 ? (
              <div className="rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-5">
                <h3 className="font-semibold text-white">Nothing new to triage</h3>
                <p className="mt-1 text-sm text-slate-400">Rows with new items will bubble their strongest recent events here.</p>
              </div>
            ) : (
              latestImportant.map((item) => (
                <Link
                  key={item.id}
                  href={item.sourceHref}
                  prefetch={false}
                  onClick={() => {
                    if (item.savedViewId) markSeen(item.savedViewId);
                  }}
                  className="block rounded-lg border border-white/10 bg-slate-950/40 p-3 transition hover:border-emerald-300/40"
                >
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <span className="font-medium text-white">{item.title}</span>
                    {typeof item.smartScore === "number" ? (
                      <span className="rounded-lg border border-emerald-300/25 bg-emerald-300/10 px-2 py-0.5 text-xs text-emerald-100">
                        smart {item.smartScore}
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-500">
                    <span>{item.sourceName}</span>
                    <span>{item.sourceType === "watchlist" ? "watchlist" : "saved view"}</span>
                    <span>{new Date(item.ts).toLocaleString()}</span>
                  </div>
                </Link>
              ))
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
