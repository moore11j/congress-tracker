"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import {
  getEntitlements,
  getEvents,
  getMonitoringInbox,
  getSignalsAll,
  listWatchlists,
  markMonitoringItemsRead,
  markMonitoringItemsUnread,
  listSavedScreenEvents,
  getWatchlistConfirmationEvents,
  getWatchlistEvents,
  type EventItem,
  type SignalItem,
  type SignalMode,
  type SignalSort,
} from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import type { ConfirmationMonitoringEvent, MonitoringAlert, MonitoringInboxResponse, SavedScreenEvent, WatchlistSummary } from "@/lib/types";
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
  scoreLabel?: string | null;
  body?: string | null;
  alertId?: number;
  readAt?: string | null;
};

type MonitoringDashboardProps = {
  initialWatchlists: WatchlistSummary[];
};

type InboxFilter = "all" | "unread" | "read";

function isNewer(ts: string | null | undefined, checkpoint: string | null | undefined) {
  if (!ts || !checkpoint) return false;
  return new Date(ts).getTime() > new Date(checkpoint).getTime();
}

function sourceHrefForWatchlist(watchlist: WatchlistSummary) {
  const params = new URLSearchParams({ mode: "all", recent_days: "30", limit: "25" });
  const unreadCount = Math.max(Number(watchlist.unread_count ?? watchlist.unseen_count) || 0, 0);
  if (unreadCount > 0 && watchlist.unseen_since) {
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
    scoreLabel: typeof event.smart_score === "number" ? `smart ${event.smart_score}` : null,
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
    scoreLabel: typeof signal.smart_score === "number" ? `smart ${signal.smart_score}` : null,
  };
}

function confirmationEventToMonitoredEvent(event: ConfirmationMonitoringEvent, sourceName: string): MonitoredEvent {
  const delta =
    typeof event.score_before === "number" && typeof event.score_after === "number"
      ? event.score_after - event.score_before
      : null;
  const scoreLabel = delta && delta !== 0 ? `score ${delta > 0 ? "+" : ""}${delta}` : `score ${event.score_after}`;
  return {
    id: `watchlist:confirmation:${event.watchlist_id}:${event.id}`,
    ts: event.created_at,
    symbol: event.ticker,
    title: event.title,
    body: event.body ?? null,
    sourceName,
    sourceHref: `/ticker/${encodeURIComponent(event.ticker)}`,
    sourceType: "watchlist",
    smartScore: event.score_after ?? null,
    scoreLabel,
  };
}

function savedScreenEventToMonitoredEvent(event: SavedScreenEvent): MonitoredEvent {
  const after = event.after_snapshot;
  const before = event.before_snapshot;
  const delta =
    typeof before?.confirmation_score === "number" && typeof after?.confirmation_score === "number"
      ? after.confirmation_score - before.confirmation_score
      : typeof after?.confirmation_score === "number"
        ? after.confirmation_score
        : null;
  const scoreLabel = delta && delta !== 0 ? `score ${delta > 0 ? "+" : ""}${delta}` : after ? `score ${after.confirmation_score}` : null;
  return {
    id: `saved-screen:event:${event.saved_screen_id}:${event.id}`,
    ts: event.created_at,
    symbol: event.ticker,
    title: event.title,
    body: event.description,
    sourceName: event.screen_name ?? "Saved screen",
    sourceHref: `/ticker/${encodeURIComponent(event.ticker)}`,
    sourceType: "saved-view",
    smartScore: after?.confirmation_score ?? null,
    scoreLabel,
  };
}

function monitoringAlertToMonitoredEvent(alert: MonitoringAlert): MonitoredEvent {
  return {
    id: `monitoring-alert:${alert.id}`,
    alertId: alert.id,
    ts: alert.event_created_at || alert.created_at,
    symbol: alert.symbol ?? null,
    title: alert.title,
    body: alert.body ?? null,
    sourceName: alert.source_name,
    sourceHref:
      alert.source_type === "watchlist"
        ? `/watchlists/${encodeURIComponent(alert.source_id)}?mode=all&recent_days=30&limit=25&only_new=1`
        : "/monitoring",
    sourceType: alert.source_type === "watchlist" ? "watchlist" : "saved-view",
    readAt: alert.read_at ?? null,
  };
}

const SAVED_SCREEN_VIEW_PREFIX = "saved-screen:";

function parseSavedScreenId(view: SavedView): number | null {
  if (!view.id.startsWith(SAVED_SCREEN_VIEW_PREFIX)) return null;
  const parsed = Number(view.id.slice(SAVED_SCREEN_VIEW_PREFIX.length));
  return Number.isFinite(parsed) ? parsed : null;
}

function validSignalMode(value: string | undefined): SignalMode {
  return value === "congress" || value === "insider" ? value : "all";
}

function validSignalSort(value: string | undefined): SignalSort {
  return value === "multiple" || value === "recent" || value === "amount" || value === "confirmation" || value === "freshness" ? value : "smart";
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
  const [watchlists, setWatchlists] = useState(initialWatchlists);
  const [inbox, setInbox] = useState<MonitoringInboxResponse | null>(null);
  const [watchlistLatest, setWatchlistLatest] = useState<MonitoredEvent[]>([]);
  const [confirmationLatest, setConfirmationLatest] = useState<MonitoredEvent[]>([]);
  const [screenLatest, setScreenLatest] = useState<MonitoredEvent[]>([]);
  const [savedStatuses, setSavedStatuses] = useState<SavedViewStatus[]>([]);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [pendingReadAction, setPendingReadAction] = useState<string | null>(null);
  const [readActionMessage, setReadActionMessage] = useState<string | null>(null);
  const [selectedItemIds, setSelectedItemIds] = useState<number[]>([]);
  const [inboxFilter, setInboxFilter] = useState<InboxFilter>("all");

  const savedViews = useMemo(() => (store?.views ?? []).filter((view) => view.surface === "screener"), [store]);
  const canUseMonitoringSources = hasEntitlement(entitlements, "monitoring_sources");
  const canUseScreenMonitoring = hasEntitlement(entitlements, "screener_monitoring");
  const monitoringLimit = canUseMonitoringSources ? limitFor(entitlements, "monitoring_sources") : 0;
  const visibleWatchlists = useMemo(() => watchlists.slice(0, monitoringLimit), [watchlists, monitoringLimit]);
  const remainingSourceSlots = Math.max(monitoringLimit - visibleWatchlists.length, 0);
  const visibleSavedViews = useMemo(
    () => (canUseScreenMonitoring ? savedViews.slice(0, remainingSourceSlots) : []),
    [canUseScreenMonitoring, remainingSourceSlots, savedViews],
  );
  const hiddenSourceCount = Math.max(
    watchlists.length + (canUseScreenMonitoring ? savedViews.length : 0) - monitoringLimit,
    0,
  );
  const hiddenScreenSourceCount = canUseScreenMonitoring ? 0 : savedViews.length;
  const inboxSourceCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const source of inbox?.sources ?? []) {
      map.set(`${source.type}:${source.id}`, Math.max(Number(source.unread_count) || 0, 0));
    }
    return map;
  }, [inbox]);
  const totalWatchlistNew = inbox
    ? (inbox.sources ?? []).filter((source) => source.type === "watchlist").reduce((sum, item) => sum + Math.max(item.unread_count ?? 0, 0), 0)
    : visibleWatchlists.reduce((sum, item) => sum + Math.max(Number(item.unread_count ?? item.unseen_count) || 0, 0), 0);
  const totalSavedViewNew = inbox
    ? (inbox.sources ?? []).filter((source) => source.type !== "watchlist").reduce((sum, item) => sum + Math.max(item.unread_count ?? 0, 0), 0)
    : savedStatuses.reduce((sum, item) => sum + item.unseenCount, 0);
  const inboxItems = useMemo(
    () =>
      [...(inbox?.items ?? inbox?.alerts ?? inbox?.latest_important ?? [])].sort(
        (a, b) => new Date(b.timestamp ?? b.event_created_at ?? b.created_at).getTime() - new Date(a.timestamp ?? a.event_created_at ?? a.created_at).getTime(),
      ),
    [inbox],
  );
  const filteredInboxItems = useMemo(() => {
    if (inboxFilter === "unread") return inboxItems.filter((item) => item.is_unread ?? !item.read_at);
    if (inboxFilter === "read") return inboxItems.filter((item) => item.is_read ?? Boolean(item.read_at));
    return inboxItems;
  }, [inboxFilter, inboxItems]);
  const selectedItemSet = useMemo(() => new Set(selectedItemIds), [selectedItemIds]);
  const hasSelection = selectedItemIds.length > 0;

  const refreshInbox = () => {
    getMonitoringInbox()
      .then(setInbox)
      .catch(() => setInbox(null));
  };

  const refreshWatchlists = () => {
    listWatchlists()
      .then(setWatchlists)
      .catch(() => {});
  };

  const dispatchUnreadUpdated = () => {
    window.dispatchEvent(new Event("ct:monitoring-unread-updated"));
  };

  const toggleSelectedItem = (itemId: number) => {
    setSelectedItemIds((current) => (current.includes(itemId) ? current.filter((id) => id !== itemId) : [...current, itemId]));
  };

  const selectAllVisible = () => {
    setSelectedItemIds((current) => Array.from(new Set([...current, ...filteredInboxItems.map((item) => item.id)])));
  };

  const clearSelection = () => {
    setSelectedItemIds([]);
  };

  const mutateItems = async (itemIds: number[], read: boolean) => {
    if (itemIds.length === 0) return;
    const actionKey = `${read ? "items-read" : "items-unread"}:${itemIds.join(",")}`;
    setPendingReadAction(actionKey);
    setReadActionMessage(null);
    try {
      await (read ? markMonitoringItemsRead(itemIds) : markMonitoringItemsUnread(itemIds));
      setSelectedItemIds((current) => current.filter((id) => !itemIds.includes(id)));
      refreshInbox();
      refreshWatchlists();
      dispatchUnreadUpdated();
    } catch {
      refreshInbox();
      setReadActionMessage(read ? "Unable to mark the selected updates read." : "Unable to mark the selected updates unread.");
    } finally {
      setPendingReadAction((current) => (current === actionKey ? null : current));
    }
  };

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
    refreshInbox();
  }, []);

  useEffect(() => {
    refreshWatchlists();
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadWatchlists() {
      const active = visibleWatchlists.filter(
        (watchlist) => Math.max(Number(watchlist.unread_count ?? watchlist.unseen_count) || 0, 0) > 0 && watchlist.unseen_since,
      );
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

    async function loadScreenEvents() {
      if (!canUseScreenMonitoring) {
        if (!cancelled) setScreenLatest([]);
        return;
      }
      try {
        const data = await listSavedScreenEvents({ limit: 8 });
        if (!cancelled) setScreenLatest(data.items.map(savedScreenEventToMonitoredEvent));
      } catch {
        if (!cancelled) setScreenLatest([]);
      }
    }

    loadScreenEvents();
    return () => {
      cancelled = true;
    };
  }, [canUseScreenMonitoring]);

  useEffect(() => {
    let cancelled = false;

    async function loadConfirmationEvents() {
      const chunks = await Promise.all(
        visibleWatchlists.slice(0, 6).map(async (watchlist) => {
          try {
            const data = await getWatchlistConfirmationEvents(watchlist.id, { limit: 3 });
            return data.items.map((event) => confirmationEventToMonitoredEvent(event, watchlist.name));
          } catch {
            return [];
          }
        }),
      );
      if (!cancelled) setConfirmationLatest(chunks.flat());
    }

    loadConfirmationEvents();
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
            if (view.surface === "screener") {
              const savedScreenId = parseSavedScreenId(view);
              if (!Number.isFinite(savedScreenId)) {
                return { view, unseenCount: 0, latest: [], status: "ready" };
              }
              const data = await listSavedScreenEvents({ limit: 100 });
              const newItems = data.items.filter(
                (item) => item.saved_screen_id === savedScreenId && isNewer(item.created_at, view.lastSeenAt),
              );
              return {
                view,
                unseenCount: newItems.length,
                latest: newItems.slice(0, 3).map(savedScreenEventToMonitoredEvent),
                status: "ready",
              };
            }

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
              error: error instanceof Error ? error.message : "Unable to load saved screen.",
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

  const inboxLatest = useMemo(() => (inbox?.latest_important ?? []).map(monitoringAlertToMonitoredEvent), [inbox]);
  const latestImportant = [...inboxLatest, ...screenLatest, ...confirmationLatest, ...watchlistLatest, ...savedStatuses.flatMap((status) => status.latest)]
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
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Saved screens</div>
          <div className="mt-2 text-3xl font-semibold text-white">{totalSavedViewNew}</div>
          <p className="mt-1 text-sm text-slate-400">new items</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Sources</div>
          <div className="mt-2 text-3xl font-semibold text-white">{visibleWatchlists.length + visibleSavedViews.length}</div>
          <p className="mt-1 text-sm text-slate-400">monitored</p>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">Screen Changes</h2>
            <p className="text-sm text-slate-400">New entries, exits, upgrades, and state changes from saved screens.</p>
          </div>
          <span className="rounded-lg border border-white/10 px-2.5 py-1 text-xs text-slate-400">
            {screenLatest.length} recent
          </span>
        </div>

        <div className="mt-4 space-y-3">
          {!canUseScreenMonitoring ? (
            <UpgradePrompt
              title="Upgrade to monitor saved screens"
              body={`Free keeps saved screens useful for manual discovery, but background monitoring and saved-screen events unlock with Premium${hiddenScreenSourceCount > 0 ? ` for your ${hiddenScreenSourceCount} saved source${hiddenScreenSourceCount === 1 ? "" : "s"}` : ""}.`}
              compact={true}
            />
          ) : screenLatest.length === 0 ? (
            <div className="rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-5">
              <h3 className="font-semibold text-white">No saved screen changes yet</h3>
              <p className="mt-1 text-sm text-slate-400">Saved screener updates will appear here after the background refresh runs.</p>
            </div>
          ) : (
            screenLatest.map((item) => (
              <Link
                key={item.id}
                href={item.sourceHref}
                prefetch={false}
                className="block rounded-2xl border border-white/10 bg-slate-950/40 p-3 transition hover:border-emerald-300/35"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="font-medium text-white">{item.title}</span>
                  {item.scoreLabel ? (
                    <span className="rounded-lg border border-emerald-300/25 bg-emerald-300/10 px-2 py-0.5 text-xs text-emerald-100">
                      {item.scoreLabel}
                    </span>
                  ) : null}
                </div>
                {item.body ? <p className="mt-1 text-sm text-slate-400">{item.body}</p> : null}
                <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-500">
                  <span>{item.sourceName}</span>
                  <span>{item.symbol}</span>
                  <span>{new Date(item.ts).toLocaleString()}</span>
                </div>
              </Link>
            ))
          )}
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
              <p className="text-sm text-slate-400">Open a source or manage unread monitoring updates explicitly.</p>
            </div>
            <Link href="/watchlists" prefetch={false} className={secondaryActionClassName}>
              Manage watchlists
            </Link>
          </div>
          {readActionMessage ? (
            <div className="mt-3 rounded-lg border border-amber-300/25 bg-amber-300/10 px-3 py-2 text-sm text-amber-100" role="status">
              {readActionMessage}
            </div>
          ) : null}

          <div className="mt-4 divide-y divide-white/10">
            {visibleWatchlists.map((watchlist) => {
              const count = inboxSourceCounts.get(`watchlist:${watchlist.id}`) ?? Math.max(Number(watchlist.unread_count ?? watchlist.unseen_count) || 0, 0);
              return (
                <div
                  key={`watchlist-${watchlist.id}`}
                  className={`${compactInteractiveSurfaceClassName} grid gap-2 rounded-2xl px-4 py-3 text-sm sm:grid-cols-[1fr_auto_auto] sm:items-center`}
                >
                  <div>
                    <div className={`font-medium ${compactInteractiveTitleClassName}`}>{watchlist.name}</div>
                    <div className="text-xs text-slate-500">Watchlist #{watchlist.id}</div>
                  </div>
                  <span className={`w-fit rounded-lg border px-2.5 py-1 text-xs font-semibold ${count > 0 ? "border-red-300/35 bg-red-500/15 text-red-100" : "border-white/10 text-slate-400"}`}>
                    {count > 0 ? `${count} new` : "0 new"}
                  </span>
                  <Link href={sourceHrefForWatchlist(watchlist)} prefetch={false} className="text-sm font-semibold text-slate-300 transition hover:text-white">
                    Open
                  </Link>
                </div>
              );
            })}
            {savedStatuses.map((status) => {
              const href = savedViewHref(status.view);
              const savedScreenId = parseSavedScreenId(status.view);
              const inboxCount = savedScreenId ? inboxSourceCounts.get(`saved_screen:${savedScreenId}`) : undefined;
              const unseenCount = inboxCount ?? status.unseenCount;
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
                      Saved screen
                      {status.error ? " · refresh failed" : ""}
                    </div>
                  </div>
                  <span className={`w-fit rounded-lg border px-2.5 py-1 text-xs font-semibold ${unseenCount > 0 ? "border-sky-300/30 bg-sky-300/15 text-sky-100" : "border-white/10 text-slate-400"}`}>
                    {status.status === "loading" && inboxCount === undefined ? "checking" : `${unseenCount} new`}
                  </span>
                  <span className="text-sm text-slate-400">Open</span>
                </Link>
              );
            })}
            {visibleWatchlists.length === 0 && visibleSavedViews.length === 0 ? (
              <div className="py-8 text-sm text-slate-400">Create a watchlist or save a screen to start monitoring from here.</div>
            ) : null}
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-white">Monitoring updates</h2>
              <p className="text-sm text-slate-400">Select the exact updates to mark read or unread.</p>
            </div>
            <span className="rounded-lg border border-white/10 px-2.5 py-1 text-xs text-slate-400">{inbox?.unread_total ?? 0} unread</span>
          </div>

          <div className="mt-4 flex flex-wrap items-center gap-2">
            {(["all", "unread", "read"] as InboxFilter[]).map((filter) => (
              <button
                key={filter}
                type="button"
                onClick={() => setInboxFilter(filter)}
                className={`rounded-lg border px-3 py-1.5 text-xs font-semibold capitalize transition ${
                  inboxFilter === filter
                    ? "border-emerald-300/40 bg-emerald-300/15 text-emerald-100"
                    : "border-white/10 text-slate-300 hover:border-white/20 hover:text-white"
                }`}
              >
                {filter}
              </button>
            ))}
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-2 rounded-lg border border-white/10 bg-slate-950/40 p-2">
            <button type="button" onClick={selectAllVisible} className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:bg-white/[0.06] hover:text-white">
              Select all
            </button>
            <button type="button" onClick={clearSelection} className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:bg-white/[0.06] hover:text-white">
              Clear selection
            </button>
            <span className="text-xs text-slate-500">{selectedItemIds.length} selected</span>
            <div className="ml-auto flex flex-wrap gap-2">
              <button
                type="button"
                disabled={!hasSelection || Boolean(pendingReadAction)}
                onClick={() => mutateItems(selectedItemIds, true)}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-200 transition hover:border-emerald-300/35 hover:text-emerald-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Mark selected read
              </button>
              <button
                type="button"
                disabled={!hasSelection || Boolean(pendingReadAction)}
                onClick={() => mutateItems(selectedItemIds, false)}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-200 transition hover:border-sky-300/35 hover:text-sky-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Mark selected unread
              </button>
            </div>
          </div>

          <div className="mt-4 space-y-3">
            {filteredInboxItems.length === 0 ? (
              <div className="rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-5">
                <h3 className="font-semibold text-white">{inboxFilter === "unread" ? "No unread monitoring updates." : "No monitoring updates yet"}</h3>
                <p className="mt-1 text-sm text-slate-400">
                  {inboxFilter === "read" ? "Read updates will stay available here after you mark items read." : "New watchlist and saved-screen updates will appear here."}
                </p>
              </div>
            ) : (
              filteredInboxItems.map((item) => {
                const unread = item.is_unread ?? !item.read_at;
                const href =
                  item.source_type === "watchlist"
                    ? `/watchlists/${encodeURIComponent(item.source_id)}?mode=all&recent_days=30&limit=25&only_new=1`
                    : item.symbol
                      ? `/ticker/${encodeURIComponent(item.symbol)}`
                      : "/monitoring";
                return (
                <div
                  key={item.id}
                  className={`rounded-lg border p-3 transition hover:border-emerald-300/40 ${
                    unread ? "border-emerald-300/25 bg-emerald-300/[0.06]" : "border-white/10 bg-slate-950/35 opacity-80"
                  }`}
                >
                  <div className="grid gap-3 sm:grid-cols-[auto_1fr_auto]">
                    <input
                      type="checkbox"
                      checked={selectedItemSet.has(item.id)}
                      onChange={() => toggleSelectedItem(item.id)}
                      className="mt-1 h-4 w-4 rounded border-white/20 bg-slate-950 text-emerald-400"
                      aria-label={`Select ${item.title}`}
                    />
                    <Link href={href} prefetch={false} className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        {unread ? <span className="h-2 w-2 rounded-full bg-emerald-300" aria-hidden="true" /> : null}
                        <span className={`font-medium ${unread ? "text-white" : "text-slate-300"}`}>{item.title}</span>
                        {typeof item.score === "number" ? (
                        <span className="rounded-lg border border-emerald-300/25 bg-emerald-300/10 px-2 py-0.5 text-xs text-emerald-100">
                          score {item.score}
                        </span>
                      ) : null}
                      </div>
                      {item.description || item.body ? <p className="mt-1 text-sm text-slate-400">{item.description ?? item.body}</p> : null}
                      <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-500">
                        <span>{item.source_name}</span>
                        <span>{item.source_type === "watchlist" ? "watchlist" : "saved screen"}</span>
                        {item.symbol ? <span>{item.symbol}</span> : null}
                        <span>{new Date(item.timestamp ?? item.event_created_at ?? item.created_at).toLocaleString()}</span>
                      </div>
                    </Link>
                    <div className="flex w-fit items-center gap-1 rounded-lg border border-white/10 bg-slate-900/70 p-1 sm:self-start">
                      <button
                        type="button"
                        disabled={Boolean(pendingReadAction)}
                        onClick={() => mutateItems([item.id], true)}
                        className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:bg-white/[0.06] hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        Mark read
                      </button>
                      <button
                        type="button"
                        disabled={Boolean(pendingReadAction)}
                        onClick={() => mutateItems([item.id], false)}
                        className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:bg-white/[0.06] hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        Mark unread
                      </button>
                    </div>
                  </div>
                </div>
                );
              })
            )}
          </div>
        </div>
      </section>
    </div>
  );
}
