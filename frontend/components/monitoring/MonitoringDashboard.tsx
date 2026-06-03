"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import {
  ApiError,
  getEntitlements,
  getEvents,
  hasClientAuthHint,
  getMonitoringInbox,
  getSignalsAll,
  listWatchlists,
  dismissMonitoringItems,
  markMonitoringItemsRead,
  markMonitoringItemsUnread,
  listSavedScreenEvents,
  getWatchlistEvents,
  type EventItem,
  type MonitoringReadMutationResponse,
  type SignalItem,
  type SignalMode,
  type SignalSort,
} from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { buildMonitoringEventTitle, displayMonitoringAlertTitle } from "@/lib/monitoringTitles";
import type { MonitoringCounts, MonitoringInboxResponse, SavedScreenEvent, WatchlistSummary } from "@/lib/types";
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
  initialAuthPending?: boolean;
};

type InboxFilter = "all" | "unread" | "read";
type InboxMutationKind = "read" | "unread" | "dismiss";

function isNewer(ts: string | null | undefined, checkpoint: string | null | undefined) {
  if (!ts || !checkpoint) return false;
  return new Date(ts).getTime() > new Date(checkpoint).getTime();
}

function isUnreadMonitoringItem(item: { is_unread?: boolean; read_at?: string | null }) {
  return Boolean(item.is_unread ?? !item.read_at);
}

function monitoringSourceKey(sourceType: string, sourceId: string) {
  return `${sourceType}:${sourceId}`;
}

function summarizeMonitoringCounts(sources: MonitoringCounts["sources"]): Pick<MonitoringCounts, "watchlist_unread" | "saved_screen_unread" | "unread_sources_count"> {
  const watchlistUnread = sources
    .filter((source) => source.type === "watchlist")
    .reduce((sum, source) => sum + Math.max(Number(source.unread_count) || 0, 0), 0);
  const savedScreenUnread = sources
    .filter((source) => source.type !== "watchlist")
    .reduce((sum, source) => sum + Math.max(Number(source.unread_count) || 0, 0), 0);
  return {
    watchlist_unread: watchlistUnread,
    saved_screen_unread: savedScreenUnread,
    unread_sources_count: sources.filter((source) => Math.max(Number(source.unread_count) || 0, 0) > 0).length,
  };
}

function mergeInboxCounts(current: MonitoringInboxResponse | null, counts?: MonitoringCounts | null): MonitoringInboxResponse | null {
  if (!current || !counts) return current;
  return {
    ...current,
    unread_total: Math.max(Number(counts.total_unread) || 0, 0),
    sources: counts.sources,
    counts,
  };
}

function applyInboxMutation(current: MonitoringInboxResponse | null, itemIds: number[], mutation: InboxMutationKind): MonitoringInboxResponse | null {
  if (!current || itemIds.length === 0) return current;
  const selected = new Set(itemIds);
  const unreadDeltas = new Map<string, number>();
  let unreadDelta = 0;
  const nowIso = new Date().toISOString();
  const nextItems = [...(current.items ?? current.alerts ?? [])].flatMap((item) => {
    if (!selected.has(item.id)) return [item];
    const unread = isUnreadMonitoringItem(item);
    const key = monitoringSourceKey(item.source_type, item.source_id);
    if (mutation === "dismiss") {
      if (unread) {
        unreadDelta -= 1;
        unreadDeltas.set(key, (unreadDeltas.get(key) ?? 0) - 1);
      }
      return [];
    }
    if (mutation === "read") {
      if (unread) {
        unreadDelta -= 1;
        unreadDeltas.set(key, (unreadDeltas.get(key) ?? 0) - 1);
      }
      return [{ ...item, read_at: item.read_at ?? nowIso, is_read: true, is_unread: false }];
    }
    if (!unread) {
      unreadDelta += 1;
      unreadDeltas.set(key, (unreadDeltas.get(key) ?? 0) + 1);
    }
    return [{ ...item, read_at: null, is_read: false, is_unread: true }];
  });

  const nextSources = current.sources.map((source) => {
    const delta = unreadDeltas.get(monitoringSourceKey(source.type, source.id)) ?? 0;
    const unreadCount = Math.max((Number(source.unread_count) || 0) + delta, 0);
    return { ...source, unread_count: unreadCount, new_count: unreadCount };
  });
  const nextUnreadTotal = Math.max((Number(current.unread_total) || 0) + unreadDelta, 0);
  const countSummary = summarizeMonitoringCounts(nextSources);
  const nextCounts: MonitoringCounts = {
    total_unread: nextUnreadTotal,
    sources: nextSources,
    ...countSummary,
  };
  return {
    ...current,
    unread_total: nextUnreadTotal,
    sources: nextSources,
    counts: nextCounts,
    latest_important: nextItems.filter((item) => isUnreadMonitoringItem(item)).slice(0, 8),
    alerts: nextItems,
    items: nextItems,
  };
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
  const normalizedTitle = buildMonitoringEventTitle(event, event.payload ?? {});
  if (normalizedTitle) return normalizedTitle;
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
    scoreLabel: typeof event.smart_score === "number" ? `conviction ${event.smart_score}` : null,
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
    scoreLabel: typeof signal.smart_score === "number" ? `conviction ${signal.smart_score}` : null,
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

function MonitoringPanelSkeleton() {
  return (
    <div className="space-y-3" aria-busy="true" aria-live="polite">
      {Array.from({ length: 3 }).map((_, index) => (
        <div key={index} className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
          <SkeletonBlock className="h-4 w-44" />
          <SkeletonBlock className="mt-3 h-3 w-full max-w-md" />
        </div>
      ))}
    </div>
  );
}

const monitoredSourceCardClassName = `${compactInteractiveSurfaceClassName} grid gap-2 rounded-2xl px-4 py-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/40 sm:grid-cols-[1fr_auto_auto] sm:items-center`;

function MonitoredSourceCard({
  href,
  title,
  subtitle,
  countLabel,
  countClassName,
  onClick,
}: {
  href: string;
  title: string;
  subtitle: string;
  countLabel: string;
  countClassName: string;
  onClick?: () => void;
}) {
  return (
    <Link href={href} prefetch={false} onClick={onClick} className={monitoredSourceCardClassName}>
      <div className="min-w-0">
        <div className={`truncate font-medium ${compactInteractiveTitleClassName}`}>{title}</div>
        <div className="text-xs text-slate-500">{subtitle}</div>
      </div>
      <span className={`w-fit rounded-lg border px-2.5 py-1 text-xs font-semibold ${countClassName}`}>
        {countLabel}
      </span>
      <span className="text-sm font-semibold text-slate-300 transition group-hover:text-white group-focus-visible:text-white">Open</span>
    </Link>
  );
}

export function MonitoringDashboard({ initialWatchlists, initialAuthPending = false }: MonitoringDashboardProps) {
  const { store, markSeen } = useSavedViews();
  const [watchlists, setWatchlists] = useState(initialWatchlists);
  const [inbox, setInbox] = useState<MonitoringInboxResponse | null>(null);
  const [savedStatuses, setSavedStatuses] = useState<SavedViewStatus[]>([]);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoading, setEntitlementsLoading] = useState(initialAuthPending);
  const [pendingReadAction, setPendingReadAction] = useState<string | null>(null);
  const [readActionMessage, setReadActionMessage] = useState<string | null>(null);
  const [inboxStatus, setInboxStatus] = useState<string | null>(null);
  const [watchlistsStatus, setWatchlistsStatus] = useState<string | null>(null);
  const [selectedItemIds, setSelectedItemIds] = useState<number[]>([]);
  const [inboxFilter, setInboxFilter] = useState<InboxFilter>("all");
  const [inboxPageSize, setInboxPageSize] = useState<5 | 10 | 25>(5);
  const [inboxPage, setInboxPage] = useState(1);

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
  const totalInboxPages = Math.max(1, Math.ceil(filteredInboxItems.length / inboxPageSize));
  const currentInboxPage = Math.min(inboxPage, totalInboxPages);
  const pagedInboxItems = useMemo(() => {
    const start = (currentInboxPage - 1) * inboxPageSize;
    return filteredInboxItems.slice(start, start + inboxPageSize);
  }, [currentInboxPage, filteredInboxItems, inboxPageSize]);
  const selectedItemSet = useMemo(() => new Set(selectedItemIds), [selectedItemIds]);
  const hasSelection = selectedItemIds.length > 0;

  const refreshInbox = async () => {
    setInboxStatus(null);
    try {
      const nextInbox = await getMonitoringInbox(undefined, { source: "MonitoringInbox" });
      setInbox(nextInbox);
      dispatchUnreadUpdated(nextInbox.counts?.total_unread ?? nextInbox.unread_total ?? 0);
    } catch (error) {
      setInboxStatus(error instanceof ApiError && error.status === 401 ? "Sign in to load monitoring updates." : "Monitoring updates are temporarily unavailable.");
    }
  };

  const refreshWatchlists = async () => {
    setWatchlistsStatus(null);
    try {
      setWatchlists(await listWatchlists());
    } catch (error) {
      setWatchlistsStatus(error instanceof ApiError && error.status === 401 ? "Sign in to load monitored sources." : "Monitored sources are temporarily unavailable.");
    }
  };

  const dispatchUnreadUpdated = (unreadCount?: number) => {
    window.dispatchEvent(new CustomEvent("ct:monitoring-unread-updated", { detail: typeof unreadCount === "number" ? { unreadCount } : undefined }));
  };

  const toggleSelectedItem = (itemId: number) => {
    setSelectedItemIds((current) => (current.includes(itemId) ? current.filter((id) => id !== itemId) : [...current, itemId]));
  };

  const selectAllVisible = () => {
    setSelectedItemIds((current) => Array.from(new Set([...current, ...pagedInboxItems.map((item) => item.id)])));
  };

  const clearSelection = () => {
    setSelectedItemIds([]);
  };

  const applyMutationSuccess = (response: MonitoringReadMutationResponse) => {
    setInbox((current) => mergeInboxCounts(current, response.counts));
    dispatchUnreadUpdated(response.counts?.total_unread ?? response.unread_count);
    void refreshInbox();
    void refreshWatchlists();
  };

  const mutateItems = async (itemIds: number[], read: boolean) => {
    if (itemIds.length === 0) return;
    const actionKey = `${read ? "items-read" : "items-unread"}:${itemIds.join(",")}`;
    setPendingReadAction(actionKey);
    setReadActionMessage(null);
    setInbox((current) => applyInboxMutation(current, itemIds, read ? "read" : "unread"));
    try {
      const response = await (read ? markMonitoringItemsRead(itemIds) : markMonitoringItemsUnread(itemIds));
      setSelectedItemIds((current) => current.filter((id) => !itemIds.includes(id)));
      applyMutationSuccess(response);
    } catch {
      void refreshInbox();
      void refreshWatchlists();
      setReadActionMessage(read ? "Unable to mark the selected updates read." : "Unable to mark the selected updates unread.");
    } finally {
      setPendingReadAction((current) => (current === actionKey ? null : current));
    }
  };

  const dismissItems = async (itemIds: number[]) => {
    if (itemIds.length === 0) return;
    const actionKey = `items-dismiss:${itemIds.join(",")}`;
    setPendingReadAction(actionKey);
    setReadActionMessage(null);
    setInbox((current) => applyInboxMutation(current, itemIds, "dismiss"));
    try {
      const response = await dismissMonitoringItems(itemIds);
      setSelectedItemIds((current) => current.filter((id) => !itemIds.includes(id)));
      applyMutationSuccess(response);
    } catch {
      void refreshInbox();
      void refreshWatchlists();
      setReadActionMessage("Unable to delete the selected updates.");
    } finally {
      setPendingReadAction((current) => (current === actionKey ? null : current));
    }
  };

  useEffect(() => {
    let cancelled = false;
    const likelyAuthenticated = initialAuthPending || hasClientAuthHint();
    setEntitlementsLoading(likelyAuthenticated);
    getEntitlements(undefined, { source: "MonitoringInbox" })
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      })
      .finally(() => {
        if (!cancelled) setEntitlementsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [initialAuthPending]);

  useEffect(() => {
    void refreshInbox();
  }, []);

  useEffect(() => {
    void refreshWatchlists();
  }, []);

  useEffect(() => {
    setInboxPage((page) => Math.min(Math.max(page, 1), totalInboxPages));
  }, [totalInboxPages]);

  useEffect(() => {
    setInboxPage(1);
  }, [inboxFilter, inboxPageSize]);

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
                source: "MonitoringInbox",
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
            const data = await getEvents({ ...params, source: "MonitoringInbox" });
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

  return (
    <div className="space-y-6">
      <section className="grid gap-3 sm:grid-cols-3">
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Watchlists</div>
          <div className="mt-2 text-3xl font-semibold text-white">{watchlistsStatus && watchlists.length === 0 ? "-" : totalWatchlistNew}</div>
          <p className="mt-1 text-sm text-slate-400">new items</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Saved screens</div>
          <div className="mt-2 text-3xl font-semibold text-white">{inboxStatus && !inbox ? "-" : totalSavedViewNew}</div>
          <p className="mt-1 text-sm text-slate-400">new items</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Sources</div>
          <div className="mt-2 text-3xl font-semibold text-white">{watchlistsStatus && watchlists.length === 0 ? "-" : visibleWatchlists.length + visibleSavedViews.length}</div>
          <p className="mt-1 text-sm text-slate-400">monitored</p>
        </div>
      </section>

      {!entitlementsLoading && hiddenSourceCount > 0 ? (
        <UpgradePrompt
          title="Monitor every source with Premium"
          body={`Free monitors ${monitoringLimit} sources in the inbox. ${hiddenSourceCount} saved source${hiddenSourceCount === 1 ? " is" : "s are"} waiting behind the Premium limit.`}
        />
      ) : null}

      <section className="grid gap-6 xl:grid-cols-[minmax(280px,0.8fr)_minmax(520px,1.4fr)]">
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
          {watchlistsStatus && visibleWatchlists.length === 0 ? (
            <div className="mt-3 rounded-lg border border-amber-300/25 bg-amber-300/10 px-3 py-2 text-sm text-amber-100" role="status">
              {watchlistsStatus}
            </div>
          ) : null}

          <div className="mt-4 divide-y divide-white/10">
            {entitlementsLoading ? (
              <MonitoringPanelSkeleton />
            ) : visibleWatchlists.map((watchlist) => {
              const count = inboxSourceCounts.get(`watchlist:${watchlist.id}`) ?? Math.max(Number(watchlist.unread_count ?? watchlist.unseen_count) || 0, 0);
              return (
                <MonitoredSourceCard
                  key={`watchlist-${watchlist.id}`}
                  href={sourceHrefForWatchlist(watchlist)}
                  title={watchlist.name}
                  subtitle={`Watchlist #${watchlist.id}`}
                  countLabel={count > 0 ? `${count} new` : "0 new"}
                  countClassName={count > 0 ? "border-red-300/35 bg-red-500/15 text-red-100" : "border-white/10 text-slate-400"}
                />
              );
            })}
            {!entitlementsLoading ? savedStatuses.map((status) => {
              const href = savedViewHref(status.view);
              const savedScreenId = parseSavedScreenId(status.view);
              const inboxCount = savedScreenId ? inboxSourceCounts.get(`saved_screen:${savedScreenId}`) : undefined;
              const unseenCount = inboxCount ?? status.unseenCount;
              return (
                <MonitoredSourceCard
                  key={`saved-${status.view.id}`}
                  href={href}
                  onClick={() => markSeen(status.view.id)}
                  title={status.view.name}
                  subtitle={`Saved screen${status.error ? " - refresh failed" : ""}`}
                  countLabel={status.status === "loading" && inboxCount === undefined ? "checking" : `${unseenCount} new`}
                  countClassName={unseenCount > 0 ? "border-sky-300/30 bg-sky-300/15 text-sky-100" : "border-white/10 text-slate-400"}
                />
              );
            }) : null}
            {!entitlementsLoading && visibleWatchlists.length === 0 && visibleSavedViews.length === 0 ? (
              <div className="py-8 text-sm text-slate-400">Create a watchlist or save a screen to start monitoring from here.</div>
            ) : null}
          </div>
        </div>

        <div className="rounded-lg border border-white/10 bg-slate-900/70 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-white">Monitoring Updates</h2>
              <p className="text-sm text-slate-400">Select updates to mark read, mark unread, or delete.</p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-lg border border-white/10 px-2.5 py-1 text-xs text-slate-400">{inboxStatus && !inbox ? "-" : (inbox?.unread_total ?? 0)} unread</span>
              <label className="flex items-center gap-2 text-xs font-semibold text-slate-400">
                Page size
                <select
                  value={inboxPageSize}
                  onChange={(event) => setInboxPageSize(Number(event.target.value) as 5 | 10 | 25)}
                  className="rounded-lg border border-white/10 bg-slate-950/70 px-2 py-1 text-xs font-semibold text-slate-200 outline-none transition focus:border-emerald-300/40"
                >
                  {[5, 10, 25].map((size) => (
                    <option key={size} value={size}>
                      {size}
                    </option>
                  ))}
                </select>
              </label>
            </div>
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
              Select all visible
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
              <button
                type="button"
                disabled={!hasSelection || Boolean(pendingReadAction)}
                onClick={() => dismissItems(selectedItemIds)}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-200 transition hover:border-red-300/35 hover:text-red-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                Delete selected
              </button>
            </div>
          </div>

          <div className="mt-4 max-h-[34rem] space-y-3 overflow-y-auto pr-1">
            {inboxStatus ? (
              <div className="rounded-lg border border-amber-300/25 bg-amber-300/10 p-5">
                <h3 className="font-semibold text-amber-100">Monitoring updates could not load.</h3>
                <p className="mt-1 text-sm text-amber-100/80">{inboxStatus}</p>
                <button
                  type="button"
                  onClick={() => void refreshInbox()}
                  className="mt-3 rounded-lg border border-amber-200/30 px-3 py-1.5 text-xs font-semibold text-amber-50 transition hover:border-amber-100/60"
                >
                  Retry
                </button>
              </div>
            ) : filteredInboxItems.length === 0 ? (
              <div className="rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-5">
                <h3 className="font-semibold text-white">{inboxFilter === "unread" ? "No unread monitoring updates." : "No monitoring updates yet"}</h3>
                <p className="mt-1 text-sm text-slate-400">
                  {inboxFilter === "read" ? "Read updates will stay available here after you mark items read." : "Deleted notifications are removed from this inbox."}
                </p>
              </div>
            ) : (
              pagedInboxItems.map((item) => {
                const unread = item.is_unread ?? !item.read_at;
                const displayTitle = displayMonitoringAlertTitle(item);
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
                      aria-label={`Select ${displayTitle}`}
                    />
                    <Link href={href} prefetch={false} className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        {unread ? <span className="h-2 w-2 rounded-full bg-emerald-300" aria-hidden="true" /> : null}
                        <span className={`font-medium ${unread ? "text-white" : "text-slate-300"}`}>{displayTitle}</span>
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
                      <button
                        type="button"
                        disabled={Boolean(pendingReadAction)}
                        onClick={() => dismissItems([item.id])}
                        className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:bg-red-400/10 hover:text-red-100 disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
                );
              })
            )}
          </div>
          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t border-white/10 pt-3">
            <span className="text-xs text-slate-500">
              Page {currentInboxPage} of {totalInboxPages}
            </span>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                disabled={currentInboxPage <= 1}
                onClick={() => setInboxPage(1)}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                First
              </button>
              <button
                type="button"
                disabled={currentInboxPage <= 1}
                onClick={() => setInboxPage((page) => Math.max(1, page - 1))}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                Previous
              </button>
              <button
                type="button"
                disabled={currentInboxPage >= totalInboxPages}
                onClick={() => setInboxPage((page) => Math.min(totalInboxPages, page + 1))}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                Next
              </button>
              <button
                type="button"
                disabled={currentInboxPage >= totalInboxPages}
                onClick={() => setInboxPage(totalInboxPages)}
                className="rounded-md border border-white/10 px-2.5 py-1 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                Last
              </button>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
