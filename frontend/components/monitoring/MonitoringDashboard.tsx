"use client";

import Link from "next/link";
import dynamic from "next/dynamic";
import { useEffect, useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { WalnutConfirmDialog } from "@/components/ui/WalnutConfirmDialog";
import {
  ApiError,
  getEntitlements,
  hasClientAuthHint,
  getMonitoringInbox,
  getMonitoringSourceCounts,
  getMonitoringSources,
  listWatchlists,
  dismissMonitoringItems,
  markMonitoringItemsRead,
  markMonitoringItemsUnread,
  markMonitoringSourceRead,
  stopMonitoringSource,
  type MonitoringReadMutationResponse,
} from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { displayMonitoringAlertTitle } from "@/lib/monitoringTitles";
import type { MonitoringCounts, MonitoringInboxResponse, WatchlistSummary } from "@/lib/types";
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

const EventCalendarPanel = dynamic(
  () => import("@/components/monitoring/EventCalendarPanel").then((module) => module.EventCalendarPanel),
  {
    ssr: false,
    loading: () => (
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-4" aria-label="Loading event calendar">
        <SkeletonBlock className="h-5 w-40" />
        <SkeletonBlock className="mt-3 h-4 w-72" />
        <SkeletonBlock className="mt-5 h-64 w-full" />
      </section>
    ),
  },
);

type MonitoringInboxSource = MonitoringInboxResponse["sources"][number];

const secondaryActionClassName =
  "inline-flex items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white";

type SourceStatus = "idle" | "loading" | "ready";

type SavedViewStatus = {
  view: SavedView;
  unseenCount: number;
  status: SourceStatus;
  error?: string | null;
};

type MonitoringDashboardProps = {
  initialWatchlists: WatchlistSummary[];
  initialAuthPending?: boolean;
};

type InboxFilter = "all" | "unread" | "read";
type InboxMutationKind = "read" | "unread" | "dismiss";
type MonitoringSourceGroup = "watchlists" | "saved_screens" | "strategies" | "other";

type MonitoredSourceRow = {
  key: string;
  sourceType: string;
  sourceId: string;
  subscriptionId?: number | null;
  title: string;
  subtitle: string;
  href: string;
  alertHref?: string;
  count: number | null;
  countLabel: string;
  countClassName: string;
  group: MonitoringSourceGroup;
  onOpen?: () => void;
};

function isUnreadMonitoringItem(item: { is_unread?: boolean; read_at?: string | null }) {
  return Boolean(item.is_unread ?? !item.read_at);
}

function monitoringSourceKey(sourceType: string, sourceId: string) {
  return `${sourceType}:${sourceId}`;
}

function isSavedScreenSourceType(sourceType: string) {
  return sourceType === "saved_screen" || sourceType === "saved-screen" || sourceType === "saved_view";
}

function normalizedMonitoringSourceType(sourceType: string) {
  return isSavedScreenSourceType(sourceType) ? "saved_screen" : sourceType;
}

function sourceTypeLabel(sourceType: string) {
  if (sourceType === "watchlist") return "watchlist";
  if (isSavedScreenSourceType(sourceType)) return "saved screen";
  if (sourceType === "strategy") return "strategy";
  if (sourceType === "confirmation_monitoring") return "confirmation monitoring";
  return sourceType.replaceAll("_", " ");
}

function monitoringCategoryLabel(value: string | null | undefined) {
  if (!value) return "Monitoring update";
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function sourceGroup(sourceType: string): MonitoringSourceGroup {
  if (sourceType === "watchlist") return "watchlists";
  if (isSavedScreenSourceType(sourceType)) return "saved_screens";
  if (sourceType === "strategy") return "strategies";
  return "other";
}

const sourceGroupLabels: Record<MonitoringSourceGroup, string> = {
  watchlists: "WATCHLISTS",
  saved_screens: "SAVED SCREENS",
  strategies: "STRATEGIES",
  other: "OTHER",
};

function hrefForInboxSource(source: MonitoringInboxSource) {
  if (source.type === "watchlist") {
    return `/watchlists/${encodeURIComponent(source.id)}?mode=all&recent_days=30&limit=25&only_new=1`;
  }
  if (source.type === "strategy") {
    return `/strategies/${encodeURIComponent(source.id)}`;
  }
  if (isSavedScreenSourceType(source.type)) {
    return "/screener";
  }
  if (source.type === "confirmation_monitoring") {
    return "/monitoring";
  }
  return "/monitoring";
}

function manageAlertsHrefForSource(sourceType: string, sourceId: string, fallbackHref: string) {
  if (sourceType === "watchlist") return `/watchlists/${encodeURIComponent(sourceId)}#monitoring-alerts`;
  if (isSavedScreenSourceType(sourceType)) return `/screener?manage_alerts=${encodeURIComponent(`saved-screen:${sourceId}`)}`;
  if (sourceType === "strategy") return `${fallbackHref}#strategy-monitoring`;
  return "/account/settings#monitoring-email-preferences";
}

function manageAlertsHrefForSavedView(view: SavedView) {
  const href = savedViewHref(view);
  return `${href}${href.includes("?") ? "&" : "?"}manage_alerts=${encodeURIComponent(view.id)}`;
}

function sourceCountClassName(sourceType: string, count: number) {
  if (count <= 0) return "border-white/10 text-slate-400";
  if (sourceType === "watchlist") return "border-red-300/35 bg-red-500/15 text-red-100";
  if (isSavedScreenSourceType(sourceType)) return "border-sky-300/30 bg-sky-300/15 text-sky-100";
  if (sourceType === "strategy") return "border-violet-300/30 bg-violet-300/15 text-violet-100";
  return "border-emerald-300/30 bg-emerald-300/15 text-emerald-100";
}

function summarizeMonitoringCounts(sources: MonitoringCounts["sources"]): Pick<MonitoringCounts, "watchlist_unread" | "saved_screen_unread" | "unread_sources_count"> {
  const watchlistUnread = sources
    .filter((source) => source.type === "watchlist")
    .reduce((sum, source) => sum + Math.max(Number(source.unread_count) || 0, 0), 0);
  const savedScreenUnread = sources
    .filter((source) => isSavedScreenSourceType(source.type))
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

const SAVED_SCREEN_VIEW_PREFIX = "saved-screen:";

function parseSavedScreenId(view: SavedView): number | null {
  if (!view.id.startsWith(SAVED_SCREEN_VIEW_PREFIX)) return null;
  const parsed = Number(view.id.slice(SAVED_SCREEN_VIEW_PREFIX.length));
  return Number.isFinite(parsed) ? parsed : null;
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

const monitoredSourceCardClassName = `${compactInteractiveSurfaceClassName} relative grid gap-2 rounded-2xl px-4 py-3 text-sm sm:grid-cols-[1fr_auto_auto_auto] sm:items-center`;

function MonitoredSourceCard({
  href,
  title,
  subtitle,
  countLabel,
  countClassName,
  alertHref,
  onClick,
  onClearNew,
  clearNewDisabled = false,
  clearNewPending = false,
  onRemove,
  removeDisabled = false,
}: {
  href: string;
  title: string;
  subtitle: string;
  countLabel: string;
  countClassName: string;
  alertHref?: string;
  onClick?: () => void;
  onClearNew?: () => void;
  clearNewDisabled?: boolean;
  clearNewPending?: boolean;
  onRemove?: () => void;
  removeDisabled?: boolean;
}) {
  return (
    <div className={monitoredSourceCardClassName}>
      <Link
        href={href}
        prefetch={false}
        onClick={onClick}
        aria-label={`Open ${title}`}
        className="absolute inset-0 z-0 rounded-2xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/40"
      />
      <div className="pointer-events-none relative z-10 min-w-0">
        <div className={`truncate font-medium ${compactInteractiveTitleClassName}`}>{title}</div>
        <div className="text-xs text-slate-500">{subtitle}</div>
      </div>
      <span className={`pointer-events-none relative z-10 w-fit rounded-lg border px-2.5 py-1 text-xs font-semibold ${countClassName}`}>
        {countLabel}
      </span>
      <span className="relative z-10 flex items-center gap-2">
        <Link href={href} prefetch={false} onClick={onClick} className="text-sm font-semibold text-slate-300 transition hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/40">
          Open
        </Link>
        {alertHref ? (
          <Link href={alertHref} prefetch={false} className="text-sm font-semibold text-emerald-200 transition hover:text-emerald-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/40">
            Manage alerts
          </Link>
        ) : null}
        {onClearNew ? (
          <button
            type="button"
            onClick={onClearNew}
            disabled={clearNewDisabled}
            className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold text-slate-300 transition hover:border-emerald-300/35 hover:text-emerald-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/40 disabled:cursor-not-allowed disabled:opacity-50"
            aria-label={`Mark all new updates in ${title} as read`}
          >
            {clearNewPending ? "Marking…" : "Mark all new read"}
          </button>
        ) : null}
        {onRemove ? (
          <button
            type="button"
            onClick={onRemove}
            disabled={removeDisabled}
            className="inline-flex h-7 w-7 items-center justify-center rounded-md border border-white/10 text-base font-semibold leading-none text-slate-500 transition hover:border-rose-300/35 hover:bg-rose-400/10 hover:text-rose-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-rose-300/35 disabled:cursor-not-allowed disabled:opacity-50"
            aria-label={`Stop monitoring ${title}`}
            title={`Stop monitoring ${title}`}
          >
            &times;
          </button>
        ) : null}
      </span>
    </div>
  );
}

function removeInboxSource(current: MonitoringInboxResponse | null, source: MonitoredSourceRow): MonitoringInboxResponse | null {
  if (!current) return current;
  const removedType = normalizedMonitoringSourceType(source.sourceType);
  const removedId = String(source.sourceId);
  const removedUnread = current.sources
    .filter((item) => normalizedMonitoringSourceType(item.type) === removedType && String(item.id) === removedId)
    .reduce((sum, item) => sum + Math.max(Number(item.unread_count) || 0, 0), 0);
  const nextSources = current.sources.filter((item) => !(normalizedMonitoringSourceType(item.type) === removedType && String(item.id) === removedId));
  const nextUnreadTotal = Math.max((Number(current.unread_total) || 0) - removedUnread, 0);
  const countSummary = summarizeMonitoringCounts(nextSources);
  const nextCounts: MonitoringCounts = {
    total_unread: nextUnreadTotal,
    sources: nextSources,
    ...countSummary,
  };
  const excludesSource = (item: { source_type: string; source_id: string }) =>
    normalizedMonitoringSourceType(item.source_type) === removedType && String(item.source_id) === removedId;
  return {
    ...current,
    unread_total: nextUnreadTotal,
    sources: nextSources,
    counts: nextCounts,
    latest_important: (current.latest_important ?? []).filter((item) => !excludesSource(item)),
    alerts: (current.alerts ?? []).filter((item) => !excludesSource(item)),
    items: (current.items ?? []).filter((item) => !excludesSource(item)),
  };
}

export function MonitoringDashboard({ initialWatchlists, initialAuthPending = false }: MonitoringDashboardProps) {
  const { store, markSeen } = useSavedViews();
  const [watchlists, setWatchlists] = useState(initialWatchlists);
  const [inbox, setInbox] = useState<MonitoringInboxResponse | null>(null);
  const [sourceMetadata, setSourceMetadata] = useState<MonitoringInboxSource[] | null>(null);
  const [sourceCounts, setSourceCounts] = useState<MonitoringCounts | null>(null);
  const [savedStatuses, setSavedStatuses] = useState<SavedViewStatus[]>([]);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoading, setEntitlementsLoading] = useState(initialAuthPending);
  const [pendingReadAction, setPendingReadAction] = useState<string | null>(null);
  const [readActionMessage, setReadActionMessage] = useState<string | null>(null);
  const [inboxStatus, setInboxStatus] = useState<string | null>(null);
  const [watchlistsStatus, setWatchlistsStatus] = useState<string | null>(null);
  const [selectedItemIds, setSelectedItemIds] = useState<number[]>([]);
  const [inboxFilter, setInboxFilter] = useState<InboxFilter>("all");
  const [inboxSourceFilter, setInboxSourceFilter] = useState("all");
  const [inboxCategoryFilter, setInboxCategoryFilter] = useState("all");
  const [inboxPageSize, setInboxPageSize] = useState<5 | 10 | 25 | 50 | 100>(5);
  const [inboxPage, setInboxPage] = useState(1);
  const [pendingRemoveSource, setPendingRemoveSource] = useState<MonitoredSourceRow | null>(null);
  const [pendingRemoveKey, setPendingRemoveKey] = useState<string | null>(null);

  const savedViews = useMemo(() => (store?.views ?? []).filter((view) => view.surface === "screener"), [store]);
  const canUseMonitoringSources = hasEntitlement(entitlements, "monitoring_sources");
  const canUseScreenMonitoring = hasEntitlement(entitlements, "screener_monitoring");
  const canUseEventCalendar = hasEntitlement(entitlements, "event_calendar");
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
  const inboxLoading = inbox === null && inboxStatus === null;
  const monitoringSources = sourceMetadata ?? inbox?.sources ?? [];
  const freshCountSources = sourceCounts?.sources ?? inbox?.sources ?? null;
  const sourceCountsLoading = freshCountSources === null;
  const inboxSourceCounts = useMemo(() => {
    const map = new Map<string, number>();
    for (const source of freshCountSources ?? []) {
      map.set(monitoringSourceKey(normalizedMonitoringSourceType(source.type), source.id), Math.max(Number(source.unread_count) || 0, 0));
    }
    return map;
  }, [freshCountSources]);
  const inboxSourceMetadata = useMemo(() => {
    const map = new Map<string, MonitoringInboxSource>();
    for (const source of monitoringSources) {
      map.set(monitoringSourceKey(normalizedMonitoringSourceType(source.type), source.id), source);
    }
    return map;
  }, [monitoringSources]);
  const totalWatchlistNew = freshCountSources
    ? freshCountSources.filter((source) => source.type === "watchlist").reduce((sum, item) => sum + Math.max(item.unread_count ?? 0, 0), 0)
    : null;
  const totalSavedViewNew = freshCountSources
    ? freshCountSources.filter((source) => isSavedScreenSourceType(source.type)).reduce((sum, item) => sum + Math.max(item.unread_count ?? 0, 0), 0)
    : null;
  const visibleSourceKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const watchlist of visibleWatchlists) {
      keys.add(monitoringSourceKey("watchlist", String(watchlist.id)));
    }
    for (const status of savedStatuses) {
      const savedScreenId = parseSavedScreenId(status.view);
      if (savedScreenId) keys.add(monitoringSourceKey("saved_screen", String(savedScreenId)));
    }
    return keys;
  }, [savedStatuses, visibleWatchlists]);
  const additionalInboxSources = useMemo(
    () =>
      monitoringSources.filter((source) => {
        return !visibleSourceKeys.has(monitoringSourceKey(normalizedMonitoringSourceType(source.type), source.id));
      }),
    [monitoringSources, visibleSourceKeys],
  );
  const monitoredSourceRows = useMemo(() => {
    const rows: MonitoredSourceRow[] = [];
    const shouldShowSource = (sourceType: string, sourceId: string) => {
      if (sourceMetadata === null && inbox === null) return true;
      const metadata = inboxSourceMetadata.get(monitoringSourceKey(normalizedMonitoringSourceType(sourceType), sourceId));
      return Boolean(metadata);
    };

    for (const watchlist of visibleWatchlists) {
      const sourceId = String(watchlist.id);
      const metadata = inboxSourceMetadata.get(monitoringSourceKey("watchlist", sourceId));
      if (!shouldShowSource("watchlist", sourceId)) continue;
      const count = inboxSourceCounts.get(monitoringSourceKey("watchlist", sourceId)) ?? null;
      rows.push({
        key: `watchlist-${watchlist.id}`,
        sourceType: "watchlist",
        sourceId,
        subscriptionId: metadata?.subscription_id,
        title: watchlist.name,
        subtitle: `Watchlist #${watchlist.id}`,
        href: sourceHrefForWatchlist(watchlist),
        alertHref: `/watchlists/${watchlist.id}#monitoring-alerts`,
        count,
        countLabel: count === null ? "Loading" : count > 0 ? `${count} new` : "0 new",
        countClassName: count !== null && count > 0 ? "border-red-300/35 bg-red-500/15 text-red-100" : "border-white/10 text-slate-400",
        group: "watchlists",
      });
    }

    for (const status of savedStatuses) {
      const savedScreenId = parseSavedScreenId(status.view);
      const sourceId = savedScreenId ? String(savedScreenId) : status.view.id;
      const metadata = savedScreenId ? inboxSourceMetadata.get(monitoringSourceKey("saved_screen", sourceId)) : undefined;
      if (savedScreenId && !shouldShowSource("saved_screen", sourceId)) continue;
      const inboxCount = savedScreenId ? inboxSourceCounts.get(monitoringSourceKey("saved_screen", sourceId)) : undefined;
      const count = inboxCount ?? null;
      rows.push({
        key: `saved-${status.view.id}`,
        sourceType: "saved_screen",
        sourceId,
        subscriptionId: metadata?.subscription_id,
        title: status.view.name,
        subtitle: `Saved screen${status.error ? " - refresh failed" : ""}`,
        href: savedViewHref(status.view),
        alertHref: manageAlertsHrefForSavedView(status.view),
        count,
        countLabel: count === null ? "Loading" : `${count} new`,
        countClassName: count !== null && count > 0 ? "border-sky-300/30 bg-sky-300/15 text-sky-100" : "border-white/10 text-slate-400",
        group: "saved_screens",
        onOpen: () => markSeen(status.view.id),
      });
    }

    for (const source of additionalInboxSources) {
      const count = inboxSourceCounts.get(monitoringSourceKey(normalizedMonitoringSourceType(source.type), source.id)) ?? null;
      const href = hrefForInboxSource(source);
      rows.push({
        key: `inbox-source-${source.type}-${source.id}`,
        sourceType: normalizedMonitoringSourceType(source.type),
        sourceId: source.id,
        subscriptionId: source.subscription_id,
        title: source.name,
        subtitle: sourceTypeLabel(source.type),
        href,
        alertHref: manageAlertsHrefForSource(source.type, source.id, href),
        count,
        countLabel: count === null ? "Loading" : count > 0 ? `${count} new` : "0 new",
        countClassName: count === null ? "border-white/10 text-slate-400" : sourceCountClassName(source.type, count),
        group: sourceGroup(source.type),
      });
    }

    return rows;
  }, [additionalInboxSources, inbox, inboxSourceCounts, inboxSourceMetadata, markSeen, savedStatuses, sourceMetadata, visibleWatchlists]);
  const groupedMonitoredSourceRows = useMemo(
    () =>
      (["watchlists", "saved_screens", "strategies", "other"] as MonitoringSourceGroup[])
        .map((group) => ({ group, rows: monitoredSourceRows.filter((row) => row.group === group) }))
        .filter((section) => section.rows.length > 0),
    [monitoredSourceRows],
  );
  const monitoredSourceCount = monitoredSourceRows.length;
  const inboxItems = useMemo(
    () =>
      [...(inbox?.items ?? inbox?.alerts ?? inbox?.latest_important ?? [])].sort(
        (a, b) => new Date(b.timestamp ?? b.event_created_at ?? b.created_at).getTime() - new Date(a.timestamp ?? a.event_created_at ?? a.created_at).getTime(),
      ),
    [inbox],
  );
  const inboxSourceFilterOptions = useMemo(
    () => Array.from(new Set(inboxItems.map((item) => normalizedMonitoringSourceType(item.monitoring_source_type ?? item.source_type)))).sort(),
    [inboxItems],
  );
  const inboxCategoryFilterOptions = useMemo(
    () =>
      Array.from(
        new Set(
          inboxItems
            .map((item) => item.data_category ?? item.trigger_type ?? item.alert_type)
            .filter((category): category is string => Boolean(category)),
        ),
      ).sort(),
    [inboxItems],
  );
  const filteredInboxItems = useMemo(() => {
    return inboxItems.filter((item) => {
      const unread = item.is_unread ?? !item.read_at;
      if (inboxFilter === "unread" && !unread) return false;
      if (inboxFilter === "read" && unread) return false;
      if (inboxSourceFilter !== "all" && normalizedMonitoringSourceType(item.monitoring_source_type ?? item.source_type) !== inboxSourceFilter) return false;
      const category = item.data_category ?? item.trigger_type ?? item.alert_type;
      return inboxCategoryFilter === "all" || category === inboxCategoryFilter;
    });
  }, [inboxCategoryFilter, inboxFilter, inboxItems, inboxSourceFilter]);
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
      // Alert materialization runs in scheduled/background work. Rendering
      // this page should read the persisted inbox, never wait on a provider
      // refresh for every watched symbol.
      const nextInbox = await getMonitoringInbox(undefined, { source: "MonitoringInbox" });
      setInbox(nextInbox);
      setSourceMetadata((current) => current ?? nextInbox.sources);
      if (nextInbox.counts) setSourceCounts(nextInbox.counts);
      dispatchUnreadUpdated(nextInbox.counts?.total_unread ?? nextInbox.unread_total ?? 0);
    } catch (error) {
      setInboxStatus(error instanceof ApiError && error.status === 401 ? "Sign in to load monitoring updates." : "Monitoring updates are temporarily unavailable.");
    }
  };

  const refreshSourceMetadata = async () => {
    try {
      const response = await getMonitoringSources({ source: "MonitoringSources" });
      setSourceMetadata(response.sources);
    } catch {
      // The inbox response remains a safe fallback if this lightweight
      // metadata request is temporarily unavailable.
    }
  };

  const refreshSourceCounts = async () => {
    try {
      const response = await getMonitoringSourceCounts({ source: "MonitoringSourceCounts" });
      setSourceCounts(response.counts);
      dispatchUnreadUpdated(response.unread_total);
    } catch {
      // Keep the count neutral while the full inbox request is in flight.
    }
  };

  const refreshWatchlists = async () => {
    setWatchlistsStatus(null);
    try {
      const nextWatchlists = await listWatchlists();
      setWatchlists(nextWatchlists);
    } catch (error) {
      setWatchlistsStatus(error instanceof ApiError && error.status === 401 ? "Sign in to load watchlists." : "Watchlists are temporarily unavailable.");
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
    if (response.counts) setSourceCounts(response.counts);
    dispatchUnreadUpdated(response.counts?.total_unread ?? response.unread_count);
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
      // Counts can cover more alerts than the initial inbox window. Refill
      // after a read-state mutation so an active unread/read filter never
      // appears empty while matching alerts still exist on the server.
      void refreshInbox();
    } catch {
      void refreshInbox();
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
      setReadActionMessage("Unable to delete the selected updates.");
    } finally {
      setPendingReadAction((current) => (current === actionKey ? null : current));
    }
  };

  const markSourceNewRead = async (source: MonitoredSourceRow) => {
    const sourceType = normalizedMonitoringSourceType(source.sourceType);
    if (!(["watchlist", "saved_screen", "strategy"] as string[]).includes(sourceType) || source.count === null || source.count <= 0) return;
    const actionKey = `source-read:${source.key}`;
    setPendingReadAction(actionKey);
    setReadActionMessage(null);
    try {
      const response = await markMonitoringSourceRead(source.sourceId, sourceType);
      applyMutationSuccess(response);
      setSelectedItemIds((current) =>
        current.filter((id) => {
          const item = inboxItems.find((candidate) => candidate.id === id);
          return !item || normalizedMonitoringSourceType(item.source_type) !== sourceType || String(item.source_id) !== source.sourceId;
        }),
      );
      setReadActionMessage(`All new updates in ${source.title} are marked read.`);
      void refreshInbox();
      if (sourceType === "watchlist") void refreshWatchlists();
    } catch {
      setReadActionMessage(`Unable to mark new updates in ${source.title} as read.`);
    } finally {
      setPendingReadAction((current) => (current === actionKey ? null : current));
    }
  };

  const confirmRemoveMonitoring = async () => {
    const source = pendingRemoveSource;
    if (!source) return;
    const actionKey = `remove-source:${source.key}`;
    setPendingRemoveKey(actionKey);
    setReadActionMessage(null);
    try {
      await stopMonitoringSource(normalizedMonitoringSourceType(source.sourceType), source.sourceId);
      setInbox((current) => removeInboxSource(current, source));
      setSourceMetadata((current) => current?.filter((item) => !(normalizedMonitoringSourceType(item.type) === normalizedMonitoringSourceType(source.sourceType) && item.id === source.sourceId)) ?? null);
      setSourceCounts((current) => (current ? {
        ...current,
        sources: current.sources.filter((item) => !(normalizedMonitoringSourceType(item.type) === normalizedMonitoringSourceType(source.sourceType) && item.id === source.sourceId)),
      } : current));
      setPendingRemoveSource(null);
      setReadActionMessage("Monitoring removed.");
      dispatchUnreadUpdated(Math.max((inbox?.unread_total ?? 0) - (source.count ?? 0), 0));
    } catch {
      setReadActionMessage("Couldn't remove monitoring. Try again.");
    } finally {
      setPendingRemoveKey((current) => (current === actionKey ? null : current));
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
    void refreshSourceMetadata();
    void refreshSourceCounts();
    void refreshInbox();
    void refreshWatchlists();
  }, []);

  useEffect(() => {
    setInboxPage((page) => Math.min(Math.max(page, 1), totalInboxPages));
  }, [totalInboxPages]);

  useEffect(() => {
    setInboxPage(1);
  }, [inboxCategoryFilter, inboxFilter, inboxPageSize, inboxSourceFilter]);

  useEffect(() => {
    // Saved-screen monitoring is already represented by the inbox response.
    // Do not issue a full saved-screen events request per card just to render
    // a count that the inbox has already calculated.
    if (sourceMetadata === null && inbox === null) return;
    setSavedStatuses(
      visibleSavedViews.map((view) => ({
        view,
        unseenCount: (() => {
          const savedScreenId = parseSavedScreenId(view);
          return savedScreenId ? inboxSourceCounts.get(monitoringSourceKey("saved_screen", String(savedScreenId))) ?? 0 : 0;
        })(),
        status: "ready",
        error: null,
      })),
    );
  }, [inbox, inboxSourceCounts, sourceMetadata, visibleSavedViews]);

  return (
    <div className="flex flex-col gap-6">
      <section className="order-1 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Watchlist events</div>
          <div className="mt-2 text-3xl font-semibold text-white">{watchlistsStatus && watchlists.length === 0 ? "-" : sourceCountsLoading ? "—" : totalWatchlistNew}</div>
          <p className="mt-1 text-sm text-slate-400">new</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Saved screen events</div>
          <div className="mt-2 text-3xl font-semibold text-white">{sourceCountsLoading ? "—" : inboxStatus && !inbox ? "-" : totalSavedViewNew}</div>
          <p className="mt-1 text-sm text-slate-400">new</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Sources monitored</div>
          <div className="mt-2 text-3xl font-semibold text-white">{inboxLoading && watchlists.length === 0 ? "—" : watchlistsStatus && watchlists.length === 0 ? "-" : monitoredSourceCount}</div>
          <p className="mt-1 text-sm text-slate-400">active</p>
        </div>
        <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Alerts today</div>
          <div className="mt-2 text-3xl font-semibold text-white">{inboxLoading ? "—" : inboxStatus && !inbox ? "-" : inbox?.unread_total ?? 0}</div>
          <p className="mt-1 text-sm text-slate-400">unread</p>
        </div>
      </section>

      {!entitlementsLoading && hiddenSourceCount > 0 ? (
        <div className="order-2">
          <UpgradePrompt
          title="Monitor every source with Premium"
          body={`Free monitors ${monitoringLimit} sources in the inbox. ${hiddenSourceCount} saved source${hiddenSourceCount === 1 ? " is" : "s are"} waiting behind the Premium limit.`}
          />
        </div>
      ) : null}

      <div className="order-4 xl:order-3">
        <EventCalendarPanel canUseEventCalendar={canUseEventCalendar} loadingEntitlements={entitlementsLoading} />
      </div>

      <section className="order-3 grid gap-6 xl:order-4 xl:grid-cols-[minmax(280px,0.8fr)_minmax(520px,1.4fr)]">
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

          <div className="mt-4 space-y-5">
            {entitlementsLoading ? (
              <MonitoringPanelSkeleton />
            ) : groupedMonitoredSourceRows.map((section) => (
              <div key={section.group}>
                <div className="mb-2 flex items-center gap-2 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  <span>{sourceGroupLabels[section.group]}</span>
                  <span className="text-slate-600">/</span>
                  <span>{section.rows.length}</span>
                </div>
                <div className="divide-y divide-white/10">
                  {section.rows.map((source) => (
                    <MonitoredSourceCard
                      key={source.key}
                      href={source.href}
                      onClick={source.onOpen}
                      title={source.title}
                      subtitle={source.subtitle}
                      countLabel={source.countLabel}
                      countClassName={source.countClassName}
                      alertHref={source.alertHref}
                      onClearNew={(["watchlist", "saved_screen", "strategy"] as string[]).includes(normalizedMonitoringSourceType(source.sourceType)) && source.count !== null && source.count > 0 ? () => void markSourceNewRead(source) : undefined}
                      clearNewDisabled={Boolean(pendingReadAction)}
                      clearNewPending={pendingReadAction === `source-read:${source.key}`}
                      onRemove={() => setPendingRemoveSource(source)}
                      removeDisabled={Boolean(pendingRemoveKey)}
                    />
                  ))}
                </div>
              </div>
            ))}
            {!entitlementsLoading && monitoredSourceRows.length === 0 ? (
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
              <span className="rounded-lg border border-white/10 px-2.5 py-1 text-xs text-slate-400">{inboxLoading ? "Loading" : inboxStatus && !inbox ? "-" : (inbox?.unread_total ?? 0)} unread</span>
              <label className="flex items-center gap-2 text-xs font-semibold text-slate-400">
                Page size
                <select
                  value={inboxPageSize}
                  onChange={(event) => setInboxPageSize(Number(event.target.value) as 5 | 10 | 25 | 50 | 100)}
                  className="rounded-lg border border-white/10 bg-slate-950/70 px-2 py-1 text-xs font-semibold text-slate-200 outline-none transition focus:border-emerald-300/40"
                >
                  {[5, 10, 25, 50, 100].map((size) => (
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
            {inboxSourceFilterOptions.length > 1 ? (
              <label className="ml-1 flex items-center gap-2 text-xs font-semibold text-slate-400">
                Source
                <select
                  value={inboxSourceFilter}
                  onChange={(event) => setInboxSourceFilter(event.target.value)}
                  className="rounded-lg border border-white/10 bg-slate-950/70 px-2 py-1.5 text-xs font-semibold text-slate-200 outline-none transition focus:border-emerald-300/40"
                >
                  <option value="all">All sources</option>
                  {inboxSourceFilterOptions.map((sourceType) => (
                    <option key={sourceType} value={sourceType}>{sourceTypeLabel(sourceType)}</option>
                  ))}
                </select>
              </label>
            ) : null}
            {inboxCategoryFilterOptions.length > 1 ? (
              <label className="flex items-center gap-2 text-xs font-semibold text-slate-400">
                Event
                <select
                  value={inboxCategoryFilter}
                  onChange={(event) => setInboxCategoryFilter(event.target.value)}
                  className="rounded-lg border border-white/10 bg-slate-950/70 px-2 py-1.5 text-xs font-semibold text-slate-200 outline-none transition focus:border-emerald-300/40"
                >
                  <option value="all">All events</option>
                  {inboxCategoryFilterOptions.map((category) => (
                    <option key={category} value={category}>{monitoringCategoryLabel(category)}</option>
                  ))}
                </select>
              </label>
            ) : null}
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

          <div className="mt-4 max-h-[34rem] space-y-3 overflow-y-auto pr-1" aria-busy={inboxLoading}>
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
            ) : inboxLoading ? (
              <MonitoringPanelSkeleton />
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
                  item.symbol
                    ? `/ticker/${encodeURIComponent(item.symbol)}`
                    : item.source_type === "watchlist"
                      ? `/watchlists/${encodeURIComponent(item.source_id)}?mode=all&recent_days=30&limit=25&only_new=1`
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
                      <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-slate-500">
                        {item.symbol ? <span className="font-semibold text-slate-300">{item.symbol}</span> : null}
                        <span><span className="text-slate-600">Monitored through </span>{item.monitoring_source_name ?? item.source_name}</span>
                        <span><span className="text-slate-600">Trigger </span>{monitoringCategoryLabel(item.trigger_type ?? item.alert_type)}</span>
                        <span>{sourceTypeLabel(item.monitoring_source_type ?? item.source_type)}</span>
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
      <WalnutConfirmDialog
        open={Boolean(pendingRemoveSource)}
        eyebrow="Monitoring source"
        title={`Stop monitoring this ${pendingRemoveSource ? sourceTypeLabel(pendingRemoveSource.sourceType) : "source"}?`}
        description={
          pendingRemoveSource ? (
            <>
              You will no longer receive monitoring updates or email alerts for "{pendingRemoveSource.title}."
              <br />
              The {sourceTypeLabel(pendingRemoveSource.sourceType)} itself will not be deleted.
            </>
          ) : null
        }
        confirmLabel="Stop monitoring"
        tone="danger"
        isBusy={Boolean(pendingRemoveKey)}
        onClose={() => {
          if (!pendingRemoveKey) setPendingRemoveSource(null);
        }}
        onConfirm={() => void confirmRemoveMonitoring()}
      />
    </div>
  );
}
