"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { FeedItem } from "@/lib/types";
import { FeedCard } from "@/components/feed/FeedCard";
import { FeedTable } from "@/components/feed/FeedTable";
import { FeedMountLogger } from "@/components/feed/FeedMountLogger";
import { ghostButtonClassName, selectClassName } from "@/lib/styles";

type FeedListProps = {
  items: FeedItem[];
  page?: number;
  pageSize?: 25 | 50 | 100;
  total?: number | null;
  totalPages?: number;
  hasMore?: boolean | null;
  overlaySignals?: SignalOverlayMap;
  canViewPremiumMetrics?: boolean;
  debugLifecycle?: boolean;
};

type SignalOverlayMap = Record<string, { score: number; band: string }>;
type SignalOverlay = { score: number; band: string } | null;
type FeedViewMode = "table" | "cards";

const feedViewStorageKey = "walnut:feed:view";
const DEFAULT_FEED_PAGE_SIZE = 25;

export function FeedList({ items, page: initialPage = 1, pageSize: initialPageSize = DEFAULT_FEED_PAGE_SIZE, total: initialTotal = null, totalPages: initialTotalPages = 1, hasMore: initialHasMore = null, overlaySignals, canViewPremiumMetrics = false, debugLifecycle = false }: FeedListProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [page, setPage] = useState(initialPage);
  const [pageSize, setPageSize] = useState(initialPageSize);
  const [total, setTotal] = useState<number | null>(initialTotal);
  const [hasMore, setHasMore] = useState<boolean | null>(initialHasMore);
  const [viewMode, setViewMode] = useState<FeedViewMode>("table");
  const [expandedItemId, setExpandedItemId] = useState<number | null>(null);

  useEffect(() => {
    const saved = window.localStorage.getItem(feedViewStorageKey);
    if (saved === "cards") setViewMode("cards");
  }, []);

  useEffect(() => {
    setPage(initialPage);
    setPageSize(initialPageSize);
    setTotal(initialTotal);
    setHasMore(initialHasMore);
  }, [initialPage, initialPageSize, initialTotal, initialHasMore]);

  const totalPages = useMemo(() => {
    if (total) return Math.max(1, Math.ceil(total / pageSize));
    return initialTotalPages;
  }, [initialTotalPages, pageSize, total]);

  function updateParams(nextPage: number, nextPageSize: number) {
    const params = new URLSearchParams(searchParams.toString());
    params.set("page", String(nextPage));
    params.set("page_size", String(nextPageSize));
    params.set("limit", String(nextPageSize));
    params.set("offset", String((nextPage - 1) * nextPageSize));
    params.delete("cursor");
    params.delete("cursor_stack");
    router.push(`${pathname}?${params.toString()}`);
  }

  function goToPage(p: number) {
    const next = total !== null ? Math.min(Math.max(1, p), totalPages) : Math.max(1, p);
    setPage(next);
    setExpandedItemId(null);
    updateParams(next, pageSize);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function changePageSize(size: 25 | 50 | 100) {
    setPageSize(size);
    setExpandedItemId(null);
    setPage(1);
    updateParams(1, size);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function changeViewMode(nextMode: FeedViewMode) {
    setViewMode(nextMode);
    setExpandedItemId(null);
    window.localStorage.setItem(feedViewStorageKey, nextMode);
  }

  function toggleExpandedItem(id: number) {
    setExpandedItemId((current) => (current === id ? null : id));
  }

  return (
    <div className="flex flex-col gap-6">
      <FeedMountLogger
        name="FeedList"
        enabled={debugLifecycle}
        detail={{ items: items.length, page, pageSize, total: total ?? null }}
      />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="inline-flex rounded-xl border border-white/10 bg-slate-950/50 p-1" aria-label="Feed view">
          {(["table", "cards"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              onClick={() => changeViewMode(mode)}
              className={`rounded-lg px-3 py-1.5 text-xs font-semibold uppercase tracking-[0.14em] transition ${
                viewMode === mode ? "bg-emerald-400/15 text-emerald-100" : "text-slate-400 hover:text-slate-100"
              }`}
              aria-pressed={viewMode === mode}
            >
              {mode === "table" ? "Table" : "Cards"}
            </button>
          ))}
        </div>
        <div className="text-xs text-slate-500">
          {viewMode === "table" ? "Compact rows. Open a row for full card details." : "Full card layout."}
        </div>
      </div>

      {!items.length ? (
        <div className="rounded-3xl border border-dashed border-white/20 bg-white/5 p-8 text-center">
          <div className="text-lg font-semibold">No trades yet</div>
          <p className="mt-2 text-sm text-slate-400">Try broadening your filters or lowering the minimum amount.</p>
        </div>
      ) : viewMode === "table" ? (
        <FeedTable
          items={items}
          expandedItemId={expandedItemId}
          onToggleExpanded={toggleExpandedItem}
          overlaySignals={overlaySignals}
          canViewPremiumMetrics={canViewPremiumMetrics}
        />
      ) : (
        items.map((item) => {
          const overlay: SignalOverlay = overlaySignals ? overlaySignals[String(item.id)] ?? null : null;
          return <FeedCard key={item.id} item={item} signalOverlay={overlay} canViewPremiumMetrics={canViewPremiumMetrics} />;
        })
      )}

      <div className="mt-2 flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <span className="text-sm text-slate-400">Rows</span>
          <select
            value={pageSize}
            onChange={(e) => changePageSize(Number(e.target.value) as 25 | 50 | 100)}
            className={`${selectClassName} w-auto min-w-[88px] rounded-lg px-2 py-1 text-sm`}
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>

        <div className="text-sm text-slate-400">
          {total !== null ? `Page ${page} of ${totalPages}` : hasMore ? `Page ${page} · More available` : `Page ${page}`}
        </div>

        <div className="flex gap-1">
          <button type="button" onClick={() => goToPage(1)} disabled={page <= 1} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{"<<"}</b>
          </button>
          <button type="button" onClick={() => goToPage(page - 1)} disabled={page <= 1} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{"<"}</b>
          </button>
          <button type="button" onClick={() => goToPage(page + 1)} disabled={total !== null ? page >= totalPages : hasMore === false} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{">"}</b>
          </button>
          <button type="button" onClick={() => goToPage(totalPages)} disabled={total === null || page >= totalPages} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{">>"}</b>
          </button>
        </div>
      </div>
    </div>
  );
}
