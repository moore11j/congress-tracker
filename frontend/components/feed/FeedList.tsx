"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { FeedItem } from "@/lib/types";
import { FeedCard } from "@/components/feed/FeedCard";
import { ghostButtonClassName, selectClassName } from "@/lib/styles";

type FeedListProps = {
  items: FeedItem[];
  page?: number;
  pageSize?: 25 | 50 | 100;
  total?: number | null;
  totalPages?: number;
};

export function FeedList({ items, page: initialPage = 1, pageSize: initialPageSize = 50, total: initialTotal = null, totalPages: initialTotalPages = 1 }: FeedListProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const [page, setPage] = useState(initialPage);
  const [pageSize, setPageSize] = useState(initialPageSize);
  const [total, setTotal] = useState<number | null>(initialTotal);

  useEffect(() => {
    setPage(initialPage);
    setPageSize(initialPageSize);
    setTotal(initialTotal);
  }, [initialPage, initialPageSize, initialTotal]);

  const totalPages = useMemo(() => {
    if (total) return Math.max(1, Math.ceil(total / pageSize));
    return initialTotalPages;
  }, [initialTotalPages, pageSize, total]);

  function updateParams(nextPage: number, nextPageSize: number) {
    const params = new URLSearchParams(searchParams.toString());
    params.set("page", String(nextPage));
    params.set("limit", String(nextPageSize));
    params.set("offset", String((nextPage - 1) * nextPageSize));
    params.set("include_total", "true");
    params.delete("cursor");
    params.delete("cursor_stack");
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  }

  function goToPage(p: number) {
    const next = Math.min(Math.max(1, p), totalPages);
    setPage(next);
    updateParams(next, pageSize);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function changePageSize(size: 25 | 50 | 100) {
    setPageSize(size);
    setPage(1);
    updateParams(1, size);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  return (
    <div className="flex flex-col gap-6">
      {!items.length ? (
        <div className="rounded-3xl border border-dashed border-white/20 bg-white/5 p-8 text-center">
          <div className="text-lg font-semibold">No trades yet</div>
          <p className="mt-2 text-sm text-slate-400">Try broadening your filters or lowering the minimum amount.</p>
        </div>
      ) : (
        items.map((item) => <FeedCard key={item.id} item={item} />)
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
          Page {page} of {totalPages}
        </div>

        <div className="flex gap-1">
          <button type="button" onClick={() => goToPage(1)} disabled={page <= 1} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{"<<"}</b>
          </button>
          <button type="button" onClick={() => goToPage(page - 1)} disabled={page <= 1} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{"<"}</b>
          </button>
          <button type="button" onClick={() => goToPage(page + 1)} disabled={total !== null && page >= totalPages} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{">"}</b>
          </button>
          <button type="button" onClick={() => goToPage(totalPages)} disabled={total !== null && page >= totalPages} className={`${ghostButtonClassName} rounded-lg px-2 py-1 disabled:cursor-not-allowed disabled:opacity-50`}>
            <b>{">>"}</b>
          </button>
        </div>
      </div>
    </div>
  );
}
