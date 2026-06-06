"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { getInsightsNews } from "@/lib/api";
import { cardClassName } from "@/lib/styles";
import type { InsightsNewsResponse } from "@/lib/types";

type Props = {
  page: number;
  limit: number;
};

function pageHref(page: number): string {
  return page <= 0 ? "/insights" : `/insights?page=${page}`;
}

function NewsSkeleton() {
  return (
    <div className="space-y-4">
      {Array.from({ length: 5 }).map((_, index) => (
        <div key={index} className="grid gap-4 border-b border-white/5 pb-4 last:border-0 sm:grid-cols-[120px_1fr]">
          <div className="h-20 w-full animate-pulse rounded-xl bg-white/10" />
          <div className="space-y-3">
            <div className="h-4 w-3/4 animate-pulse rounded bg-white/10" />
            <div className="h-3 w-full animate-pulse rounded bg-white/10" />
            <div className="h-3 w-2/3 animate-pulse rounded bg-white/10" />
          </div>
        </div>
      ))}
    </div>
  );
}

export function InsightsNewsClient({ page, limit }: Props) {
  const [response, setResponse] = useState<InsightsNewsResponse | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    getInsightsNews({ page, limit, signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setResponse(payload);
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setResponse({
            items: [],
            status: "unavailable",
            message: "Market data is temporarily unavailable.",
            page,
            limit,
            has_next: false,
          });
        }
      });
    return () => controller.abort();
  }, [limit, page]);

  return (
    <section className={cardClassName}>
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Market Headlines</p>
          <p className="mt-2 text-sm text-slate-400">A restrained market news feed built for discovery, not blog noise.</p>
        </div>
        <p className="text-xs text-slate-500">Page {(response?.page ?? page) + 1}</p>
      </div>

      {response ? (
        <NewsArticleList
          items={response.items}
          status={response.status}
          message={response.message}
          emptyMessage="No recent market news found."
          showImage
          compact={false}
        />
      ) : (
        <NewsSkeleton />
      )}

      <div className="mt-6 flex items-center justify-between gap-3">
        <Link
          href={pageHref(Math.max(page - 1, 0))}
          prefetch={false}
          aria-disabled={page === 0}
          className={`rounded-2xl border px-4 py-2 text-sm font-semibold ${
            page === 0
              ? "pointer-events-none border-white/10 bg-slate-950/40 text-slate-600"
              : "border-white/10 bg-slate-950/60 text-slate-200 hover:text-white"
          }`}
        >
          Previous
        </Link>
        <Link
          href={pageHref(page + 1)}
          prefetch={false}
          aria-disabled={!response?.has_next}
          className={`rounded-2xl border px-4 py-2 text-sm font-semibold ${
            response?.has_next
              ? "border-emerald-300/30 bg-emerald-400/10 text-emerald-100 hover:bg-emerald-400/15"
              : "pointer-events-none border-white/10 bg-slate-950/40 text-slate-600"
          }`}
        >
          Next
        </Link>
      </div>
    </section>
  );
}
