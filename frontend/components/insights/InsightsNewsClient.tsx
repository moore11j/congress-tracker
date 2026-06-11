"use client";

import { useEffect, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { getInsightsNews } from "@/lib/api";
import { cardClassName } from "@/lib/styles";
import type { InsightsNewsResponse } from "@/lib/types";

type Props = {
  page: number;
  limit: number;
};

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
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();
  const [response, setResponse] = useState<InsightsNewsResponse | null>(null);
  const [shouldScrollToHeadlines, setShouldScrollToHeadlines] = useState(false);

  useEffect(() => {
    if (!shouldScrollToHeadlines) return;

    const frame = window.requestAnimationFrame(() => {
      document.getElementById("market-headlines")?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    });

    setShouldScrollToHeadlines(false);
    return () => window.cancelAnimationFrame(frame);
  }, [page, shouldScrollToHeadlines]);

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

  function goToPage(nextPage: number) {
    const params = new URLSearchParams(searchParams.toString());
    if (nextPage <= 0) {
      params.delete("page");
    } else {
      params.set("page", String(nextPage));
    }
    const query = params.toString();
    setShouldScrollToHeadlines(true);
    router.push(`${pathname}${query ? `?${query}` : ""}`, { scroll: false });
  }

  return (
    <section id="market-headlines" className={cardClassName}>
      <div className="mb-6 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h2 className="text-2xl font-semibold text-white">Market Headlines</h2>
          <p className="mt-2 text-sm text-slate-400">A restrained market-news feed built for discovery, not blog noise.</p>
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
        <button
          type="button"
          onClick={() => goToPage(Math.max(page - 1, 0))}
          disabled={page === 0}
          className={`rounded-2xl border px-4 py-2 text-sm font-semibold ${
            page === 0
              ? "cursor-not-allowed border-white/10 bg-slate-950/40 text-slate-600"
              : "border-white/10 bg-slate-950/60 text-slate-200 hover:text-white"
          }`}
        >
          Previous
        </button>
        <button
          type="button"
          onClick={() => goToPage(page + 1)}
          disabled={!response?.has_next}
          className={`rounded-2xl border px-4 py-2 text-sm font-semibold ${
            response?.has_next
              ? "border-emerald-300/30 bg-emerald-400/10 text-emerald-100 hover:bg-emerald-400/15"
              : "cursor-not-allowed border-white/10 bg-slate-950/40 text-slate-600"
          }`}
        >
          Next
        </button>
      </div>
    </section>
  );
}
