"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import { getTickerNews, type InsightsNewsResponse } from "@/lib/api";
import { cardClassName } from "@/lib/styles";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type Props = {
  symbol: string;
  overview: ReactNode;
};

type ContextTab = "overview" | "news";

const TAB_CLASS =
  "rounded-lg px-3 py-1.5 text-xs font-semibold transition";

function NewsTabSkeleton() {
  return (
    <div className="space-y-3">
      {[0, 1, 2].map((row) => (
        <div key={row} className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-4">
          <SkeletonBlock className="h-3 w-36" />
          <SkeletonBlock className="mt-3 h-5 w-full" />
          <SkeletonBlock className="mt-2 h-4 w-11/12" />
          <SkeletonBlock className="mt-2 h-4 w-4/5" />
        </div>
      ))}
    </div>
  );
}

export function TickerContextCard({ symbol, overview }: Props) {
  const [activeTab, setActiveTab] = useState<ContextTab>("overview");
  const [newsResponse, setNewsResponse] = useState<InsightsNewsResponse | null>(null);
  const [loadingNews, setLoadingNews] = useState(false);

  useEffect(() => {
    let cancelled = false;

    if (activeTab !== "news" || newsResponse || loadingNews) {
      return () => {
        cancelled = true;
      };
    }

    setLoadingNews(true);
    getTickerNews(symbol, { limit: 8 })
      .then((response) => {
        if (!cancelled) setNewsResponse(response);
      })
      .catch(() => {
        if (!cancelled) {
          setNewsResponse({
            items: [],
            status: "unavailable",
            message: "News is unavailable under the current data plan.",
          });
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingNews(false);
      });

    return () => {
      cancelled = true;
    };
  }, [activeTab, loadingNews, newsResponse, symbol]);

  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold text-white">Ticker Context</h2>
          <p className="mt-2 text-sm text-slate-400">News, intelligence, and market context for {symbol}.</p>
        </div>
        <div className="flex flex-wrap rounded-xl border border-white/10 bg-slate-950/80 p-1">
          <button
            type="button"
            onClick={() => setActiveTab("overview")}
            className={`${TAB_CLASS} ${activeTab === "overview" ? "bg-emerald-400/15 text-emerald-200" : "text-slate-300 hover:bg-white/5"}`}
          >
            Overview
          </button>
          <button
            type="button"
            onClick={() => setActiveTab("news")}
            className={`${TAB_CLASS} ${activeTab === "news" ? "bg-emerald-400/15 text-emerald-200" : "text-slate-300 hover:bg-white/5"}`}
          >
            News
          </button>
          <button type="button" disabled className={`${TAB_CLASS} cursor-not-allowed text-slate-600`}>
            Events / Filings
          </button>
        </div>
      </div>

      <div className="mt-6">
        {activeTab === "overview" ? (
          overview
        ) : loadingNews ? (
          <NewsTabSkeleton />
        ) : (
          <div className="space-y-4">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Latest News</p>
              <p className="mt-2 text-sm text-slate-400">Recent headlines tied to {symbol}.</p>
            </div>
            <NewsArticleList
              items={newsResponse?.items ?? []}
              status={newsResponse?.status}
              message={newsResponse?.message}
              emptyMessage="No recent news found for this ticker."
              showSymbol={false}
            />
            <Link
              href={`/insights?ticker=${encodeURIComponent(symbol)}`}
              prefetch={false}
              className="inline-flex text-sm font-semibold text-emerald-200 transition hover:text-emerald-100"
            >
              View all insights for {symbol}
            </Link>
          </div>
        )}
      </div>
    </section>
  );
}
