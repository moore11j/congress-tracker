"use client";

import type { ReactNode } from "react";
import { useEffect, useState } from "react";
import {
  getTickerNews,
  getTickerPressReleases,
  getTickerSecFilings,
  type InsightsNewsResponse,
  type PressReleasesResponse,
  type SecFilingsResponse,
} from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { cardClassName } from "@/lib/styles";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";

type Props = {
  symbol: string;
  overview: ReactNode;
};

type ContextTab = "overview" | "news" | "events";

const TAB_CLASS = "rounded-lg px-3 py-1.5 text-xs font-semibold transition";

function isoDay(value: Date) {
  return value.toISOString().slice(0, 10);
}

function defaultWindow() {
  const today = new Date();
  const from = new Date(today);
  from.setDate(today.getDate() - 7);
  return { from: isoDay(from), to: isoDay(today) };
}

function TabSkeleton({ rows = 3 }: { rows?: number }) {
  return (
    <div className="space-y-3">
      {Array.from({ length: rows }, (_, index) => (
        <div key={index} className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-4">
          <SkeletonBlock className="h-3 w-32" />
          <SkeletonBlock className="mt-3 h-5 w-full" />
          <SkeletonBlock className="mt-2 h-4 w-5/6" />
        </div>
      ))}
    </div>
  );
}

function EventsSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
      <h3 className="text-sm font-semibold text-white">{title}</h3>
      <div className="mt-3">{children}</div>
    </section>
  );
}

function LoadMoreButton({
  disabled,
  label,
  onClick,
}: {
  disabled: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={`rounded-xl border px-3 py-2 text-sm font-semibold ${
        disabled
          ? "cursor-not-allowed border-white/10 bg-slate-950/40 text-slate-600"
          : "border-emerald-300/30 bg-emerald-400/10 text-emerald-100 hover:bg-emerald-400/15"
      }`}
    >
      {label}
    </button>
  );
}

export function TickerContextCard({ symbol, overview }: Props) {
  const [activeTab, setActiveTab] = useState<ContextTab>("overview");

  const [newsPages, setNewsPages] = useState<InsightsNewsResponse[]>([]);
  const [loadingNews, setLoadingNews] = useState(false);

  const [pressPages, setPressPages] = useState<PressReleasesResponse[]>([]);
  const [loadingPress, setLoadingPress] = useState(false);

  const [secPages, setSecPages] = useState<SecFilingsResponse[]>([]);
  const [loadingSec, setLoadingSec] = useState(false);

  const window = defaultWindow();

  useEffect(() => {
    setNewsPages([]);
    setPressPages([]);
    setSecPages([]);
  }, [symbol]);

  useEffect(() => {
    let cancelled = false;
    if (activeTab !== "news" || newsPages.length > 0 || loadingNews) return;

    setLoadingNews(true);
    getTickerNews(symbol, { page: 0, limit: 20 })
      .then((response) => {
        if (!cancelled) setNewsPages([response]);
      })
      .catch(() => {
        if (!cancelled) {
          setNewsPages([
            {
              items: [],
              status: "unavailable",
              message: "News data is unavailable from the current provider.",
              page: 0,
              limit: 20,
              has_next: false,
            },
          ]);
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingNews(false);
      });

    return () => {
      cancelled = true;
    };
  }, [activeTab, loadingNews, newsPages.length, symbol]);

  useEffect(() => {
    let cancelled = false;
    if (activeTab !== "events" || pressPages.length > 0 || loadingPress) return;

    setLoadingPress(true);
    getTickerPressReleases(symbol, { page: 0, limit: 20 })
      .then((response) => {
        if (!cancelled) setPressPages([response]);
      })
      .catch(() => {
        if (!cancelled) {
          setPressPages([
            {
              items: [],
              status: "unavailable",
              message: "News data is unavailable from the current provider.",
              page: 0,
              limit: 20,
              has_next: false,
            },
          ]);
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingPress(false);
      });

    return () => {
      cancelled = true;
    };
  }, [activeTab, loadingPress, pressPages.length, symbol]);

  useEffect(() => {
    let cancelled = false;
    if (activeTab !== "events" || secPages.length > 0 || loadingSec) return;

    setLoadingSec(true);
    getTickerSecFilings(symbol, { from: window.from, to: window.to, page: 0, limit: 100 })
      .then((response) => {
        if (!cancelled) setSecPages([response]);
      })
      .catch(() => {
        if (!cancelled) {
          setSecPages([
            {
              items: [],
              status: "unavailable",
              message: "News data is unavailable from the current provider.",
              page: 0,
              limit: 100,
              has_next: false,
            },
          ]);
        }
      })
      .finally(() => {
        if (!cancelled) setLoadingSec(false);
      });

    return () => {
      cancelled = true;
    };
  }, [activeTab, loadingSec, secPages.length, symbol, window.from, window.to]);

  const newsResponse = newsPages[newsPages.length - 1] ?? null;
  const newsItems = newsPages.flatMap((page) => page.items);

  const pressResponse = pressPages[pressPages.length - 1] ?? null;
  const pressItems = pressPages.flatMap((page) => page.items);

  const secResponse = secPages[secPages.length - 1] ?? null;
  const secItems = secPages.flatMap((page) => page.items);

  const hasAnyEvents = pressItems.length > 0 || secItems.length > 0;

  const loadMoreNews = async () => {
    if (!newsResponse?.has_next || loadingNews) return;
    setLoadingNews(true);
    try {
      const next = await getTickerNews(symbol, { page: newsResponse.page + 1, limit: newsResponse.limit });
      setNewsPages((current) => [...current, next]);
    } finally {
      setLoadingNews(false);
    }
  };

  const loadMorePress = async () => {
    if (!pressResponse?.has_next || loadingPress) return;
    setLoadingPress(true);
    try {
      const next = await getTickerPressReleases(symbol, { page: pressResponse.page + 1, limit: pressResponse.limit });
      setPressPages((current) => [...current, next]);
    } finally {
      setLoadingPress(false);
    }
  };

  const loadMoreSec = async () => {
    if (!secResponse?.has_next || loadingSec) return;
    setLoadingSec(true);
    try {
      const next = await getTickerSecFilings(symbol, {
        from: window.from,
        to: window.to,
        page: secResponse.page + 1,
        limit: secResponse.limit,
      });
      setSecPages((current) => [...current, next]);
    } finally {
      setLoadingSec(false);
    }
  };

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
          <button
            type="button"
            onClick={() => setActiveTab("events")}
            className={`${TAB_CLASS} ${activeTab === "events" ? "bg-emerald-400/15 text-emerald-200" : "text-slate-300 hover:bg-white/5"}`}
          >
            Events / Filings
          </button>
        </div>
      </div>

      <div className="mt-6">
        {activeTab === "overview" ? (
          overview
        ) : activeTab === "news" ? (
          <div className="space-y-4">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">News</p>
              <p className="mt-2 text-sm text-slate-400">Recent headlines tied to {symbol}.</p>
            </div>
            <div className="max-h-[34rem] overflow-y-auto pr-1">
              {loadingNews && newsPages.length === 0 ? (
                <TabSkeleton />
              ) : (
                <NewsArticleList
                  items={newsItems}
                  status={newsResponse?.status}
                  message={newsResponse?.message}
                  emptyMessage="No recent news found for this ticker."
                  showSymbol={false}
                  compact
                />
              )}
            </div>
            <LoadMoreButton
              disabled={!newsResponse?.has_next || loadingNews}
              label={loadingNews ? "Loading..." : "Load more"}
              onClick={loadMoreNews}
            />
          </div>
        ) : (
          <div className="space-y-4">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Events / Filings</p>
              <p className="mt-2 text-sm text-slate-400">Press releases and SEC filings from the last 7 days.</p>
            </div>
            <div className="max-h-[34rem] space-y-4 overflow-y-auto pr-1">
              <EventsSection title="Press Releases">
                {loadingPress && pressPages.length === 0 ? (
                  <TabSkeleton rows={2} />
                ) : (
                  <>
                    <NewsArticleList
                      items={pressItems.map((item) => ({
                        title: item.title,
                        site: item.site ?? "Press release",
                        published_at: item.published_at,
                        url: item.url ?? "",
                        summary: item.summary,
                        symbol: item.symbol,
                        source: item.source,
                      }))}
                      status={pressResponse?.status}
                      message={pressResponse?.message}
                      emptyMessage={!hasAnyEvents ? "No recent press releases or SEC filings found in the selected window." : "No recent press releases found."}
                      showSymbol={false}
                      compact
                    />
                    <div className="mt-3">
                      <LoadMoreButton
                        disabled={!pressResponse?.has_next || loadingPress}
                        label={loadingPress ? "Loading..." : "Load more press releases"}
                        onClick={loadMorePress}
                      />
                    </div>
                  </>
                )}
              </EventsSection>

              <EventsSection title="SEC Filings">
                {loadingSec && secPages.length === 0 ? (
                  <TabSkeleton rows={3} />
                ) : secResponse?.status === "unavailable" ? (
                  <div className="text-sm text-slate-400">{secResponse.message}</div>
                ) : secItems.length === 0 ? (
                  <div className="text-sm text-slate-400">
                    {!hasAnyEvents ? "No recent press releases or SEC filings found in the selected window." : "No recent SEC filings found in the selected window."}
                  </div>
                ) : (
                  <>
                    <div className="overflow-hidden rounded-xl border border-white/10">
                      <div className="grid grid-cols-[8rem_6rem_minmax(0,1fr)_5rem] gap-3 border-b border-white/10 bg-slate-950/70 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                        <span>Date</span>
                        <span>Form</span>
                        <span>Title</span>
                        <span>Link</span>
                      </div>
                      {secItems.map((item) => (
                        <div
                          key={`${item.form_type}-${item.filing_date}-${item.url ?? item.title ?? "row"}`}
                          className="grid grid-cols-[8rem_6rem_minmax(0,1fr)_5rem] gap-3 border-b border-white/10 px-3 py-2.5 text-sm text-slate-300 last:border-b-0"
                        >
                          <span>{formatDateShort(item.filing_date ?? null)}</span>
                          <span className="font-semibold text-slate-100">{item.form_type}</span>
                          <span className="truncate">{item.title ?? "SEC filing"}</span>
                          <span>
                            {item.url ? (
                              <a href={item.url} target="_blank" rel="noreferrer" className="font-semibold text-emerald-200 hover:text-emerald-100">
                                Open
                              </a>
                            ) : (
                              <span className="text-slate-500">-</span>
                            )}
                          </span>
                        </div>
                      ))}
                    </div>
                    <div className="mt-3">
                      <LoadMoreButton
                        disabled={!secResponse?.has_next || loadingSec}
                        label={loadingSec ? "Loading..." : "Load more filings"}
                        onClick={loadMoreSec}
                      />
                    </div>
                  </>
                )}
              </EventsSection>
            </div>
          </div>
        )}
      </div>
    </section>
  );
}
