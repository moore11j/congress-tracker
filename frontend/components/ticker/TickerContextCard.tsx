"use client";

import type { MutableRefObject, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
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
const TICKER_UNAVAILABLE_MESSAGE = "Ticker news is temporarily unavailable.";

function isoDay(value: Date) {
  return value.toISOString().slice(0, 10);
}

function defaultWindow() {
  const today = new Date();
  const from = new Date(today);
  from.setDate(today.getDate() - 30);
  return { from: isoDay(from), to: isoDay(today) };
}

function unavailableNewsPage(limit = 20): InsightsNewsResponse {
  return {
    items: [],
    status: "unavailable",
    message: TICKER_UNAVAILABLE_MESSAGE,
    page: 0,
    limit,
    has_next: false,
  };
}

function unavailablePressPage(limit = 20): PressReleasesResponse {
  return {
    items: [],
    status: "unavailable",
    message: TICKER_UNAVAILABLE_MESSAGE,
    page: 0,
    limit,
    has_next: false,
  };
}

function unavailableSecPage(limit = 100): SecFilingsResponse {
  return {
    items: [],
    status: "unavailable",
    message: TICKER_UNAVAILABLE_MESSAGE,
    page: 0,
    limit,
    has_next: false,
  };
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function abortRequest(ref: MutableRefObject<AbortController | null>) {
  ref.current?.abort();
  ref.current = null;
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
  meta,
  children,
}: {
  title: string;
  meta?: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-white">{title}</h3>
        {meta ? <span className="text-[11px] uppercase tracking-[0.14em] text-slate-500">{meta}</span> : null}
      </div>
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
  const [dateWindow] = useState(defaultWindow);

  const [newsPages, setNewsPages] = useState<InsightsNewsResponse[]>([]);
  const [loadingNews, setLoadingNews] = useState(false);

  const [pressPages, setPressPages] = useState<PressReleasesResponse[]>([]);
  const [loadingPress, setLoadingPress] = useState(false);

  const [secPages, setSecPages] = useState<SecFilingsResponse[]>([]);
  const [loadingSec, setLoadingSec] = useState(false);

  const newsAbortRef = useRef<AbortController | null>(null);
  const pressAbortRef = useRef<AbortController | null>(null);
  const secAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    abortRequest(newsAbortRef);
    abortRequest(pressAbortRef);
    abortRequest(secAbortRef);
    setNewsPages([]);
    setPressPages([]);
    setSecPages([]);
    setLoadingNews(false);
    setLoadingPress(false);
    setLoadingSec(false);
  }, [symbol]);

  useEffect(() => {
    if (activeTab !== "news") {
      abortRequest(newsAbortRef);
      setLoadingNews(false);
      return;
    }
    if (newsPages.length > 0 || loadingNews) return;

    const controller = new AbortController();
    abortRequest(newsAbortRef);
    newsAbortRef.current = controller;
    setLoadingNews(true);

    getTickerNews(symbol, { page: 0, limit: 20, signal: controller.signal })
      .then((response) => {
        if (!controller.signal.aborted) setNewsPages([response]);
      })
      .catch((error) => {
        if (!isAbortError(error)) setNewsPages([unavailableNewsPage(20)]);
      })
      .finally(() => {
        if (newsAbortRef.current === controller) {
          newsAbortRef.current = null;
          setLoadingNews(false);
        }
      });

    return () => {
      controller.abort();
      if (newsAbortRef.current === controller) newsAbortRef.current = null;
    };
  }, [activeTab, loadingNews, newsPages.length, symbol]);

  useEffect(() => {
    if (activeTab !== "events") {
      abortRequest(pressAbortRef);
      setLoadingPress(false);
      return;
    }
    if (pressPages.length > 0 || loadingPress) return;

    const controller = new AbortController();
    abortRequest(pressAbortRef);
    pressAbortRef.current = controller;
    setLoadingPress(true);

    getTickerPressReleases(symbol, { page: 0, limit: 20, signal: controller.signal })
      .then((response) => {
        if (!controller.signal.aborted) setPressPages([response]);
      })
      .catch((error) => {
        if (!isAbortError(error)) setPressPages([unavailablePressPage(20)]);
      })
      .finally(() => {
        if (pressAbortRef.current === controller) {
          pressAbortRef.current = null;
          setLoadingPress(false);
        }
      });

    return () => {
      controller.abort();
      if (pressAbortRef.current === controller) pressAbortRef.current = null;
    };
  }, [activeTab, loadingPress, pressPages.length, symbol]);

  useEffect(() => {
    if (activeTab !== "events") {
      abortRequest(secAbortRef);
      setLoadingSec(false);
      return;
    }
    if (secPages.length > 0 || loadingSec) return;

    const controller = new AbortController();
    abortRequest(secAbortRef);
    secAbortRef.current = controller;
    setLoadingSec(true);

    getTickerSecFilings(symbol, {
      from: dateWindow.from,
      to: dateWindow.to,
      page: 0,
      limit: 100,
      signal: controller.signal,
    })
      .then((response) => {
        if (!controller.signal.aborted) setSecPages([response]);
      })
      .catch((error) => {
        if (!isAbortError(error)) setSecPages([unavailableSecPage(100)]);
      })
      .finally(() => {
        if (secAbortRef.current === controller) {
          secAbortRef.current = null;
          setLoadingSec(false);
        }
      });

    return () => {
      controller.abort();
      if (secAbortRef.current === controller) secAbortRef.current = null;
    };
  }, [activeTab, dateWindow.from, dateWindow.to, loadingSec, secPages.length, symbol]);

  const newsResponse = newsPages[newsPages.length - 1] ?? null;
  const newsItems = newsPages.flatMap((page) => page.items);

  const pressResponse = pressPages[pressPages.length - 1] ?? null;
  const pressItems = pressPages.flatMap((page) => page.items);

  const secResponse = secPages[secPages.length - 1] ?? null;
  const secItems = secPages.flatMap((page) => page.items);

  const hasAnyEvents = pressItems.length > 0 || secItems.length > 0;

  const loadMoreNews = async () => {
    if (!newsResponse?.has_next || loadingNews) return;
    const controller = new AbortController();
    abortRequest(newsAbortRef);
    newsAbortRef.current = controller;
    setLoadingNews(true);
    try {
      const next = await getTickerNews(symbol, {
        page: newsResponse.page + 1,
        limit: newsResponse.limit,
        signal: controller.signal,
      });
      if (!controller.signal.aborted) setNewsPages((current) => [...current, next]);
    } catch (error) {
      if (!isAbortError(error) && newsPages.length === 0) setNewsPages([unavailableNewsPage(newsResponse.limit)]);
    } finally {
      if (newsAbortRef.current === controller) {
        newsAbortRef.current = null;
        setLoadingNews(false);
      }
    }
  };

  const loadMorePress = async () => {
    if (!pressResponse?.has_next || loadingPress) return;
    const controller = new AbortController();
    abortRequest(pressAbortRef);
    pressAbortRef.current = controller;
    setLoadingPress(true);
    try {
      const next = await getTickerPressReleases(symbol, {
        page: pressResponse.page + 1,
        limit: pressResponse.limit,
        signal: controller.signal,
      });
      if (!controller.signal.aborted) setPressPages((current) => [...current, next]);
    } catch (error) {
      if (!isAbortError(error) && pressPages.length === 0) setPressPages([unavailablePressPage(pressResponse.limit)]);
    } finally {
      if (pressAbortRef.current === controller) {
        pressAbortRef.current = null;
        setLoadingPress(false);
      }
    }
  };

  const loadMoreSec = async () => {
    if (!secResponse?.has_next || loadingSec) return;
    const controller = new AbortController();
    abortRequest(secAbortRef);
    secAbortRef.current = controller;
    setLoadingSec(true);
    try {
      const next = await getTickerSecFilings(symbol, {
        from: dateWindow.from,
        to: dateWindow.to,
        page: secResponse.page + 1,
        limit: secResponse.limit,
        signal: controller.signal,
      });
      if (!controller.signal.aborted) setSecPages((current) => [...current, next]);
    } catch (error) {
      if (!isAbortError(error) && secPages.length === 0) setSecPages([unavailableSecPage(secResponse.limit)]);
    } finally {
      if (secAbortRef.current === controller) {
        secAbortRef.current = null;
        setLoadingSec(false);
      }
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
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">News</p>
                <p className="mt-2 text-sm text-slate-400">Recent headlines tied to {symbol}.</p>
              </div>
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
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Events / Filings</p>
                <p className="mt-2 text-sm text-slate-400">Press releases and SEC filings from the last 30 days.</p>
              </div>
              <span className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Last 30 days.</span>
            </div>
            <div className="max-h-[34rem] space-y-4 overflow-y-auto pr-1">
              <EventsSection title="Press Releases">
                {loadingPress && pressPages.length === 0 ? (
                  <TabSkeleton rows={2} />
                ) : (
                  <>
                    <NewsArticleList
                      items={pressItems.map((item) => ({
                        symbol: item.symbol,
                        title: item.title,
                        site: item.site ?? "Press release",
                        published_at: item.published_at,
                        url: item.url ?? "",
                        image_url: item.image_url,
                        summary: item.summary,
                        market_read: item.market_read,
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

              <EventsSection title="SEC Filings" meta="Last 30 days">
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
