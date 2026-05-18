"use client";

import type { MutableRefObject, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import {
  getEvents,
  getTickerFinancials,
  getTickerNews,
  getTickerPressReleases,
  getTickerSecFilings,
  type EventItem,
  type InsightsNewsResponse,
  type NewsItem,
  type PressReleasesResponse,
  type SecFilingsResponse,
  type TickerFinancialsResponse,
} from "@/lib/api";
import { formatDateShort } from "@/lib/format";
import { cardClassName } from "@/lib/styles";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { TickerFinancialsPanel, TickerFinancialsSkeleton } from "@/components/ticker/TickerFinancialsPanel";

type Props = {
  symbol: string;
  overview: ReactNode;
  className?: string;
};

type ContextTab = "overview" | "news" | "financials" | "events";

const TAB_CLASS = "rounded-lg px-3 py-1.5 text-xs font-semibold transition";
const NEWS_UNAVAILABLE_MESSAGE = "News is temporarily unavailable.";
const PRESS_UNAVAILABLE_MESSAGE = "Press releases are temporarily unavailable.";
const FILINGS_UNAVAILABLE_MESSAGE = "Filings are temporarily unavailable.";
const FINANCIALS_UNAVAILABLE_MESSAGE = "Financial data is not available for this ticker yet.";
const NEWS_EMPTY_MESSAGE = "No recent news found for this ticker.";
const PRESS_EMPTY_MESSAGE = "No press releases are available for this ticker right now.";
const FILINGS_EMPTY_MESSAGE = "No recent filings are available for this ticker right now.";
const IMPLEMENTATION_DETAIL_TERMS = [
  ["current", "data", "plan"].join(" "),
  ["data", "plan"].join(" "),
  ["f", "mp"].join(""),
  ["a", "pi"].join(""),
  ["prov", "ider"].join(""),
  ["end", "point"].join(""),
  ["unavailable", "under"].join(" "),
];
const PRESS_RELEASE_SITES = ["business wire", "globenewswire", "pr newswire", "prnewswire", "accesswire", "newsfile", "businesswire"];
const DISCLOSURE_EVENT_TYPES = new Set(["congress_trade", "insider_trade"]);
const PRESS_REQUEST_TIMEOUT_MS = 12000;
type PressFallbackKind = "none" | "press_like" | "company_updates";
const SCROLL_REGION_CLASS = [
  "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
  "[&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-track]:bg-white/[0.03]",
  "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60]",
].join(" ");

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
    message: NEWS_UNAVAILABLE_MESSAGE,
    page: 0,
    limit,
    has_next: false,
  };
}

function unavailablePressPage(limit = 20): PressReleasesResponse {
  return {
    items: [],
    status: "unavailable",
    message: PRESS_UNAVAILABLE_MESSAGE,
    page: 0,
    limit,
    has_next: false,
  };
}

function unavailableSecPage(limit = 100): SecFilingsResponse {
  return {
    items: [],
    status: "unavailable",
    message: FILINGS_UNAVAILABLE_MESSAGE,
    page: 0,
    limit,
    has_next: false,
  };
}

function userFacingMessage(message: string | null | undefined, fallback: string): string {
  if (!message) return fallback;
  const normalized = message.toLowerCase();
  return IMPLEMENTATION_DETAIL_TERMS.some((term) => normalized.includes(term)) ? fallback : message;
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function startRequestTimeout(controller: AbortController, timeoutMs: number) {
  let timedOut = false;
  const timeoutId = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);
  return {
    get timedOut() {
      return timedOut;
    },
    clear() {
      clearTimeout(timeoutId);
    },
  };
}

function normalizePressPage(response: PressReleasesResponse, limit = 20): PressReleasesResponse {
  const items = Array.isArray(response.items) ? response.items : [];
  const status = response.status ?? (items.length > 0 ? "ok" : "empty");
  return {
    ...response,
    items,
    status,
    message: response.message ?? (status === "empty" ? PRESS_EMPTY_MESSAGE : undefined),
    page: Number.isFinite(response.page) ? response.page : 0,
    limit: Number.isFinite(response.limit) ? response.limit : limit,
    has_next: Boolean(response.has_next),
  };
}

function isPressReleaseLikeNews(item: NewsItem): boolean {
  const site = (item.site ?? "").trim().toLowerCase();
  const title = (item.title ?? "").trim().toLowerCase();
  const source = (item.source ?? "").trim().toLowerCase();
  return PRESS_RELEASE_SITES.some((needle) => site.includes(needle) || source.includes(needle) || title.includes(needle));
}

function pressReleaseArticles(items: PressReleasesResponse["items"]): NewsItem[] {
  return items.map((item) => ({
    symbol: item.symbol,
    title: item.title,
    site: item.site ?? "Press release",
    published_at: item.published_at,
    url: item.url ?? "",
    image_url: item.image_url,
    summary: item.summary,
    market_read: item.market_read,
    source: item.source,
  }));
}

function disclosureTypeLabel(eventType: string): string {
  if (eventType === "congress_trade") return "Congress";
  if (eventType === "insider_trade") return "Insider";
  return "Disclosure";
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

export function TickerContextCard({ symbol, overview, className }: Props) {
  const [activeTab, setActiveTab] = useState<ContextTab>("overview");
  const [dateWindow] = useState(defaultWindow);

  const [newsPages, setNewsPages] = useState<InsightsNewsResponse[]>([]);
  const [loadingNews, setLoadingNews] = useState(false);

  const [pressPages, setPressPages] = useState<PressReleasesResponse[]>([]);
  const [pressFallbackItems, setPressFallbackItems] = useState<NewsItem[]>([]);
  const [pressFallbackKind, setPressFallbackKind] = useState<PressFallbackKind>("none");
  const [loadingPress, setLoadingPress] = useState(false);

  const [secPages, setSecPages] = useState<SecFilingsResponse[]>([]);
  const [disclosureEvents, setDisclosureEvents] = useState<EventItem[]>([]);
  const [loadingSec, setLoadingSec] = useState(false);

  const [financials, setFinancials] = useState<TickerFinancialsResponse | null>(null);
  const [loadingFinancials, setLoadingFinancials] = useState(false);

  const newsAbortRef = useRef<AbortController | null>(null);
  const pressAbortRef = useRef<AbortController | null>(null);
  const secAbortRef = useRef<AbortController | null>(null);
  const financialsAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    abortRequest(newsAbortRef);
    abortRequest(pressAbortRef);
    abortRequest(secAbortRef);
    abortRequest(financialsAbortRef);
    setNewsPages([]);
    setPressPages([]);
    setPressFallbackItems([]);
    setPressFallbackKind("none");
    setSecPages([]);
    setDisclosureEvents([]);
    setFinancials(null);
    setLoadingNews(false);
    setLoadingPress(false);
    setLoadingSec(false);
    setLoadingFinancials(false);
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
  }, [activeTab, newsPages.length, symbol]);

  useEffect(() => {
    if (activeTab !== "financials") {
      abortRequest(financialsAbortRef);
      setLoadingFinancials(false);
      return;
    }
    if (financials || loadingFinancials) return;

    const controller = new AbortController();
    abortRequest(financialsAbortRef);
    financialsAbortRef.current = controller;
    setLoadingFinancials(true);

    getTickerFinancials(symbol, { signal: controller.signal })
      .then((response) => {
        if (!controller.signal.aborted) setFinancials(response);
      })
      .catch((error) => {
        if (!isAbortError(error)) {
          setFinancials({
            symbol,
            companyName: null,
            status: "unavailable",
            message: FINANCIALS_UNAVAILABLE_MESSAGE,
            summary: {},
            annual: [],
            quarterly: [],
            earnings: [],
            updatedAt: new Date().toISOString(),
          });
        }
      })
      .finally(() => {
        if (financialsAbortRef.current === controller) {
          financialsAbortRef.current = null;
          setLoadingFinancials(false);
        }
      });

    return () => {
      controller.abort();
      if (financialsAbortRef.current === controller) financialsAbortRef.current = null;
    };
  }, [activeTab, financials, loadingFinancials, symbol]);

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
    const timeoutGuard = startRequestTimeout(controller, PRESS_REQUEST_TIMEOUT_MS);
    setLoadingPress(true);
    setPressFallbackItems([]);
    setPressFallbackKind("none");

    function applyPressFallback(items: NewsItem[]) {
      const pressLikeItems = items.filter(isPressReleaseLikeNews).slice(0, 20);
      if (pressLikeItems.length > 0) {
        setPressFallbackItems(pressLikeItems);
        setPressFallbackKind("press_like");
        return;
      }
      setPressFallbackItems(items.slice(0, 20));
      setPressFallbackKind(items.length > 0 ? "company_updates" : "none");
    }

    async function loadPress() {
      try {
        const response = normalizePressPage(await getTickerPressReleases(symbol, { page: 0, limit: 20, signal: controller.signal }), 20);
        if (controller.signal.aborted) return;
        setPressPages([response]);

        if (response.items.length === 0 && response.status !== "unavailable") {
          const fallback = await getTickerNews(symbol, { page: 0, limit: 50, signal: controller.signal });
          if (!controller.signal.aborted) applyPressFallback(Array.isArray(fallback.items) ? fallback.items : []);
        }
      } catch (error) {
        if (isAbortError(error) && !timeoutGuard.timedOut) return;
        if (timeoutGuard.timedOut) {
          setPressPages([unavailablePressPage(20)]);
          return;
        }
        try {
          const fallback = await getTickerNews(symbol, { page: 0, limit: 50, signal: controller.signal });
          if (!controller.signal.aborted) applyPressFallback(Array.isArray(fallback.items) ? fallback.items : []);
        } catch (fallbackError) {
          if (isAbortError(fallbackError) && !timeoutGuard.timedOut) return;
          setPressFallbackItems([]);
        }
        if (!controller.signal.aborted || timeoutGuard.timedOut) setPressPages([unavailablePressPage(20)]);
      } finally {
        timeoutGuard.clear();
        if (pressAbortRef.current === controller) {
          pressAbortRef.current = null;
          setLoadingPress(false);
        }
      }
    }

    loadPress();

    return () => {
      timeoutGuard.clear();
      controller.abort();
      if (pressAbortRef.current === controller) pressAbortRef.current = null;
    };
  }, [activeTab, pressPages.length, symbol]);

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
    setDisclosureEvents([]);

    let active = true;

    async function loadFilings() {
      try {
        const response = await getTickerSecFilings(symbol, {
          from: dateWindow.from,
          to: dateWindow.to,
          page: 0,
          limit: 100,
          signal: controller.signal,
        });
        if (!active || controller.signal.aborted) return;
        setSecPages([response]);

        if (response.items.length === 0) {
          const fallback = await getEvents({ symbol, recent_days: 30, limit: 50 });
          if (!active) return;
          setDisclosureEvents(
            fallback.items
              .filter((item) => DISCLOSURE_EVENT_TYPES.has(item.event_type))
              .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
              .slice(0, 12),
          );
        }
      } catch (error) {
        if (isAbortError(error)) return;
        if (active) setSecPages([unavailableSecPage(100)]);
        try {
          const fallback = await getEvents({ symbol, recent_days: 30, limit: 50 });
          if (!active) return;
          setDisclosureEvents(
            fallback.items
              .filter((item) => DISCLOSURE_EVENT_TYPES.has(item.event_type))
              .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
              .slice(0, 12),
          );
        } catch {
          if (active) setDisclosureEvents([]);
        }
      } finally {
        if (secAbortRef.current === controller) {
          secAbortRef.current = null;
          setLoadingSec(false);
        }
      }
    }

    loadFilings();

    return () => {
      active = false;
      controller.abort();
      if (secAbortRef.current === controller) secAbortRef.current = null;
    };
  }, [activeTab, dateWindow.from, dateWindow.to, secPages.length, symbol]);

  const newsResponse = newsPages[newsPages.length - 1] ?? null;
  const newsItems = newsPages.flatMap((page) => page.items);

  const pressResponse = pressPages[pressPages.length - 1] ?? null;
  const pressItems = pressPages.flatMap((page) => page.items);
  const pressArticleItems = pressItems.length > 0 ? pressReleaseArticles(pressItems) : pressFallbackItems;
  const pressSectionTitle = pressItems.length > 0 || pressFallbackKind !== "company_updates" ? "Press Releases" : "Recent Company Updates";
  const pressMessage = pressResponse?.status === "unavailable"
    ? PRESS_UNAVAILABLE_MESSAGE
    : userFacingMessage(pressResponse?.message, PRESS_EMPTY_MESSAGE);
  const canLoadMorePress = Boolean(pressResponse?.has_next && pressItems.length > 0 && pressFallbackKind === "none");

  const secResponse = secPages[secPages.length - 1] ?? null;
  const secItems = secPages.flatMap((page) => page.items);
  const filingsMessage = userFacingMessage(
    secResponse?.message,
    secResponse?.status === "unavailable" ? FILINGS_UNAVAILABLE_MESSAGE : FILINGS_EMPTY_MESSAGE,
  );

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
    const timeoutGuard = startRequestTimeout(controller, PRESS_REQUEST_TIMEOUT_MS);
    setLoadingPress(true);
    try {
      const next = normalizePressPage(await getTickerPressReleases(symbol, {
        page: pressResponse.page + 1,
        limit: pressResponse.limit,
        signal: controller.signal,
      }), pressResponse.limit);
      if (!controller.signal.aborted) setPressPages((current) => [...current, next]);
    } catch (error) {
      if ((!isAbortError(error) || timeoutGuard.timedOut) && pressPages.length === 0) {
        setPressPages([unavailablePressPage(pressResponse.limit)]);
      }
    } finally {
      timeoutGuard.clear();
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
    <section className={`${cardClassName} ${className ?? ""} xl:flex xl:min-h-0 xl:flex-col`}>
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
            onClick={() => setActiveTab("financials")}
            className={`${TAB_CLASS} ${activeTab === "financials" ? "bg-emerald-400/15 text-emerald-200" : "text-slate-300 hover:bg-white/5"}`}
          >
            Financials
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

      <div className="relative mt-6 xl:flex-1 xl:min-h-0">
        <div
          className={`${
            activeTab === "overview" ? "relative" : "invisible pointer-events-none select-none"
          } xl:h-full xl:min-h-0 xl:overflow-y-auto xl:pr-1 ${SCROLL_REGION_CLASS}`}
          aria-hidden={activeTab !== "overview"}
        >
          {overview}
        </div>
        {activeTab === "news" ? (
          <div className="absolute inset-0 flex min-h-0 flex-col space-y-4 overflow-hidden">
            <div className="flex flex-wrap items-center justify-between gap-3 xl:shrink-0">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">News</p>
                <p className="mt-2 text-sm text-slate-400">Recent headlines tied to {symbol}.</p>
              </div>
            </div>
            <div className={`min-h-0 flex-1 overflow-y-auto pr-1 ${SCROLL_REGION_CLASS}`}>
              {loadingNews && newsPages.length === 0 ? (
                <TabSkeleton />
              ) : (
                <NewsArticleList
                  items={newsItems}
                  status={newsResponse?.status}
                  message={userFacingMessage(newsResponse?.message, newsResponse?.status === "unavailable" ? NEWS_UNAVAILABLE_MESSAGE : NEWS_EMPTY_MESSAGE)}
                  emptyMessage={NEWS_EMPTY_MESSAGE}
                  showSymbol={false}
                  showImage
                  compact
                />
              )}
              <div className="mt-4">
                <LoadMoreButton
                  disabled={!newsResponse?.has_next || loadingNews}
                  label={loadingNews ? "Loading..." : "Load more"}
                  onClick={loadMoreNews}
                />
              </div>
            </div>
          </div>
        ) : null}
        {activeTab === "financials" ? (
          <div className="absolute inset-0 flex min-h-0 flex-col space-y-4 overflow-hidden">
            <div className="flex flex-wrap items-center justify-between gap-3 xl:shrink-0">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Financials</p>
                <p className="mt-2 text-sm text-slate-400">Fundamental trends and earnings quality for {symbol}.</p>
              </div>
              {financials?.updatedAt ? (
                <span className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Updated {formatDateShort(financials.updatedAt)}</span>
              ) : null}
            </div>
            <div className={`min-h-0 flex-1 overflow-y-auto pr-1 ${SCROLL_REGION_CLASS}`}>
              {loadingFinancials || !financials ? <TickerFinancialsSkeleton /> : <TickerFinancialsPanel data={financials} />}
            </div>
          </div>
        ) : null}
        {activeTab === "events" ? (
          <div className="absolute inset-0 flex min-h-0 flex-col space-y-4 overflow-hidden">
            <div className="flex flex-wrap items-center justify-between gap-3 xl:shrink-0">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Events / Filings</p>
                <p className="mt-2 text-sm text-slate-400">Press releases, filings, and disclosure activity from the last 30 days.</p>
              </div>
              <span className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Last 30 days.</span>
            </div>
            <div className={`min-h-0 flex-1 space-y-4 overflow-y-auto pr-1 ${SCROLL_REGION_CLASS}`}>
              <EventsSection title={pressSectionTitle}>
                {loadingPress && pressPages.length === 0 && pressFallbackItems.length === 0 ? (
                  <TabSkeleton rows={2} />
                ) : (
                  <>
                    <NewsArticleList
                      items={pressArticleItems}
                      status={pressArticleItems.length > 0 ? undefined : pressResponse?.status}
                      message={pressMessage}
                      emptyMessage={PRESS_EMPTY_MESSAGE}
                      showSymbol={false}
                      showImage
                      compact
                    />
                    {canLoadMorePress ? (
                      <div className="mt-3">
                        <LoadMoreButton
                          disabled={loadingPress}
                          label={loadingPress ? "Loading..." : "Load more press releases"}
                          onClick={loadMorePress}
                        />
                      </div>
                    ) : null}
                  </>
                )}
              </EventsSection>

              <EventsSection title="Filings / Disclosures" meta="Last 30 days">
                {loadingSec && secPages.length === 0 ? (
                  <TabSkeleton rows={3} />
                ) : secItems.length > 0 ? (
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
                ) : disclosureEvents.length > 0 ? (
                  <div className="overflow-hidden rounded-xl border border-white/10">
                    <div className="grid grid-cols-[8rem_7rem_minmax(0,1fr)_5rem] gap-3 border-b border-white/10 bg-slate-950/70 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                      <span>Date</span>
                      <span>Type</span>
                      <span>Title</span>
                      <span>Link</span>
                    </div>
                    {disclosureEvents.map((event) => (
                      <div
                        key={event.id}
                        className="grid grid-cols-[8rem_7rem_minmax(0,1fr)_5rem] gap-3 border-b border-white/10 px-3 py-2.5 text-sm text-slate-300 last:border-b-0"
                      >
                        <span>{formatDateShort(event.ts ?? null)}</span>
                        <span className="font-semibold text-slate-100">{disclosureTypeLabel(event.event_type)}</span>
                        <span className="truncate">{event.headline ?? event.summary ?? "Disclosure activity"}</span>
                        <span>
                          {event.url ? (
                            <a href={event.url} target="_blank" rel="noreferrer" className="font-semibold text-emerald-200 hover:text-emerald-100">
                              Open
                            </a>
                          ) : (
                            <span className="text-slate-500">-</span>
                          )}
                        </span>
                      </div>
                    ))}
                  </div>
                ) : secResponse?.status === "unavailable" ? (
                  <div className="text-sm text-slate-400">{filingsMessage}</div>
                ) : secItems.length === 0 ? (
                  <div className="text-sm text-slate-400">{FILINGS_EMPTY_MESSAGE}</div>
                ) : (
                  <div className="text-sm text-slate-400">{FILINGS_EMPTY_MESSAGE}</div>
                )}
              </EventsSection>
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
