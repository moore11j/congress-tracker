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
  type TickerValuationMetrics,
  type TickerValuationSection,
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
const ACTIVITY_UNAVAILABLE_MESSAGE = "Activity is temporarily unavailable.";
const NEWS_LOADING_MESSAGE = "Loading news.";
const FINANCIALS_LOADING_MESSAGE = "Loading financials.";
const PRESS_LOADING_MESSAGE = "Loading press releases.";
const FILINGS_LOADING_MESSAGE = "Loading filings.";
const ACTIVITY_LOADING_MESSAGE = "Loading activity.";
const NEWS_EMPTY_MESSAGE = "No recent news found.";
const PRESS_EMPTY_MESSAGE = "No press releases found.";
const FILINGS_EMPTY_MESSAGE = "No recent filings found.";
const ACTIVITY_EMPTY_MESSAGE = "No recent disclosure activity found.";
const EVENTS_EMPTY_MESSAGE = "No recent filings or disclosure activity found.";
const IMPLEMENTATION_DETAIL_TERMS = [
  ["current", "data", "plan"].join(" "),
  ["data", "plan"].join(" "),
  ["f", "mp"].join(""),
  ["a", "pi"].join(""),
  ["prov", "ider"].join(""),
  ["end", "point"].join(""),
  ["unavailable", "under"].join(" "),
];
const DISCLOSURE_EVENT_TYPES = new Set(["congress_trade", "insider_trade"]);
const NEWS_REQUEST_TIMEOUT_MS = 12000;
const PRESS_REQUEST_TIMEOUT_MS = 12000;
const FINANCIALS_REQUEST_TIMEOUT_MS = 15000;
const SEC_REQUEST_TIMEOUT_MS = 12000;
const SEC_FORM_TITLES: Record<string, string> = {
  "3": "Initial Statement of Beneficial Ownership",
  "4": "Statement of Changes in Beneficial Ownership",
  "5": "Annual Statement of Beneficial Ownership",
  "6-K": "Report of Foreign Private Issuer",
  "8-K": "Current Report",
  "10-K": "Annual Report",
  "10-Q": "Quarterly Report",
  "20-F": "Annual Report of Foreign Private Issuer",
  "13F-HR": "Institutional Holdings Report",
  "SD": "Specialized Disclosure Report",
  "13D": "Beneficial Ownership Report",
  "13G": "Passive Beneficial Ownership Report",
  "144": "Notice of Proposed Sale of Securities",
  "FORM 3": "Initial Statement of Beneficial Ownership",
  "FORM 4": "Statement of Changes in Beneficial Ownership",
  "FORM 5": "Annual Statement of Beneficial Ownership",
  "FORM 144": "Notice of Proposed Sale of Securities",
  "S-3": "Shelf Registration Statement",
  "S-8": "Securities Registration: Employee Benefit Plans",
  "S-8 POS": "Post-Effective Amendment to Registration Statement",
  "POS AM": "Post-Effective Amendment",
};
const SCROLL_REGION_CLASS = [
  "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
  "[&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-track]:bg-white/[0.03]",
  "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60]",
].join(" ");

function normalizeSecForm(value: string | null | undefined) {
  return (value ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function getSecFormTitle(form: string | null | undefined, rawTitle: string | null | undefined) {
  const title = rawTitle?.trim();
  if (title && title.toLowerCase() !== "sec filing") return title;
  const normalized = normalizeSecForm(form);
  const mapped = SEC_FORM_TITLES[normalized] ?? SEC_FORM_TITLES[normalized.replace(/^FORM\s+/, "")] ?? SEC_FORM_TITLES[`FORM ${normalized}`];
  if (mapped) return mapped;
  return "SEC Filing";
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

function normalizeNewsPage(response: InsightsNewsResponse, limit = 20): InsightsNewsResponse {
  const items = Array.isArray(response.items) ? response.items : [];
  const rawStatus = response.status ?? (items.length > 0 ? "ok" : "empty");
  const status = rawStatus === "warming" || rawStatus === "loading" && items.length === 0 ? "no_data" : rawStatus === "empty" ? "no_data" : rawStatus;
  return {
    ...response,
    items,
    status,
    item_count: typeof response.item_count === "number" ? response.item_count : items.length,
    message: status === "loading" ? NEWS_LOADING_MESSAGE : response.message ?? (status === "no_data" ? NEWS_EMPTY_MESSAGE : undefined),
    page: Number.isFinite(response.page) ? response.page : 0,
    limit: Number.isFinite(response.limit) ? response.limit : limit,
    has_next: Boolean(response.has_next),
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

function unavailableFinancials(symbol: string, message = "Financial data is not available for this ticker yet."): TickerFinancialsResponse {
  return {
    symbol,
    companyName: null,
    status: "unavailable",
    message,
    summary: {},
    annual: [],
    quarterly: [],
    earnings: [],
    forecasts: { nextQuarter: null, nextFiscalYear: null },
    updatedAt: new Date().toISOString(),
  };
}

function normalizeFinancialsResponse(symbol: string, response: TickerFinancialsResponse): TickerFinancialsResponse {
  const rawStatus = typeof response.status === "string" ? response.status : "partial";
  const status = rawStatus === "warming" ? "loading" : rawStatus;
  const sections = response.sections && typeof response.sections === "object" ? response.sections : {};
  const sectionsPresent = Array.isArray(response.sections_present)
    ? response.sections_present
    : Object.entries(sections)
        .filter(([, value]) => Array.isArray(value) ? value.length > 0 : Boolean(value && typeof value === "object" && Object.keys(value).length > 0))
        .map(([key]) => key);
  const incomeSection = sections.income && typeof sections.income === "object" ? (sections.income as { annual?: unknown; quarterly?: unknown }) : null;
  const earningsSection = Array.isArray(sections.earnings) ? sections.earnings : null;
  const estimatesSection = sections.analyst_estimates && typeof sections.analyst_estimates === "object" ? (sections.analyst_estimates as TickerFinancialsResponse["forecasts"]) : null;
  const valuationSection = sections.valuation && typeof sections.valuation === "object" ? (sections.valuation as TickerValuationSection) : null;
  const summary = response.summary && typeof response.summary === "object" ? response.summary : {};
  const valuationMetrics =
    response.valuation_metrics && typeof response.valuation_metrics === "object"
      ? response.valuation_metrics
      : valuationSection?.valuation_metrics && typeof valuationSection.valuation_metrics === "object"
        ? valuationSection.valuation_metrics
        : ({
            forward_pe: summary.forwardPE ?? valuationSection?.forwardPE ?? valuationSection?.forward_pe ?? null,
            forward_pe_source: summary.forwardPESource ?? valuationSection?.forwardPESource ?? valuationSection?.forward_pe_source ?? null,
            forward_peg: summary.forwardPEG ?? valuationSection?.forwardPEG ?? valuationSection?.forward_peg ?? null,
            expected_eps_growth_rate_percent:
              summary.expectedEpsGrowthRatePercent ??
              valuationSection?.expectedEpsGrowthRatePercent ??
              valuationSection?.expected_eps_growth_rate_percent ??
              null,
            as_of: valuationSection?.as_of ?? null,
            status:
              summary.forwardPE ?? valuationSection?.forwardPE ?? valuationSection?.forward_pe ?? summary.forwardPEG ?? valuationSection?.forwardPEG ?? valuationSection?.forward_peg
                ? "ok"
                : "unavailable",
          } satisfies TickerValuationMetrics);
  const healthSection = sections.health && typeof sections.health === "object" ? (sections.health as { debtToEquity?: number | null; currentRatio?: number | null; assetRatio?: number | null }) : null;
  return {
    ...response,
    symbol: response.symbol || symbol,
    status,
    sections_present: sectionsPresent,
    summary: {
      ...summary,
      trailingPE: summary.trailingPE ?? valuationSection?.trailingPE ?? null,
      forwardPE: summary.forwardPE ?? valuationSection?.forwardPE ?? valuationMetrics.forward_pe ?? null,
      forwardPESource: summary.forwardPESource ?? valuationSection?.forwardPESource ?? valuationMetrics.forward_pe_source ?? null,
      forwardPEG: summary.forwardPEG ?? valuationSection?.forwardPEG ?? valuationMetrics.forward_peg ?? null,
      expectedEpsGrowthRatePercent:
        summary.expectedEpsGrowthRatePercent ?? valuationSection?.expectedEpsGrowthRatePercent ?? valuationMetrics.expected_eps_growth_rate_percent ?? null,
      debtToEquity: summary.debtToEquity ?? healthSection?.debtToEquity ?? null,
      currentRatio: summary.currentRatio ?? healthSection?.currentRatio ?? null,
      assetRatio: summary.assetRatio ?? healthSection?.assetRatio ?? null,
    },
    valuation_metrics: valuationMetrics,
    annual: Array.isArray(response.annual) ? response.annual : Array.isArray(incomeSection?.annual) ? (incomeSection.annual as TickerFinancialsResponse["annual"]) : [],
    quarterly: Array.isArray(response.quarterly) ? response.quarterly : Array.isArray(incomeSection?.quarterly) ? (incomeSection.quarterly as TickerFinancialsResponse["quarterly"]) : [],
    earnings: Array.isArray(response.earnings) ? response.earnings : (earningsSection as TickerFinancialsResponse["earnings"] | null) ?? [],
    forecasts: response.forecasts && typeof response.forecasts === "object" ? response.forecasts : estimatesSection ?? { nextQuarter: null, nextFiscalYear: null },
    message: status === "loading" ? FINANCIALS_LOADING_MESSAGE : response.message,
    updated_at: response.updated_at || response.updatedAt || new Date().toISOString(),
    updatedAt: response.updatedAt || response.updated_at || new Date().toISOString(),
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
  const rawStatus = response.status ?? (items.length > 0 ? "ok" : "empty");
  const status = rawStatus === "warming" || rawStatus === "loading" && items.length === 0 ? "no_data" : rawStatus === "empty" ? "no_data" : rawStatus;
  return {
    ...response,
    items,
    status,
    item_count: typeof response.item_count === "number" ? response.item_count : items.length,
    message: status === "loading" ? PRESS_LOADING_MESSAGE : response.message ?? (status === "no_data" ? PRESS_EMPTY_MESSAGE : undefined),
    page: Number.isFinite(response.page) ? response.page : 0,
    limit: Number.isFinite(response.limit) ? response.limit : limit,
    has_next: Boolean(response.has_next),
  };
}

function normalizeSecPage(response: SecFilingsResponse, limit = 100): SecFilingsResponse {
  const items = Array.isArray(response.items) ? response.items : [];
  const rawStatus = response.status ?? (items.length > 0 ? "ok" : "empty");
  const status = rawStatus === "warming" || rawStatus === "loading" && items.length === 0 ? "no_data" : rawStatus === "empty" ? "no_data" : rawStatus;
  return {
    ...response,
    items,
    status,
    item_count: typeof response.item_count === "number" ? response.item_count : items.length,
    message: status === "loading" ? FILINGS_LOADING_MESSAGE : response.message ?? (status === "no_data" ? FILINGS_EMPTY_MESSAGE : undefined),
    page: Number.isFinite(response.page) ? response.page : 0,
    limit: Number.isFinite(response.limit) ? response.limit : limit,
    has_next: Boolean(response.has_next),
  };
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

function trimmedString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.trim();
  return cleaned ? cleaned : null;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function firstDisclosureUrl(...records: Record<string, unknown>[]): string | null {
  const keys = [
    "url",
    "source_url",
    "sourceUrl",
    "filing_url",
    "filingUrl",
    "report_url",
    "reportUrl",
    "document_url",
    "documentUrl",
    "sec_url",
    "secUrl",
    "finalLink",
    "link",
  ];
  for (const record of records) {
    for (const key of keys) {
      const value = trimmedString(record[key]);
      if (value) return value;
    }
  }
  return null;
}

function disclosureEventUrl(event: EventItem): string | null {
  const payload = asRecord(event.payload);
  const nestedPayload = asRecord(payload.payload);
  const raw = asRecord(payload.raw);
  return firstDisclosureUrl({ url: event.url }, payload, nestedPayload, raw);
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

  const [newsPages, setNewsPages] = useState<InsightsNewsResponse[]>([]);
  const [loadingNews, setLoadingNews] = useState(false);

  const [pressPages, setPressPages] = useState<PressReleasesResponse[]>([]);
  const [loadingPress, setLoadingPress] = useState(false);

  const [secPages, setSecPages] = useState<SecFilingsResponse[]>([]);
  const [disclosureEvents, setDisclosureEvents] = useState<EventItem[]>([]);
  const [loadingSec, setLoadingSec] = useState(false);
  const [eventsStatus, setEventsStatus] = useState<string | null>(null);
  const [loadingEvents, setLoadingEvents] = useState(false);

  const [financials, setFinancials] = useState<TickerFinancialsResponse | null>(null);
  const [loadingFinancials, setLoadingFinancials] = useState(false);

  const newsAbortRef = useRef<AbortController | null>(null);
  const pressAbortRef = useRef<AbortController | null>(null);
  const secAbortRef = useRef<AbortController | null>(null);
  const eventsAbortRef = useRef<AbortController | null>(null);
  const financialsAbortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    abortRequest(newsAbortRef);
    abortRequest(pressAbortRef);
    abortRequest(secAbortRef);
    abortRequest(eventsAbortRef);
    abortRequest(financialsAbortRef);
    setNewsPages([]);
    setPressPages([]);
    setSecPages([]);
    setDisclosureEvents([]);
    setEventsStatus(null);
    setFinancials(null);
    setLoadingNews(false);
    setLoadingPress(false);
    setLoadingSec(false);
    setLoadingEvents(false);
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
    const timeoutGuard = startRequestTimeout(controller, NEWS_REQUEST_TIMEOUT_MS);
    setLoadingNews(true);

    getTickerNews(symbol, { page: 0, limit: 20, signal: controller.signal })
      .then((response) => {
        if (!controller.signal.aborted) setNewsPages([normalizeNewsPage(response, 20)]);
      })
      .catch((error) => {
        if (timeoutGuard.timedOut || !isAbortError(error)) setNewsPages([unavailableNewsPage(20)]);
      })
      .finally(() => {
        timeoutGuard.clear();
        if (newsAbortRef.current === controller) {
          newsAbortRef.current = null;
          setLoadingNews(false);
        }
      });

    return () => {
      timeoutGuard.clear();
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
    if (financials || financialsAbortRef.current) return;

    const controller = new AbortController();
    abortRequest(financialsAbortRef);
    financialsAbortRef.current = controller;
    const timeoutGuard = startRequestTimeout(controller, FINANCIALS_REQUEST_TIMEOUT_MS);
    setLoadingFinancials(true);

    getTickerFinancials(symbol, { signal: controller.signal })
      .then((response) => {
        if (!controller.signal.aborted) setFinancials(normalizeFinancialsResponse(symbol, response));
      })
      .catch((error) => {
        if (timeoutGuard.timedOut || !isAbortError(error)) {
          setFinancials(unavailableFinancials(symbol));
        }
      })
      .finally(() => {
        timeoutGuard.clear();
        if (financialsAbortRef.current === controller) {
          financialsAbortRef.current = null;
          setLoadingFinancials(false);
        }
      });

    return () => {
      timeoutGuard.clear();
      controller.abort();
      if (financialsAbortRef.current === controller) financialsAbortRef.current = null;
    };
  }, [activeTab, financials, symbol]);

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

    async function loadPress() {
      try {
        const response = normalizePressPage(await getTickerPressReleases(symbol, { page: 0, limit: 20, signal: controller.signal }), 20);
        if (controller.signal.aborted) return;
        setPressPages([response]);
      } catch (error) {
        if (isAbortError(error) && !timeoutGuard.timedOut) return;
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
    const timeoutGuard = startRequestTimeout(controller, SEC_REQUEST_TIMEOUT_MS);
    setLoadingSec(true);

    let active = true;

    async function loadFilings() {
      try {
        const response = normalizeSecPage(await getTickerSecFilings(symbol, {
          page: 0,
          limit: 100,
          signal: controller.signal,
        }), 100);
        if (!active || controller.signal.aborted) return;
        setSecPages([response]);
      } catch (error) {
        if (isAbortError(error) && !timeoutGuard.timedOut) return;
        if (active) setSecPages([unavailableSecPage(100)]);
      } finally {
        timeoutGuard.clear();
        if (secAbortRef.current === controller) {
          secAbortRef.current = null;
          setLoadingSec(false);
        }
      }
    }

    loadFilings();

    return () => {
      active = false;
      timeoutGuard.clear();
      controller.abort();
      if (secAbortRef.current === controller) secAbortRef.current = null;
    };
  }, [activeTab, secPages.length, symbol]);

  useEffect(() => {
    if (activeTab !== "events") {
      abortRequest(eventsAbortRef);
      setLoadingEvents(false);
      return;
    }
    if (eventsStatus || loadingEvents) return;

    const controller = new AbortController();
    abortRequest(eventsAbortRef);
    eventsAbortRef.current = controller;
    const timeoutGuard = startRequestTimeout(controller, SEC_REQUEST_TIMEOUT_MS);
    setLoadingEvents(true);

    let active = true;

    async function loadEvents() {
      try {
        const response = await getEvents({
          symbol,
          recent_days: 365,
          limit: 50,
          enrich_prices: 0,
          signal: controller.signal,
          source: "TickerPage",
        });
        if (!active || controller.signal.aborted) return;
        const items = response.items
          .filter((item) => DISCLOSURE_EVENT_TYPES.has(item.event_type))
          .sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime())
          .slice(0, 12);
        setDisclosureEvents(items);
        setEventsStatus(items.length > 0 ? "ok" : response.status === "loading" ? "loading" : "no_data");
      } catch (error) {
        if (isAbortError(error) && !timeoutGuard.timedOut) return;
        if (active) {
          setDisclosureEvents([]);
          setEventsStatus("unavailable");
        }
      } finally {
        timeoutGuard.clear();
        if (eventsAbortRef.current === controller) {
          eventsAbortRef.current = null;
          setLoadingEvents(false);
        }
      }
    }

    loadEvents();

    return () => {
      active = false;
      timeoutGuard.clear();
      controller.abort();
      if (eventsAbortRef.current === controller) eventsAbortRef.current = null;
    };
  }, [activeTab, eventsStatus, symbol]);

  const newsResponse = newsPages[newsPages.length - 1] ?? null;
  const newsItems = newsPages.flatMap((page) => page.items);

  const pressResponse = pressPages[pressPages.length - 1] ?? null;
  const pressItems = pressPages.flatMap((page) => page.items);
  const pressArticleItems = pressReleaseArticles(pressItems);
  const pressSectionTitle = "Press Releases";
  const pressMessage = pressResponse?.status === "unavailable"
    ? PRESS_UNAVAILABLE_MESSAGE
    : pressResponse?.status === "loading"
      ? PRESS_LOADING_MESSAGE
      : userFacingMessage(pressResponse?.message, PRESS_EMPTY_MESSAGE);
  const canLoadMorePress = Boolean(pressResponse?.has_next && pressItems.length > 0);

  const secResponse = secPages[secPages.length - 1] ?? null;
  const secItems = secPages.flatMap((page) => page.items);
  const showSecSection = true;
  const filingsMessage = userFacingMessage(
    secResponse?.message,
    secResponse?.status === "loading"
      ? FILINGS_LOADING_MESSAGE
      : secResponse?.status === "unavailable"
        ? FILINGS_UNAVAILABLE_MESSAGE
        : FILINGS_EMPTY_MESSAGE,
  );
  const activityMessage =
    eventsStatus === "loading"
      ? ACTIVITY_LOADING_MESSAGE
      : eventsStatus === "unavailable"
        ? ACTIVITY_UNAVAILABLE_MESSAGE
        : ACTIVITY_EMPTY_MESSAGE;
  const eventsSettled = !loadingPress && !loadingSec && !loadingEvents && Boolean(pressResponse) && Boolean(secResponse) && Boolean(eventsStatus);
  const allEventsSourcesEmpty = eventsSettled && pressItems.length === 0 && secItems.length === 0 && disclosureEvents.length === 0;

  const loadMoreNews = async () => {
    if (!newsResponse?.has_next || loadingNews) return;
    const controller = new AbortController();
    abortRequest(newsAbortRef);
    newsAbortRef.current = controller;
    setLoadingNews(true);
    try {
      const next = normalizeNewsPage(await getTickerNews(symbol, {
        page: newsResponse.page + 1,
        limit: newsResponse.limit,
        signal: controller.signal,
      }), newsResponse.limit);
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
      const next = normalizeSecPage(await getTickerSecFilings(symbol, {
        page: secResponse.page + 1,
        limit: secResponse.limit,
        signal: controller.signal,
      }), secResponse.limit);
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
                  message={userFacingMessage(
                    newsResponse?.message,
                    newsResponse?.status === "loading"
                      ? NEWS_LOADING_MESSAGE
                      : newsResponse?.status === "unavailable"
                        ? NEWS_UNAVAILABLE_MESSAGE
                        : NEWS_EMPTY_MESSAGE,
                  )}
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
                <p className="mt-2 text-sm text-slate-400">Latest press releases, filings, and disclosure activity.</p>
              </div>
              <span className="text-[11px] uppercase tracking-[0.14em] text-slate-500">Latest available.</span>
            </div>
            <div className={`min-h-0 flex-1 space-y-4 overflow-y-auto pr-1 ${SCROLL_REGION_CLASS}`}>
              <EventsSection title={pressSectionTitle}>
                {loadingPress && pressPages.length === 0 ? (
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

              {showSecSection ? (
                <EventsSection title="SEC Filings" meta="Latest available">
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
                            <span className="truncate">{getSecFormTitle(item.form_type, item.title)}</span>
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
                  ) : secResponse?.status === "loading" ? (
                    <div className="text-sm text-slate-400">{FILINGS_LOADING_MESSAGE}</div>
                  ) : secResponse?.status === "unavailable" ? (
                    <div className="text-sm text-slate-400">{filingsMessage}</div>
                  ) : (
                    <div className="text-sm text-slate-400">{FILINGS_EMPTY_MESSAGE}</div>
                  )}
                </EventsSection>
              ) : null}

              <EventsSection title="Disclosure Activity" meta="365D">
                {loadingEvents && !eventsStatus ? (
                  <TabSkeleton rows={2} />
                ) : disclosureEvents.length > 0 ? (
                  <div className="overflow-hidden rounded-xl border border-white/10">
                    <div className="grid grid-cols-[8rem_7rem_minmax(0,1fr)_5rem] gap-3 border-b border-white/10 bg-slate-950/70 px-3 py-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                      <span>Date</span>
                      <span>Type</span>
                      <span>Title</span>
                      <span>Link</span>
                    </div>
                    {disclosureEvents.map((event) => {
                      const sourceUrl = disclosureEventUrl(event);
                      return (
                        <div
                          key={event.id}
                          className="grid grid-cols-[8rem_7rem_minmax(0,1fr)_5rem] gap-3 border-b border-white/10 px-3 py-2.5 text-sm text-slate-300 last:border-b-0"
                        >
                          <span>{formatDateShort(event.ts ?? null)}</span>
                          <span className="font-semibold text-slate-100">{disclosureTypeLabel(event.event_type)}</span>
                          <span className="truncate">{event.headline ?? event.summary ?? "Disclosure activity"}</span>
                          <span>
                            {sourceUrl ? (
                              <a href={sourceUrl} target="_blank" rel="noreferrer" className="font-semibold text-emerald-200 hover:text-emerald-100">
                                Open
                              </a>
                            ) : (
                              <span className="text-slate-500">-</span>
                            )}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="text-sm text-slate-400">{activityMessage}</div>
                )}
              </EventsSection>

              {allEventsSourcesEmpty ? (
                <div className="rounded-2xl border border-white/10 bg-slate-950/50 px-4 py-5 text-sm text-slate-400">
                  {EVENTS_EMPTY_MESSAGE}
                </div>
              ) : null}
            </div>
          </div>
        ) : null}
      </div>
    </section>
  );
}
