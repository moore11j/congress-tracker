"use client";

import { useEffect, useRef, useState } from "react";
import {
  ApiError,
  getTickerAnalystConsensusEvents,
  getTickerAnalystConsensusHistory,
  getTickerChartBundle,
  getTickerHydrationStatus,
  requestTickerHydration,
  type TickerAnalystConsensusEventsResponse,
  type TickerAnalystConsensusHistoryResponse,
  type TickerAnalystGradeEvent,
  type TickerChartBundle,
  type TickerChartMarker,
  type TickerHydrationStatus,
} from "@/lib/api";
import { runHeavyTickerRequest } from "@/lib/heavyTickerRequests";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { cardClassName } from "@/lib/styles";

const CHART_HYDRATION_DELAY_MS = 1200;
const CHART_INITIAL_LOAD_DELAY_MS = 7500;
const CHART_VISIBILITY_FALLBACK_MS = 12_000;
const requestedHydrationSymbols = new Set<string>();

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

function analystSide(action?: string | null): TickerChartMarker["side"] {
  const normalized = action?.trim().toLowerCase() ?? "";
  if (normalized.includes("upgrade")) return "buy";
  if (normalized.includes("downgrade")) return "sell";
  return null;
}

function analystEventToMarker(event: TickerAnalystGradeEvent, index: number): TickerChartMarker | null {
  const publishedDate = event.publishedDate?.trim();
  if (!publishedDate) return null;
  const action = event.action?.trim() || event.providerAction?.trim() || "Rating action";
  const actor = event.gradingCompany?.trim() || event.analystName?.trim() || "Analyst";
  const gradeMove = event.previousGrade && event.newGrade
    ? `${event.previousGrade} to ${event.newGrade}`
    : event.newGrade || event.previousGrade || null;
  return {
    id: `analyst-${event.id ?? `${publishedDate}-${index}`}`,
    kind: "analyst",
    date: publishedDate,
    actor,
    action,
    side: analystSide(action),
    detail: gradeMove,
    label: event.newGrade || action,
    meta: {
      grading_company: event.gradingCompany ?? null,
      analyst_name: event.analystName ?? null,
      previous_grade: event.previousGrade ?? null,
      new_grade: event.newGrade ?? null,
      provider_action: event.providerAction ?? null,
      published_date: publishedDate,
    },
  };
}

function analystEventMarkers(response: TickerAnalystConsensusEventsResponse | null): TickerChartMarker[] {
  return (response?.items ?? [])
    .map((event, index) => analystEventToMarker(event, index))
    .filter((marker): marker is TickerChartMarker => marker !== null);
}

async function loadOptionalAnalystChartData(
  symbol: string,
  days: number,
  signal: AbortSignal,
): Promise<{
  history: TickerAnalystConsensusHistoryResponse | null;
  events: TickerAnalystConsensusEventsResponse | null;
}> {
  const tolerateMissing = <T,>(promise: Promise<T>): Promise<T | null> =>
    promise.catch((error) => {
      if (isAbortError(error)) throw error;
      if (error instanceof ApiError && [401, 402, 403, 404].includes(error.status)) return null;
      return null;
    });

  const [history, events] = await Promise.all([
    tolerateMissing(
      getTickerAnalystConsensusHistory(symbol, {
        days,
        signal,
        source: "TickerAnalystChartHistory",
      }),
    ),
    tolerateMissing(
      getTickerAnalystConsensusEvents(symbol, {
        limit: 150,
        signal,
        source: "TickerAnalystChartEvents",
      }),
    ),
  ]);

  return { history, events };
}

function chartHydrationKey(days: number): keyof TickerHydrationStatus["critical"] {
  return days <= 30 ? "chart_30d" : "chart_365d";
}

function chartCanLoad(status: TickerHydrationStatus | null, days: number): boolean {
  if (!status) return false;
  return status.critical[chartHydrationKey(days)] === "ok";
}

function shouldRequestHydration(status: TickerHydrationStatus | null): boolean {
  if (!status) return true;
  if (status.should_request_hydration) return true;
  return [...Object.values(status.critical), ...Object.values(status.optional)].some((state) => state === "missing" || state === "loading");
}

function waitForHydrationWindow(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException("Request aborted", "AbortError"));
      return;
    }
    const timeoutId = window.setTimeout(resolve, CHART_HYDRATION_DELAY_MS);
    signal.addEventListener(
      "abort",
      () => {
        window.clearTimeout(timeoutId);
        reject(new DOMException("Request aborted", "AbortError"));
      },
      { once: true },
    );
  });
}

export function TickerChartLoader({ symbol, days: initialDays, deferLoad = false, eager = false }: { symbol: string; days: number; deferLoad?: boolean; eager?: boolean }) {
  const [days, setDays] = useState(initialDays);
  useEffect(() => setDays(initialDays), [initialDays, symbol]);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [bundle, setBundle] = useState<TickerChartBundle | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  const [attempt, setAttempt] = useState(0);
  const [shouldLoad, setShouldLoad] = useState(false);
  useEffect(() => setBundle(null), [symbol]);

  useEffect(() => {
    if (deferLoad) return;
    if (shouldLoad) return;
    if (eager) {
      setShouldLoad(true);
      return;
    }
    const node = rootRef.current;
    let initialLoadTimer: number | undefined;
    const scheduleLoad = () => {
      if (initialLoadTimer !== undefined) return;
      initialLoadTimer = window.setTimeout(() => setShouldLoad(true), CHART_INITIAL_LOAD_DELAY_MS);
    };
    const fallbackTimer = window.setTimeout(() => setShouldLoad(true), CHART_VISIBILITY_FALLBACK_MS);

    if (!node || typeof IntersectionObserver === "undefined") {
      return () => {
        window.clearTimeout(fallbackTimer);
        if (initialLoadTimer !== undefined) window.clearTimeout(initialLoadTimer);
      };
    }

    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries.some((entry) => entry.isIntersecting)) return;
        window.clearTimeout(fallbackTimer);
        scheduleLoad();
        observer.disconnect();
      },
      { rootMargin: "420px 0px" },
    );
    observer.observe(node);
    return () => {
      window.clearTimeout(fallbackTimer);
      if (initialLoadTimer !== undefined) window.clearTimeout(initialLoadTimer);
      observer.disconnect();
    };
  }, [deferLoad, eager, shouldLoad]);

  useEffect(() => {
    if (!shouldLoad) return;
    const controller = new AbortController();
    setLoading(true);
    setFailed(false);

    async function loadChartAfterHydration() {
      let status: TickerHydrationStatus | null = null;
      try {
        status = await getTickerHydrationStatus(symbol, {
          signal: controller.signal,
          source: "TickerChartHydrationStatus",
        });
      } catch (error) {
        if (isAbortError(error)) throw error;
      }

      const requestKey = symbol.trim().toUpperCase();
      if (shouldRequestHydration(status) && !requestedHydrationSymbols.has(requestKey)) {
        requestedHydrationSymbols.add(requestKey);
        try {
          await requestTickerHydration(symbol, {
            reason: "ticker_page_view",
            priority: 20,
            signal: controller.signal,
            source: "TickerChartHydrationRequest",
          });
          status = await getTickerHydrationStatus(symbol, {
            signal: controller.signal,
            source: "TickerChartHydrationStatus",
          }).catch(() => status);
        } catch (error) {
          if (isAbortError(error)) throw error;
        }
      }

      if (!chartCanLoad(status, days)) {
        await waitForHydrationWindow(controller.signal);
      }

      const chartBundle = await runHeavyTickerRequest(
        () => getTickerChartBundle(symbol, days, { signal: controller.signal, source: "TickerChart" }),
        controller.signal,
      );
      const analystData = await loadOptionalAnalystChartData(symbol, chartBundle.days ?? days, controller.signal);
      return {
        ...chartBundle,
        analystConsensusHistory: analystData.history,
        analystConsensusEvents: analystData.events,
        markers: [...(chartBundle.markers ?? []), ...analystEventMarkers(analystData.events)],
      };
    }

    loadChartAfterHydration()
      .then((response) => {
        if (controller.signal.aborted) return;
        setBundle(response);
        setFailed(false);
      })
      .catch((error) => {
        if (isAbortError(error)) return;
        console.error("[ticker-chart] bundle unavailable", error);
        setFailed(true);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => controller.abort();
  }, [attempt, days, shouldLoad, symbol]);

  const rangeControls = <div className="flex flex-wrap items-center gap-2" role="group" aria-label="Chart time range">
    <span className="mr-1 text-xs font-medium text-slate-400">Range</span>
    {([[1, "1D"], [5, "5D"], [30, "1M"], [90, "3M"], [180, "6M"], [365, "1Y"]] as const).map(([value, label]) => (
      <button key={value} type="button" aria-pressed={days === value} onClick={() => { setDays(value); setShouldLoad(true); }}
        className={`rounded-lg px-3 py-2 text-xs font-semibold transition focus-visible:outline focus-visible:outline-2 focus-visible:outline-cyan-300 ${days === value ? "bg-cyan-300/15 text-cyan-100 ring-1 ring-cyan-300/30" : "text-slate-400 hover:bg-white/5 hover:text-white"}`}>{label}</button>
    ))}
    <span className="text-[11px] text-slate-500">Daily prices</span>
    {loading && bundle ? <span role="status" className="text-xs text-cyan-200">Updating range…</span> : null}
  </div>;

  if (!shouldLoad || (loading && !bundle)) {
    return (
      <div ref={rootRef}>
        <PremiumTickerChartSkeleton headerControls={rangeControls} />
      </div>
    );
  }
  if (failed) {
    return (
      <div ref={rootRef}>
        <section className={cardClassName}>
          {rangeControls}
          <h2 className="text-lg font-semibold text-white">Ticker chart</h2>
          <p className="mt-2 text-sm text-slate-400">Chart unavailable.</p>
          <button
            type="button"
            onClick={() => setAttempt((value) => value + 1)}
            className="mt-4 rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 py-1.5 text-sm font-semibold text-emerald-100 hover:bg-emerald-300/15"
          >
            Retry
          </button>
        </section>
      </div>
    );
  }
  return (
    <div ref={rootRef} aria-busy={loading}>
      <PremiumTickerChart bundle={bundle} headerControls={rangeControls} />
    </div>
  );
}
