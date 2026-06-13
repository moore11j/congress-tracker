"use client";

import { useEffect, useState } from "react";
import {
  getTickerChartBundle,
  getTickerHydrationStatus,
  requestTickerHydration,
  type TickerChartBundle,
  type TickerHydrationStatus,
} from "@/lib/api";
import { runHeavyTickerRequest } from "@/lib/heavyTickerRequests";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { cardClassName } from "@/lib/styles";

const CHART_HYDRATION_DELAY_MS = 1200;
const requestedHydrationSymbols = new Set<string>();

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
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

export function TickerChartLoader({ symbol, days }: { symbol: string; days: number }) {
  const [bundle, setBundle] = useState<TickerChartBundle | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setBundle(null);
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

      return runHeavyTickerRequest(
        () => getTickerChartBundle(symbol, days, { signal: controller.signal, source: "TickerChart" }),
        controller.signal,
      );
    }

    loadChartAfterHydration()
      .then((response) => {
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
  }, [attempt, days, symbol]);

  if (loading) return <PremiumTickerChartSkeleton />;
  if (failed) {
    return (
      <section className={cardClassName}>
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
    );
  }
  return <PremiumTickerChart bundle={bundle} />;
}
