"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { getTickerContextBundle, requestTickerHydration } from "@/lib/api";

const attemptedRefreshes = new Set<string>();

type Props = {
  enabled: boolean;
  symbol: string;
  side: string;
  lookbackDays: number;
};

export function TickerLiveContextRefresh({ enabled, symbol, side, lookbackDays }: Props) {
  const router = useRouter();

  useEffect(() => {
    if (!enabled) return;
    const key = `${symbol.trim().toUpperCase()}:${side}:${lookbackDays}`;
    if (attemptedRefreshes.has(key)) return;
    attemptedRefreshes.add(key);

    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), 15_000);
    const hydrationController = new AbortController();
    let hydrationTimer: number | undefined;

    getTickerContextBundle(symbol, {
        side,
        limit: 3,
        lookback_days: lookbackDays,
        activeUser: true,
        signal: controller.signal,
        source: "TickerLiveContextRefresh",
        requestSource: "client",
      })
      .then(() => {
        if (!controller.signal.aborted) router.refresh();
      })
      .catch(() => {
        // The visible shell remains usable. A later navigation retries the live refresh.
      })
      .finally(() => {
        window.clearTimeout(timeoutId);
        if (controller.signal.aborted) return;
        hydrationTimer = window.setTimeout(() => {
          void requestTickerHydration(symbol, {
            reason: "ticker_page_cache_miss",
            priority: 1,
            live: true,
            signal: hydrationController.signal,
            source: "TickerLiveHydration",
          }).catch(() => {
            // Context has already refreshed; the regular deferred loaders retry enrichment.
          });
        }, 1_500);
      });

    return () => {
      window.clearTimeout(timeoutId);
      if (hydrationTimer !== undefined) window.clearTimeout(hydrationTimer);
      controller.abort();
      hydrationController.abort();
    };
  }, [enabled, lookbackDays, router, side, symbol]);

  return null;
}
