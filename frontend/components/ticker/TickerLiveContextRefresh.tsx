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

    Promise.allSettled([
      requestTickerHydration(symbol, {
        reason: "ticker_page_cache_miss",
        priority: 1,
        live: true,
        signal: controller.signal,
        source: "TickerLiveHydration",
      }),
      getTickerContextBundle(symbol, {
        side,
        limit: 3,
        lookback_days: lookbackDays,
        activeUser: true,
        signal: controller.signal,
        source: "TickerLiveContextRefresh",
        requestSource: "client",
      }),
    ])
      .then(() => {
        if (!controller.signal.aborted) router.refresh();
      })
      .catch(() => {
        // The visible shell remains usable. A later navigation retries the live refresh.
      })
      .finally(() => window.clearTimeout(timeoutId));

    return () => {
      window.clearTimeout(timeoutId);
      controller.abort();
    };
  }, [enabled, lookbackDays, router, side, symbol]);

  return null;
}
