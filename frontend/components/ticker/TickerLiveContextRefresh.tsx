"use client";

import { useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { getTickerContextBundle, requestTickerHydration } from "@/lib/api";

type Props = {
  enabled: boolean;
  symbol: string;
  side: string;
  lookbackDays: number;
};

export function TickerLiveContextRefresh({ enabled, symbol, side, lookbackDays }: Props) {
  const router = useRouter();
  const completedKey = useRef<string | null>(null);
  const [retry, setRetry] = useState(0);
  const [exhausted, setExhausted] = useState(false);

  useEffect(() => {
    if (!enabled) return;
    const key = `${symbol.trim().toUpperCase()}:${side}:${lookbackDays}`;
    if (completedKey.current === key) return;
    setExhausted(false);
    let active = true;
    let controller: AbortController;
    let timeoutId: number | undefined;
    let retryTimer: number | undefined;
    const hydrationController = new AbortController();
    let hydrationTimer: number | undefined;

    const load = async (attempt: number) => {
      controller = new AbortController();
      timeoutId = window.setTimeout(() => controller.abort(), 15_000);
      try {
        const bundle = await getTickerContextBundle(symbol, {
          side,
          limit: 3,
          lookback_days: lookbackDays,
          activeUser: true,
          signal: controller.signal,
          source: "TickerLiveContextRefresh",
          requestSource: "client",
        });
        if (!active || controller.signal.aborted) return;
        if (!bundle.ticker || bundle.ticker.identity_status === "loading") throw new Error("Ticker context still loading");
        completedKey.current = key;
        router.refresh();
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
      } catch {
        if (!active) return;
        if (attempt < 2) {
          retryTimer = window.setTimeout(() => void load(attempt + 1), 1_000 * (attempt + 1));
        } else {
          setExhausted(true);
        }
      } finally {
        window.clearTimeout(timeoutId);
      }
    };
    void load(0);

    return () => {
      active = false;
      window.clearTimeout(timeoutId);
      window.clearTimeout(retryTimer);
      if (hydrationTimer !== undefined) window.clearTimeout(hydrationTimer);
      controller?.abort();
      hydrationController.abort();
    };
  }, [enabled, lookbackDays, retry, router, side, symbol]);

  if (!enabled || !exhausted) return null;
  return (
    <p role="status" className="text-sm text-slate-400">
      Some sections couldn’t finish loading.{" "}
      <button type="button" className="text-emerald-200 underline underline-offset-4" onClick={() => setRetry((value) => value + 1)}>Retry</button>
    </p>
  );
}
