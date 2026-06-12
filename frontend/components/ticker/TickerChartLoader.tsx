"use client";

import { useEffect, useState } from "react";
import { getTickerChartBundle, type TickerChartBundle } from "@/lib/api";
import { runHeavyTickerRequest } from "@/lib/heavyTickerRequests";
import { PremiumTickerChart, PremiumTickerChartSkeleton } from "@/components/ticker/PremiumTickerChart";
import { cardClassName } from "@/lib/styles";

export function TickerChartLoader({ symbol, days }: { symbol: string; days: number }) {
  const [bundle, setBundle] = useState<TickerChartBundle | null>(null);
  const [loading, setLoading] = useState(true);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    setBundle(null);
    setLoading(true);
    setFailed(false);

    runHeavyTickerRequest(
      () => getTickerChartBundle(symbol, days, { signal: controller.signal, source: "TickerChart" }),
      controller.signal,
    )
      .then((response) => {
        setBundle(response);
        setFailed(false);
      })
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        console.error("[ticker-chart] bundle unavailable", error);
        setFailed(true);
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });

    return () => controller.abort();
  }, [days, symbol]);

  if (loading) return <PremiumTickerChartSkeleton />;
  if (failed) {
    return (
      <section className={cardClassName}>
        <h2 className="text-lg font-semibold text-white">Ticker chart</h2>
        <p className="mt-2 text-sm text-slate-400">Chart unavailable.</p>
      </section>
    );
  }
  return <PremiumTickerChart bundle={bundle} />;
}
