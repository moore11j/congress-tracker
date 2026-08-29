"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { getEvents } from "@/lib/api";
import { tickerHref } from "@/lib/ticker";

type ActivitySource = "congress" | "insider";
type Lookback = "1" | "5" | "30" | "90" | "180" | "365";
type SideFilter = "all" | "buy" | "sell";

const ACTIVITY_LIMIT = 20;

function sideToTradeType(side: SideFilter): "purchase" | "sale" | null {
  if (side === "buy") return "purchase";
  if (side === "sell") return "sale";
  return null;
}

// Congress data uses both human-readable labels ("Purchase" / "Sale") and
// source-specific short codes. Keep the counters aligned with the row labels.
function normalizeTradeSide(value?: string | null): "buy" | "sell" | null {
  const normalized = (value ?? "").trim().toLowerCase();
  if (!normalized) return null;
  if (normalized.includes("buy") || normalized.includes("purchase") || normalized.startsWith("p-")) return "buy";
  if (normalized.includes("sell") || normalized.includes("sale") || normalized.startsWith("s-")) return "sell";
  return null;
}

function activityHref(symbol: string, lookback: Lookback, source: ActivitySource, side: Exclude<SideFilter, "all">): string {
  const base = tickerHref(symbol) ?? `/ticker/${encodeURIComponent(symbol)}`;
  const query = new URLSearchParams({ lookback, source, side });
  return `${base}?${query.toString()}`;
}

function ActivityHeaderStat({ href, label, value, toneClass }: { href: string; label: string; value: number; toneClass: string }) {
  return (
    <Link
      href={href}
      prefetch={false}
      className="inline-grid min-w-[4.75rem] grid-cols-[1fr_auto] items-center gap-2 rounded-md border border-white/10 bg-slate-950/70 px-2.5 py-1.5 transition hover:border-white/20 hover:bg-slate-900/80"
    >
      <span className="truncate text-[10px] font-semibold uppercase tracking-[0.12em] text-slate-400">{label}</span>
      <span className={`text-sm font-semibold tabular-nums ${toneClass}`}>{value}</span>
    </Link>
  );
}

export function TickerActivityHeaderStatsClient({
  symbol,
  lookback,
  source,
  side,
  buys: initialBuys,
  sells: initialSells,
}: {
  symbol: string;
  lookback: Lookback;
  source: ActivitySource;
  side: SideFilter;
  buys: number;
  sells: number;
}) {
  const [counts, setCounts] = useState({ buys: initialBuys, sells: initialSells });

  useEffect(() => {
    const controller = new AbortController();
    const tradeType = sideToTradeType(side);

    getEvents({
      symbol,
      recent_days: Number(lookback),
      limit: ACTIVITY_LIMIT,
      offset: 0,
      enrich_prices: 0,
      tape: source,
      ...(tradeType ? { trade_type: tradeType } : {}),
      requestSource: "visibility",
      routeFamily: "ticker",
      signal: controller.signal,
      source: `${source}-header-counts`,
    })
      .then((response) => {
        if (controller.signal.aborted) return;
        const items = Array.isArray(response.items) ? response.items.slice(0, ACTIVITY_LIMIT) : [];
        setCounts({
          buys: items.filter((event) => normalizeTradeSide(event.trade_type) === "buy").length,
          sells: items.filter((event) => normalizeTradeSide(event.trade_type) === "sell").length,
        });
      })
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        // Preserve the server-rendered counts if the reconciliation request fails.
      });

    return () => controller.abort();
  }, [lookback, side, source, symbol]);

  return (
    <div className="flex shrink-0 flex-wrap items-center justify-start gap-2 sm:justify-center">
      <ActivityHeaderStat href={activityHref(symbol, lookback, source, "buy")} label="Buys" value={counts.buys} toneClass="text-emerald-300" />
      <ActivityHeaderStat href={activityHref(symbol, lookback, source, "sell")} label="Sells" value={counts.sells} toneClass="text-rose-300" />
    </div>
  );
}
