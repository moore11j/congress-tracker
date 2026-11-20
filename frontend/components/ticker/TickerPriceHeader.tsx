"use client";

import { useEffect, useState } from "react";
import { getTickerMarketQuote, type TickerContextBundleResponse } from "@/lib/api";
import { formatPriceSession, tickerHeaderPrice, type DailyHeaderPrice, type MarketQuote } from "@/lib/tickerHeaderPrice";

export function TickerPriceHeader({ symbol, initialQuote, daily }: {
  symbol: string;
  initialQuote?: TickerContextBundleResponse["quote"];
  daily: DailyHeaderPrice;
}) {
  const [state, setState] = useState<{ symbol: string; quote: MarketQuote | null; failed: boolean }>(() => ({
    symbol, failed: false, quote: initialQuote ? {
      symbol, current_price: initialQuote.current_price, day_change_pct: initialQuote.change_percent,
      as_of: initialQuote.as_of, is_stale: initialQuote.stale, is_delayed: initialQuote.is_delayed,
      delay_minutes: initialQuote.delay_minutes, price_kind: initialQuote.price_kind,
    } : null,
  }));
  useEffect(() => {
    let active = true;
    let request: AbortController | null = null;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    const refresh = async () => {
      if (document.hidden || request) return;
      const controller = new AbortController();
      request = controller;
      deadline = setTimeout(() => controller.abort(), 10_000);
      try {
        const quote = await getTickerMarketQuote(symbol, controller.signal);
        if (active) setState({ symbol, quote, failed: false });
      } catch {
        if (active) setState((previous) => ({ symbol, quote: previous.symbol === symbol ? previous.quote : null, failed: true }));
      } finally {
        clearTimeout(deadline);
        request = null;
      }
    };
    void refresh();
    const timer = setInterval(() => void refresh(), 60_000);
    const onVisibility = () => { if (!document.hidden) void refresh(); };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      active = false;
      clearInterval(timer);
      clearTimeout(deadline);
      document.removeEventListener("visibilitychange", onVisibility);
      request?.abort();
    };
  }, [symbol]);
  const shown = tickerHeaderPrice(symbol, state.symbol === symbol ? state.quote : null, daily, state.failed);
  const tone = shown.changePercent === null ? "text-slate-500" : shown.changePercent < 0 ? "text-rose-300" : shown.changePercent > 0 ? "text-emerald-300" : "text-slate-400";
  const price = shown.price === null ? "—" : new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 2, maximumFractionDigits: shown.price < 1 ? 4 : 2,
  }).format(shown.price);
  const relativeVolume = typeof daily.relativeVolume === "number" && Number.isFinite(daily.relativeVolume)
    ? `${daily.relativeVolume.toFixed(2)}x` : "—";
  return <div className="flex min-w-0 flex-wrap items-start justify-between gap-3" aria-label={`${symbol} latest quote`}>
    <div className="min-w-0">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-slate-500">{shown.label} · {symbol}</p>
      <p className="text-xl font-semibold tabular-nums text-white">{price}{shown.changePercent !== null ? <span className={`ml-2 text-xs ${tone}`}>{shown.changePercent > 0 ? "+" : ""}{shown.changePercent.toFixed(2)}% <span className="text-slate-500">1D</span></span> : null}</p>
      <p className="mt-1 text-xs text-slate-400">{shown.observed}</p>
      {shown.delay ? <p className="mt-1 text-[11px] text-slate-400">{shown.delay}</p> : null}
      {shown.stale ? <p className="mt-1 text-[11px] text-amber-200">Quote refresh delayed</p>
        : shown.fallback ? <p className="mt-1 text-[11px] text-slate-400">Latest quote not yet available</p> : null}
    </div>
    <div className="text-right">
      <p className="text-[10px] uppercase tracking-wider text-slate-500">Daily volume vs 30D avg</p>
      <p className="text-sm font-semibold tabular-nums text-slate-200">{relativeVolume}</p>
      {formatPriceSession(daily.date) ? <p className="mt-1 text-[10px] text-slate-500">{formatPriceSession(daily.date)}</p> : null}
    </div>
  </div>;
}
