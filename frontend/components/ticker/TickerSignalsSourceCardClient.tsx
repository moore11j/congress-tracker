"use client";

import { useEffect, useMemo, useState } from "react";
import { ApiError, getTickerSignalsSummary, type SignalItem, type TickerSignalsSummaryResponse } from "@/lib/api";
import type { ConfirmationScoreSource } from "@/lib/types";

type SignalCardSource = ConfirmationScoreSource & {
  locked?: boolean;
  lock_state?: string | null;
  required_plan?: string | null;
};

type InitialTopSignal = {
  smart_score?: number | null;
  smart_band?: string | null;
  trade_type?: string | null;
};

type CardState = {
  source: SignalCardSource;
  body: string;
  support: string;
  loading: boolean;
};

const inactiveSource: SignalCardSource = {
  present: false,
  direction: "neutral",
  strength: 0,
  quality: 0,
  freshness_days: null,
  label: "No current smart signal",
  score_contribution: 0,
};

function normalizeTradeSide(value?: string | null): "buy" | "sell" | null {
  const normalized = (value ?? "").trim().toLowerCase();
  if (["buy", "purchase", "p-purchase"].includes(normalized)) return "buy";
  if (["sell", "sale", "s-sale"].includes(normalized)) return "sell";
  return null;
}

function sourceDirectionForSignal(signal: InitialTopSignal | null | undefined, fallback: SignalCardSource): SignalCardSource["direction"] {
  const side = normalizeTradeSide(signal?.trade_type);
  if (side === "buy") return "bullish";
  if (side === "sell") return "bearish";
  return fallback.direction === "neutral" ? "mixed" : fallback.direction;
}

function cardToneClass(source: SignalCardSource, loading: boolean): string {
  if (!source.present || loading) return "text-slate-500";
  if (source.direction === "bullish") return "text-emerald-300";
  if (source.direction === "bearish") return "text-rose-300";
  if (source.direction === "mixed") return "text-amber-300";
  return "text-slate-400";
}

function cardBorderClass(source: SignalCardSource, loading: boolean): string {
  if (!source.present || loading) return "border-white/10 bg-white/[0.025]";
  if (source.direction === "bullish") return "border-emerald-400/20 bg-emerald-400/[0.045]";
  if (source.direction === "bearish") return "border-rose-400/20 bg-rose-400/[0.045]";
  return "border-amber-400/20 bg-amber-400/[0.04]";
}

function sourceLocked(source: SignalCardSource): boolean {
  return source.locked === true || source.lock_state === "premium_locked" || source.lock_state === "pro_locked";
}

function sourceUnavailable(source: SignalCardSource): boolean {
  const status = (source.status ?? "").toLowerCase();
  return !source.present && ["unavailable", "provider_error", "error"].includes(status);
}

function stateLabel(source: SignalCardSource, loading: boolean): string {
  if (loading && !source.present) return "CHECKING";
  if (sourceLocked(source)) return "LOCKED";
  if (sourceUnavailable(source)) return "UNAVAILABLE";
  if (source.present && source.score_contribution && source.score_contribution > 0) return "BULLISH SUPPORT";
  return source.present ? source.direction.toUpperCase() : "INACTIVE";
}

function signalIcon(className = "h-3.5 w-3.5") {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 12h2.5l2-5 3.5 10 3-7 2 2H20" />
      <path d="M4 19h16" opacity="0.45" />
    </svg>
  );
}

function windowLabel(lookbackDays: number): string {
  return `${lookbackDays}D confirmation`;
}

function windowNoun(lookbackDays: number): string {
  return `${lookbackDays} Day context window`;
}

function lastWindowLabel(lookbackDays: number): string {
  return `last ${lookbackDays} Days`;
}

function signalSupport(source: SignalCardSource, topSignal: InitialTopSignal | null, lookbackDays: number): string {
  if (!source.present) return `No qualifying signal entries found in the ${windowNoun(lookbackDays)}.`;
  if (topSignal?.smart_score !== null && topSignal?.smart_score !== undefined) {
    return `${topSignal.smart_band ?? "signal"} ${topSignal.smart_score} - ${windowLabel(lookbackDays)}`;
  }
  return windowLabel(lookbackDays);
}

function latestSignal(items: SignalItem[]): InitialTopSignal | null {
  if (items.length === 0) return null;
  return items.reduce<SignalItem>((best, item) => {
    const bestScore = typeof best.smart_score === "number" ? best.smart_score : -1;
    const itemScore = typeof item.smart_score === "number" ? item.smart_score : -1;
    if (itemScore > bestScore) return item;
    if (itemScore === bestScore && String(item.ts ?? "") > String(best.ts ?? "")) return item;
    return best;
  }, items[0]);
}

function sourceFromSummary(response: TickerSignalsSummaryResponse, visibleItems: SignalItem[], lookbackDays: number, fallback: SignalCardSource): CardState {
  const bundleSource = response.confirmation_score_bundle?.sources?.signals as SignalCardSource | undefined;
  const topSignal = latestSignal(visibleItems);
  const hasVisibleSignals = visibleItems.length > 0;
  const hasActiveBundleSource = bundleSource?.present === true;
  const baseSource = hasActiveBundleSource ? { ...inactiveSource, ...bundleSource } : fallback;

  if (hasVisibleSignals) {
    const source: SignalCardSource = {
      ...baseSource,
      present: true,
      direction: sourceDirectionForSignal(topSignal, baseSource),
      strength: typeof topSignal?.smart_score === "number" ? topSignal.smart_score : baseSource.strength,
      quality: baseSource.quality || 60,
      freshness_days: baseSource.freshness_days ?? null,
      label: topSignal?.smart_band ? `${topSignal.smart_band} smart signal` : "Signal conviction active",
    };
    return {
      source,
      body: "Signal conviction active",
      support: signalSupport(source, topSignal, lookbackDays),
      loading: false,
    };
  }

  if (hasActiveBundleSource) {
    return {
      source: baseSource,
      body: "Signal source active",
      support: signalSupport(baseSource, topSignal, lookbackDays),
      loading: false,
    };
  }

  const source = { ...inactiveSource, ...bundleSource, present: false };
  return {
    source,
    body: `No active signal stack in the ${lastWindowLabel(lookbackDays)}.`,
    support: signalSupport(source, null, lookbackDays),
    loading: false,
  };
}

function initialCardState(source: SignalCardSource, topSignal: InitialTopSignal | null, lookbackDays: number): CardState {
  if (sourceLocked(source)) {
    return {
      source,
      body: "Premium feature",
      support: "Signal stack unlocks with Premium.",
      loading: false,
    };
  }
  if (source.present) {
    return {
      source,
      body: topSignal ? "Signal conviction active" : "Signal source active",
      support: signalSupport(source, topSignal, lookbackDays),
      loading: false,
    };
  }
  return {
    source,
    body: "Checking signal activity",
    support: windowLabel(lookbackDays),
    loading: true,
  };
}

export function TickerSignalsSourceCardClient({
  symbol,
  side,
  lookbackDays,
  lookbackStartKey,
  initialSource,
  initialTopSignal,
}: {
  symbol: string;
  side: string;
  lookbackDays: number;
  lookbackStartKey: string;
  initialSource: SignalCardSource;
  initialTopSignal: InitialTopSignal | null;
}) {
  const fallbackSource = useMemo<SignalCardSource>(() => ({ ...inactiveSource, ...initialSource }), [initialSource]);
  const [state, setState] = useState<CardState>(() => initialCardState(fallbackSource, initialTopSignal, lookbackDays));

  useEffect(() => {
    if (fallbackSource.present) {
      setState(initialCardState(fallbackSource, initialTopSignal, lookbackDays));
      return;
    }

    let alive = true;
    const controller = new AbortController();
    setState((current) => ({ ...current, loading: true, body: "Checking signal activity", support: windowLabel(lookbackDays) }));
    getTickerSignalsSummary(symbol, {
      side,
      limit: 3,
      lookback_days: lookbackDays,
      signal: controller.signal,
      source: "TickerSignalsSourceCard",
    })
      .then((response) => {
        if (!alive) return;
        const visibleItems = response.items.filter((item) => item.ts && item.ts.slice(0, 10) >= lookbackStartKey);
        setState(sourceFromSummary(response, visibleItems, lookbackDays, fallbackSource));
      })
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        console.error("[ticker-signals-source-card] client fetch failed", error);
        if (alive) {
          const gateLocked = error instanceof ApiError && [401, 402, 403].includes(error.status);
          const source: SignalCardSource = gateLocked
            ? {
                ...fallbackSource,
                present: false,
                locked: true,
                lock_state: "premium_locked",
                required_plan: "premium",
                status: "premium_locked",
              }
            : {
                ...fallbackSource,
                present: false,
                status: "unavailable",
              };
          setState(
            gateLocked
              ? {
                  source,
                  body: "Premium feature",
                  support: "Signal stack unlocks with Premium.",
                  loading: false,
                }
              : {
                  source,
                  body: "Signal activity unavailable.",
                  support: "Ticker signals are temporarily unavailable.",
                  loading: false,
                },
          );
        }
      });
    return () => {
      alive = false;
      controller.abort();
    };
  }, [fallbackSource, initialTopSignal, lookbackDays, lookbackStartKey, side, symbol]);

  return (
    <div className={`rounded-xl border px-3 py-2.5 ${cardBorderClass(state.source, state.loading)}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${cardToneClass(state.source, state.loading)}`}>{signalIcon()}</span>
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">Signals</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${cardToneClass(state.source, state.loading)}`}>
          {stateLabel(state.source, state.loading)}
        </p>
      </div>
      <p className="mt-2.5 text-sm font-semibold leading-snug text-slate-100">{state.body}</p>
      <p className="mt-1 text-xs leading-snug text-slate-500">{state.support}</p>
    </div>
  );
}
