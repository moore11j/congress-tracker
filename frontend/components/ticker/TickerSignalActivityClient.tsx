"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import Link from "next/link";
import { Badge } from "@/components/Badge";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { ApiError, getSignalsAll, type SignalItem } from "@/lib/api";
import { formatCurrencyRange, formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";

type GateReason = "auth" | "upgrade" | "unavailable";

function signalTone(band?: string | null): "pos" | "neutral" | "neg" {
  const value = (band ?? "").toLowerCase();
  if (value === "strong" || value === "notable" || value === "exceptional" || value === "strong_plus") return "pos";
  if (value === "mild" || value === "moderate") return "neutral";
  return "neg";
}

function gateFromError(error: unknown): { reason: GateReason; message: string } {
  if (error instanceof ApiError) {
    if (error.status === 401) {
      return { reason: "auth", message: "Create an account or log in to unlock signal activity." };
    }
    if (error.status === 402 || error.status === 403) {
      return { reason: "upgrade", message: "Upgrade to unlock ticker-level signal context." };
    }
  }
  return { reason: "unavailable", message: "Ticker signals are temporarily unavailable." };
}

function ActivityCard({ children }: { children: ReactNode }) {
  return (
    <div className="w-full max-w-full min-w-0 overflow-hidden rounded-2xl border border-white/10 bg-white/5 px-3 py-2.5 sm:px-4">
      {children}
    </div>
  );
}

function ActivityScrollRegion({ children }: { children: ReactNode }) {
  return (
    <div
      className={[
        "min-w-0 max-w-full max-h-[35rem] space-y-3 overflow-y-auto pr-1",
        "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
        "[&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-track]:bg-white/[0.03]",
        "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60",
      ].join(" ")}
    >
      {children}
    </div>
  );
}

function ActivityCardGrid({
  identity,
  sideBadge,
  dateLabel,
  tradeValue,
  signal,
}: {
  identity: ReactNode;
  sideBadge: ReactNode;
  dateLabel: ReactNode;
  tradeValue: ReactNode;
  signal: ReactNode;
}) {
  const metricLabelClassName = "text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500";
  const metricValueClassName = "truncate text-sm font-semibold tabular-nums";

  return (
    <div className="grid min-w-0 gap-x-3 gap-y-2 sm:grid-cols-[minmax(150px,1.45fr)_minmax(76px,.7fr)_minmax(104px,.9fr)_minmax(64px,.6fr)_minmax(84px,auto)] sm:items-center lg:grid-cols-[minmax(170px,1.65fr)_minmax(84px,.72fr)_minmax(120px,.95fr)_minmax(72px,.62fr)_minmax(92px,auto)]">
      <div className="min-w-0 sm:col-start-1 sm:row-start-1">{identity}</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-2 sm:row-start-1`}>Price</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-3 sm:row-start-1`}>Trade value</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-4 sm:row-start-1`}>PnL</div>
      <div className="flex min-w-0 items-center justify-start sm:col-start-5 sm:row-start-1 sm:justify-end">{sideBadge}</div>

      <div className="text-xs text-slate-400 sm:col-start-1 sm:row-start-2">{dateLabel}</div>
      <div className="min-w-0 sm:col-start-2 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Price</div>
        <div className={`${metricValueClassName} text-slate-400`}>-</div>
      </div>
      <div className="min-w-0 sm:col-start-3 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Trade value</div>
        <div className={`${metricValueClassName} text-white`}>{tradeValue}</div>
      </div>
      <div className="min-w-0 sm:col-start-4 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>PnL</div>
        <div className={`${metricValueClassName} text-slate-400`}>-</div>
      </div>
      <div className="flex min-w-0 items-center justify-start sm:col-start-5 sm:row-start-2 sm:justify-end">{signal}</div>
    </div>
  );
}

function SignalActivitySkeleton() {
  return (
    <div className="space-y-3" aria-live="polite" aria-busy="true">
      {Array.from({ length: 4 }).map((_, index) => (
        <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
          <SkeletonBlock className="h-4 w-40" />
          <div className="mt-3 grid gap-3 sm:grid-cols-5">
            <SkeletonBlock className="h-4 w-full" />
            <SkeletonBlock className="h-4 w-full" />
            <SkeletonBlock className="h-4 w-full" />
            <SkeletonBlock className="h-4 w-full" />
            <SkeletonBlock className="h-4 w-full" />
          </div>
        </div>
      ))}
    </div>
  );
}

export function TickerSignalActivityClient({
  symbol,
  side,
  lookbackStartKey,
  returnTo,
  className,
}: {
  symbol: string;
  side: string;
  lookbackStartKey: string;
  returnTo: string;
  className: string;
}) {
  const [items, setItems] = useState<SignalItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [gate, setGate] = useState<{ reason: GateReason; message: string } | null>(null);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setGate(null);
    getSignalsAll({
      mode: "all",
      side,
      sort: "smart",
      limit: 100,
      symbol,
    })
      .then((response) => {
        if (!alive) return;
        setItems(response.items);
        setGate(null);
      })
      .catch((error) => {
        console.error("[ticker-signal-activity] client fetch failed", error);
        if (alive) setGate(gateFromError(error));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [side, symbol]);

  const visibleItems = useMemo(
    () => items.filter((item) => item.ts && item.ts.slice(0, 10) >= lookbackStartKey),
    [items, lookbackStartKey],
  );
  const gateHref = gate?.reason === "upgrade" ? "/pricing" : `/login?return_to=${encodeURIComponent(returnTo)}`;
  const gateLabel = gate?.reason === "upgrade" ? "View Premium" : "Login or register";
  const gateTitle = gate?.reason === "upgrade" ? "Signal Activity is a premium feature." : "Signals are gated for this view.";

  return (
    <section className={className}>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">Signal activity</h2>
        <span className="text-xs text-slate-400">
          {loading ? "loading" : gate ? "locked" : `${visibleItems.length} signals`}
        </span>
      </div>
      <div className="space-y-3">
        {loading ? (
          <SignalActivitySkeleton />
        ) : gate ? (
          <div className="rounded-lg border border-white/10 bg-white/[0.03] p-4">
            <p className="text-sm font-semibold text-white">{gateTitle}</p>
            <p className="mt-1 text-sm text-slate-400">{gate.message}</p>
            {gate.reason === "unavailable" ? null : (
              <Link
                href={gateHref}
                prefetch={false}
                className="mt-3 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-1.5 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
              >
                {gateLabel}
              </Link>
            )}
          </div>
        ) : visibleItems.length === 0 ? (
          <p className="text-sm text-slate-400">No signal conviction entries for this symbol in current filters.</p>
        ) : (
          <ActivityScrollRegion>
            {visibleItems.slice(0, 20).map((signal) => {
              const isInsiderSignal = signal.kind === "insider";
              const displayName = getInsiderDisplayName(signal.who) ?? "Unknown";
              const insiderProfileHref = isInsiderSignal ? insiderHref(displayName, signal.reporting_cik ?? null) : null;

              return (
                <ActivityCard key={`${signal.kind}-${signal.event_id}-${signal.ts}`}>
                  <ActivityCardGrid
                    identity={
                      <div className="flex flex-wrap items-center gap-2">
                        {isInsiderSignal && insiderProfileHref ? (
                          <Link href={insiderProfileHref} prefetch={false} className="text-sm font-semibold text-emerald-200">
                            {displayName}
                          </Link>
                        ) : (
                          <span className="text-sm font-semibold text-slate-100">{displayName}</span>
                        )}
                        <Badge tone={isInsiderSignal ? "ind" : "house"}>{signal.kind ?? "signal"}</Badge>
                        <Badge tone={signalTone(signal.smart_band)}>{signal.smart_band ?? "signal"}</Badge>
                      </div>
                    }
                    sideBadge={<Badge tone={transactionTone(signal.trade_type)}>{formatTransactionLabel(signal.trade_type)}</Badge>}
                    dateLabel={formatDateShort(signal.ts)}
                    tradeValue={formatCurrencyRange(signal.amount_min ?? null, signal.amount_max ?? null)}
                    signal={<SmartSignalPill score={signal.smart_score ?? null} band={signal.smart_band ?? null} size="compact" />}
                  />
                </ActivityCard>
              );
            })}
          </ActivityScrollRegion>
        )}
      </div>
    </section>
  );
}
