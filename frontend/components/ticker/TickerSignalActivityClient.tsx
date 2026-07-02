"use client";

import { useEffect, useState, type ReactNode } from "react";
import Link from "next/link";
import { Badge } from "@/components/Badge";
import { SmartSignalPill } from "@/components/ui/SmartSignalPill";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { ApiError, getSignalsAll, type SignalItem } from "@/lib/api";
import {
  chamberBadge,
  formatCongressAffiliationText,
  formatCurrency,
  formatCurrencyRange,
  formatDateShort,
  formatTransactionLabel,
  transactionTone,
} from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, resolveInsiderRoleBadge } from "@/lib/insiderRole";
import { gainLossLabel, tickerGainLossTooltip } from "@/lib/gainLossCopy";

type GateReason = "auth" | "upgrade" | "unavailable";
type SignalActivityState = "unlocked" | "locked" | "unavailable" | string;

function signalKind(item: SignalItem): string {
  return (item.kind ?? "").trim().toLowerCase();
}

function formatSignalStrengthText(band?: string | null): string {
  const cleaned = (band ?? "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return "Signal";
  const label = cleaned.replace(/\b\w/g, (letter) => letter.toUpperCase());
  return `${label} signal`;
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

function gateFromActivityState(state: SignalActivityState | null | undefined): { reason: GateReason; message: string } | null {
  if (state === "locked") {
    return { reason: "upgrade", message: "Upgrade to unlock ticker-level signal context." };
  }
  if (state === "unavailable") {
    return { reason: "unavailable", message: "Ticker signals are temporarily unavailable." };
  }
  return null;
}

function readSignalNumber(item: SignalItem, ...keys: Array<keyof SignalItem>): number | null {
  for (const key of keys) {
    const value = item[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return null;
}

function formatPnl(value: number): string {
  const marker = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${marker} ${Math.abs(value).toFixed(1)}%`;
}

function pnlClass(value: number): string {
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-300";
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
  price,
  tradeValue,
  pnl,
  pnlClassName,
  signal,
}: {
  identity: ReactNode;
  sideBadge: ReactNode;
  dateLabel: ReactNode;
  price: ReactNode;
  tradeValue: ReactNode;
  pnl: ReactNode;
  pnlClassName?: string;
  signal: ReactNode;
}) {
  const metricLabelClassName = "text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500";
  const metricValueClassName = "truncate text-sm font-semibold tabular-nums";
  const gainLossLabelNode = (
    <span
      className="cursor-help whitespace-nowrap"
      title={tickerGainLossTooltip}
      aria-label={`${gainLossLabel}: ${tickerGainLossTooltip}`}
    >
      {gainLossLabel}
    </span>
  );

  return (
    <div className="grid min-w-0 gap-x-3 gap-y-2 sm:grid-cols-[minmax(150px,1.45fr)_minmax(76px,.7fr)_minmax(104px,.9fr)_minmax(88px,.65fr)_minmax(84px,auto)] sm:items-center lg:grid-cols-[minmax(170px,1.65fr)_minmax(84px,.72fr)_minmax(120px,.95fr)_minmax(92px,.68fr)_minmax(92px,auto)]">
      <div className="min-w-0 sm:col-start-1 sm:row-start-1">{identity}</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-2 sm:row-start-1`}>Price</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-3 sm:row-start-1`}>Trade value</div>
      <div className={`${metricLabelClassName} hidden sm:block sm:col-start-4 sm:row-start-1`}>{gainLossLabelNode}</div>
      <div className="flex min-w-0 items-center justify-start sm:col-start-5 sm:row-start-1 sm:justify-end">{sideBadge}</div>

      <div className="text-xs text-slate-400 sm:col-start-1 sm:row-start-2">{dateLabel}</div>
      <div className="min-w-0 sm:col-start-2 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Price</div>
        <div className={`${metricValueClassName} ${price === "-" ? "text-slate-400" : "text-white"}`}>{price}</div>
      </div>
      <div className="min-w-0 sm:col-start-3 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>Trade value</div>
        <div className={`${metricValueClassName} text-white`}>{tradeValue}</div>
      </div>
      <div className="min-w-0 sm:col-start-4 sm:row-start-2">
        <div className={`${metricLabelClassName} sm:hidden`}>{gainLossLabelNode}</div>
        <div className={`${metricValueClassName} ${pnlClassName ?? "text-slate-400"}`}>{pnl}</div>
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
  lookbackDays,
  returnTo,
  className,
  initialItems,
  initialTotal,
  initialState,
}: {
  symbol: string;
  side: string;
  lookbackDays: number;
  returnTo: string;
  className: string;
  initialItems?: SignalItem[] | null;
  initialTotal?: number | null;
  initialState?: SignalActivityState | null;
}) {
  const hasInitialItems = Array.isArray(initialItems);
  const [items, setItems] = useState<SignalItem[]>(() => initialItems ?? []);
  const [total, setTotal] = useState<number | null>(() => initialTotal ?? (hasInitialItems ? initialItems?.length ?? 0 : null));
  const [loading, setLoading] = useState(!hasInitialItems);
  const [gate, setGate] = useState<{ reason: GateReason; message: string } | null>(() => gateFromActivityState(initialState));

  useEffect(() => {
    if (hasInitialItems) {
      setItems(initialItems ?? []);
      setTotal(initialTotal ?? initialItems?.length ?? 0);
      setGate(gateFromActivityState(initialState));
      setLoading(false);
      return;
    }

    let alive = true;
    const controller = new AbortController();
    setLoading(true);
    setGate(null);
    getSignalsAll({
      mode: "all",
      side,
      sort: "recent",
      limit: 20,
      symbol,
      congress_recent_days: lookbackDays,
      insider_recent_days: lookbackDays,
      signal: controller.signal,
      source: "TickerSignalActivity",
    })
      .then((response) => {
        if (!alive) return;
        setItems(response.items);
        setTotal(response.items.length);
        setGate(null);
      })
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        console.error("[ticker-signal-activity] client fetch failed", error);
        if (alive) setGate(gateFromError(error));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
      controller.abort();
    };
  }, [hasInitialItems, initialItems, initialState, initialTotal, lookbackDays, side, symbol]);

  const visibleItems = items;
  const gateHref = gate?.reason === "upgrade" ? "/pricing" : `/login?return_to=${encodeURIComponent(returnTo)}`;
  const gateLabel = gate?.reason === "upgrade" ? "View Premium" : "Login or register";
  const gateTitle = gate?.reason === "upgrade" ? "Signal Activity is a premium feature." : "Signals are gated for this view.";
  const visibleTotal = total ?? visibleItems.length;
  const statusLabel = loading ? "loading" : gate ? (gate.reason === "unavailable" ? "unavailable" : "locked") : `${visibleTotal} signals`;

  return (
    <section className={className}>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-white">Signal activity</h2>
        <span className="text-xs text-slate-400">{statusLabel}</span>
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
          <p className="text-sm text-slate-400">No abnormal signal activity found for this ticker in the selected lookback.</p>
        ) : (
          <ActivityScrollRegion>
            {visibleItems.slice(0, 20).map((signal) => {
              const kind = signalKind(signal);
              const isInsiderSignal = kind === "insider";
              const isCongressSignal = kind === "congress";
              const displayName = getInsiderDisplayName(signal.who) ?? "Unknown";
              const insiderProfileHref = isInsiderSignal ? insiderHref(displayName, signal.reporting_cik ?? null) : null;
              const price = readSignalNumber(signal, "estimated_price", "price");
              const pnl = readSignalNumber(signal, "pnl_pct", "pnlPct");
              const insiderRole = isInsiderSignal ? resolveInsiderRoleBadge(signal.position) : null;
              const congressChamber = isCongressSignal ? chamberBadge(signal.chamber) : null;
              const hasCongressChamber = isCongressSignal && Boolean(signal.chamber?.trim());
              const congressAffiliation = isCongressSignal ? formatCongressAffiliationText(signal.party, signal.state) : null;
              const strengthLabel = formatSignalStrengthText(signal.smart_band);

              return (
                <ActivityCard key={`${signal.kind}-${signal.event_id}-${signal.ts}`}>
                  <ActivityCardGrid
                    identity={
                      <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
                        {isInsiderSignal && insiderProfileHref ? (
                          <Link href={insiderProfileHref} prefetch={false} className="text-sm font-semibold text-emerald-200">
                            {displayName}
                          </Link>
                        ) : (
                          <span className="text-sm font-semibold text-slate-100">{displayName}</span>
                        )}
                        {isInsiderSignal && insiderRole ? (
                          <Badge tone={insiderRoleBadgeTone(insiderRole)} className="px-2 py-0.5 text-[10px]">{insiderRole}</Badge>
                        ) : isCongressSignal && hasCongressChamber && congressChamber ? (
                          <Badge tone={congressChamber.tone} className="px-2 py-0.5 text-[10px]">{congressChamber.label}</Badge>
                        ) : isCongressSignal ? (
                          <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">Congress</span>
                        ) : (
                          <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-500">{signal.kind ?? "Signal"}</span>
                        )}
                        {isCongressSignal && congressAffiliation ? (
                          <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{congressAffiliation}</span>
                        ) : null}
                        <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{strengthLabel}</span>
                      </div>
                    }
                    sideBadge={<Badge tone={transactionTone(signal.trade_type)}>{formatTransactionLabel(signal.trade_type)}</Badge>}
                    dateLabel={formatDateShort(signal.ts)}
                    price={price !== null ? formatCurrency(price) : "-"}
                    tradeValue={formatCurrencyRange(signal.amount_min ?? null, signal.amount_max ?? null)}
                    pnl={pnl !== null ? formatPnl(pnl) : "-"}
                    pnlClassName={pnl !== null ? pnlClass(pnl) : "text-slate-400"}
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
