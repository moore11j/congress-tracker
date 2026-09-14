"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import {
  chamberTextClassName,
  insiderRoleTextClassName,
  partyTextClassName,
  signalWeightTextClassName,
  TickerActivitySignalScore,
  tradeTypeTextClassName,
} from "@/components/ticker/TickerActivityText";
import { TickerActivityTable, tickerActivityCellClassName } from "@/components/ticker/TickerActivityTable";
import { getEvents, type EventItem } from "@/lib/api";
import { chamberBadge, formatCurrencyRange, formatDateShort, formatTransactionLabel, partyBadge } from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { resolveInsiderRoleBadge } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { resolveCongressActivityPrice, resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { resolveSmartSignalValue } from "@/lib/smartSignal";

type ActivityKind = "congress" | "insider";
type SideFilter = "all" | "buy" | "sell" | string;

const ACTIVITY_LIMIT = 20;

function sideToTradeType(side: SideFilter): "purchase" | "sale" | null {
  if (side === "buy") return "purchase";
  if (side === "sell") return "sale";
  return null;
}

function ActivitySkeleton() {
  return (
    <div className="space-y-3" aria-live="polite" aria-busy="true">
      {Array.from({ length: 3 }).map((_, index) => (
        <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
          <SkeletonBlock className="h-4 w-44" />
          <div className="mt-3 grid gap-3 sm:grid-cols-4">
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

function formatPrice(value: number | null): string {
  if (value === null) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: value >= 100 ? 2 : 2,
    maximumFractionDigits: value >= 100 ? 2 : 4,
  }).format(value);
}

function formatSignalStrengthText(band?: string | null): string {
  const cleaned = (band ?? "")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return "Noise signal";
  return `${cleaned.charAt(0).toUpperCase()}${cleaned.slice(1).toLowerCase()} signal`;
}

function congressIdentity(event: EventItem) {
  const memberName = event.member_name ?? "Unknown";
  const href = memberName.trim() && memberName !== "Unknown" ? memberHref({ name: memberName, memberId: event.member_bioguide_id }) : null;
  return (
    href ? <Link href={href} prefetch={false} className="text-sm font-semibold text-emerald-200">{memberName}</Link> : <span className="text-sm font-semibold text-slate-100">{memberName}</span>
  );
}

function insiderIdentity(event: EventItem) {
  const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
  const fallbackName = getInsiderDisplayName(event.member_name) ?? event.member_name ?? "Unknown";
  const name = display.insiderName || fallbackName;
  const href = insiderHref(name, display.reportingCik ?? null);
  return (
    href ? <Link href={href} prefetch={false} className="text-sm font-semibold text-emerald-200">{name}</Link> : <span className="text-sm font-semibold text-slate-100">{name}</span>
  );
}

function rowDateLabel(kind: ActivityKind, event: EventItem) {
  if (kind === "insider") {
    const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
    return <>{formatDateShort(display.filingDate ?? event.ts ?? null)}</>;
  }
  return <>{formatDateShort(event.ts ?? null)}</>;
}

function emptyCopy(kind: ActivityKind) {
  return kind === "congress" ? "No Congress trades in the selected window." : "No insider trades in the selected window.";
}

function unavailableCopy(kind: ActivityKind) {
  return kind === "congress" ? "Congress activity is temporarily unavailable." : "Insider activity is temporarily unavailable.";
}

function activityStatusLabel({
  loading,
  unavailable,
  itemCount,
}: {
  loading: boolean;
  unavailable: boolean;
  itemCount: number;
}) {
  if (loading) return "loading";
  if (unavailable) return "unavailable";
  return `${itemCount} event${itemCount === 1 ? "" : "s"}`;
}

function ActivityRangeFooter({ itemCount }: { itemCount: number }) {
  if (itemCount <= 0) return null;
  return (
    <div className="border-t border-white/10 pt-3">
      <span className="text-xs text-slate-500">
        Showing 1-{itemCount}
      </span>
    </div>
  );
}

export function TickerActivityDetailClient({
  kind,
  symbol,
  lookbackDays,
  side,
  statusElementId,
  canViewPremiumMetrics = false,
}: {
  kind: ActivityKind;
  symbol: string;
  lookbackDays: number;
  side: SideFilter;
  statusElementId?: string;
  canViewPremiumMetrics?: boolean;
}) {
  const markerRef = useRef<HTMLDivElement | null>(null);
  const requestedRef = useRef(false);
  const [items, setItems] = useState<EventItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [unavailable, setUnavailable] = useState(false);

  useEffect(() => {
    if (!statusElementId) return;
    const element = document.getElementById(statusElementId);
    if (!element) return;
    element.textContent = activityStatusLabel({ loading, unavailable, itemCount: items.length });
  }, [items.length, loading, statusElementId, unavailable]);

  useEffect(() => {
    let alive = true;
    let observer: IntersectionObserver | null = null;
    let timer: number | null = null;
    const controller = new AbortController();

    const fetchActivity = () => {
      if (!alive || requestedRef.current || document.hidden) return;
      requestedRef.current = true;
      setLoading(true);
      setUnavailable(false);
      const tradeType = sideToTradeType(side);
      getEvents({
        symbol,
        recent_days: lookbackDays,
        limit: ACTIVITY_LIMIT,
        offset: 0,
        enrich_prices: 1,
        tape: kind,
        ...(tradeType ? { trade_type: tradeType } : {}),
        requestSource: "visibility",
        routeFamily: "ticker",
        signal: controller.signal,
        source: kind === "congress" ? "congress-detail" : "insider-detail",
      })
        .then((response) => {
          if (!alive || controller.signal.aborted) return;
          setItems(Array.isArray(response.items) ? response.items.slice(0, ACTIVITY_LIMIT) : []);
        })
        .catch((error) => {
          if (error instanceof Error && error.name === "AbortError") return;
          if (alive) {
            setItems([]);
            setUnavailable(true);
          }
        })
        .finally(() => {
          if (alive) setLoading(false);
        });
    };

    const handleVisibility = () => {
      if (!document.hidden) fetchActivity();
    };

    document.addEventListener("visibilitychange", handleVisibility);
    const node = markerRef.current;
    if (!node || typeof IntersectionObserver === "undefined") {
      timer = window.setTimeout(fetchActivity, 200);
    } else {
      observer = new IntersectionObserver(
        (entries) => {
          if (entries.some((entry) => entry.isIntersecting)) fetchActivity();
        },
        { rootMargin: "700px 0px" },
      );
      observer.observe(node);
    }

    return () => {
      alive = false;
      controller.abort();
      document.removeEventListener("visibilitychange", handleVisibility);
      if (timer !== null) window.clearTimeout(timer);
      observer?.disconnect();
    };
  }, [kind, lookbackDays, side, symbol]);

  if (loading) {
    return (
      <div ref={markerRef}>
        <ActivitySkeleton />
      </div>
    );
  }

  if (unavailable) {
    return (
      <div ref={markerRef} className="text-sm text-slate-400">
        {unavailableCopy(kind)}
      </div>
    );
  }

  if (items.length === 0) {
    return (
      <div ref={markerRef} className="text-sm text-slate-400">
        {emptyCopy(kind)}
      </div>
    );
  }

  return (
    <div ref={markerRef} className="space-y-3">
      <TickerActivityTable
        ariaLabel={`${kind === "congress" ? "Congress" : "Insider"} activity`}
        minWidthClassName={kind === "congress" ? "min-w-[74rem]" : "min-w-[68rem]"}
        headers={kind === "congress"
          ? ["Trader", "Chamber", "Party", "Signal weight", "Date", "Price", "Trade value", "Side", "Score"]
          : ["Insider", "Role", "Signal weight", "Filed", "Price", "Trade value", "Side", "Score"]}
      >
        {items.map((event) => (
          <tr key={event.id} className="transition-colors hover:bg-white/[0.035]">
            {(() => {
              const insiderDisplay = kind === "insider" ? resolveInsiderActivityDisplay(event as Record<string, unknown>) : null;
              const price = kind === "congress" ? resolveCongressActivityPrice(event as Record<string, unknown>) : insiderDisplay?.displayPrice ?? null;
              const tradeValue = insiderDisplay?.tradeValue ?? null;
              const smartSignal = kind === "insider" ? insiderDisplay?.signal ?? resolveSmartSignalValue(event as Record<string, unknown>) : resolveSmartSignalValue(event as Record<string, unknown>);
              const chamber = chamberBadge(event.chamber);
              const party = partyBadge(event.party);
              const role = kind === "insider" ? resolveInsiderRoleBadge(insiderDisplay?.role ?? null) : null;
              const strengthLabel = formatSignalStrengthText(smartSignal.band);

              return (
                <>
                  <td className={`${tickerActivityCellClassName} min-w-[12rem]`}>{kind === "congress" ? congressIdentity(event) : insiderIdentity(event)}</td>
                  {kind === "congress" ? <td className={`${tickerActivityCellClassName} whitespace-nowrap text-xs font-semibold uppercase ${chamberTextClassName(chamber.label)}`}>{chamber.label}</td> : <td className={`${tickerActivityCellClassName} whitespace-nowrap text-xs font-semibold uppercase ${insiderRoleTextClassName(role)}`}>{role ?? "—"}</td>}
                  {kind === "congress" ? <td className={`${tickerActivityCellClassName} whitespace-nowrap text-xs font-bold ${partyTextClassName(party.label)}`}>{party.label}</td> : null}
                  <td className={`${tickerActivityCellClassName} whitespace-nowrap text-xs font-medium ${signalWeightTextClassName(smartSignal.band)}`}>{strengthLabel}</td>
                  <td className={`${tickerActivityCellClassName} whitespace-nowrap text-xs text-slate-400`}>{rowDateLabel(kind, event)}</td>
                  <td className={`${tickerActivityCellClassName} whitespace-nowrap font-semibold tabular-nums text-white`}>{formatPrice(price)}</td>
                  <td className={`${tickerActivityCellClassName} whitespace-nowrap font-semibold tabular-nums text-white`}>{tradeValue !== null ? formatPrice(tradeValue) : formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}</td>
                  <td className={`${tickerActivityCellClassName} whitespace-nowrap text-xs font-semibold uppercase tracking-[0.06em] ${tradeTypeTextClassName(event.trade_type)}`}>{formatTransactionLabel(event.trade_type)}</td>
                  <td className={`${tickerActivityCellClassName} whitespace-nowrap`}><TickerActivitySignalScore score={smartSignal.score} band={smartSignal.band} unlocked={canViewPremiumMetrics} /></td>
                </>
              );
            })()}
          </tr>
        ))}
      </TickerActivityTable>
      <ActivityRangeFooter itemCount={items.length} />
    </div>
  );
}
