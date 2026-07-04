"use client";

import { useEffect, useRef, useState, type ReactNode } from "react";
import Link from "next/link";
import { Badge } from "@/components/Badge";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { getEvents, type EventItem } from "@/lib/api";
import { chamberBadge, formatCongressAffiliationText, formatCurrencyRange, formatDateShort, formatTransactionLabel, transactionTone } from "@/lib/format";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { resolveInsiderRoleBadge, insiderRoleBadgeTone } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { resolveCongressActivityPrice, resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";

type ActivityKind = "congress" | "insider";
type SideFilter = "all" | "buy" | "sell" | string;

const ACTIVITY_LIMIT = 20;

function sideToTradeType(side: SideFilter): "purchase" | "sale" | null {
  if (side === "buy") return "purchase";
  if (side === "sell") return "sale";
  return null;
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
      data-activity-scroll-region
    >
      {children}
    </div>
  );
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

function EventGrid({
  identity,
  sideBadge,
  dateLabel,
  price,
  tradeValue,
}: {
  identity: ReactNode;
  sideBadge: ReactNode;
  dateLabel: ReactNode;
  price: ReactNode;
  tradeValue: ReactNode;
}) {
  return (
    <div className="grid min-w-0 gap-x-3 gap-y-2 sm:grid-cols-[minmax(150px,1.4fr)_minmax(90px,.7fr)_minmax(120px,.8fr)_auto] sm:items-center">
      <div className="min-w-0">{identity}</div>
      <div className="text-xs text-slate-400">{dateLabel}</div>
      <div className="min-w-0">
        <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500 sm:hidden">Price</div>
        <div className="truncate text-sm font-semibold tabular-nums text-white">{price}</div>
      </div>
      <div className="min-w-0">
        <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500 sm:hidden">Trade value</div>
        <div className="truncate text-sm font-semibold tabular-nums text-white">{tradeValue}</div>
      </div>
      <div className="flex justify-start sm:justify-end">{sideBadge}</div>
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

function congressIdentity(event: EventItem) {
  const memberName = event.member_name ?? "Unknown";
  const href = event.member_bioguide_id ? memberHref({ name: memberName, memberId: event.member_bioguide_id }) : null;
  const chamber = chamberBadge(event.chamber);
  const affiliation = formatCongressAffiliationText(event.party, event.payload && typeof event.payload === "object" ? (event.payload as { state?: string | null }).state : null);
  return (
    <div className="flex flex-wrap items-center gap-2">
      {href ? (
        <Link href={href} prefetch={false} className="text-sm font-semibold text-emerald-200">
          {memberName}
        </Link>
      ) : (
        <span className="text-sm font-semibold text-slate-100">{memberName}</span>
      )}
      <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">{chamber.label}</Badge>
      {affiliation ? <span className="text-xs font-medium text-slate-400">{"\u00b7 "}{affiliation}</span> : null}
    </div>
  );
}

function insiderIdentity(event: EventItem) {
  const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
  const fallbackName = getInsiderDisplayName(event.member_name) ?? event.member_name ?? "Unknown";
  const name = display.insiderName || fallbackName;
  const href = insiderHref(name, display.reportingCik ?? null);
  const role = resolveInsiderRoleBadge(display.role ?? null);
  return (
    <div className="flex flex-wrap items-center gap-2">
      {href ? (
        <Link href={href} prefetch={false} className="text-sm font-semibold text-emerald-200">
          {name}
        </Link>
      ) : (
        <span className="text-sm font-semibold text-slate-100">{name}</span>
      )}
      <Badge tone={insiderRoleBadgeTone(role)} className="px-2 py-0.5 text-[10px]">{role}</Badge>
    </div>
  );
}

function rowDateLabel(kind: ActivityKind, event: EventItem) {
  if (kind === "insider") {
    const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
    return <>Reported {formatDateShort(display.filingDate ?? event.ts ?? null)}</>;
  }
  return <>Filed {formatDateShort(event.ts ?? null)}</>;
}

function emptyCopy(kind: ActivityKind) {
  return kind === "congress" ? "No Congress trades in the selected window." : "No insider trades in the selected window.";
}

function unavailableCopy(kind: ActivityKind) {
  return kind === "congress" ? "Congress activity is temporarily unavailable." : "Insider activity is temporarily unavailable.";
}

export function TickerActivityDetailClient({
  kind,
  symbol,
  lookbackDays,
  side,
}: {
  kind: ActivityKind;
  symbol: string;
  lookbackDays: number;
  side: SideFilter;
}) {
  const markerRef = useRef<HTMLDivElement | null>(null);
  const requestedRef = useRef(false);
  const [items, setItems] = useState<EventItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [unavailable, setUnavailable] = useState(false);

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
        enrich_prices: 0,
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
    <div ref={markerRef}>
      <ActivityScrollRegion>
        {items.map((event) => (
          <ActivityCard key={event.id}>
            {(() => {
              const insiderDisplay = kind === "insider" ? resolveInsiderActivityDisplay(event as Record<string, unknown>) : null;
              const price = kind === "congress" ? resolveCongressActivityPrice(event as Record<string, unknown>) : insiderDisplay?.displayPrice ?? null;
              const tradeValue = insiderDisplay?.tradeValue ?? null;

              return (
                <EventGrid
                  identity={kind === "congress" ? congressIdentity(event) : insiderIdentity(event)}
                  sideBadge={<Badge tone={transactionTone(event.trade_type)}>{formatTransactionLabel(event.trade_type)}</Badge>}
                  dateLabel={rowDateLabel(kind, event)}
                  price={formatPrice(price)}
                  tradeValue={tradeValue !== null ? formatPrice(tradeValue) : formatCurrencyRange(event.amount_min ?? null, event.amount_max ?? null)}
                />
              );
            })()}
          </ActivityCard>
        ))}
      </ActivityScrollRegion>
    </div>
  );
}
