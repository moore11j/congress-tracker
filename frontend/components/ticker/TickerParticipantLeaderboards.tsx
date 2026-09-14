"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { getEvents, type EventItem } from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { insiderHref } from "@/lib/insider";
import { resolveInsiderRoleBadge } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { resolveInsiderActivityDisplay } from "@/lib/tradeDisplay";
import { chamberTextClassName, insiderRoleTextClassName, partyTextClassName } from "@/components/ticker/TickerActivityText";

type ActivityKind = "congress" | "insider";
type SideFilter = "all" | "buy" | "sell" | string;

type Participant = {
  key: string;
  name: string;
  href?: string;
  chamber?: string;
  party?: string;
  role?: string;
  trades: number;
  netFlow: number;
};

const RANKING_LIMIT = 100;

function sideToTradeType(side: SideFilter): "purchase" | "sale" | null {
  if (side === "buy") return "purchase";
  if (side === "sell") return "sale";
  return null;
}

function normalizeTradeSide(value?: string | null): "buy" | "sell" | null {
  const normalized = (value ?? "").trim().toLowerCase();
  if (["buy", "purchase", "p-purchase"].includes(normalized)) return "buy";
  if (["sell", "sale", "s-sale"].includes(normalized)) return "sell";
  return null;
}

function amountForEvent(event: EventItem): number {
  const value = Number(event.amount_max ?? event.amount_min ?? 0);
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function formatCompactUsd(value: number): string {
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`;
  return value.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function congressParticipant(event: EventItem): Omit<Participant, "trades" | "netFlow"> {
  const name = event.member_name?.trim() || "Unknown member";
  const memberId = event.member_bioguide_id?.trim() || null;
  const candidateHref = memberHref({ name: event.member_name ?? undefined, memberId: event.member_bioguide_id ?? undefined });
  const href = candidateHref && candidateHref !== "/member/UNKNOWN" ? candidateHref : undefined;
  const chamber = chamberBadge(event.chamber);
  return {
    key: memberId ? `member:${memberId}` : `name:${name.toLowerCase()}`,
    name,
    href,
    chamber: chamber.label,
    party: partyBadge(event.party).label,
  };
}

function insiderParticipant(event: EventItem): Omit<Participant, "trades" | "netFlow"> {
  const display = resolveInsiderActivityDisplay(event as Record<string, unknown>);
  const name = display.insiderName || event.member_name?.trim() || "Unknown insider";
  const reportingCik = display.reportingCik?.trim() || null;
  const role = resolveInsiderRoleBadge(display.role ?? null);
  return {
    key: reportingCik ? `cik:${reportingCik}` : `name:${name.toLowerCase()}`,
    name,
    href: insiderHref(name, reportingCik) ?? undefined,
    role,
  };
}

function rankParticipants(kind: ActivityKind, events: EventItem[]): Participant[] {
  const participants = new Map<string, Participant>();
  for (const event of events) {
    const identity = kind === "congress" ? congressParticipant(event) : insiderParticipant(event);
    const existing = participants.get(identity.key) ?? { ...identity, trades: 0, netFlow: 0 };
    existing.trades += 1;
    const side = normalizeTradeSide(event.trade_type);
    const amount = amountForEvent(event);
    if (side === "buy") existing.netFlow += amount;
    if (side === "sell") existing.netFlow -= amount;
    participants.set(identity.key, existing);
  }
  return [...participants.values()].sort((left, right) => (
    right.trades - left.trades
    || Math.abs(right.netFlow) - Math.abs(left.netFlow)
    || right.netFlow - left.netFlow
    || left.name.localeCompare(right.name)
  ));
}

function LeaderboardSkeleton() {
  return (
    <div className="space-y-2" aria-live="polite" aria-busy="true">
      {Array.from({ length: 3 }).map((_, index) => <SkeletonBlock key={index} className="h-11 w-full" />)}
    </div>
  );
}

function ParticipantLeaderboard({
  title,
  kind,
  participants,
  loading,
  unavailable,
}: {
  title: string;
  kind: ActivityKind;
  participants: Participant[];
  loading: boolean;
  unavailable: boolean;
}) {
  const emptyCopy = unavailable
    ? `${title} are temporarily unavailable.`
    : kind === "congress"
      ? "No Congress traders in the selected window."
      : "No insiders in the selected window.";
  return (
    <section id={kind === "congress" ? "top-congress-traders" : "top-insiders"} className="rounded-2xl border border-white/10 bg-white/5 p-4 scroll-mt-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">{title}</h2>
          <p className="mt-1 text-[11px] text-slate-500">Ranked by trade count · net flow breaks ties</p>
        </div>
        <span className="rounded-md border border-white/10 bg-slate-950/45 px-2 py-1 text-[10px] font-semibold text-slate-400">{participants.length} ranked</span>
      </div>
      <div className="mt-3 overflow-x-auto">
        {loading && participants.length === 0 ? <LeaderboardSkeleton /> : participants.length === 0 ? (
          <div className="rounded-md border border-dashed border-white/15 bg-slate-950/40 px-3 py-4 text-sm text-slate-400">{emptyCopy}</div>
        ) : (
          <table className={`w-full text-left text-xs ${kind === "congress" ? "min-w-[39rem]" : "min-w-[32rem]"}`}>
            <thead className="border-y border-white/10 bg-white/[0.025] text-[10px] font-medium uppercase tracking-[0.1em] text-slate-500">
              <tr>
                <th className="px-2 py-2.5">#</th>
                <th className="px-2 py-2.5">Participant</th>
                {kind === "congress" ? <th className="px-2 py-2.5">Chamber</th> : <th className="px-2 py-2.5">Role</th>}
                {kind === "congress" ? <th className="px-2 py-2.5">Party</th> : null}
                <th className="px-2 py-2.5 text-right">Trades</th>
                <th className="px-2 py-2.5 text-right">Net flow</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10">
              {participants.slice(0, 10).map((participant, index) => (
                <tr key={participant.key} className="transition-colors hover:bg-white/[0.025]">
                  <td className="px-2 py-2.5 font-mono font-semibold text-slate-400">{index + 1}</td>
                  <td className="min-w-[12rem] px-2 py-2.5">
                    {participant.href ? <Link href={participant.href} prefetch={false} className="font-semibold text-slate-100 hover:text-emerald-200">{participant.name}</Link> : <span className="font-semibold text-slate-100">{participant.name}</span>}
                  </td>
                  {kind === "congress" ? (
                    <td className={`px-2 py-2.5 text-[11px] font-semibold uppercase tracking-[0.08em] ${chamberTextClassName(participant.chamber)}`}>{participant.chamber ?? "—"}</td>
                  ) : (
                    <td className={`px-2 py-2.5 text-[11px] font-semibold uppercase tracking-[0.08em] ${insiderRoleTextClassName(participant.role)}`}>{participant.role ?? "—"}</td>
                  )}
                  {kind === "congress" ? <td className={`px-2 py-2.5 text-[11px] font-bold ${partyTextClassName(participant.party)}`}>{participant.party ?? "—"}</td> : null}
                  <td className="px-2 py-2.5 text-right font-mono font-semibold tabular-nums text-slate-200">{participant.trades}</td>
                  <td className={`px-2 py-2.5 text-right font-mono font-semibold tabular-nums ${participant.netFlow > 0 ? "text-emerald-300" : participant.netFlow < 0 ? "text-rose-300" : "text-slate-400"}`}>{participant.netFlow > 0 ? "+" : participant.netFlow < 0 ? "-" : ""}${formatCompactUsd(Math.abs(participant.netFlow))}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </section>
  );
}

export function TickerParticipantLeaderboards({
  symbol,
  lookbackDays,
  side,
  initialCongressEvents,
  initialInsiderEvents,
}: {
  symbol: string;
  lookbackDays: number;
  side: SideFilter;
  initialCongressEvents: EventItem[];
  initialInsiderEvents: EventItem[];
}) {
  const markerRef = useRef<HTMLDivElement | null>(null);
  const requestedRef = useRef(false);
  const [congressEvents, setCongressEvents] = useState<EventItem[]>(initialCongressEvents);
  const [insiderEvents, setInsiderEvents] = useState<EventItem[]>(initialInsiderEvents);
  const [loading, setLoading] = useState(initialCongressEvents.length === 0 && initialInsiderEvents.length === 0);
  const [unavailable, setUnavailable] = useState(false);

  useEffect(() => {
    setCongressEvents(initialCongressEvents);
    setInsiderEvents(initialInsiderEvents);
    requestedRef.current = false;
    setLoading(initialCongressEvents.length === 0 && initialInsiderEvents.length === 0);
    setUnavailable(false);
  }, [initialCongressEvents, initialInsiderEvents, lookbackDays, side, symbol]);

  useEffect(() => {
    let alive = true;
    const controller = new AbortController();
    let observer: IntersectionObserver | null = null;
    let timer: number | null = null;
    const load = () => {
      if (!alive || requestedRef.current || document.hidden) return;
      requestedRef.current = true;
      setLoading(true);
      const tradeType = sideToTradeType(side);
      const request = (tape: ActivityKind) => getEvents({
        symbol,
        recent_days: lookbackDays,
        limit: RANKING_LIMIT,
        offset: 0,
        enrich_prices: 0,
        tape,
        ...(tradeType ? { trade_type: tradeType } : {}),
        requestSource: "visibility",
        routeFamily: "ticker",
        signal: controller.signal,
        source: `ticker-${tape}-leaderboard`,
      });
      Promise.all([request("congress"), request("insider")])
        .then(([congress, insiders]) => {
          if (!alive || controller.signal.aborted) return;
          setCongressEvents(Array.isArray(congress.items) ? congress.items.slice(0, RANKING_LIMIT) : []);
          setInsiderEvents(Array.isArray(insiders.items) ? insiders.items.slice(0, RANKING_LIMIT) : []);
        })
        .catch((error) => {
          if (error instanceof Error && error.name === "AbortError") return;
          if (alive) setUnavailable(true);
        })
        .finally(() => {
          if (alive) setLoading(false);
        });
    };
    const node = markerRef.current;
    if (!node || typeof IntersectionObserver === "undefined") timer = window.setTimeout(load, 200);
    else {
      observer = new IntersectionObserver((entries) => {
        if (entries.some((entry) => entry.isIntersecting)) load();
      }, { rootMargin: "700px 0px" });
      observer.observe(node);
    }
    return () => {
      alive = false;
      controller.abort();
      observer?.disconnect();
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [lookbackDays, side, symbol]);

  const congressParticipants = useMemo(() => rankParticipants("congress", congressEvents), [congressEvents]);
  const insiderParticipants = useMemo(() => rankParticipants("insider", insiderEvents), [insiderEvents]);
  return (
    <div ref={markerRef} className="min-w-0 space-y-5">
      <ParticipantLeaderboard title="Top Congress traders" kind="congress" participants={congressParticipants} loading={loading} unavailable={unavailable} />
      <ParticipantLeaderboard title="Top insiders" kind="insider" participants={insiderParticipants} loading={loading} unavailable={unavailable} />
    </div>
  );
}
