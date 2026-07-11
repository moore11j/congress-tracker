"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getInsightsMacroPositioning,
  type InsightsMacroPositioningMarket,
  type InsightsMacroPositioningResponse,
} from "@/lib/api";
import { cardClassName, ghostButtonClassName } from "@/lib/styles";

function biasLabel(value?: string | null): string {
  if (value === "bullish") return "Bullish";
  if (value === "bearish") return "Bearish";
  return "Neutral";
}

function biasClassName(value?: string | null): string {
  if (value === "bullish") return "text-emerald-300";
  if (value === "bearish") return "text-rose-300";
  return "text-slate-300";
}

function biasPillClassName(value?: string | null): string {
  if (value === "bullish") return "border-emerald-300/30 bg-emerald-300/10 text-emerald-200";
  if (value === "bearish") return "border-rose-300/30 bg-rose-300/10 text-rose-200";
  return "border-slate-300/20 bg-slate-300/10 text-slate-200";
}

function trendLabel(value?: string | null): string {
  if (value === "increasing") return "Increasing";
  if (value === "decreasing") return "Decreasing";
  return "Stable";
}

function percentileText(value?: number | null): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  return `${Math.round(value)}th percentile`;
}

function updatedText(value?: string | null): string {
  if (!value) return "Updated weekly";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Updated weekly";
  const formatted = new Intl.DateTimeFormat("en-US", { year: "numeric", month: "short", day: "2-digit", timeZone: "UTC" }).format(date);
  return `Updated ${formatted}`;
}

function convictionText(value?: number | null): string {
  const rating = Math.max(1, Math.min(5, Math.round(value ?? 3)));
  return `${rating}/5 conviction`;
}

function SectionSkeleton() {
  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <div className="flex flex-col gap-2">
        <div className="h-5 w-44 animate-pulse rounded bg-white/10" />
        <div className="h-4 w-full max-w-md animate-pulse rounded bg-white/10" />
      </div>
      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="min-h-[9rem] rounded-lg border border-white/10 bg-slate-950/45 p-4">
            <div className="h-4 w-24 animate-pulse rounded bg-white/10" />
            <div className="mt-4 h-5 w-20 animate-pulse rounded bg-white/10" />
            <div className="mt-4 h-3 w-full animate-pulse rounded bg-white/10" />
          </div>
        ))}
      </div>
    </section>
  );
}

function LockedSection({ data }: { data: InsightsMacroPositioningResponse }) {
  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="max-w-3xl">
          <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            See whether institutional futures positioning is bullish, bearish, crowded, or shifting across major markets.
          </p>
          <p className="mt-3 text-sm font-semibold text-emerald-100">{data.subtitle ?? "Included with Walnut Pro."}</p>
        </div>
        <Link href="/pricing" className={ghostButtonClassName}>
          Upgrade to Pro
        </Link>
      </div>
      <div className="mt-5 rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
        <p className="text-sm font-semibold text-white">Macro Positioning</p>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          Institutional futures positioning across major markets.
        </p>
      </div>
    </section>
  );
}

function EmptyState({ status, message }: { status?: string | null; message?: string | null }) {
  const text =
    status === "awaiting_first_refresh"
      ? "Macro positioning will appear after the next weekly data refresh."
      : message ?? "Macro positioning is temporarily unavailable.";
  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
      <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning across major markets.</p>
      <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/45 p-4 text-sm text-slate-300">{text}</div>
    </section>
  );
}

function MacroCard({ market, onOpen }: { market: InsightsMacroPositioningMarket; onOpen: () => void }) {
  const percentile = percentileText(market.percentile);
  return (
    <button
      type="button"
      onClick={onOpen}
      className="min-h-[10rem] rounded-lg border border-white/10 bg-slate-950/45 p-4 text-left transition hover:border-emerald-300/35 hover:bg-slate-950/65 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/25"
    >
      <div className="flex items-start justify-between gap-3">
        <h3 className="min-w-0 text-sm font-semibold leading-5 text-white">{market.name}</h3>
        <span className={`shrink-0 rounded-md border px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] ${biasPillClassName(market.bias)}`}>
          {biasLabel(market.bias)}
        </span>
      </div>
      <p className="mt-4 text-sm leading-5 text-slate-300">{market.headline ?? "Institutional positioning is stable."}</p>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
        <span>{percentile ?? trendLabel(market.trend)}</span>
        <span>{updatedText(market.updated_at)}</span>
      </div>
    </button>
  );
}

function MacroFlyout({ market, onClose }: { market: InsightsMacroPositioningMarket; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/70 backdrop-blur-sm" role="dialog" aria-modal="true" aria-label={`${market.name} Positioning`}>
      <button type="button" aria-label="Close Macro Positioning" className="absolute inset-0 cursor-default" onClick={onClose} />
      <aside className="relative z-10 flex h-full w-full max-w-md flex-col overflow-y-auto border-l border-white/10 bg-slate-950 p-5 shadow-2xl shadow-black/50">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">{market.name} Positioning</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning across major markets.</p>
          </div>
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-2.5 py-1.5 text-xs font-semibold text-slate-300 transition hover:border-white/25 hover:text-white">
            Close
          </button>
        </div>

        <div className="mt-8 space-y-7">
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Overall Bias</p>
            <p className={`mt-3 text-lg font-semibold ${biasClassName(market.bias)}`}>{biasLabel(market.bias)}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Rating</p>
            <p className="mt-3 text-sm font-semibold text-slate-100">{convictionText(market.rating)}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section className="grid gap-4 sm:grid-cols-2">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Positioning</p>
              <p className="mt-3 text-sm font-semibold text-slate-100">{percentileText(market.percentile) ?? "Latest available"}</p>
            </div>
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Weekly Trend</p>
              <p className="mt-3 text-sm font-semibold text-slate-100">
                {trendLabel(market.trend)}
                {market.trend_weeks ? ` for ${market.trend_weeks} weeks` : ""}
              </p>
            </div>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Interpretation</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">{market.interpretation ?? "Positioning is balanced across the latest weekly data."}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Updated</p>
            <p className="mt-3 text-sm font-semibold text-slate-100">{updatedText(market.updated_at)}</p>
          </section>
        </div>
      </aside>
    </div>
  );
}

export function InsightsMacroPositioningClient() {
  const [data, setData] = useState<InsightsMacroPositioningResponse | null>(null);
  const [failed, setFailed] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    getInsightsMacroPositioning({ signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setData(payload);
      })
      .catch((error) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        setFailed(true);
        setData({
          status: "unavailable",
          entitlement: { required_plan: "pro", unlocked: true },
          markets: [],
          summary: null,
          message: "Macro positioning is temporarily unavailable.",
        });
      });
    return () => controller.abort();
  }, []);

  const selectedMarket = useMemo(
    () => data?.markets.find((market) => market.id === selectedId) ?? null,
    [data?.markets, selectedId],
  );

  if (!data) return <SectionSkeleton />;
  if (!data.entitlement.unlocked || data.status === "locked") return <LockedSection data={data} />;
  if (failed || data.markets.length === 0 || ["unavailable", "awaiting_first_refresh"].includes(data.status)) {
    return <EmptyState status={data.status} message={data.message} />;
  }

  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="max-w-4xl">
          <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
          <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning across major markets.</p>
          {data.stale ? (
            <p className="mt-3 rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-2 text-sm text-amber-100">
              Latest weekly positioning data is delayed.
            </p>
          ) : null}
        </div>
        {data.updated_at ? <p className="text-xs text-slate-500 lg:text-right">{updatedText(data.updated_at)}</p> : null}
      </div>

      {data.summary ? (
        <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/45 p-4">
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Weekly Positioning Summary</p>
          <p className="mt-3 text-sm leading-6 text-slate-200">{data.summary}</p>
        </div>
      ) : null}

      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {data.markets.map((market) => (
          <MacroCard key={market.id} market={market} onOpen={() => setSelectedId(market.id)} />
        ))}
      </div>

      {selectedMarket ? <MacroFlyout market={selectedMarket} onClose={() => setSelectedId(null)} /> : null}
    </section>
  );
}
