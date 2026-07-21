"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { getInsightsMacroPositioning, type InsightsMacroPositioningMarket, type InsightsMacroPositioningResponse } from "@/lib/api";

const macroRows = [
  { label: "Equity Positioning", match: /(equity|s&p|nasdaq|russell|index)/i },
  { label: "Rates", match: /(rate|treasury|bond|yield)/i },
  { label: "US Dollar", match: /(dollar|usd|dxy|fx)/i },
  { label: "Gold", match: /gold/i },
  { label: "Oil", match: /(oil|crude|wti|brent|energy)/i },
  { label: "Bitcoin", match: /(bitcoin|btc|crypto)/i },
  { label: "COT Signals", match: /(cot|positioning|futures)/i },
  { label: "Risk-On / Risk-Off", match: /(risk|equity)/i },
] as const;

function biasLabel(value?: string | null): string {
  if (value === "bullish") return "Bullish";
  if (value === "bearish") return "Bearish";
  if (value === "neutral") return "Neutral";
  return "Insufficient data";
}

function biasClassName(value?: string | null): string {
  if (value === "bullish") return "border-emerald-300/35 bg-emerald-300/10 text-emerald-200";
  if (value === "bearish") return "border-rose-300/35 bg-rose-300/10 text-rose-200";
  if (value === "neutral") return "border-amber-300/35 bg-amber-300/10 text-amber-200";
  return "border-slate-300/20 bg-slate-300/10 text-slate-300";
}

function trendCopy(market?: InsightsMacroPositioningMarket | null): string {
  if (!market) return "No recent positioning update.";
  if (market.headline) return market.headline;
  if (market.interpretation) return market.interpretation;
  if (market.trend === "increasing") return "Positioning is increasing.";
  if (market.trend === "decreasing") return "Positioning is decreasing.";
  if (market.trend === "stable") return "Positioning is stable.";
  return "Latest weekly positioning is available.";
}

function findMarket(markets: InsightsMacroPositioningMarket[], label: string, match: RegExp): InsightsMacroPositioningMarket | null {
  const exact = markets.find((market) => market.name.toLowerCase() === label.toLowerCase());
  if (exact) return exact;
  return markets.find((market) => match.test(`${market.id} ${market.name}`)) ?? null;
}

function PanelSkeleton() {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/55 p-4">
      <div className="h-4 w-44 animate-pulse rounded bg-white/10" />
      <div className="mt-4 space-y-3">
        {Array.from({ length: 7 }).map((_, index) => (
          <div key={index} className="grid grid-cols-[1fr_5rem] gap-3 rounded-lg border border-white/10 bg-slate-950/45 p-3">
            <div className="space-y-2">
              <div className="h-3 w-28 animate-pulse rounded bg-white/10" />
              <div className="h-3 w-full animate-pulse rounded bg-white/10" />
            </div>
            <div className="h-6 animate-pulse rounded bg-white/10" />
          </div>
        ))}
      </div>
    </section>
  );
}

export function InsightsMacroPositioningPanel() {
  const [data, setData] = useState<InsightsMacroPositioningResponse | null>(null);
  const [failed, setFailed] = useState(false);

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

  const rows = useMemo(() => {
    const markets = data?.entitlement.unlocked ? data.markets : [];
    return macroRows.map((row) => ({
      ...row,
      market: findMarket(markets, row.label, row.match),
    }));
  }, [data]);

  if (!data) return <PanelSkeleton />;

  const locked = !data.entitlement.unlocked || data.status === "locked";
  const unavailable = failed || ["unavailable", "awaiting_first_refresh"].includes(data.status);

  return (
    <section id="macro-positioning" className="rounded-lg border border-white/10 bg-slate-950/55 p-4 shadow-[0_18px_60px_-42px_rgba(16,185,129,0.55)] lg:sticky lg:top-20">
      <div className="flex items-center justify-between gap-3 border-b border-white/10 pb-3">
        <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-100">Macro Positioning</h2>
        <Link href="/feed/macro-positioning" className="text-xs font-semibold text-blue-300 transition hover:text-blue-200">
          View full
        </Link>
      </div>

      {locked ? (
        <div className="mt-4 rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
          <p className="text-sm font-semibold text-white">Macro positioning requires Pro.</p>
          <p className="mt-2 text-sm leading-6 text-slate-300">{data.subtitle ?? "Unlock institutional futures positioning across major markets."}</p>
          <Link href="/pricing" className="mt-4 inline-flex rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100">
            Upgrade to Pro
          </Link>
        </div>
      ) : unavailable ? (
        <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/45 p-4 text-sm text-slate-400">
          {data.message ?? "Macro positioning is temporarily unavailable."}
        </div>
      ) : (
        <div className="mt-3 divide-y divide-white/10 overflow-hidden rounded-lg border border-white/10 bg-slate-950/35">
          {rows.map(({ label, market }) => (
            <Link key={label} href="/feed/macro-positioning" className="grid gap-3 p-3 transition hover:bg-white/[0.03] sm:grid-cols-[minmax(0,1fr)_auto]">
              <div className="min-w-0">
                <div className="flex min-w-0 items-center gap-2">
                  <span className="h-2 w-2 shrink-0 rounded-full bg-cyan-300/70" />
                  <p className="truncate text-sm font-semibold text-white">{label}</p>
                </div>
                <p className="mt-2 text-xs leading-5 text-slate-400 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">
                  {trendCopy(market)}
                </p>
              </div>
              <div className="flex items-start justify-between gap-3 sm:block sm:text-right">
                <span className={`inline-flex rounded-md border px-2 py-1 text-[11px] font-semibold ${biasClassName(market?.bias)}`}>
                  {biasLabel(market?.bias)}
                </span>
                {market?.trend ? <p className="mt-2 text-xs capitalize text-slate-500">{market.trend}</p> : null}
              </div>
            </Link>
          ))}
        </div>
      )}

      <div className="mt-4 border-t border-white/10 pt-3 text-center">
        <Link href="/feed/macro-positioning" className="text-sm font-semibold text-blue-300 transition hover:text-blue-200">
          View full Macro Positioning feed
        </Link>
      </div>
    </section>
  );
}
