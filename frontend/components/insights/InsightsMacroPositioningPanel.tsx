"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { getInsightsMacroPositioning, type InsightsMacroPositioningMarket, type InsightsMacroPositioningResponse } from "@/lib/api";

const MACRO_POSITIONING_HREF = "/feed/macro-positioning";

type MacroIconProps = {
  className?: string;
};

function ChartPulseIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M4 18V8M9 18V5M14 18v-7M19 18V9" />
      <path d="M4 14l4-3 4 2 5-6 3 2" />
    </svg>
  );
}

function RatesIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M4 17h16M6 17V9M12 17V5M18 17v-4" />
      <path d="M7 7l5-4 5 4" />
    </svg>
  );
}

function DollarIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M12 3v18M16 7.5h-5.5a2.5 2.5 0 0 0 0 5H14a2.5 2.5 0 0 1 0 5H8" />
    </svg>
  );
}

function GoldIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M8 11h8l3 8H5l3-8Z" />
      <path d="M10 5h4l2 6H8l2-6Z" />
    </svg>
  );
}

function DropletIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M12 3.5s6 6.4 6 11a6 6 0 0 1-12 0c0-4.6 6-11 6-11Z" />
    </svg>
  );
}

function BitcoinIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <circle cx="12" cy="12" r="8.5" />
      <path d="M10 7v10M14 7v10M9 8h4.5a2 2 0 0 1 0 4H9M9 12h5a2 2 0 0 1 0 4H9" />
    </svg>
  );
}

function FuturesIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M5 17l4-4 3 3 7-8" />
      <path d="M16 8h3v3" />
      <path d="M5 7h7" />
    </svg>
  );
}

function RiskIcon({ className = "h-3.5 w-3.5" }: MacroIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M12 3l8 5v5c0 4.5-3.4 7.3-8 8-4.6-.7-8-3.5-8-8V8l8-5Z" />
      <path d="M9 12h6M12 9v6" />
    </svg>
  );
}

const macroRows = [
  { label: "Equity Positioning", match: /(equity|s&p|nasdaq|russell|index)/i, icon: <ChartPulseIcon />, tone: "text-cyan-200 bg-cyan-300/10 border-cyan-300/20" },
  { label: "Rates", match: /(rate|treasury|bond|yield)/i, icon: <RatesIcon />, tone: "text-blue-200 bg-blue-300/10 border-blue-300/20" },
  { label: "US Dollar", match: /(dollar|usd|dxy|fx)/i, icon: <DollarIcon />, tone: "text-rose-200 bg-rose-300/10 border-rose-300/20" },
  { label: "Gold", match: /gold/i, icon: <GoldIcon />, tone: "text-amber-200 bg-amber-300/10 border-amber-300/20" },
  { label: "Oil", match: /(oil|crude|wti|brent|energy)/i, icon: <DropletIcon />, tone: "text-orange-200 bg-orange-300/10 border-orange-300/20" },
  { label: "Bitcoin", match: /(bitcoin|btc|crypto)/i, icon: <BitcoinIcon />, tone: "text-yellow-200 bg-yellow-300/10 border-yellow-300/20" },
  { label: "COT Signals", match: /(cot|positioning|futures)/i, icon: <FuturesIcon />, tone: "text-indigo-200 bg-indigo-300/10 border-indigo-300/20" },
  { label: "Risk-On / Risk-Off", match: /(risk|equity)/i, icon: <RiskIcon />, tone: "text-red-200 bg-red-300/10 border-red-300/20" },
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
        <a href={MACRO_POSITIONING_HREF} className="text-xs font-semibold text-blue-300 transition hover:text-blue-200">
          View full
        </a>
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
          {rows.map(({ label, market, icon, tone }) => (
            <Link key={label} href={MACRO_POSITIONING_HREF} className="grid gap-3 p-3 transition hover:bg-white/[0.03] sm:grid-cols-[minmax(0,1fr)_auto]">
              <div className="min-w-0">
                <div className="flex min-w-0 items-center gap-2">
                  <span className={`grid h-6 w-6 shrink-0 place-items-center rounded-md border ${tone}`}>{icon}</span>
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
        <a href={MACRO_POSITIONING_HREF} className="text-sm font-semibold text-blue-300 transition hover:text-blue-200">
          View full Macro Positioning feed
        </a>
      </div>
    </section>
  );
}
