"use client";

import { type ReactNode } from "react";
import { cardClassName } from "@/lib/styles";

type SourceFilter = "all" | "congress" | "insider" | "signals" | "government_contract";
type SideFilter = "all" | "buy" | "sell";
type MetricIconKind = "congress" | "insider-buy" | "insider-sell" | "signals" | "flow" | "people";

type KpiTile = {
  key: string;
  label: string;
  value: ReactNode;
  toneClass: string;
  icon: MetricIconKind;
  targetId?: string;
  title?: string;
  source?: SourceFilter;
  side?: SideFilter;
};

function metricIcon(kind: MetricIconKind, className = "h-4 w-4") {
  if (kind === "congress") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 9h16" />
        <path d="M5 19h14" />
        <path d="M7 9v10" />
        <path d="M12 9v10" />
        <path d="M17 9v10" />
        <path d="M3 21h18" />
        <path d="M12 3 4 7h16l-8-4Z" />
      </svg>
    );
  }
  if (kind === "signals") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 12h2.5l2-5 3.5 10 3-7 2 2H20" />
        <path d="M4 19h16" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "flow") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M7 7h9.5a3.5 3.5 0 0 1 0 7H8" />
        <path d="m11 4-4 3 4 3" />
        <path d="M17 17H7.5a3.5 3.5 0 0 1 0-7H16" opacity="0.45" />
      </svg>
    );
  }
  if (kind === "people") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
        <path d="M16 19v-1.5a3.5 3.5 0 0 0-3.5-3.5h-5A3.5 3.5 0 0 0 4 17.5V19" />
        <path d="M10 10a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z" />
        <path d="M20 19v-1a3 3 0 0 0-2.2-2.9" opacity="0.55" />
        <path d="M16 4.4a3 3 0 0 1 0 5.8" opacity="0.55" />
      </svg>
    );
  }
  const isSell = kind === "insider-sell";
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8Z" />
      <path d="M5 20a7 7 0 0 1 14 0" />
      <path d={isSell ? "M18 8v7" : "M18 15V8"} />
      <path d={isSell ? "m15 12 3 3 3-3" : "m15 11 3-3 3 3"} />
    </svg>
  );
}

function scrollToSection(targetId: string) {
  document.getElementById(targetId)?.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}

export function TickerKpiNavigation({
  tiles,
}: {
  symbol: string;
  lookback: string;
  source: SourceFilter;
  side: SideFilter;
  tiles: KpiTile[];
}) {
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-8">
      {tiles.map((tile) => {
        const isClickable = Boolean(tile.targetId);
        const className = [
          cardClassName,
          "p-3.5 text-left transition duration-200",
          isClickable ? "cursor-pointer hover:-translate-y-0.5 hover:border-emerald-300/35 hover:bg-white/[0.07] hover:shadow-lg hover:shadow-emerald-950/25 focus:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50" : "",
        ].filter(Boolean).join(" ");

        if (!isClickable) {
          return (
            <div key={tile.key} className={className}>
              <div className="flex items-start justify-between gap-3">
                <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">{tile.label}</p>
                <span className={`shrink-0 ${tile.toneClass}`}>{metricIcon(tile.icon)}</span>
              </div>
              <div className={`mt-2 text-right text-2xl font-semibold tabular-nums ${tile.toneClass}`}>{tile.value}</div>
            </div>
          );
        }

        return (
          <button
            key={tile.key}
            type="button"
            className={className}
            title={tile.title}
            aria-label={tile.title ?? tile.label}
            onClick={(event) => {
              event.preventDefault();
              event.stopPropagation();
              if (!tile.targetId) return;
              scrollToSection(tile.targetId);
            }}
          >
            <div className="flex items-start justify-between gap-3">
              <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">{tile.label}</p>
              <span className={`shrink-0 ${tile.toneClass}`}>{metricIcon(tile.icon)}</span>
            </div>
            <div className={`mt-2 text-right text-2xl font-semibold tabular-nums ${tile.toneClass}`}>{tile.value}</div>
          </button>
        );
      })}
    </div>
  );
}
