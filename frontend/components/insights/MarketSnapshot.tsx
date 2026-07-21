import type { ReactNode } from "react";
import Link from "next/link";

import { formatDateShort } from "@/lib/format";
import type { MacroSnapshotIndex, MacroSnapshotPoint, MacroSnapshotResponse, SectorPerformancePoint, SnapshotInstrument } from "@/lib/types";

type Props = {
  snapshot: MacroSnapshotResponse;
};

const FALLBACK_WORLD_INDEXES: SnapshotInstrument[] = [
  { label: "China \u2014 MCHI", symbol: "MCHI", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "Germany \u2014 EWG", symbol: "EWG", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "Japan \u2014 IJP", symbol: "IJP", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "UK \u2014 ISF", symbol: "ISF", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "Canada \u2014 VFV", symbol: "VFV", timeframe_label: "Daily Change", status: "unavailable" },
];

const FALLBACK_US_INDEXES: SnapshotInstrument[] = [
  { label: "S&P 500", symbol: "SPY", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "NASDAQ", symbol: "QQQ", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Dow", symbol: "DIA", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Russell 2000", symbol: "IWM", timeframe_label: "EOD Change", status: "unavailable" },
];

const FALLBACK_COMMODITIES: SnapshotInstrument[] = [
  { label: "Gold \u2014 GCUSD", symbol: "GCUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "Silver \u2014 SILUSD", symbol: "SILUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "Copper \u2014 HGUSD", symbol: "HGUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
];

const FALLBACK_CURRENCIES: SnapshotInstrument[] = [
  { label: "DXY", symbol: "DXY", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "USD/CAD", symbol: "USDCAD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/USD", symbol: "EURUSD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "GBP/USD", symbol: "GBPUSD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "USD/JPY", symbol: "USDJPY", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/CAD", symbol: "EURCAD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
];

const FALLBACK_CRYPTO: SnapshotInstrument[] = [
  { label: "BTC/USD", symbol: "BTCUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "ETH/USD", symbol: "ETHUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "SOL/USD", symbol: "SOLUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "XRP/USD", symbol: "XRPUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "BNB/USD", symbol: "BNBUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
];

const FALLBACK_MACRO: MacroSnapshotPoint[] = [
  { label: "Fed Overnight Rate", value: null, value_format: "percent", change_format: "bps" },
  { label: "Core CPI", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Unemployment", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Debt/GDP", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Retail Sales", value: null, value_format: "currency", change_format: "percent" },
  { label: "GDP Growth", value: null, value_format: "percent", change_format: "percentage_points" },
];

const FALLBACK_TREASURY: MacroSnapshotPoint[] = [
  { label: "3M Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "2Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "5Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "10Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "30Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
];

function formatValue(value: number | string | null | undefined, digits = 2): string {
  if (typeof value === "string") return value || "Unavailable";
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(value);
}

function valueDigits(value: number | string | null | undefined, unitLabel?: string | null): number {
  if (unitLabel === "rate") return 4;
  if (typeof value === "number" && Math.abs(value) > 0 && Math.abs(value) < 10) return 4;
  return 2;
}

function formatPercent(value: number | null | undefined): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatAbsoluteNumber(value: number, digits = 2): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(value);
}

function formatSignedNumber(value: number | null | undefined, digits = 2): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${formatAbsoluteNumber(Math.abs(value), digits)}`;
}

function formatPercentValue(value: number | null | undefined): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  return `${new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(value)}%`;
}

function formatSignedPercent(value: number | null | undefined): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${formatAbsoluteNumber(Math.abs(value), 2)}%`;
}

function formatCurrencyCompact(value: number | null | undefined): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    minimumFractionDigits: 0,
    maximumFractionDigits: 1,
  }).format(value);
}

function formatSignedCurrencyCompact(value: number | null | undefined): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  const absolute = formatCurrencyCompact(Math.abs(value));
  return absolute ? `${sign}${absolute}` : null;
}

function normalizeTimeframeLabel(value: string | null | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  const lowered = trimmed.toLowerCase();
  if (lowered === "1d change" || lowered === "1d average change" || lowered === "1d avg change") {
    return "Daily Change";
  }
  return trimmed;
}

function isInternalSnapshotMetaLabel(value?: string | null): boolean {
  const lowered = (value ?? "").trim().toLowerCase();
  const internalTerms = ["fr" + "ed", "ca" + "che", "pro" + "xy", "provider", "backend"];
  return Boolean(lowered && internalTerms.some((term) => lowered.includes(term)));
}

function publicSnapshotMetaLabel(value?: string | null): string | null {
  if (!value || isInternalSnapshotMetaLabel(value)) return null;
  const trimmed = value.trim();
  return trimmed === "Latest available" ? "Latest" : trimmed;
}

function formatMacroMainValue(item: MacroSnapshotPoint): string {
  if (typeof item.value !== "number" || Number.isNaN(item.value)) return "Unavailable";
  const format = item.value_format ?? (item.unit_label === "%" || item.unit_label === "yield" ? "percent" : "number");
  switch (format) {
    case "percent":
      return formatPercentValue(item.value) ?? "Unavailable";
    case "currency":
      return formatCurrencyCompact(item.value) ?? "Unavailable";
    case "bps":
      return `${formatSignedNumber(item.value, 0) ?? "0"} bps`;
    case "number":
    default:
      return formatValue(item.value);
  }
}

function formatMacroChange(item: MacroSnapshotPoint): string | null {
  const value = item.change_value ?? item.change;
  const format = item.change_format ?? item.change_unit ?? null;
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  switch (format) {
    case "bps":
      return `${formatSignedNumber(value, 0) ?? "0"} bps`;
    case "percentage_points":
      return `${formatSignedNumber(value, 2) ?? "0"} pp`;
    case "percent":
      return formatSignedPercent(value);
    case "currency":
      return formatSignedCurrencyCompact(value);
    case "number":
    default:
      return formatSignedNumber(value);
  }
}

function formatMacroMeta(item: MacroSnapshotPoint): string {
  const bits = [
    formatDateShort(item.date ?? null),
    publicSnapshotMetaLabel(item.change_label),
    publicSnapshotMetaLabel(item.context_label),
  ].filter((value): value is string => Boolean(value));
  return bits.length > 0 ? bits.join(" / ") : "-";
}

function formatSnapshotUpdatedAt(value: string | null | undefined): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return `Updated: ${formatDateShort(value)}`;
}

function deltaClassName(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "text-slate-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-400";
}

function isUnavailableInstrument(item: SnapshotInstrument): boolean {
  return item.status === "unavailable" || item.status === "disabled" || item.value == null;
}

type SnapshotIconProps = {
  className?: string;
};

function IconFrame({ children, tone = "cyan" }: { children: ReactNode; tone?: "cyan" | "blue" | "violet" | "teal" | "amber" | "indigo" | "rose" | "emerald" }) {
  const tones = {
    cyan: "border-cyan-300/25 bg-cyan-300/10 text-cyan-200",
    blue: "border-blue-300/25 bg-blue-300/10 text-blue-200",
    violet: "border-violet-300/25 bg-violet-300/10 text-violet-200",
    teal: "border-teal-300/25 bg-teal-300/10 text-teal-200",
    amber: "border-amber-300/25 bg-amber-300/10 text-amber-200",
    indigo: "border-indigo-300/25 bg-indigo-300/10 text-indigo-200",
    rose: "border-rose-300/25 bg-rose-300/10 text-rose-200",
    emerald: "border-emerald-300/25 bg-emerald-300/10 text-emerald-200",
  };
  return <span className={`mt-0.5 grid h-6 w-6 shrink-0 place-items-center rounded-md border ${tones[tone]}`}>{children}</span>;
}

function GlobeIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <circle cx="12" cy="12" r="9" />
      <path d="M3 12h18M12 3c2.2 2.5 3.3 5.5 3.3 9s-1.1 6.5-3.3 9M12 3c-2.2 2.5-3.3 5.5-3.3 9s1.1 6.5 3.3 9" />
    </svg>
  );
}

function CurrencyIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M7 7h8.5a3.5 3.5 0 0 1 0 7H9a3 3 0 0 0 0 6h8" />
      <path d="M12 3v18" />
    </svg>
  );
}

function DropletIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M12 3.5s6 6.4 6 11a6 6 0 0 1-12 0c0-4.6 6-11 6-11Z" />
    </svg>
  );
}

function BitcoinIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <circle cx="12" cy="12" r="9" />
      <path d="M10 7v10M14 7v10M9 8h4.5a2 2 0 0 1 0 4H9M9 12h5a2 2 0 0 1 0 4H9" />
    </svg>
  );
}

function LandmarkIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M4 10h16M6 10v7M10 10v7M14 10v7M18 10v7M3 19h18M12 4l8 4H4l8-4Z" />
    </svg>
  );
}

function BarsIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M5 19V9M12 19V5M19 19v-7" />
      <path d="M3 19h18" />
    </svg>
  );
}

function PieIcon({ className = "h-3.5 w-3.5" }: SnapshotIconProps) {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.9" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
      <path d="M12 3v9h9" />
      <path d="M20.5 15.5A9 9 0 1 1 8.5 3.8" />
      <path d="M14 3.2A9 9 0 0 1 20.8 10H14V3.2Z" />
    </svg>
  );
}

function SectionShell({
  title,
  icon,
  tone,
  href,
  children,
}: {
  title: string;
  icon: ReactNode;
  tone?: "blue" | "violet" | "teal" | "amber" | "indigo" | "rose" | "emerald";
  href: string;
  children: ReactNode;
}) {
  return (
    <Link
      href={href}
      prefetch={false}
      className="group flex h-full min-h-[9.75rem] cursor-pointer flex-col rounded-lg border border-white/10 bg-slate-950/55 p-3.5 transition hover:border-teal-300/45 hover:bg-slate-950/75 hover:shadow-[0_0_28px_-18px_rgba(45,212,191,0.9)] focus-visible:border-teal-300/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-300/25"
      aria-label={`Open ${title}`}
    >
      <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-start gap-2">
        <IconFrame tone={tone}>{icon}</IconFrame>
        <h3 className="min-w-0 text-[10px] font-semibold uppercase leading-4 tracking-[0.08em] text-blue-100 transition-colors group-hover:text-white">{title}</h3>
        <span className="shrink-0 text-sm leading-none text-slate-500 transition group-hover:text-teal-200">-&gt;</span>
      </div>
      <div className="mt-3 min-h-0 flex-1">{children}</div>
    </Link>
  );
}

function indexesToInstruments(items: MacroSnapshotIndex[] | undefined, fallback: SnapshotInstrument[]): SnapshotInstrument[] {
  if (!items || items.length === 0) return fallback;
  return fallback.map((fallbackItem) => {
    const match = items.find((item) => item.label === fallbackItem.label || item.symbol === fallbackItem.symbol);
    if (!match) return fallbackItem;
    return {
      label: fallbackItem.label,
      symbol: fallbackItem.symbol ?? match.symbol,
      value: match.value,
      change_pct: match.change_pct ?? null,
      timeframe_label: normalizeTimeframeLabel(match.timeframe_label) ?? "Daily Change",
      status: match.value == null ? "unavailable" : "ok",
    };
  });
}

function instrumentsOrFallback(items: SnapshotInstrument[] | undefined, fallback: SnapshotInstrument[]): SnapshotInstrument[] {
  if (!items || items.length === 0) return fallback;
  return fallback.map((fallbackItem) => {
    const match = items.find((item) => item.label === fallbackItem.label || item.symbol === fallbackItem.symbol);
    if (!match) return fallbackItem;
    return {
      ...match,
      label: fallbackItem.label,
      symbol: fallbackItem.symbol ?? match.symbol,
      timeframe_label: normalizeTimeframeLabel(match.timeframe_label) ?? fallbackItem.timeframe_label,
      unit_label: fallbackItem.unit_label ?? match.unit_label,
    };
  });
}

function pointsOrFallback(items: MacroSnapshotPoint[] | undefined, fallback: MacroSnapshotPoint[]): MacroSnapshotPoint[] {
  if (!items || items.length === 0) return fallback;
  return fallback.map((fallbackItem) => items.find((item) => item.label === fallbackItem.label) ?? fallbackItem);
}

function InstrumentList({ items, unavailableText = "Unavailable" }: { items: SnapshotInstrument[]; unavailableText?: string }) {
  const visibleItems = items.slice(0, 3);
  return (
    <div className="space-y-3">
      {visibleItems.map((item, index) => {
        const unavailable = isUnavailableInstrument(item);
        const changeValue = item.change_pct ?? item.change;
        const changeText = item.change_pct != null ? formatPercent(item.change_pct) ?? "-" : formatSignedNumber(item.change) ?? "-";

        return (
          <div key={`${item.label}-${item.symbol ?? "na"}`} className={index === 0 ? "min-w-0" : "grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-2"}>
            <div className="min-w-0">
              <div className={`overflow-hidden font-semibold leading-4 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] ${index === 0 ? "text-sm" : "text-xs"} ${unavailable ? "text-slate-400" : "text-slate-200"}`}>{item.label}</div>
              {index === 0 ? (
                <div className="mt-1 flex items-baseline gap-2">
                  <span className={`text-xl font-semibold leading-6 ${unavailable ? "text-slate-500" : "text-white"}`}>
                    {unavailable ? unavailableText : formatValue(item.value, valueDigits(item.value, item.unit_label))}
                  </span>
                  {!unavailable ? <span className={`text-xs font-semibold ${deltaClassName(changeValue)}`}>{changeText}</span> : null}
                </div>
              ) : (
                <div className="truncate text-[10px] leading-4 text-slate-500">{item.symbol ?? item.timeframe_label}</div>
              )}
            </div>
            {index === 0 ? null : (
              <div className="shrink-0 text-right">
                <div className={`text-xs font-semibold leading-4 ${unavailable ? "text-slate-500" : "text-slate-100"}`}>
                  {unavailable ? unavailableText : formatValue(item.value, valueDigits(item.value, item.unit_label))}
                </div>
                {!unavailable ? <div className={`text-[10px] leading-4 ${deltaClassName(changeValue)}`}>{changeText}</div> : null}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function MacroPointList({ items, showChange = false }: { items: MacroSnapshotPoint[]; showChange?: boolean }) {
  const visibleItems = items.slice(0, 3);
  return (
    <div className="space-y-3">
      {visibleItems.map((item, index) => {
        const unavailable = item.value == null;
        const changeValue = item.change_value ?? item.change;
        const changeText = formatMacroChange(item);

        return (
          <div key={`${item.label}-${item.date ?? "na"}`} className={index === 0 ? "min-w-0" : "grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-2"}>
            <div className="min-w-0">
              <div className={`overflow-hidden font-semibold leading-4 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] ${index === 0 ? "text-sm" : "text-xs"} ${unavailable ? "text-slate-400" : "text-slate-200"}`}>{item.label}</div>
              {index === 0 ? (
                <div className="mt-1 flex items-baseline gap-2">
                  <span className={`text-xl font-semibold leading-6 ${unavailable ? "text-slate-500" : "text-white"}`}>{formatMacroMainValue(item)}</span>
                  {showChange && changeText ? <span className={`text-xs font-semibold ${deltaClassName(changeValue)}`}>{changeText}</span> : null}
                </div>
              ) : (
                <div className="truncate text-[10px] leading-4 text-slate-500">{formatMacroMeta(item)}</div>
              )}
            </div>
            {index === 0 ? null : (
              <div className="shrink-0 text-right">
                <div className={`text-xs font-semibold leading-4 ${unavailable ? "text-slate-500" : "text-slate-100"}`}>{formatMacroMainValue(item)}</div>
                {showChange && changeText ? (
                  <div className={`text-[10px] leading-4 ${deltaClassName(changeValue)}`}>{changeText}</div>
                ) : null}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function SectorList({ items }: { items: SectorPerformancePoint[] }) {
  if (items.length === 0) return <div className="text-sm text-slate-500">Unavailable</div>;
  const visibleItems = items.slice(0, 3);
  return (
    <div className="space-y-3">
      {visibleItems.map((item, index) => (
        <div key={item.sector} className={index === 0 ? "min-w-0" : "grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-2"}>
          <div className={`min-w-0 overflow-hidden font-semibold leading-4 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] ${index === 0 ? "text-sm text-slate-200" : "text-xs text-slate-200"}`}>{item.sector}</div>
          {index === 0 ? (
            <div className={`mt-1 text-xl font-semibold leading-6 ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct) ?? "Unavailable"}</div>
          ) : (
            <div className={`shrink-0 text-right text-xs font-semibold leading-4 ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct) ?? "Unavailable"}</div>
          )}
        </div>
      ))}
    </div>
  );
}

export function MarketSnapshot({ snapshot }: Props) {
  const worldIndexes = indexesToInstruments(snapshot.world_indexes, FALLBACK_WORLD_INDEXES);
  const currencies = instrumentsOrFallback(snapshot.currencies, FALLBACK_CURRENCIES);
  const commodities = instrumentsOrFallback(snapshot.commodities, FALLBACK_COMMODITIES);
  const crypto = instrumentsOrFallback(snapshot.crypto, FALLBACK_CRYPTO);
  const economics = pointsOrFallback(snapshot.economics, FALLBACK_MACRO);
  const treasury = pointsOrFallback(snapshot.treasury, FALLBACK_TREASURY);
  const usIndexes = indexesToInstruments(snapshot.indexes, FALLBACK_US_INDEXES);
  const sectorPerformance = snapshot.sector_performance ?? [];
  const asOf = snapshot.as_of ?? (snapshot.status === "unavailable" ? null : snapshot.generated_at);
  const updatedLabel = formatSnapshotUpdatedAt(asOf);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-950/45 p-4 shadow-[0_18px_60px_-42px_rgba(16,185,129,0.55)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-100">Market Snapshot</h2>
          {snapshot.status === "ok" || snapshot.status === "partial" ? (
            <span className="inline-flex items-center gap-1.5 text-xs font-semibold text-emerald-300">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-300" />
              Live
            </span>
          ) : (
            <span className="text-xs font-semibold text-slate-500">Unavailable</span>
          )}
        </div>
        {updatedLabel ? <p className="text-xs text-slate-500 sm:text-right">{updatedLabel}</p> : null}
      </div>

      <div className="mt-4 grid auto-rows-fr gap-3 sm:grid-cols-2 lg:grid-cols-4 2xl:grid-cols-8">
        <SectionShell title="World Indexes" icon={<GlobeIcon />} tone="blue" href="/insights/world-indexes">
          <InstrumentList items={worldIndexes} />
        </SectionShell>

        <SectionShell title="Currencies" icon={<CurrencyIcon />} tone="violet" href="/insights/currencies">
          <InstrumentList items={currencies} unavailableText="-" />
        </SectionShell>

        <SectionShell title="Commodities" icon={<DropletIcon />} tone="teal" href="/insights/commodities">
          <InstrumentList items={commodities} />
        </SectionShell>

        <SectionShell title="Crypto" icon={<BitcoinIcon />} tone="amber" href="/insights/crypto">
          <InstrumentList items={crypto} unavailableText="-" />
        </SectionShell>

        <SectionShell title="US Macro" icon={<LandmarkIcon />} tone="indigo" href="/insights/us-macro">
          <MacroPointList items={economics} showChange />
        </SectionShell>

        <SectionShell title="Treasury" icon={<LandmarkIcon />} tone="rose" href="/insights/us-treasury">
          <MacroPointList items={treasury} showChange />
        </SectionShell>

        <SectionShell title="US Indexes" icon={<BarsIcon />} tone="emerald" href="/insights/us-indexes">
          <InstrumentList items={usIndexes} />
        </SectionShell>

        <SectionShell title="US Sectors" icon={<PieIcon />} tone="blue" href="/insights/us-sectors">
          <SectorList items={sectorPerformance} />
        </SectionShell>
      </div>
    </section>
  );
}
