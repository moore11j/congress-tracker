import type { ReactNode } from "react";

import { formatDateShort } from "@/lib/format";
import { cardClassName } from "@/lib/styles";
import type { MacroSnapshotIndex, MacroSnapshotPoint, MacroSnapshotResponse, SectorPerformancePoint, SnapshotInstrument } from "@/lib/types";

type Props = {
  snapshot: MacroSnapshotResponse;
};

const FALLBACK_WORLD_INDEXES: SnapshotInstrument[] = [
  { label: "Canada TSX", symbol: "^GSPTSE", timeframe_label: "1D change", status: "unavailable" },
  { label: "FTSE 100", symbol: "^FTSE", timeframe_label: "1D change", status: "unavailable" },
  { label: "DAX", symbol: "^GDAXI", timeframe_label: "1D change", status: "unavailable" },
  { label: "Nikkei 225", symbol: "^N225", timeframe_label: "1D change", status: "unavailable" },
  { label: "Hang Seng", symbol: "^HSI", timeframe_label: "1D change", status: "unavailable" },
];

const FALLBACK_US_INDEXES: SnapshotInstrument[] = [
  { label: "S&P 500", symbol: "^GSPC", timeframe_label: "1D change", status: "unavailable" },
  { label: "Nasdaq", symbol: "^IXIC", timeframe_label: "1D change", status: "unavailable" },
  { label: "Dow", symbol: "^DJI", timeframe_label: "1D change", status: "unavailable" },
  { label: "Russell 2000", symbol: "^RUT", timeframe_label: "1D change", status: "unavailable" },
  { label: "VIX", symbol: "^VIX", timeframe_label: "1D change", status: "unavailable" },
];

const FALLBACK_COMMODITIES: SnapshotInstrument[] = [
  { label: "Gold", symbol: "GCUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Silver", symbol: "SIUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Copper", symbol: "HGUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Brent Crude", symbol: "BZUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Wheat", symbol: "ZWUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
];

const FALLBACK_CURRENCIES: SnapshotInstrument[] = [
  { label: "USD/CAD", symbol: "USDCAD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/USD", symbol: "EURUSD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "GBP/USD", symbol: "GBPUSD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "USD/JPY", symbol: "USDJPY", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/CAD", symbol: "EURCAD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
];

const FALLBACK_CRYPTO: SnapshotInstrument[] = [
  { label: "BTC/USD", symbol: "BTCUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "ETH/USD", symbol: "ETHUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "SOL/USD", symbol: "SOLUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "XRP/USD", symbol: "XRPUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "BNB/USD", symbol: "BNBUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
];

const FALLBACK_MACRO: MacroSnapshotPoint[] = [
  { label: "Fed Overnight Rate", value: null, value_format: "percent", change_format: "bps" },
  { label: "Core CPI", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Unemployment", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Debt/GDP", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Retail Sales", value: null, value_format: "currency", change_format: "percent" },
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
  const bits = [formatDateShort(item.date ?? null), item.change_label].filter((value): value is string => Boolean(value));
  return bits.length > 0 ? bits.join(" • ") : "—";
}

function deltaClassName(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "text-slate-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-400";
}

function SectionShell({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: ReactNode;
}) {
  return (
    <section className="flex h-full min-h-[18rem] flex-col rounded-2xl border border-white/10 bg-slate-950/55 p-4">
      <div className="shrink-0">
        <h3 className="text-sm font-semibold text-white">{title}</h3>
        <p className="mt-1 text-[11px] font-medium uppercase tracking-[0.12em] text-slate-500">{subtitle}</p>
      </div>
      <div className="mt-3 min-h-0 flex-1">{children}</div>
    </section>
  );
}

function indexesToInstruments(items: MacroSnapshotIndex[] | undefined, fallback: SnapshotInstrument[]): SnapshotInstrument[] {
  if (!items || items.length === 0) return fallback;
  return fallback.map((fallbackItem) => {
    const match = items.find((item) => item.label === fallbackItem.label || item.symbol === fallbackItem.symbol);
    if (!match) return fallbackItem;
    return {
      label: match.label,
      symbol: match.symbol,
      value: match.value,
      change_pct: match.change_pct ?? null,
      timeframe_label: match.timeframe_label ?? "1D change",
      status: match.value == null ? "unavailable" : "ok",
    };
  });
}

function instrumentsOrFallback(items: SnapshotInstrument[] | undefined, fallback: SnapshotInstrument[]): SnapshotInstrument[] {
  if (!items || items.length === 0) return fallback;
  return fallback.map((fallbackItem) => items.find((item) => item.label === fallbackItem.label) ?? fallbackItem);
}

function pointsOrFallback(items: MacroSnapshotPoint[] | undefined, fallback: MacroSnapshotPoint[]): MacroSnapshotPoint[] {
  if (!items || items.length === 0) return fallback;
  return fallback.map((fallbackItem) => items.find((item) => item.label === fallbackItem.label) ?? fallbackItem);
}

function InstrumentList({ items }: { items: SnapshotInstrument[] }) {
  return (
    <div className="grid h-full gap-1.5" style={{ gridTemplateRows: `repeat(${items.length}, minmax(0, 1fr))` }}>
      {items.map((item) => {
        const unavailable = item.status === "unavailable" || item.value == null;
        const changeValue = item.change_pct ?? item.change;
        const changeText = item.change_pct != null ? formatPercent(item.change_pct) : formatSignedNumber(item.change);

        return (
          <div key={`${item.label}-${item.symbol ?? "na"}`} className="grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
            <div className="min-w-0">
              <div className={`truncate text-sm font-semibold leading-5 ${unavailable ? "text-slate-400" : "text-slate-100"}`}>{item.label}</div>
              <div className="truncate text-[11px] text-slate-500">{item.symbol ?? "Latest available"}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className={`text-sm font-semibold leading-5 ${unavailable ? "text-slate-500" : "text-slate-200"}`}>
                {unavailable ? "Unavailable" : formatValue(item.value, valueDigits(item.value, item.unit_label))}
              </div>
              {changeText ? <div className={`text-[11px] leading-4 ${deltaClassName(changeValue)}`}>{changeText}</div> : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function MacroPointList({ items, showChange = false }: { items: MacroSnapshotPoint[]; showChange?: boolean }) {
  return (
    <div className="grid h-full gap-1.5" style={{ gridTemplateRows: `repeat(${items.length}, minmax(0, 1fr))` }}>
      {items.map((item) => {
        const unavailable = item.value == null;
        const changeValue = item.change_value ?? item.change;
        const changeText = formatMacroChange(item);

        return (
          <div key={`${item.label}-${item.date ?? "na"}`} className="grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
            <div className="min-w-0">
              <div className={`truncate text-sm font-semibold leading-5 ${unavailable ? "text-slate-400" : "text-slate-100"}`}>{item.label}</div>
              <div className="truncate text-[11px] text-slate-500">{formatMacroMeta(item)}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className={`text-sm font-semibold leading-5 ${unavailable ? "text-slate-500" : "text-slate-200"}`}>{formatMacroMainValue(item)}</div>
              {showChange && changeText ? (
                <div className={`text-[11px] leading-4 ${deltaClassName(changeValue)}`}>{changeText}</div>
              ) : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function SectorList({ items }: { items: SectorPerformancePoint[] }) {
  if (items.length === 0) return <div className="text-sm text-slate-500">Unavailable</div>;
  const visibleItems = items.slice(0, 6);
  return (
    <div className="grid h-full gap-1.5" style={{ gridTemplateRows: `repeat(${visibleItems.length}, minmax(0, 1fr))` }}>
      {visibleItems.map((item) => (
        <div key={item.sector} className="grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
          <div className="min-w-0 truncate text-sm font-semibold leading-5 text-slate-100">{item.sector}</div>
          <div className={`shrink-0 text-right text-sm font-semibold ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct) ?? "Unavailable"}</div>
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

  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Market Snapshot</p>
          <h2 className="mt-2 text-2xl font-semibold text-white">Market Snapshot</h2>
          <p className="mt-2 text-sm text-slate-400">A compact macro read on global markets, US rates, economics, commodities, currencies, crypto, and sector breadth.</p>
        </div>
        <div className="text-right text-xs text-slate-500">
          <div>Status: {snapshot.status}</div>
          <div>{formatDateShort(snapshot.generated_at)}</div>
        </div>
      </div>

      <div className="mt-6 grid auto-rows-fr gap-4 md:grid-cols-2 lg:grid-cols-4">
        <SectionShell title="World Indexes" subtitle="1D change">
          <InstrumentList items={worldIndexes} />
        </SectionShell>

        <SectionShell title="Currencies" subtitle="1D change">
          <InstrumentList items={currencies} />
        </SectionShell>

        <SectionShell title="Commodities" subtitle="1D change">
          <InstrumentList items={commodities} />
        </SectionShell>

        <SectionShell title="Crypto" subtitle="1D change">
          <InstrumentList items={crypto} />
        </SectionShell>

        <SectionShell title="US Macro" subtitle="Latest available">
          <MacroPointList items={economics} showChange />
        </SectionShell>

        <SectionShell title="US Treasury" subtitle="Yield and daily change">
          <MacroPointList items={treasury} showChange />
        </SectionShell>

        <SectionShell title="US Indexes" subtitle="1D change">
          <InstrumentList items={usIndexes} />
        </SectionShell>

        <SectionShell title="US Sectors" subtitle="1D average change">
          <SectorList items={sectorPerformance} />
        </SectionShell>
      </div>
    </section>
  );
}
