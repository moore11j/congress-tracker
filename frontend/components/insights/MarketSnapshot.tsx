import type { ReactNode } from "react";
import Link from "next/link";

import { formatDateShort } from "@/lib/format";
import { cardClassName } from "@/lib/styles";
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
  { label: "Brent Crude Oil \u2014 BZUSD", symbol: "BZUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "Copper \u2014 HGUSD", symbol: "HGUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
];

const FALLBACK_CURRENCIES: SnapshotInstrument[] = [
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

function SectionShell({
  title,
  subtitle,
  href,
  children,
}: {
  title: string;
  subtitle: string;
  href: string;
  children: ReactNode;
}) {
  return (
    <Link
      href={href}
      prefetch={false}
      className="group flex h-full min-h-[18rem] cursor-pointer flex-col rounded-2xl border border-white/10 bg-slate-950/55 p-4 transition hover:border-teal-300/45 hover:bg-slate-950/75 hover:shadow-[0_0_28px_-18px_rgba(45,212,191,0.9)] focus-visible:border-teal-300/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-300/25"
      aria-label={`Open ${title}`}
    >
      <div className="shrink-0">
        <div className="flex items-start justify-between gap-3">
          <h3 className="text-sm font-semibold leading-tight text-slate-50 transition-colors group-hover:text-white md:text-base">{title}</h3>
          <span className="shrink-0 text-[10px] font-semibold uppercase tracking-[0.16em] text-teal-300/70 transition-colors group-hover:text-teal-200">
            Open -&gt;
          </span>
        </div>
        <p className="mt-1.5 text-[10px] font-medium leading-4 tracking-[0.18em] text-teal-300/60">{subtitle}</p>
      </div>
      <div className="mt-4 min-h-0 flex-1">{children}</div>
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
  return (
    <div className="grid h-full gap-1.5" style={{ gridTemplateRows: `repeat(${items.length}, minmax(0, 1fr))` }}>
      {items.map((item) => {
        const unavailable = isUnavailableInstrument(item);
        const changeValue = item.change_pct ?? item.change;
        const changeText = item.change_pct != null ? formatPercent(item.change_pct) ?? "-" : formatSignedNumber(item.change) ?? "-";

        return (
          <div key={`${item.label}-${item.symbol ?? "na"}`} className="grid min-h-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
            <div className="min-w-0">
              <div className={`truncate text-xs font-semibold leading-4 ${unavailable ? "text-slate-400" : "text-slate-200"}`}>{item.label}</div>
              <div className="truncate text-[10px] leading-4 text-slate-500">{item.symbol ?? item.timeframe_label}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className={`text-xs font-semibold leading-4 ${unavailable ? "text-slate-500" : "text-slate-100"}`}>
                {unavailable ? unavailableText : formatValue(item.value, valueDigits(item.value, item.unit_label))}
              </div>
              {!unavailable ? <div className={`text-[10px] leading-4 ${deltaClassName(changeValue)}`}>{changeText}</div> : null}
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
              <div className={`truncate text-xs font-semibold leading-4 ${unavailable ? "text-slate-400" : "text-slate-200"}`}>{item.label}</div>
              <div className="truncate text-[10px] leading-4 text-slate-500">{formatMacroMeta(item)}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className={`text-xs font-semibold leading-4 ${unavailable ? "text-slate-500" : "text-slate-100"}`}>{formatMacroMainValue(item)}</div>
              {showChange && changeText ? (
                <div className={`text-[10px] leading-4 ${deltaClassName(changeValue)}`}>{changeText}</div>
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
          <div className="min-w-0 truncate text-xs font-semibold leading-4 text-slate-200">{item.sector}</div>
          <div className={`shrink-0 text-right text-xs font-semibold leading-4 ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct) ?? "Unavailable"}</div>
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
    <section className={cardClassName}>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">INSIGHTS</p>
          <h2 className="mt-2 text-2xl font-semibold text-white">Market Snapshot</h2>
          <p className="mt-2 text-sm text-slate-400">A quick view of global markets, rates, economic signals, and sector momentum.</p>
        </div>
        {updatedLabel ? <p className="text-xs text-slate-500 sm:text-right">{updatedLabel}</p> : null}
      </div>

      <div className="mt-6 grid auto-rows-fr gap-4 md:grid-cols-2 lg:grid-cols-4">
        <SectionShell title="Global Markets" subtitle="Daily Change" href="/insights/world-indexes">
          <InstrumentList items={worldIndexes} />
        </SectionShell>

        <SectionShell title="Commodities" subtitle="Daily Change" href="/insights/commodities">
          <InstrumentList items={commodities} />
        </SectionShell>

        <SectionShell title="Currencies" subtitle="Daily Change" href="/insights/currencies">
          <InstrumentList items={currencies} unavailableText="-" />
        </SectionShell>

        <SectionShell title="Crypto" subtitle="Daily Change" href="/insights/crypto">
          <InstrumentList items={crypto} unavailableText="-" />
        </SectionShell>

        <SectionShell title="US Macro" subtitle="Macro Data" href="/insights/us-macro">
          <MacroPointList items={economics} showChange />
        </SectionShell>

        <SectionShell title="US Treasury" subtitle="Yield and Daily Change" href="/insights/us-treasury">
          <MacroPointList items={treasury} showChange />
        </SectionShell>

        <SectionShell title="US Markets" subtitle="EOD Change" href="/insights/us-indexes">
          <InstrumentList items={usIndexes} />
        </SectionShell>

        <SectionShell title="Sectors" subtitle="EOD Change" href="/insights/us-sectors">
          <SectorList items={sectorPerformance} />
        </SectionShell>
      </div>
    </section>
  );
}
