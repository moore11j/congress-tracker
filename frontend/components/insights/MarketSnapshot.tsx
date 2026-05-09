import type { ReactNode } from "react";

import { formatDateShort } from "@/lib/format";
import { cardClassName } from "@/lib/styles";
import type { MacroSnapshotPoint, MacroSnapshotResponse, SectorPerformancePoint, SnapshotInstrument } from "@/lib/types";

type Props = {
  snapshot: MacroSnapshotResponse;
};

const FALLBACK_COMMODITIES: SnapshotInstrument[] = [
  { label: "Gold", symbol: "GCUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Silver", symbol: "SIUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Crude Oil WTI", symbol: "CLUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Natural Gas", symbol: "NGUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Copper", symbol: "HGUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "Brent Crude", symbol: "BZUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
];

const FALLBACK_CURRENCIES: SnapshotInstrument[] = [
  { label: "USD/CAD", symbol: "USDCAD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/USD", symbol: "EURUSD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "GBP/USD", symbol: "GBPUSD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "USD/JPY", symbol: "USDJPY", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "AUD/USD", symbol: "AUDUSD", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
  { label: "USD/CHF", symbol: "USDCHF", timeframe_label: "1D change", unit_label: "rate", status: "unavailable" },
];

const FALLBACK_CRYPTO: SnapshotInstrument[] = [
  { label: "BTC/USD", symbol: "BTCUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "ETH/USD", symbol: "ETHUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "SOL/USD", symbol: "SOLUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "XRP/USD", symbol: "XRPUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
  { label: "BNB/USD", symbol: "BNBUSD", timeframe_label: "1D change", unit_label: "USD", status: "unavailable" },
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

function formatPercent(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatSignedNumber(value: number | null | undefined, digits = 2): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
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
  subtitle?: string;
  children: ReactNode;
}) {
  return (
    <section className="mb-4 break-inside-avoid rounded-2xl border border-white/10 bg-slate-950/55 p-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-white">{title}</h3>
          {subtitle ? <p className="mt-1 text-[11px] font-medium uppercase tracking-[0.12em] text-slate-500">{subtitle}</p> : null}
        </div>
      </div>
      <div className="mt-4">{children}</div>
    </section>
  );
}

function UnavailableState() {
  return <div className="text-sm text-slate-500">Unavailable</div>;
}

function joinContext(...parts: (string | null | undefined)[]): string {
  return parts.filter(Boolean).join(" - ");
}

function instrumentsOrFallback(items: SnapshotInstrument[] | undefined, fallback: SnapshotInstrument[]): SnapshotInstrument[] {
  return items && items.length > 0 ? items : fallback;
}

function MacroPointList({
  items,
  defaultSuffix = "",
  showChange = false,
}: {
  items: MacroSnapshotPoint[];
  defaultSuffix?: string;
  showChange?: boolean;
}) {
  if (items.length === 0) return <UnavailableState />;
  return (
    <div className="space-y-3">
      {items.map((item) => {
        const suffix = item.unit_label && item.unit_label !== "yield" ? item.unit_label : defaultSuffix;
        const context = joinContext(item.context_label ?? (item.unit_label === "yield" ? "Yield" : undefined), formatDateShort(item.date ?? null));
        const changeLabel = item.timeframe_label ?? "1D change";
        const changeText =
          item.change_unit === "bps"
            ? `${formatSignedNumber(item.change, 0)} bps`
            : item.change != null
              ? formatSignedNumber(item.change)
              : "Change unavailable";

        return (
          <div key={`${item.label}-${item.date ?? "na"}`} className="flex items-baseline justify-between gap-3">
            <div className="min-w-0">
              <div className="truncate text-sm font-medium text-slate-100">{item.label}</div>
              <div className="text-xs text-slate-500">{context || "Latest available"}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className="text-sm font-semibold text-slate-200">
                {formatValue(item.value)}
                {suffix}
              </div>
              {showChange ? (
                <div className={`text-[11px] ${deltaClassName(item.change)}`}>{item.change == null ? `${changeLabel} unavailable` : `${changeText} - ${changeLabel}`}</div>
              ) : null}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function InstrumentList({ items }: { items: SnapshotInstrument[] }) {
  if (items.length === 0) return <UnavailableState />;
  return (
    <div className="space-y-3">
      {items.map((item) => {
        const unavailable = item.status === "unavailable" || item.value == null;
        const changeValue = item.change_pct ?? item.change;
        const changeText = item.change_pct != null ? formatPercent(item.change_pct) : formatSignedNumber(item.change);
        const label = item.timeframe_label || "1D change";

        return (
          <div key={`${item.label}-${item.symbol ?? "na"}`} className="flex items-baseline justify-between gap-3">
            <div className="min-w-0">
              <div className={`truncate text-sm font-medium ${unavailable ? "text-slate-400" : "text-slate-100"}`}>{item.label}</div>
              <div className="truncate text-xs text-slate-500">{item.symbol ?? "Provider quote"}</div>
            </div>
            <div className="shrink-0 text-right">
              <div className={`text-sm font-semibold ${unavailable ? "text-slate-500" : "text-slate-200"}`}>
                {unavailable ? "Unavailable" : formatValue(item.value, valueDigits(item.value, item.unit_label))}
              </div>
              <div className={`text-[11px] ${deltaClassName(changeValue)}`}>
                {unavailable || changeText === "Unavailable" ? `${label} unavailable` : `${changeText} - ${label}`}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function SectorList({ items }: { items: SectorPerformancePoint[] }) {
  if (items.length === 0) return <UnavailableState />;
  return (
    <div className="space-y-3">
      {items.slice(0, 6).map((item) => (
        <div key={item.sector} className="flex items-center justify-between gap-3">
          <div className="min-w-0 truncate text-sm font-medium text-slate-100">{item.sector}</div>
          <div className="shrink-0 text-right">
            <div className={`text-sm font-semibold ${deltaClassName(item.change_pct)}`}>{formatPercent(item.change_pct)}</div>
            <div className="text-[11px] text-slate-500">1D avg change</div>
          </div>
        </div>
      ))}
    </div>
  );
}

export function MarketSnapshot({ snapshot }: Props) {
  const indexes = snapshot.indexes ?? [];
  const sectorPerformance = snapshot.sector_performance ?? [];
  const commodities = instrumentsOrFallback(snapshot.commodities, FALLBACK_COMMODITIES);
  const currencies = instrumentsOrFallback(snapshot.currencies, FALLBACK_CURRENCIES);
  const crypto = instrumentsOrFallback(snapshot.crypto, FALLBACK_CRYPTO);

  return (
    <section className={cardClassName}>
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Market Snapshot</p>
          <h2 className="mt-2 text-2xl font-semibold text-white">Market Snapshot</h2>
          <p className="mt-2 text-sm text-slate-400">A compact macro read on indexes, rates, economics, commodities, currencies, crypto, and sector breadth.</p>
        </div>
        <div className="text-right text-xs text-slate-500">
          <div>Status: {snapshot.status}</div>
          <div>{formatDateShort(snapshot.generated_at)}</div>
        </div>
      </div>

      <div className="mt-6 columns-1 gap-4 md:columns-2 xl:columns-4 2xl:columns-5">
        <SectionShell title="Indexes" subtitle={indexes.some((item) => item.is_proxy) ? "ETF proxy - 1D change" : "Major indexes - 1D change"}>
          {indexes.length === 0 ? (
            <UnavailableState />
          ) : (
            <div className="space-y-3">
              {indexes.map((item) => (
                <div key={item.symbol} className="flex items-baseline justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-medium text-slate-100">{item.label}</div>
                    <div className="truncate text-xs text-slate-500">{item.is_proxy ? `${item.symbol} proxy` : item.symbol}</div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="text-sm font-semibold text-slate-200">{formatValue(item.value)}</div>
                    <div className={`text-[11px] ${deltaClassName(item.change_pct)}`}>
                      {item.change_pct == null ? "1D change unavailable" : `${formatPercent(item.change_pct)} - ${item.timeframe_label ?? "1D change"}`}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </SectionShell>

        <SectionShell title="Treasury" subtitle="Yield - daily change">
          <MacroPointList items={snapshot.treasury} defaultSuffix="%" showChange />
        </SectionShell>

        <SectionShell title="Macro" subtitle="Release context">
          <MacroPointList items={snapshot.economics} />
        </SectionShell>

        <SectionShell title="Commodities" subtitle="Provider quotes - 1D change">
          <InstrumentList items={commodities} />
        </SectionShell>

        <SectionShell title="Currencies" subtitle="Major FX - 1D change">
          <InstrumentList items={currencies} />
        </SectionShell>

        <SectionShell title="Crypto" subtitle="Digital assets - 1D change">
          <InstrumentList items={crypto} />
        </SectionShell>

        <SectionShell title="Sectors" subtitle="Sector performance - 1D average change">
          <SectorList items={sectorPerformance} />
        </SectionShell>
      </div>
    </section>
  );
}
