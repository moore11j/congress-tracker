import { formatDateShort } from "@/lib/format";
import type { MacroSnapshotIndex, MacroSnapshotPoint, MacroSnapshotResponse, SectorPerformancePoint, SnapshotInstrument } from "@/lib/types";

export type MarketSnapshotCategorySlug =
  | "world-indexes"
  | "currencies"
  | "commodities"
  | "crypto"
  | "us-macro"
  | "us-treasury"
  | "us-indexes"
  | "us-sectors";

type CategoryKind = "instrument" | "macro" | "sector";

export type MarketSnapshotCategory = {
  slug: MarketSnapshotCategorySlug;
  title: string;
  subtitle: string;
  description: string;
  kind: CategoryKind;
};

export type MarketSnapshotDetailRow = {
  id: string;
  name: string;
  symbol?: string | null;
  valueText: string;
  changeText: string;
  changeValue?: number | null;
  dateText: string;
  unitText?: string | null;
  unavailable?: boolean;
};

export const MARKET_SNAPSHOT_CATEGORIES: MarketSnapshotCategory[] = [
  {
    slug: "world-indexes",
    title: "Global ETF Proxies",
    subtitle: "EOD Change",
    description: "Global market proxy moves from cached ETF prices.",
    kind: "instrument",
  },
  {
    slug: "currencies",
    title: "Currencies",
    subtitle: "Launch Disabled",
    description: "Currency data is disabled for launch until a licensed FX provider is added.",
    kind: "instrument",
  },
  {
    slug: "commodities",
    title: "Commodity ETF Proxies",
    subtitle: "EOD Change",
    description: "Commodity proxy moves from cached ETF prices.",
    kind: "instrument",
  },
  {
    slug: "crypto",
    title: "Crypto",
    subtitle: "Launch Disabled",
    description: "Crypto data is disabled for launch until a licensed crypto provider is added.",
    kind: "instrument",
  },
  {
    slug: "us-macro",
    title: "US Macro",
    subtitle: "Macro Data",
    description: "Key economic indicators used to frame market conditions, inflation, growth, and consumer activity.",
    kind: "macro",
  },
  {
    slug: "us-treasury",
    title: "US Treasury",
    subtitle: "Yield and Daily Change",
    description: "Treasury yields across the curve and daily yield changes.",
    kind: "macro",
  },
  {
    slug: "us-indexes",
    title: "US Market Proxies",
    subtitle: "EOD Change",
    description: "Major US market proxy moves from cached ETF prices.",
    kind: "instrument",
  },
  {
    slug: "us-sectors",
    title: "Sector ETF Proxies",
    subtitle: "EOD Change",
    description: "Sector proxy moves from cached ETF prices.",
    kind: "sector",
  },
];

export const FALLBACK_WORLD_INDEXES: SnapshotInstrument[] = [
  { label: "Canada ETF Proxy", symbol: "EWC", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "United Kingdom ETF Proxy", symbol: "EWU", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Japan ETF Proxy", symbol: "EWJ", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Germany ETF Proxy", symbol: "EWG", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "France ETF Proxy", symbol: "EWQ", timeframe_label: "EOD Change", status: "unavailable" },
];

export const FALLBACK_US_INDEXES: SnapshotInstrument[] = [
  { label: "S&P 500 ETF Proxy", symbol: "SPY", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Nasdaq 100 ETF Proxy", symbol: "QQQ", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Dow ETF Proxy", symbol: "DIA", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Russell 2000 ETF Proxy", symbol: "IWM", timeframe_label: "EOD Change", status: "unavailable" },
];

export const FALLBACK_COMMODITIES: SnapshotInstrument[] = [
  { label: "Gold ETF Proxy", symbol: "GLD", timeframe_label: "EOD Change", unit_label: "USD", status: "unavailable" },
  { label: "Silver ETF Proxy", symbol: "SLV", timeframe_label: "EOD Change", unit_label: "USD", status: "unavailable" },
  { label: "Oil ETF Proxy", symbol: "USO", timeframe_label: "EOD Change", unit_label: "USD", status: "unavailable" },
  { label: "Copper ETF Proxy", symbol: "CPER", timeframe_label: "EOD Change", unit_label: "USD", status: "unavailable" },
];

export const FALLBACK_CURRENCIES: SnapshotInstrument[] = [
  { label: "USD/CAD", symbol: "USDCAD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/USD", symbol: "EURUSD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "GBP/USD", symbol: "GBPUSD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "USD/JPY", symbol: "USDJPY", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
  { label: "EUR/CAD", symbol: "EURCAD", timeframe_label: "Daily Change", unit_label: "rate", status: "unavailable" },
];

export const FALLBACK_CRYPTO: SnapshotInstrument[] = [
  { label: "BTC/USD", symbol: "BTCUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "ETH/USD", symbol: "ETHUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "SOL/USD", symbol: "SOLUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "XRP/USD", symbol: "XRPUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "BNB/USD", symbol: "BNBUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
];

export const FALLBACK_MACRO: MacroSnapshotPoint[] = [
  { label: "Fed Overnight Rate", value: null, value_format: "percent", change_format: "bps" },
  { label: "Core CPI", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Unemployment", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Debt/GDP", value: null, value_format: "percent", change_format: "percentage_points" },
  { label: "Retail Sales", value: null, value_format: "currency", change_format: "percent" },
  { label: "GDP Growth", value: null, value_format: "percent", change_format: "percentage_points" },
];

export const FALLBACK_TREASURY: MacroSnapshotPoint[] = [
  { label: "3M Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "2Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "5Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "10Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
  { label: "30Y Treasury", value: null, unit_label: "yield", change_unit: "bps" },
];

const FALLBACK_SECTOR_NAMES = [
  "Basic Materials",
  "Communication Services",
  "Consumer Cyclical",
  "Consumer Defensive",
  "Energy",
  "Financial Services",
  "Healthcare",
  "Industrials",
  "Real Estate",
  "Technology",
  "Utilities",
];

export function marketSnapshotCategory(slug: string): MarketSnapshotCategory | undefined {
  return MARKET_SNAPSHOT_CATEGORIES.find((category) => category.slug === slug);
}

export function formatValue(value: number | string | null | undefined, digits = 2): string {
  if (typeof value === "string") return value || "Unavailable";
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(value);
}

export function valueDigits(value: number | string | null | undefined, unitLabel?: string | null): number {
  if (unitLabel === "rate") return 4;
  if (typeof value === "number" && Math.abs(value) > 0 && Math.abs(value) < 10) return 4;
  return 2;
}

export function formatPercent(value: number | null | undefined): string | null {
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

export function formatSignedNumber(value: number | null | undefined, digits = 2): string | null {
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

export function formatMacroMainValue(item: MacroSnapshotPoint): string {
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

export function formatMacroChange(item: MacroSnapshotPoint): string | null {
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

export function formatMacroMeta(item: MacroSnapshotPoint): string {
  const contextLabel = item.context_label && item.context_label !== "Latest available" ? item.context_label : null;
  const bits = [formatDateShort(item.date ?? null), item.change_label, contextLabel].filter((value): value is string => Boolean(value));
  return bits.length > 0 ? bits.join(" / ") : "-";
}

export function formatSnapshotUpdatedAt(value: string | null | undefined): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return `Updated: ${formatDateShort(value)}`;
}

export function deltaClassName(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "text-slate-500";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-400";
}

function itemKey(item: { label?: string | null; symbol?: string | null; sector?: string | null }): string {
  return (item.symbol ?? item.label ?? item.sector ?? "").trim().toLowerCase();
}

function appendAdditional<T extends { label?: string | null; symbol?: string | null; sector?: string | null }>(ordered: T[], source: T[] | undefined): T[] {
  if (!source?.length) return ordered;
  const seen = new Set(ordered.map(itemKey));
  const additional = source.filter((item) => {
    const key = itemKey(item);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  return [...ordered, ...additional];
}

export function indexesToInstruments(items: MacroSnapshotIndex[] | undefined, fallback: SnapshotInstrument[], includeAdditional = false): SnapshotInstrument[] {
  if (!items || items.length === 0) return fallback;
  const ordered = fallback.map((fallbackItem) => {
    const match = items.find((item) => item.label === fallbackItem.label || item.symbol === fallbackItem.symbol);
    if (!match) return fallbackItem;
    return {
      label: match.label,
      symbol: match.symbol,
      value: match.value,
      change_pct: match.change_pct ?? null,
      timeframe_label: normalizeTimeframeLabel(match.timeframe_label) ?? "Daily Change",
      status: match.value == null ? "unavailable" : "ok",
    };
  });
  const normalizedAdditional = items.map((item) => ({
    label: item.label,
    symbol: item.symbol,
    value: item.value,
    change_pct: item.change_pct ?? null,
    timeframe_label: normalizeTimeframeLabel(item.timeframe_label) ?? "Daily Change",
    status: item.value == null ? "unavailable" : "ok",
  }));
  return includeAdditional ? appendAdditional(ordered, normalizedAdditional) : ordered;
}

export function instrumentsOrFallback(items: SnapshotInstrument[] | undefined, fallback: SnapshotInstrument[], includeAdditional = false): SnapshotInstrument[] {
  if (!items || items.length === 0) return fallback;
  const ordered = fallback.map((fallbackItem) => items.find((item) => item.label === fallbackItem.label || item.symbol === fallbackItem.symbol) ?? fallbackItem);
  return includeAdditional ? appendAdditional(ordered, items) : ordered;
}

export function pointsOrFallback(items: MacroSnapshotPoint[] | undefined, fallback: MacroSnapshotPoint[], includeAdditional = false): MacroSnapshotPoint[] {
  if (!items || items.length === 0) return fallback;
  const ordered = fallback.map((fallbackItem) => items.find((item) => item.label === fallbackItem.label) ?? fallbackItem);
  return includeAdditional ? appendAdditional(ordered, items) : ordered;
}

export function categoryInstruments(snapshot: MacroSnapshotResponse, slug: MarketSnapshotCategorySlug, includeAdditional = false): SnapshotInstrument[] {
  switch (slug) {
    case "world-indexes":
      return indexesToInstruments(snapshot.world_indexes, FALLBACK_WORLD_INDEXES, includeAdditional);
    case "currencies":
      return instrumentsOrFallback(snapshot.currencies, FALLBACK_CURRENCIES, includeAdditional);
    case "commodities":
      return instrumentsOrFallback(snapshot.commodities, FALLBACK_COMMODITIES, includeAdditional);
    case "crypto":
      return instrumentsOrFallback(snapshot.crypto, FALLBACK_CRYPTO, includeAdditional);
    case "us-indexes":
      return indexesToInstruments(snapshot.indexes, FALLBACK_US_INDEXES, includeAdditional);
    default:
      return [];
  }
}

export function categoryMacroPoints(snapshot: MacroSnapshotResponse, slug: MarketSnapshotCategorySlug, includeAdditional = false): MacroSnapshotPoint[] {
  switch (slug) {
    case "us-macro":
      return pointsOrFallback(snapshot.economics, FALLBACK_MACRO, includeAdditional);
    case "us-treasury":
      return pointsOrFallback(snapshot.treasury, FALLBACK_TREASURY, includeAdditional);
    default:
      return [];
  }
}

export function categorySectors(snapshot: MacroSnapshotResponse): SectorPerformancePoint[] {
  return snapshot.sector_performance ?? [];
}

export function snapshotAsOf(snapshot: MacroSnapshotResponse): string | null {
  return snapshot.as_of ?? (snapshot.status === "unavailable" ? null : snapshot.generated_at);
}

export function marketSnapshotDetailRows(snapshot: MacroSnapshotResponse, slug: MarketSnapshotCategorySlug): MarketSnapshotDetailRow[] {
  const asOf = snapshotAsOf(snapshot);
  if (slug === "us-sectors") {
    const sectors = categorySectors(snapshot);
    if (sectors.length === 0) {
      return FALLBACK_SECTOR_NAMES.map((sector) => ({
        id: sector,
        name: sector,
        valueText: "-",
        changeText: "Unavailable",
        changeValue: null,
        dateText: formatDateShort(asOf) ?? "-",
        unavailable: true,
      }));
    }

    const ordered = appendAdditional(
      FALLBACK_SECTOR_NAMES.map((sector) => sectors.find((item) => item.sector === sector) ?? { sector, change_pct: Number.NaN }),
      sectors,
    );

    return ordered.map((item) => ({
      id: item.sector,
      name: item.sector,
      valueText: "-",
      changeText: formatPercent(item.change_pct) ?? "Unavailable",
      changeValue: item.change_pct,
      dateText: formatDateShort(asOf) ?? "-",
      unavailable: !Number.isFinite(item.change_pct),
    }));
  }

  const instruments = categoryInstruments(snapshot, slug, true);
  if (instruments.length > 0) {
    return instruments.map((item) => {
      const unavailable = item.status === "unavailable" || item.value == null;
      const changeValue = item.change_pct ?? item.change;
      return {
        id: `${item.label}-${item.symbol ?? "na"}`,
        name: item.label,
        symbol: item.symbol,
        valueText: unavailable ? "Unavailable" : formatValue(item.value, valueDigits(item.value, item.unit_label)),
        changeText: item.change_pct != null ? formatPercent(item.change_pct) ?? "Unavailable" : formatSignedNumber(item.change) ?? "Unavailable",
        changeValue,
        dateText: formatDateShort(asOf) ?? "-",
        unitText: item.unit_label,
        unavailable,
      };
    });
  }

  return categoryMacroPoints(snapshot, slug, true).map((item) => {
    const changeValue = item.change_value ?? item.change;
    return {
      id: `${item.label}-${item.date ?? "na"}`,
      name: item.label,
      valueText: formatMacroMainValue(item),
      changeText: formatMacroChange(item) ?? "Unavailable",
      changeValue,
      dateText: formatDateShort(item.date ?? asOf) ?? "-",
      unitText: item.unit_label ?? item.change_label ?? null,
      unavailable: item.value == null,
    };
  });
}
