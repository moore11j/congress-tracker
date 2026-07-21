import { formatDateShort } from "@/lib/format";
import type {
  InsightsOverviewResponse,
  InsightsQuoteItem,
  MacroSnapshotIndex,
  MacroSnapshotPoint,
  MacroSnapshotResponse,
  SectorPerformancePoint,
  SnapshotInstrument,
} from "@/lib/types";

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
    title: "Global Markets",
    subtitle: "Daily Change",
    description: "Daily moves across major global markets.",
    kind: "instrument",
  },
  {
    slug: "commodities",
    title: "Commodities",
    subtitle: "Daily Change",
    description: "Daily moves across gold, silver, oil, and copper.",
    kind: "instrument",
  },
  {
    slug: "currencies",
    title: "Currencies",
    subtitle: "Daily Change",
    description: "Daily moves across major currency pairs.",
    kind: "instrument",
  },
  {
    slug: "crypto",
    title: "Crypto",
    subtitle: "Daily Change",
    description: "Daily moves across major crypto pairs.",
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
    title: "US Markets",
    subtitle: "EOD Change",
    description: "Daily moves across major US markets.",
    kind: "instrument",
  },
  {
    slug: "us-sectors",
    title: "Sectors",
    subtitle: "EOD Change",
    description: "Daily sector breadth across the US market.",
    kind: "sector",
  },
];

export const FALLBACK_WORLD_INDEXES: SnapshotInstrument[] = [
  { label: "MSCI ACWI", symbol: "ACWI", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "China \u2014 MCHI", symbol: "MCHI", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "Germany \u2014 EWG", symbol: "EWG", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "Japan \u2014 IJP", symbol: "IJP", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "UK \u2014 ISF", symbol: "ISF", timeframe_label: "Daily Change", status: "unavailable" },
  { label: "Canada \u2014 VFV", symbol: "VFV", timeframe_label: "Daily Change", status: "unavailable" },
];

export const FALLBACK_US_INDEXES: SnapshotInstrument[] = [
  { label: "S&P 500", symbol: "SPY", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "NASDAQ", symbol: "QQQ", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Dow", symbol: "DIA", timeframe_label: "EOD Change", status: "unavailable" },
  { label: "Russell 2000", symbol: "IWM", timeframe_label: "EOD Change", status: "unavailable" },
];

export const FALLBACK_COMMODITIES: SnapshotInstrument[] = [
  { label: "Gold \u2014 GCUSD", symbol: "GCUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "Silver \u2014 SILUSD", symbol: "SILUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
  { label: "Copper \u2014 HGUSD", symbol: "HGUSD", timeframe_label: "Daily Change", unit_label: "USD", status: "unavailable" },
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
  { label: "Fed Overnight Rate", value: null, value_format: "percent", change_format: "bps", unit_label: "annualized %" },
  { label: "Core CPI", value: null, value_format: "percent", change_format: "percentage_points", unit_label: "YoY" },
  { label: "Unemployment", value: null, value_format: "percent", change_format: "percentage_points", unit_label: "labor force %" },
  { label: "Debt/GDP", value: null, value_format: "percent", change_format: "percentage_points", unit_label: "% of GDP" },
  { label: "Retail Sales", value: null, value_format: "currency", change_format: "percent", unit_label: "USD" },
  { label: "GDP Growth", value: null, value_format: "percent", change_format: "percentage_points", unit_label: "QoQ annualized" },
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

const SECTOR_NAME_ALIASES: Record<string, string> = {
  "basic materials": "Basic Materials",
  materials: "Basic Materials",
  "communication services": "Communication Services",
  communications: "Communication Services",
  "consumer cyclical": "Consumer Cyclical",
  "consumer discretionary": "Consumer Cyclical",
  "consumer defensive": "Consumer Defensive",
  "consumer staples": "Consumer Defensive",
  energy: "Energy",
  "financial services": "Financial Services",
  financials: "Financial Services",
  "health care": "Healthcare",
  healthcare: "Healthcare",
  industrials: "Industrials",
  "real estate": "Real Estate",
  technology: "Technology",
  utilities: "Utilities",
};

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
  const bits = [
    formatDateShort(item.date ?? null),
    publicSnapshotMetaLabel(item.change_label),
    publicSnapshotMetaLabel(item.context_label),
  ].filter((value): value is string => Boolean(value));
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

function isUnavailableInstrument(item: SnapshotInstrument): boolean {
  return item.status === "unavailable" || item.status === "disabled" || item.value == null;
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
      label: fallbackItem.label,
      symbol: fallbackItem.symbol ?? match.symbol,
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
  const ordered = fallback.map((fallbackItem) => {
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
      return indexesToInstruments(snapshot.world_indexes, FALLBACK_WORLD_INDEXES, false);
    case "currencies":
      return instrumentsOrFallback(snapshot.currencies, FALLBACK_CURRENCIES, includeAdditional);
    case "commodities":
      return instrumentsOrFallback(snapshot.commodities, FALLBACK_COMMODITIES, false);
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

function canonicalSectorName(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  const withoutTicker = trimmed.endsWith(")") && trimmed.includes("(") ? trimmed.slice(0, trimmed.lastIndexOf("(")).trim() : trimmed;
  const key = withoutTicker.replace("&", "and").toLowerCase().split(/\s+/).filter(Boolean).join(" ");
  return SECTOR_NAME_ALIASES[key] ?? withoutTicker;
}

function sectorKey(value: string | null | undefined): string {
  return (canonicalSectorName(value) ?? "").toLowerCase();
}

function orderedSectorRows(sectors: SectorPerformancePoint[]): SectorPerformancePoint[] {
  const consumed = new Set<string>();
  const ordered = FALLBACK_SECTOR_NAMES.map((sector) => {
    const key = sectorKey(sector);
    const match = sectors.find((item) => sectorKey(item.sector) === key);
    consumed.add(key);
    if (!match) return { sector, change_pct: Number.NaN };
    return { ...match, sector };
  });
  const additional = sectors.filter((item) => {
    const key = sectorKey(item.sector);
    if (!key || consumed.has(key)) return false;
    consumed.add(key);
    return true;
  });
  return [...ordered, ...additional];
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

    const ordered = orderedSectorRows(sectors);

    return ordered.map((item) => ({
      id: item.sector,
      name: item.sector,
      valueText: "-",
      changeText: formatPercent(item.change_pct) ?? "Unavailable",
      changeValue: item.change_pct,
      dateText: formatDateShort(item.date ?? asOf) ?? "-",
      unitText: item.unit_label ?? "%",
      unavailable: !Number.isFinite(item.change_pct),
    }));
  }

  const instruments = categoryInstruments(snapshot, slug, true);
  if (instruments.length > 0) {
    return instruments.map((item) => {
      const unavailable = isUnavailableInstrument(item);
      const changeValue = item.change_pct ?? item.change;
      return {
        id: `${item.label}-${item.symbol ?? "na"}`,
        name: item.label,
        symbol: item.symbol,
        valueText: unavailable ? "-" : formatValue(item.value, valueDigits(item.value, item.unit_label)),
        changeText: item.change_pct != null ? formatPercent(item.change_pct) ?? "-" : formatSignedNumber(item.change) ?? "-",
        changeValue,
        dateText: formatDateShort(item.date ?? asOf) ?? "-",
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
      unitText: item.unit_label ?? publicSnapshotMetaLabel(item.change_label),
      unavailable: item.value == null,
    };
  });
}

function quoteDisplayLabel(item: InsightsQuoteItem): string {
  if (item.group === "global_markets" || item.group === "commodities") {
    return `${item.label} \u2014 ${item.display_symbol || item.symbol}`;
  }
  return item.label;
}

function quoteToInstrument(item: InsightsQuoteItem): SnapshotInstrument {
  return {
    label: quoteDisplayLabel(item),
    symbol: item.display_symbol || item.symbol,
    value: item.price,
    change: item.change,
    change_pct: item.change_percent,
    timeframe_label: "Daily Change",
    unit_label: item.group === "currencies" ? "rate" : item.group === "crypto" || item.group === "commodities" ? "USD" : null,
    status: item.status,
    date: item.as_of,
  };
}

function hasQuoteValue(items: InsightsQuoteItem[]): boolean {
  return items.some((item) => item.status === "ok" && item.price != null);
}

export function applyInsightsOverview(snapshot: MacroSnapshotResponse, overview: InsightsOverviewResponse): MacroSnapshotResponse {
  const quoteItems = [
    ...overview.global_markets,
    ...overview.commodities,
    ...overview.currencies,
    ...overview.crypto,
  ];
  const quoteStatus = quoteItems.some((item) => item.status === "unavailable") ? "partial" : "ok";
  const hasValues = hasQuoteValue(quoteItems);
  const status = ["loading", "warming"].includes(snapshot.status)
    ? hasValues ? quoteStatus : "unavailable"
    : hasValues && snapshot.status === "unavailable" ? quoteStatus : snapshot.status;

  return {
    ...snapshot,
    world_indexes: overview.global_markets.map((item) => ({
      label: quoteDisplayLabel(item),
      symbol: item.display_symbol || item.symbol,
      value: item.price,
      change_pct: item.change_percent,
      timeframe_label: "Daily Change",
      date: item.as_of,
      status: item.status,
    })),
    commodities: overview.commodities.map(quoteToInstrument),
    currencies: overview.currencies.map(quoteToInstrument),
    crypto: overview.crypto.map(quoteToInstrument),
    status,
    updated_at: overview.updated_at ?? snapshot.updated_at ?? null,
    as_of: overview.updated_at ?? snapshot.as_of ?? null,
  };
}
