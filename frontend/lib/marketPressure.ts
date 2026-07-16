import { API_BASE, backendSessionCookieName } from "@/lib/api";

export type MarketPressurePeriod = "1d" | "5d" | "1m" | "3m" | "ytd" | "1y";
export type MarketPressureTimeRange = "1D" | "5D" | "1M" | "3M" | "YTD" | "1Y";
export type MarketPressureUniverse = "sp500" | "nasdaq100" | "etf" | "all_us" | "watchlist";
export type MarketPressureViewMode =
  | "market_pressure"
  | "hidden_accumulation"
  | "fragile_winners"
  | "crowded_trades"
  | "rotation";

export type MarketPressureLayerKey =
  | "priceVolume"
  | "fundamentals"
  | "congress"
  | "insiders"
  | "governmentContracts"
  | "signals"
  | "institutions"
  | "optionsFlow"
  | "macroPositioning";

export type MarketPressureLayerAccess = "available" | "locked" | "unavailable" | "stale";

export type MarketPressureLayer = {
  status: MarketPressureLayerAccess;
  requiredTier?: "premium" | "pro";
  direction?: "bullish" | "bearish" | "neutral" | "conflicted" | null;
  contribution?: number | null;
  asOf?: string | null;
};

export type MarketPressureTile = {
  symbol: string;
  companyName: string | null;
  sector: string;
  exchange: string | null;
  marketCap: number | null;
  priceChangePct: number | null;
  priceStartAt: string | null;
  priceEndAt: string | null;
  confirmationScore: number | null;
  confirmationDirection: "bullish" | "bearish" | "neutral" | "conflicted" | "unavailable";
  confirmationStrength: "weak" | "moderate" | "strong" | null;
  confirmationTrend: "strengthening" | "weakening" | "stable" | null;
  divergence:
    | "hidden_accumulation"
    | "fragile_winner"
    | "aligned_bullish"
    | "aligned_bearish"
    | "conflicted"
    | "none"
    | "unavailable";
  confirmationAsOf: string | null;
  latestEvidenceAt: string | null;
  dataState: "complete" | "partial" | "unavailable" | "stale";
  availableLayerCount: number;
  eligibleLayerCount: number;
  layers: Record<MarketPressureLayerKey, MarketPressureLayer>;
};

export type MarketPressureSector = {
  sector: string;
  summary: {
    symbolCount: number;
    averagePriceChangePct: number | null;
    bullishCount: number;
    bearishCount: number;
    conflictedCount: number;
    divergenceCount: number;
  };
  tiles: MarketPressureTile[];
};

export type MarketPressureUniverseCapability = {
  supported: boolean;
  membershipCount: number | null;
  source: string | null;
  sourceKind?: string | null;
  sourcePage?: string | null;
  sourceRevisionId?: string | null;
  resolvedSourceTitle?: string | null;
  sourceLabel?: string | null;
  parserVersion?: string | null;
  sourceAsOf: string | null;
  refreshedAt: string | null;
  status: "available" | "stale" | "unavailable";
  reason?: string | null;
};

export type MarketPressureMapResult = {
  status: "loading" | "ready" | "no-data" | "error" | "entitlement" | "unsupported" | "auth-required";
  universe: MarketPressureUniverse;
  period: MarketPressurePeriod;
  view: MarketPressureViewMode;
  generatedAt: string | null;
  priceAsOf: string | null;
  confirmationAsOf: string | null;
  confirmationFreshnessWindowDays: number;
  scoringVersion: string | null;
  capabilities: {
    universes: Record<MarketPressureUniverse, boolean>;
    universeDetails?: Record<MarketPressureUniverse, MarketPressureUniverseCapability>;
    views: Record<MarketPressureViewMode, boolean>;
    pressureTrendAvailable: boolean;
  };
  entitlement: {
    tier: "free" | "premium" | "pro" | "admin";
    visibleLayers: MarketPressureLayerKey[];
    lockedLayers: MarketPressureLayerKey[];
  };
  summary: {
    symbolCount: number;
    classifiedCount: number;
    partialCount: number;
    unavailableCount: number;
    bullishCount: number;
    bearishCount: number;
    neutralCount: number;
    conflictedCount: number;
    hiddenAccumulationCount: number;
    fragileWinnerCount: number;
  };
  sectors: MarketPressureSector[];
  tiles: MarketPressureTile[];
  warnings: string[];
  latestSuccessfulDataAt: string | null;
  providerMessage: string | null;
  layerAccess: Record<MarketPressureLayerKey, MarketPressureLayerAccess>;
};

export type MarketPressureCapabilities = MarketPressureMapResult["capabilities"];

export type MarketPressureQuery = {
  timeRange: MarketPressureTimeRange;
  period?: MarketPressurePeriod;
  universe: MarketPressureUniverse;
  viewMode: MarketPressureViewMode;
  authToken?: string | null;
};

export const marketPressureTimeRanges: MarketPressureTimeRange[] = ["1D", "5D", "1M", "3M", "YTD", "1Y"];

export const marketPressureUniverses: Array<{ value: MarketPressureUniverse; label: string }> = [
  { value: "sp500", label: "S&P 500" },
  { value: "nasdaq100", label: "Nasdaq 100" },
  { value: "etf", label: "ETFs" },
  { value: "all_us", label: "All US" },
  { value: "watchlist", label: "Watchlist" },
];

export const marketPressureViewModes: Array<{ value: MarketPressureViewMode; label: string }> = [
  { value: "market_pressure", label: "Market Pressure" },
  { value: "hidden_accumulation", label: "Hidden Accumulation" },
  { value: "fragile_winners", label: "Fragile Winners" },
  { value: "crowded_trades", label: "Crowded Trades" },
  { value: "rotation", label: "Rotation" },
];

export const marketPressureLayerLabels: Record<MarketPressureLayerKey, string> = {
  priceVolume: "Price / Volume",
  fundamentals: "Fundamentals",
  congress: "Congress",
  insiders: "Insiders",
  governmentContracts: "Government Contracts",
  signals: "Premium Signals",
  institutions: "Institutional Activity",
  optionsFlow: "Options Flow",
  macroPositioning: "Macro Positioning",
};

export const defaultMarketPressureCapabilities: MarketPressureMapResult["capabilities"] = {
  universes: {
    sp500: false,
    nasdaq100: false,
    etf: false,
    all_us: false,
    watchlist: true,
  },
  universeDetails: {
    sp500: {
      supported: false,
      membershipCount: 0,
      source: null,
      sourceAsOf: null,
      refreshedAt: null,
      status: "unavailable",
      reason: "membership_not_loaded",
    },
    nasdaq100: {
      supported: false,
      membershipCount: 0,
      source: null,
      sourceAsOf: null,
      refreshedAt: null,
      status: "unavailable",
      reason: "membership_not_loaded",
    },
    etf: {
      supported: false,
      membershipCount: 0,
      source: "security_master",
      sourceKind: "security_asset_class",
      sourceAsOf: null,
      refreshedAt: null,
      status: "unavailable",
      reason: "etf_universe_not_loaded",
    },
    all_us: {
      supported: false,
      membershipCount: 0,
      source: null,
      sourceAsOf: null,
      refreshedAt: null,
      status: "unavailable",
      reason: "complete_us_equity_universe_not_available",
    },
    watchlist: {
      supported: true,
      membershipCount: null,
      source: "user_watchlist",
      sourceAsOf: null,
      refreshedAt: null,
      status: "available",
      reason: null,
    },
  },
  views: {
    market_pressure: true,
    hidden_accumulation: true,
    fragile_winners: true,
    crowded_trades: false,
    rotation: false,
  },
  pressureTrendAvailable: false,
};

const preferredUniverseOrder: MarketPressureUniverse[] = ["sp500", "nasdaq100", "etf", "watchlist"];

export function timeRangeToPeriod(value: MarketPressureTimeRange): MarketPressurePeriod {
  return value.toLowerCase() as MarketPressurePeriod;
}

export function periodToTimeRange(value: string | null | undefined): MarketPressureTimeRange {
  const normalized = (value ?? "").trim().toLowerCase();
  if (normalized === "5d") return "5D";
  if (normalized === "1m") return "1M";
  if (normalized === "3m") return "3M";
  if (normalized === "ytd") return "YTD";
  if (normalized === "1y") return "1Y";
  return "1D";
}

export function normalizeMarketPressureUniverse(value: string | string[] | undefined): MarketPressureUniverse {
  const raw = Array.isArray(value) ? value[0] : value;
  const normalized = (raw ?? "").trim().toLowerCase().replaceAll("-", "_");
  if (normalized === "nasdaq100" || normalized === "all_us" || normalized === "watchlist") return normalized;
  if (normalized === "etf" || normalized === "etfs" || normalized === "etf_fund") return "etf";
  return "sp500";
}

export function selectMarketPressureUniverse(
  capabilities: MarketPressureCapabilities,
  requested?: MarketPressureUniverse | null,
): MarketPressureUniverse {
  if (requested && capabilities.universes[requested]) return requested;
  for (const universe of preferredUniverseOrder) {
    if (capabilities.universes[universe]) return universe;
  }
  return "watchlist";
}

export function marketPressureUnavailableUniverseWarning(requested: MarketPressureUniverse, selected: MarketPressureUniverse) {
  return requested === selected ? null : `requested_universe_unavailable:${requested}`;
}

export function normalizeMarketPressureView(value: string | string[] | undefined): MarketPressureViewMode {
  const raw = Array.isArray(value) ? value[0] : value;
  const normalized = (raw ?? "").trim().toLowerCase().replaceAll("-", "_");
  if (
    normalized === "hidden_accumulation" ||
    normalized === "fragile_winners" ||
    normalized === "crowded_trades" ||
    normalized === "rotation"
  ) {
    return normalized;
  }
  return "market_pressure";
}

export function normalizeMarketPressurePeriod(value: string | string[] | undefined): MarketPressurePeriod {
  const raw = Array.isArray(value) ? value[0] : value;
  return timeRangeToPeriod(periodToTimeRange(raw));
}

export function marketPressureQueryString(query: MarketPressureQuery) {
  const params = new URLSearchParams();
  params.set("universe", query.universe);
  params.set("period", query.period ?? timeRangeToPeriod(query.timeRange));
  params.set("view", query.viewMode);
  return params.toString();
}

export function marketPressureLayerAccessFromResponse(
  response: Pick<MarketPressureMapResult, "entitlement">,
): Record<MarketPressureLayerKey, MarketPressureLayerAccess> {
  const access = Object.fromEntries(
    (Object.keys(marketPressureLayerLabels) as MarketPressureLayerKey[]).map((key) => [key, "unavailable"]),
  ) as Record<MarketPressureLayerKey, MarketPressureLayerAccess>;
  response.entitlement.visibleLayers.forEach((key) => {
    access[key] = "available";
  });
  response.entitlement.lockedLayers.forEach((key) => {
    access[key] = "locked";
  });
  return access;
}

export function emptyMarketPressureMap(
  query: MarketPressureQuery,
  status: MarketPressureMapResult["status"],
  message: string,
  warnings: string[] = [],
): MarketPressureMapResult {
  const period = query.period ?? timeRangeToPeriod(query.timeRange);
  const result: MarketPressureMapResult = {
    status,
    universe: query.universe,
    period,
    view: query.viewMode,
    generatedAt: null,
    priceAsOf: null,
    confirmationAsOf: null,
    confirmationFreshnessWindowDays: 30,
    scoringVersion: null,
    capabilities: defaultMarketPressureCapabilities,
    entitlement: { tier: "free", visibleLayers: [], lockedLayers: [] },
    summary: {
      symbolCount: 0,
      classifiedCount: 0,
      partialCount: 0,
      unavailableCount: 0,
      bullishCount: 0,
      bearishCount: 0,
      neutralCount: 0,
      conflictedCount: 0,
      hiddenAccumulationCount: 0,
      fragileWinnerCount: 0,
    },
    sectors: [],
    tiles: [],
    warnings,
    latestSuccessfulDataAt: null,
    providerMessage: message,
    layerAccess: {
      priceVolume: "unavailable",
      congress: "unavailable",
      insiders: "unavailable",
      governmentContracts: "unavailable",
      fundamentals: "unavailable",
      signals: "unavailable",
      institutions: "unavailable",
      optionsFlow: "unavailable",
      macroPositioning: "unavailable",
    },
  };
  return result;
}

function marketPressureApiUrl(query: MarketPressureQuery) {
  const base = typeof window === "undefined" ? API_BASE : window.location.origin;
  return new URL(`/api/market-pressure?${marketPressureQueryString(query)}`, base).toString();
}

function marketPressureCapabilitiesApiUrl() {
  const base = typeof window === "undefined" ? API_BASE : window.location.origin;
  return new URL("/api/market-pressure/capabilities", base).toString();
}

function responseStatus(data: MarketPressureMapResult): MarketPressureMapResult["status"] {
  if (data.warnings.some((warning) => warning.startsWith("unsupported_universe") || warning.startsWith("unsupported_view"))) {
    return "unsupported";
  }
  if (data.sectors.length === 0) return "no-data";
  return "ready";
}

function normalizeMarketPressureResponse(data: MarketPressureMapResult): MarketPressureMapResult {
  const tiles = data.sectors.flatMap((sector) => sector.tiles);
  const layerAccess = marketPressureLayerAccessFromResponse(data);
  return {
    ...data,
    status: responseStatus(data),
    tiles,
    latestSuccessfulDataAt: data.priceAsOf ?? data.confirmationAsOf ?? data.generatedAt,
    providerMessage: data.warnings.length > 0 ? data.warnings.join(", ") : null,
    layerAccess,
  };
}

export async function getMarketPressureMap(query: MarketPressureQuery): Promise<MarketPressureMapResult> {
  const url = marketPressureApiUrl(query);
  const headers: Record<string, string> = {};
  if (query.authToken && typeof window === "undefined") {
    headers.cookie = `${backendSessionCookieName}=${encodeURIComponent(query.authToken)}`;
  }
  const response = await fetch(url, {
    cache: "no-store",
    credentials: "include",
    headers,
  });
  if (response.status === 401) {
    return emptyMarketPressureMap(query, "auth-required", "Sign in with a Pro account to open Market Pressure.");
  }
  if (response.status === 403) {
    return emptyMarketPressureMap(query, "entitlement", "Market Pressure is available with Pro.", ["pro_required"]);
  }
  if (!response.ok) {
    return emptyMarketPressureMap(query, "error", `Market Pressure API returned ${response.status}.`);
  }
  const data = (await response.json()) as MarketPressureMapResult;
  return normalizeMarketPressureResponse(data);
}

export async function getMarketPressureCapabilities(authToken?: string | null): Promise<MarketPressureCapabilities> {
  const headers: Record<string, string> = {};
  if (authToken && typeof window === "undefined") {
    headers.cookie = `${backendSessionCookieName}=${encodeURIComponent(authToken)}`;
  }
  const response = await fetch(marketPressureCapabilitiesApiUrl(), {
    cache: "no-store",
    credentials: "include",
    headers,
  });
  if (!response.ok) return defaultMarketPressureCapabilities;
  return (await response.json()) as MarketPressureCapabilities;
}
