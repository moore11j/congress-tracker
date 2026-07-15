import { hasEntitlement, type Entitlements } from "@/lib/entitlements";

export type MarketPressureTimeRange = "1D" | "5D" | "1M" | "3M" | "YTD" | "1Y";
export type MarketPressureUniverse = "sp500" | "nasdaq100" | "all-us" | "watchlist";
export type MarketPressureViewMode =
  | "market-pressure"
  | "hidden-accumulation"
  | "fragile-winners"
  | "crowded-trades"
  | "rotation";

export type MarketPressureTile = {
  symbol: string;
  companyName?: string;
  sector: string;
  priceChangePct: number | null;
  pressureDirection: "bullish" | "bearish" | "neutral" | "conflicted";
  pressureStrength: number | null;
  pressureTrend: "strengthening" | "weakening" | "stable" | null;
  freshnessAt: string | null;
  availableLayers: {
    priceVolume: boolean;
    fundamentals: boolean;
    congress: boolean;
    insiders: boolean;
    governmentContracts: boolean;
    institutions: boolean;
    optionsFlow: boolean;
    macroPositioning: boolean;
  };
};

export type MarketPressureLayerKey = keyof MarketPressureTile["availableLayers"];
export type MarketPressureLayerAccess = "available" | "locked" | "unavailable";

export type MarketPressureMapResult = {
  status: "loading" | "ready" | "no-data" | "error" | "entitlement";
  tiles: MarketPressureTile[];
  latestSuccessfulDataAt: string | null;
  providerMessage: string | null;
  layerAccess: Record<MarketPressureLayerKey, MarketPressureLayerAccess>;
};

export type MarketPressureQuery = {
  timeRange: MarketPressureTimeRange;
  universe: MarketPressureUniverse;
  viewMode: MarketPressureViewMode;
  authToken?: string | null;
};

export const marketPressureTimeRanges: MarketPressureTimeRange[] = ["1D", "5D", "1M", "3M", "YTD", "1Y"];

export const marketPressureUniverses: Array<{ value: MarketPressureUniverse; label: string }> = [
  { value: "sp500", label: "S&P 500" },
  { value: "nasdaq100", label: "Nasdaq 100" },
  { value: "all-us", label: "All US" },
  { value: "watchlist", label: "Watchlist" },
];

export const marketPressureViewModes: Array<{ value: MarketPressureViewMode; label: string }> = [
  { value: "market-pressure", label: "Market Pressure" },
  { value: "hidden-accumulation", label: "Hidden Accumulation" },
  { value: "fragile-winners", label: "Fragile Winners" },
  { value: "crowded-trades", label: "Crowded Trades" },
  { value: "rotation", label: "Rotation" },
];

export const marketPressureLayerLabels: Record<MarketPressureLayerKey, string> = {
  priceVolume: "Price / Volume",
  fundamentals: "Premium Signals",
  congress: "Congress",
  insiders: "Insiders",
  governmentContracts: "Government Contracts",
  institutions: "Institutional Activity",
  optionsFlow: "Options Flow",
  macroPositioning: "Macro Positioning",
};

export function marketPressureLayerAccess(entitlements: Entitlements): Record<MarketPressureLayerKey, MarketPressureLayerAccess> {
  return {
    priceVolume: "available",
    fundamentals: hasEntitlement(entitlements, "signals") || hasEntitlement(entitlements, "ticker_confirmation") ? "available" : "locked",
    congress: hasEntitlement(entitlements, "congress_feed") ? "available" : "locked",
    insiders: hasEntitlement(entitlements, "insider_feed") ? "available" : "locked",
    governmentContracts: hasEntitlement(entitlements, "government_contracts_feed") ? "available" : "locked",
    institutions: hasEntitlement(entitlements, "institutional_feed") ? "available" : "locked",
    optionsFlow: hasEntitlement(entitlements, "options_flow_feed") ? "available" : "locked",
    macroPositioning: hasEntitlement(entitlements, "macro_positioning") ? "available" : "locked",
  };
}

export async function getMarketPressureMap(
  query: MarketPressureQuery,
  entitlements: Entitlements,
): Promise<MarketPressureMapResult> {
  void query;
  return {
    status: "no-data",
    tiles: [],
    latestSuccessfulDataAt: null,
    providerMessage: "The canonical Market Pressure batch endpoint is not connected yet.",
    layerAccess: marketPressureLayerAccess(entitlements),
  };
}
