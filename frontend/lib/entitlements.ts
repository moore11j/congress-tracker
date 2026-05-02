export type EntitlementTier = "free" | "premium";

export type EntitlementFeature =
  | "signals"
  | "leaderboards"
  | "backtesting"
  | "screener"
  | "screener_intelligence"
  | "screener_presets"
  | "screener_saved_screens"
  | "screener_monitoring"
  | "screener_csv_export"
  | "screener_results"
  | "watchlists"
  | "watchlist_tickers"
  | "saved_views"
  | "notification_digests"
  | "monitoring_sources";

export type Entitlements = {
  tier: EntitlementTier;
  limits: Record<EntitlementFeature, number>;
  features: EntitlementFeature[];
  upgrade_url: string;
  status?: string;
  user?: {
    id: number;
    email: string;
    name?: string | null;
    role: string;
    is_admin: boolean;
    is_suspended: boolean;
    subscription_status?: string | null;
    manual_tier_override?: string | null;
  } | null;
};

export const entitlementTierStorageKey = "ct:entitlementTier";

export const defaultEntitlements: Entitlements = {
  tier: "free",
  limits: {
    signals: 0,
    leaderboards: 0,
    backtesting: 0,
    screener: 0,
    screener_intelligence: 0,
    screener_presets: 0,
    screener_saved_screens: 3,
    screener_monitoring: 0,
    screener_csv_export: 0,
    screener_results: 25,
    watchlists: 1,
    watchlist_tickers: 10,
    saved_views: 3,
    notification_digests: 0,
    monitoring_sources: 2,
  },
  features: ["screener", "screener_saved_screens", "screener_results", "watchlists", "watchlist_tickers", "saved_views", "monitoring_sources"],
  upgrade_url: "/pricing",
};

export const premiumEntitlements: Entitlements = {
  tier: "premium",
  limits: {
    signals: 1,
    leaderboards: 1,
    backtesting: 1,
    screener: 1,
    screener_intelligence: 1,
    screener_presets: 1,
    screener_saved_screens: 10,
    screener_monitoring: 1,
    screener_csv_export: 1,
    screener_results: 250,
    watchlists: 10,
    watchlist_tickers: 30,
    saved_views: 50,
    notification_digests: 25,
    monitoring_sources: 100,
  },
  features: [
    "signals",
    "leaderboards",
    "backtesting",
    "screener",
    "screener_intelligence",
    "screener_presets",
    "screener_saved_screens",
    "screener_monitoring",
    "screener_csv_export",
    "screener_results",
    "watchlists",
    "watchlist_tickers",
    "saved_views",
    "notification_digests",
    "monitoring_sources",
  ],
  upgrade_url: "/pricing",
};

export function hasEntitlement(entitlements: Entitlements, feature: EntitlementFeature) {
  return entitlements.features.includes(feature);
}

export function limitFor(entitlements: Entitlements, feature: EntitlementFeature) {
  return entitlements.limits[feature];
}

export function normalizeTier(value: string | null | undefined): EntitlementTier {
  return value === "premium" ? "premium" : "free";
}

export function storedEntitlementTier() {
  if (typeof window === "undefined") return null;
  const raw = window.localStorage.getItem(entitlementTierStorageKey);
  return raw === "free" || raw === "premium" ? raw : null;
}
