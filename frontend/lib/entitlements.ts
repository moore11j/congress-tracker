export type EntitlementTier = "free" | "premium";

export type EntitlementFeature =
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
    watchlists: 3,
    watchlist_tickers: 15,
    saved_views: 5,
    notification_digests: 0,
    monitoring_sources: 8,
  },
  features: ["watchlists", "watchlist_tickers", "saved_views", "monitoring_sources"],
  upgrade_url: "/pricing",
};

export const premiumEntitlements: Entitlements = {
  tier: "premium",
  limits: {
    watchlists: 25,
    watchlist_tickers: 100,
    saved_views: 50,
    notification_digests: 25,
    monitoring_sources: 100,
  },
  features: ["watchlists", "watchlist_tickers", "saved_views", "notification_digests", "monitoring_sources"],
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
