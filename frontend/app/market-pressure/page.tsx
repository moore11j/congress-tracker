import type { Metadata } from "next";
import { MarketPressureMapClient } from "@/components/market-pressure/MarketPressureMapClient";
import { getEntitlements } from "@/lib/api";
import { defaultEntitlements, entitlementsFromTierHint, type Entitlements } from "@/lib/entitlements";
import {
  emptyMarketPressureMap,
  getMarketPressureMap,
  normalizeMarketPressurePeriod,
  normalizeMarketPressureUniverse,
  normalizeMarketPressureView,
  periodToTimeRange,
} from "@/lib/marketPressure";
import { WALNUT_MARKETING_URL } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Market Pressure Map | Walnut Markets",
  description:
    "Visualize where bullish and bearish market pressure is building across price, fundamentals, disclosures, institutions, options, and macro positioning.",
  alternates: {
    canonical: "/market-pressure",
  },
  openGraph: {
    title: "Market Pressure Map | Walnut Markets",
    description:
      "Visualize where bullish and bearish market pressure is building across price, fundamentals, disclosures, institutions, options, and macro positioning.",
    url: "/market-pressure",
  },
};

type MarketPressureSearchParams = Record<string, string | string[] | undefined>;

function canUseMarketPressure(entitlements: Entitlements, authenticated: boolean) {
  if (!authenticated) return false;
  if (entitlements.user?.is_suspended) return false;
  return Boolean(
    entitlements.tier === "pro"
      || entitlements.tier === "admin"
      || entitlements.effective_tier === "pro"
      || entitlements.effective_tier === "admin"
      || entitlements.is_admin
      || entitlements.user?.is_admin,
  );
}

export default async function MarketPressurePage({
  searchParams,
}: {
  searchParams?: Promise<MarketPressureSearchParams>;
}) {
  const resolvedSearchParams = searchParams ? await searchParams : {};
  const authState = await optionalPageAuthState();
  const query = {
    period: normalizeMarketPressurePeriod(resolvedSearchParams.period),
    timeRange: periodToTimeRange(Array.isArray(resolvedSearchParams.period) ? resolvedSearchParams.period[0] : resolvedSearchParams.period),
    universe: normalizeMarketPressureUniverse(resolvedSearchParams.universe),
    viewMode: normalizeMarketPressureView(resolvedSearchParams.view),
    authToken: authState.token,
  };
  const entitlements = authState.token
    ? await getEntitlements(authState.token, { source: "MarketPressurePage" }).catch(() => defaultEntitlements)
    : entitlementsFromTierHint(authState.entitlementHint);
  const initialData = canUseMarketPressure(entitlements, Boolean(authState.token))
    ? await getMarketPressureMap(query).catch(() =>
      emptyMarketPressureMap(query, "error", "Walnut could not load the Market Pressure endpoint."),
    )
    : emptyMarketPressureMap(
      query,
      authState.token ? "entitlement" : "auth-required",
      authState.token
        ? "Market Pressure is available with Pro."
        : "Sign in with a Pro account to open Market Pressure.",
      ["pro_required"],
    );

  return (
    <MarketPressureMapClient
      initialData={initialData}
      canonicalUrl={`${WALNUT_MARKETING_URL}/market-pressure`}
    />
  );
}
