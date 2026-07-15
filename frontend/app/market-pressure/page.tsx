import type { Metadata } from "next";
import { MarketPressureMapClient } from "@/components/market-pressure/MarketPressureMapClient";
import { getEntitlements } from "@/lib/api";
import { defaultEntitlements, entitlementsFromTierHint } from "@/lib/entitlements";
import { getMarketPressureMap } from "@/lib/marketPressure";
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

export default async function MarketPressurePage() {
  const authState = await optionalPageAuthState();
  const entitlements = authState.token
    ? await getEntitlements(authState.token, { source: "MarketPressurePage" }).catch(() => defaultEntitlements)
    : entitlementsFromTierHint(authState.entitlementHint);
  const initialData = await getMarketPressureMap(
    {
      timeRange: "1D",
      universe: "sp500",
      viewMode: "market-pressure",
      authToken: authState.token,
    },
    entitlements,
  );

  return (
    <MarketPressureMapClient
      initialData={initialData}
      canonicalUrl={`${WALNUT_MARKETING_URL}/market-pressure`}
    />
  );
}
