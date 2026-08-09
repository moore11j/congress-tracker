import type { Metadata } from "next";
import { EnhancedInsiderDashboard } from "@/components/profiles/EnhancedProfileDashboards";
import { FilterLinks } from "@/components/profiles/ProfileLanding";
import { getInsidersOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/insiders", {
  title: "Insider Trading Activity & Corporate Insider Purchases | Walnut Markets",
  description: "Track purchases and sales from executives, directors, and major shareholders.",
});

export default async function InsidersPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const params = (await searchParams) ?? {};
  const period = typeof params.period === "string" && params.period === "90" ? 90 : 365;
  const authState = await optionalPageAuthState();
  const data = await getInsidersOverview({ period_days: period, authToken: authState.token });
  return <EnhancedInsiderDashboard data={data} periodFilter={<FilterLinks label="Period" active={String(period)} options={[{ label: "TTM", value: "365", href: "/insiders" }, { label: "90D", value: "90", href: "/insiders?period=90" }]} />} />;
}
