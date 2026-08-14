import type { Metadata } from "next";
import { Suspense, type ReactNode } from "react";
import { EnhancedCongressDashboard } from "@/components/profiles/EnhancedProfileDashboards";
import { FilterLinks } from "@/components/profiles/ProfileLanding";
import { ProfileDashboardSkeleton } from "@/components/profiles/ProfileDashboardSkeleton";
import { getCongressOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/members", {
  title: "Congress Stock Trading & Member Portfolios | Walnut Markets",
  description: "Track disclosed trades, portfolio activity, and market positioning across members of Congress.",
});

export default async function MembersPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const params = (await searchParams) ?? {};
  const selectedChamber = typeof params.chamber === "string" && ["house", "senate"].includes(params.chamber) ? params.chamber : "all";
  const authState = await optionalPageAuthState();
  const chamberFilter = <FilterLinks label="Chamber" active={selectedChamber} options={[{ label: "All", value: "all", href: "/members" }, { label: "House", value: "house", href: "/members?chamber=house" }, { label: "Senate", value: "senate", href: "/members?chamber=senate" }]} />;

  return (
    <Suspense fallback={<ProfileDashboardSkeleton variant="congress" filter={chamberFilter} />}>
      <MembersDashboard selectedChamber={selectedChamber} authToken={authState.token} chamberFilter={chamberFilter} />
    </Suspense>
  );
}

async function MembersDashboard({ selectedChamber, authToken, chamberFilter }: { selectedChamber: string; authToken: string | null; chamberFilter: ReactNode }) {
  const data = await getCongressOverview({ chamber: selectedChamber, period_days: 365, authToken });
  return <EnhancedCongressDashboard data={data} chamberFilter={chamberFilter} />;
}
