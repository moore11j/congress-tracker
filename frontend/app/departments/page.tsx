import type { Metadata } from "next";
import { Suspense, type ReactNode } from "react";
import { EnhancedGovernmentDashboard } from "@/components/profiles/EnhancedGovernmentDashboard";
import { FilterLinks } from "@/components/profiles/ProfileLanding";
import { ProfileDashboardSkeleton } from "@/components/profiles/ProfileDashboardSkeleton";
import { getDepartmentsOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/departments", {
  title: "Government Contracts & Department Spending | Walnut Markets",
  description: "Track department spending, contract awards, vendors, and public-company exposure.",
});

export default async function DepartmentsPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const params = (await searchParams) ?? {};
  const period = typeof params.period === "string" && ["30", "90", "365"].includes(params.period) ? params.period : "365";
  const authState = await optionalPageAuthState();
  const periodFilter = <FilterLinks label="Period" active={period} options={[{ label: "TTM", value: "365", href: "/departments?period=365" }, { label: "90D", value: "90", href: "/departments?period=90" }, { label: "30D", value: "30", href: "/departments?period=30" }]} />;

  return (
    <Suspense fallback={<ProfileDashboardSkeleton variant="departments" filter={periodFilter} />}>
      <DepartmentsDashboard period={Number(period)} authToken={authState.token} periodFilter={periodFilter} />
    </Suspense>
  );
}

async function DepartmentsDashboard({ period, authToken, periodFilter }: { period: number; authToken: string | null; periodFilter: ReactNode }) {
  const data = await getDepartmentsOverview({ period_days: period, authToken });
  return <EnhancedGovernmentDashboard data={data} periodFilter={periodFilter} />;
}
