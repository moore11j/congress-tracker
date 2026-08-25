import type { Metadata } from "next";
import Link from "next/link";
import { Suspense, type ReactNode } from "react";
import { EnhancedGovernmentDashboard } from "@/components/profiles/EnhancedGovernmentDashboard";
import { FilterLinks } from "@/components/profiles/ProfileLanding";
import { ProfileDashboardSkeleton } from "@/components/profiles/ProfileDashboardSkeleton";
import { getDepartmentsOverview, getSeoSnapshotIndex } from "@/lib/api";
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
    <>
      <Suspense fallback={<ProfileDashboardSkeleton variant="departments" filter={periodFilter} />}>
        <DepartmentsDashboard period={Number(period)} authToken={authState.token} periodFilter={periodFilter} />
      </Suspense>
      <QualifiedDepartmentDirectory />
    </>
  );
}

async function DepartmentsDashboard({ period, authToken, periodFilter }: { period: number; authToken: string | null; periodFilter: ReactNode }) {
  const data = await getDepartmentsOverview({ period_days: period, authToken });
  return <EnhancedGovernmentDashboard data={data} periodFilter={periodFilter} />;
}

async function QualifiedDepartmentDirectory() {
  const items = await getSeoSnapshotIndex("department", { source: "DepartmentDirectorySnapshots", limit: 100 })
    .then((response) => response.items.filter((item) => item.indexable && item.canonical_path))
    .catch(() => []);
  if (!items.length) return null;
  return (
    <section className="mx-auto mt-6 max-w-7xl px-4 sm:px-6 lg:px-8" aria-label="Qualified government departments">
      <h2 className="text-lg font-semibold text-white">Browse government departments</h2>
      <div className="mt-3 flex flex-wrap gap-2">
        {items.map((item) => (
          <Link key={item.entity_key} href={item.canonical_path} prefetch={false} className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:border-slate-500 hover:text-white">
            {typeof item.payload.name === "string" ? item.payload.name : item.entity_key}
          </Link>
        ))}
      </div>
    </section>
  );
}
