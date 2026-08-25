import type { Metadata } from "next";
import Link from "next/link";
import { Suspense, type ReactNode } from "react";
import { EnhancedInsiderDashboard } from "@/components/profiles/EnhancedProfileDashboards";
import { FilterLinks } from "@/components/profiles/ProfileLanding";
import { ProfileDashboardSkeleton } from "@/components/profiles/ProfileDashboardSkeleton";
import { getInsidersOverview, getSeoSnapshotIndex } from "@/lib/api";
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
  const periodFilter = <FilterLinks label="Period" active={String(period)} options={[{ label: "TTM", value: "365", href: "/insiders" }, { label: "90D", value: "90", href: "/insiders?period=90" }]} />;

  return (
    <>
      <Suspense fallback={<ProfileDashboardSkeleton variant="insiders" filter={periodFilter} />}>
        <InsidersDashboard period={period} authToken={authState.token} periodFilter={periodFilter} />
      </Suspense>
      <QualifiedInsiderDirectory />
    </>
  );
}

async function InsidersDashboard({ period, authToken, periodFilter }: { period: number; authToken: string | null; periodFilter: ReactNode }) {
  const data = await getInsidersOverview({ period_days: period, authToken });
  return <EnhancedInsiderDashboard data={data} periodFilter={periodFilter} />;
}

async function QualifiedInsiderDirectory() {
  const items = await getSeoSnapshotIndex("insider", { source: "InsiderDirectorySnapshots", limit: 100 })
    .then((response) => response.items.filter((item) => item.indexable && item.canonical_path))
    .catch(() => []);
  if (!items.length) return null;
  return (
    <section className="mx-auto mt-6 max-w-7xl px-4 sm:px-6 lg:px-8" aria-label="Qualified insider profiles">
      <h2 className="text-lg font-semibold text-white">Browse insider profiles</h2>
      <div className="mt-3 flex flex-wrap gap-2">
        {items.map((item) => (
          <Link key={item.entity_key} href={item.canonical_path} prefetch={false} className="rounded-md border border-slate-700 px-3 py-2 text-sm text-slate-200 hover:border-slate-500 hover:text-white">
            {typeof item.payload.insider_name === "string" ? item.payload.insider_name : item.entity_key}
          </Link>
        ))}
      </div>
    </section>
  );
}
