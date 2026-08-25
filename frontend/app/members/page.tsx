import type { Metadata } from "next";
import { Suspense, type ReactNode } from "react";
import { EnhancedCongressDashboard } from "@/components/profiles/EnhancedProfileDashboards";
import { FilterLinks } from "@/components/profiles/ProfileLanding";
import { ProfileDashboardSkeleton } from "@/components/profiles/ProfileDashboardSkeleton";
import { getCongressOverview } from "@/lib/api";
import { getSeoSnapshotIndex } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { nameToSlug } from "@/lib/memberSlug";
import Link from "next/link";
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
    <>
      <Suspense fallback={<ProfileDashboardSkeleton variant="congress" filter={chamberFilter} />}>
        <MembersDashboard selectedChamber={selectedChamber} authToken={authState.token} chamberFilter={chamberFilter} />
      </Suspense>
      <QualifiedMemberDirectory />
    </>
  );
}

async function QualifiedMemberDirectory() {
  const snapshots = await getSeoSnapshotIndex("member", { source: "MembersDirectory", limit: 1000 })
    .then((response) => response.items)
    .catch(() => []);
  const members = snapshots
    .map((snapshot) => {
      const name = typeof snapshot.payload.member_name === "string" ? snapshot.payload.member_name.trim() : "";
      return name ? { name, href: `/member/${encodeURIComponent(nameToSlug(name))}` } : null;
    })
    .filter((member): member is { name: string; href: string } => Boolean(member));
  if (!members.length) return null;

  return (
    <section className="relative z-10 mt-3 rounded-lg border border-white/10 bg-[#0a1726]/95 p-4 shadow-[0_14px_34px_rgba(0,0,0,0.22)]">
      <h2 className="text-lg font-semibold text-white">Active Congress Profiles</h2>
      <p className="mt-1 text-sm text-slate-400">Browse members with public trading-disclosure snapshots available in Walnut Markets.</p>
      <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
        {members.map((member) => <Link key={member.href} href={member.href} prefetch={false} className="rounded-md border border-white/10 bg-slate-950/30 px-3 py-2 text-sm font-semibold text-emerald-200 hover:border-emerald-300/40 hover:text-emerald-100">{member.name}</Link>)}
      </div>
    </section>
  );
}

async function MembersDashboard({ selectedChamber, authToken, chamberFilter }: { selectedChamber: string; authToken: string | null; chamberFilter: ReactNode }) {
  const data = await getCongressOverview({ chamber: selectedChamber, period_days: 365, authToken });
  return <EnhancedCongressDashboard data={data} chamberFilter={chamberFilter} />;
}
