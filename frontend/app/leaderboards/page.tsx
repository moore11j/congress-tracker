import type { Metadata } from "next";
import { LeaderboardsDashboard } from "@/components/leaderboards/LeaderboardsDashboard";
import { getCachedLeaderboard, getEntitlements, type CachedLeaderboardResponse } from "@/lib/api";
import { defaultEntitlements, hasEntitlement } from "@/lib/entitlements";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const dynamic = "force-dynamic";
export const revalidate = 300;

export const metadata: Metadata = {
  title: "Stock, Congress, Insider & Institution Leaderboards | Walnut Markets",
  description: "Ranked market intelligence across stocks, Congress, insiders, and institutions, built from Walnut’s stored daily data snapshots.",
  alternates: { canonical: "https://app.walnutmarkets.com/leaderboards" },
  robots: { index: true, follow: true },
};

const empty = (key: string): CachedLeaderboardResponse => ({ key, items: [], generated_at: null });

export default async function LeaderboardsPage() {
  const { token } = await optionalPageAuthState();
  const entitlements = token ? await getEntitlements(token, { source: "LeaderboardsPage" }).catch(() => defaultEntitlements) : defaultEntitlements;
  const canViewPerformance = Boolean(token) && hasEntitlement(entitlements, "leaderboards");
  const canViewInstitutions = Boolean(token) && hasEntitlement(entitlements, "institutional_feed");
  const [topStocks, congress, insiders, institutions] = await Promise.all([
    getCachedLeaderboard("top-stocks", { source: "LeaderboardsPage" }).catch(() => empty("top-stocks")),
    canViewPerformance ? getCachedLeaderboard("congress_members", { authToken: token ?? undefined, source: "LeaderboardsPage" }).catch(() => empty("congress_members")) : Promise.resolve(null),
    canViewPerformance ? getCachedLeaderboard("insiders", { authToken: token ?? undefined, source: "LeaderboardsPage" }).catch(() => empty("insiders")) : Promise.resolve(null),
    canViewInstitutions ? getCachedLeaderboard("institutions", { authToken: token ?? undefined, source: "LeaderboardsPage" }).catch(() => empty("institutions")) : Promise.resolve(null),
  ]);

  return <div className="w-full py-4 sm:py-5"><h1 className="sr-only">Leaderboards</h1><LeaderboardsDashboard topStocks={topStocks} congress={congress} insiders={insiders} institutions={institutions} canViewPerformance={canViewPerformance} canViewInstitutions={canViewInstitutions} /><p className="mt-4 text-xs leading-5 text-slate-500">Rankings are research tools, not investment advice. Past or backtested performance does not guarantee future results. Return calculations use the stated public-data methodology and may be affected by disclosure timing, data coverage, and survivorship limitations.</p></div>;
}
