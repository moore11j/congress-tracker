import type { Metadata } from "next";
import { LeaderboardsDashboard } from "@/components/leaderboards/LeaderboardsDashboard";
import { getLeaderboardDashboard, getLeaderboardPreview, type CachedLeaderboardResponse } from "@/lib/api";
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
const gatedDashboard = {
  top_stocks: empty("top-stocks"),
  congress: null,
  insiders: null,
  institutions: null,
  can_view_performance: false,
  can_view_institutions: false,
};

export default async function LeaderboardsPage() {
  const { token } = await optionalPageAuthState();
  // Guests receive a deliberately small, public preview from the same prepared
  // daily snapshots. The protected bundle is never fetched without a session.
  const dashboard = token
    ? await getLeaderboardDashboard({ authToken: token, source: "LeaderboardsPage" }).catch(() => gatedDashboard)
    : await getLeaderboardPreview({ source: "LeaderboardsPreviewPage" }).catch(() => gatedDashboard);

  return <div className="w-full py-4 sm:py-5"><h1 className="sr-only">Leaderboards</h1><LeaderboardsDashboard topStocks={dashboard.top_stocks} congress={dashboard.congress} insiders={dashboard.insiders} institutions={dashboard.institutions} canViewTopStocks={dashboard.can_view_performance} canViewPerformance={dashboard.can_view_performance} canViewInstitutions={dashboard.can_view_institutions} /><p className="mt-4 text-xs leading-5 text-slate-500">Rankings are research tools, not investment advice. Past or backtested performance does not guarantee future results. Return calculations use the stated public-data methodology and may be affected by disclosure timing, data coverage, and survivorship limitations.</p></div>;
}
