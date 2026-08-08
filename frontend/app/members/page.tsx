import type { Metadata } from "next";
import { DataPanel, FilterLinks, MetricGrid, ProfilePageHeader, SectorStackedChart } from "@/components/profiles/ProfileLanding";
import { getCongressOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/members", {
  title: "Congress Stock Trading & Member Portfolios | Walnut Markets",
  description: "Track disclosed trades, portfolio activity, and market positioning across members of Congress.",
});

function chamber(sp: SearchParams) {
  const value = typeof sp.chamber === "string" ? sp.chamber : "all";
  return ["all", "house", "senate"].includes(value) ? value : "all";
}

export default async function MembersPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  const selectedChamber = chamber(sp);
  const authState = await optionalPageAuthState();
  const data = await getCongressOverview({ chamber: selectedChamber, period_days: 365, authToken: authState.token });

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="CONGRESS"
        title="Congress Trading"
        subtitle="Track disclosed trades, portfolio activity, and market positioning across members of Congress."
        actions={
          <FilterLinks
            label="Chamber"
            active={selectedChamber}
            options={[
              { label: "All", value: "all", href: "/members" },
              { label: "House", value: "house", href: "/members?chamber=house" },
              { label: "Senate", value: "senate", href: "/members?chamber=senate" },
            ]}
          />
        }
      />
      <MetricGrid metrics={data.summary} />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel
          title="Top Members by Portfolio Value"
          rows={data.top_members}
          empty="No congressional member activity available for this period."
          cta={{ href: "/leaderboards/congress-traders", label: "View all members" }}
          columns={[
            { key: "name", label: "Member", format: "link" },
            { key: "party", label: "Party" },
            { key: "chamber", label: "Chamber" },
            { key: "estimated_portfolio_value", label: "Estimated Portfolio Value", align: "right", format: "currency" },
            { key: "trades", label: "Trades", align: "right", format: "number" },
            { key: "recent_activity", label: "Recent Activity", align: "right", format: "date" },
          ]}
        />
        <DataPanel
          title="Most Traded Stocks"
          rows={data.most_traded_stocks}
          empty="No traded stocks available for this period."
          cta={{ href: "/feed?mode=congress", label: "View all Congress stocks" }}
          columns={[
            { key: "symbol", label: "Ticker", format: "link" },
            { key: "company", label: "Company" },
            { key: "buy_value", label: "Buy Value", align: "right", format: "currency" },
            { key: "sell_value", label: "Sell Value", align: "right", format: "currency" },
            { key: "net_value", label: "Net Value", align: "right", format: "currency" },
            { key: "actor_count", label: "Members", align: "right", format: "number" },
          ]}
        />
      </section>
      <SectorStackedChart title="Sector Exposure Over Time" rows={data.sector_exposure} note={data.note} />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel title="Top Buyers" rows={data.top_buyers} empty="No buyers available for this period." columns={[{ key: "name", label: "Member", format: "link" }, { key: "trades", label: "Trades", align: "right", format: "number" }, { key: "value", label: "Value", align: "right", format: "currency" }, { key: "last_activity", label: "Latest", align: "right", format: "date" }]} />
        <DataPanel title="Top Sellers" rows={data.top_sellers} empty="No sellers available for this period." columns={[{ key: "name", label: "Member", format: "link" }, { key: "trades", label: "Trades", align: "right", format: "number" }, { key: "value", label: "Value", align: "right", format: "currency" }, { key: "last_activity", label: "Latest", align: "right", format: "date" }]} />
      </section>
    </div>
  );
}
