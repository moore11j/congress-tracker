import type { Metadata } from "next";
import { DataPanel, FilterLinks, MetricGrid, ProfilePageHeader, SectorStackedChart } from "@/components/profiles/ProfileLanding";
import { getInsidersOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/insiders", {
  title: "Insider Trading Activity & Corporate Insider Purchases | Walnut Markets",
  description: "Track purchases and sales from executives, directors, and major shareholders.",
});

export default async function InsidersPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  const period = typeof sp.period === "string" && sp.period === "90" ? 90 : 365;
  const authState = await optionalPageAuthState();
  const data = await getInsidersOverview({ period_days: period, authToken: authState.token });

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="INSIDERS"
        title="Corporate Insider Activity"
        subtitle="Track purchases and sales from executives, directors, and major shareholders."
        actions={
          <FilterLinks
            label="Period"
            active={String(period)}
            options={[
              { label: "TTM", value: "365", href: "/insiders" },
              { label: "90D", value: "90", href: "/insiders?period=90" },
            ]}
          />
        }
      />
      <MetricGrid metrics={data.summary} />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel
          title="Top Insiders by Net Buying"
          rows={data.top_insiders}
          empty="No open-market insider purchases available for this period."
          cta={{ href: "/feed?mode=insider", label: "View all insiders" }}
          columns={[
            { key: "name", label: "Insider", format: "link" },
            { key: "company", label: "Company" },
            { key: "role", label: "Role" },
            { key: "net_buy_value", label: "Net Buy Value", align: "right", format: "currency" },
            { key: "trades", label: "Trades", align: "right", format: "number" },
            { key: "last_transaction", label: "Last Transaction", align: "right", format: "date" },
          ]}
        />
        <DataPanel
          title="Most Traded Stocks"
          rows={data.most_traded_stocks}
          empty="No insider stock activity available for this period."
          columns={[
            { key: "symbol", label: "Ticker", format: "link" },
            { key: "company", label: "Company" },
            { key: "buy_value", label: "Insider Buy Value", align: "right", format: "currency" },
            { key: "sell_value", label: "Insider Sell Value", align: "right", format: "currency" },
            { key: "net_value", label: "Net Value", align: "right", format: "currency" },
            { key: "actor_count", label: "Unique Insiders", align: "right", format: "number" },
          ]}
        />
      </section>
      <SectorStackedChart title="Insider Activity by Sector" rows={data.sector_activity} />
      <section className="grid min-w-0 gap-3 xl:grid-cols-3">
        <DataPanel title="Recent Open Market Purchases" rows={data.recent_purchases as unknown as Array<Record<string, unknown>>} empty="No recent purchases available." columns={[{ key: "profile", label: "Insider" }, { key: "symbol", label: "Ticker", format: "link" }, { key: "activity", label: "Activity" }, { key: "value", label: "Value", align: "right", format: "currency" }]} />
        <DataPanel title="Largest Insider Buys" rows={data.largest_buys as unknown as Array<Record<string, unknown>>} empty="No large insider buys available." columns={[{ key: "profile", label: "Insider" }, { key: "symbol", label: "Ticker", format: "link" }, { key: "value", label: "Value", align: "right", format: "currency" }, { key: "time", label: "Date", align: "right", format: "date" }]} />
        <DataPanel title="Cluster Buying" rows={data.cluster_buying} empty="No cluster buying detected for this period." columns={[{ key: "symbol", label: "Ticker", format: "link" }, { key: "company", label: "Company" }, { key: "unique_insiders", label: "Insiders", align: "right", format: "number" }, { key: "buy_value", label: "Buy Value", align: "right", format: "currency" }]} />
      </section>
    </div>
  );
}
