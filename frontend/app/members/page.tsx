import type { Metadata } from "next";
import Link from "next/link";
import { DataPanel, FilterLinks, MetricGrid, ProfilePageHeader, SectorStackedChart, formatCompactCurrency } from "@/components/profiles/ProfileLanding";
import { getCongressOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { cardClassName } from "@/lib/styles";

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
  const comparisonLabel = `previous ${data.period_days} days`;
  const topMember = data.top_members[0] ?? {};
  const mostTraded = data.most_traded_stocks[0] ?? {};
  const topBuyer = data.top_buyers[0] ?? {};
  const latestSectors = data.sector_exposure[data.sector_exposure.length - 1]?.segments ?? [];
  const sectorCount = new Set(data.sector_exposure.flatMap((row) => row.segments.map((segment) => segment.label))).size;
  const leadingSector = latestSectors[0];

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
      <section className={`${cardClassName} overflow-hidden p-0`}>
        <div className="border-b border-white/10 bg-slate-900/60 px-5 py-4">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">Last {data.period_days} Days</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Congress trading snapshot</h2>
          <p className="mt-1 text-sm leading-6 text-slate-400">Momentum metrics compare against the {comparisonLabel}; rankings below are deduped by member name.</p>
        </div>
        <div className="grid gap-px bg-white/10 md:grid-cols-4">
          <SnapshotTile label="Most Active Member" title={textValue(topMember, "name")} detail={`${numberValue(topMember, "trades")} trades`} href={hrefValue(topMember)} />
          <SnapshotTile label="Most Traded Ticker" title={textValue(mostTraded, "symbol")} detail={`${formatCompactCurrency(numberOrNull(mostTraded, "net_value"))} net value`} href={hrefValue(mostTraded)} />
          <SnapshotTile label="Top Buyer" title={textValue(topBuyer, "name")} detail={formatCompactCurrency(numberOrNull(topBuyer, "value"))} href={hrefValue(topBuyer)} />
          <SnapshotTile label="Sector Breadth" title={leadingSector?.label ?? "No sector data"} detail={`${sectorCount || 0} sectors tracked`} value={leadingSector ? `${leadingSector.percent.toFixed(1)}%` : "-"} />
        </div>
      </section>
      <MetricGrid metrics={data.summary} comparisonLabel={comparisonLabel} />
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

function SnapshotTile({ label, title, detail, href, value }: { label: string; title: string; detail: string; href?: string | null; value?: string }) {
  const body = (
    <>
      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <div className="mt-3 flex min-w-0 items-baseline justify-between gap-3">
        <p className="truncate text-lg font-semibold text-white">{title}</p>
        {value ? <span className="shrink-0 text-sm font-semibold tabular-nums text-emerald-200">{value}</span> : null}
      </div>
      <p className="mt-1 truncate text-sm text-slate-400">{detail}</p>
    </>
  );
  if (href) {
    return (
      <Link href={href} prefetch={false} className="min-w-0 bg-slate-950/45 p-4 transition hover:bg-slate-900/80">
        {body}
      </Link>
    );
  }
  return <div className="min-w-0 bg-slate-950/45 p-4">{body}</div>;
}

function textValue(row: Record<string, unknown>, key: string) {
  const value = row[key];
  return typeof value === "string" && value ? value : "-";
}

function hrefValue(row: Record<string, unknown>) {
  const value = row.href;
  return typeof value === "string" && value ? value : null;
}

function numberValue(row: Record<string, unknown>, key: string) {
  const value = numberOrNull(row, key);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value ?? 0);
}

function numberOrNull(row: Record<string, unknown>, key: string) {
  const value = row[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
