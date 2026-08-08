import type { Metadata } from "next";
import Link from "next/link";
import { DataPanel, FilterLinks, MetricGrid, ProfilePageHeader, SectorStackedChart, formatCompactCurrency } from "@/components/profiles/ProfileLanding";
import { getInsidersOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { cardClassName } from "@/lib/styles";

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
  const comparisonLabel = `previous ${data.period_days} days`;
  const topInsider = data.top_insiders[0] ?? {};
  const mostTraded = data.most_traded_stocks[0] ?? {};
  const cluster = data.cluster_buying[0] ?? {};
  const latestSectors = data.sector_activity[data.sector_activity.length - 1]?.segments ?? [];
  const sectorCount = new Set(data.sector_activity.flatMap((row) => row.segments.map((segment) => segment.label))).size;
  const leadingSector = latestSectors[0];

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
      <section className={`${cardClassName} overflow-hidden p-0`}>
        <div className="border-b border-white/10 bg-slate-900/60 px-5 py-4">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">Last {data.period_days} Days</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Insider trading snapshot</h2>
          <p className="mt-1 text-sm leading-6 text-slate-400">Metrics use verified open-market Form 4 purchases and sales, compared with the {comparisonLabel}.</p>
        </div>
        <div className="grid gap-px bg-white/10 md:grid-cols-4">
          <SnapshotTile label="Top Net Buyer" title={textValue(topInsider, "name")} detail={textValue(topInsider, "company")} href={hrefValue(topInsider)} value={formatCompactCurrency(numberOrNull(topInsider, "net_buy_value"))} />
          <SnapshotTile label="Most Traded Ticker" title={textValue(mostTraded, "symbol")} detail={textValue(mostTraded, "company")} href={hrefValue(mostTraded)} value={formatCompactCurrency(numberOrNull(mostTraded, "net_value"))} />
          <SnapshotTile label="Cluster Buying" title={textValue(cluster, "symbol")} detail={`${numberValue(cluster, "unique_insiders")} insiders`} href={hrefValue(cluster)} value={formatCompactCurrency(numberOrNull(cluster, "buy_value"))} />
          <SnapshotTile label="Sector Breadth" title={leadingSector?.label ?? "No sector data"} detail={`${sectorCount || 0} sectors tracked`} value={leadingSector ? `${leadingSector.percent.toFixed(1)}%` : "-"} />
        </div>
      </section>
      <MetricGrid metrics={data.summary} comparisonLabel={comparisonLabel} />
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
        <DataPanel title="Recent Open Market Purchases" rows={data.recent_purchases as unknown as Array<Record<string, unknown>>} empty="No recent purchases available." columns={[{ key: "profile", label: "Insider", format: "link" }, { key: "symbol", label: "Ticker", format: "link" }, { key: "activity", label: "Activity" }, { key: "value", label: "Value", align: "right", format: "currency" }]} />
        <DataPanel title="Largest Insider Buys" rows={data.largest_buys as unknown as Array<Record<string, unknown>>} empty="No large insider buys available." columns={[{ key: "profile", label: "Insider", format: "link" }, { key: "symbol", label: "Ticker", format: "link" }, { key: "value", label: "Value", align: "right", format: "currency" }, { key: "time", label: "Date", align: "right", format: "date" }]} />
        <DataPanel title="Cluster Buying" rows={data.cluster_buying} empty="No cluster buying detected for this period." columns={[{ key: "symbol", label: "Ticker", format: "link" }, { key: "company", label: "Company" }, { key: "unique_insiders", label: "Insiders", align: "right", format: "number" }, { key: "buy_value", label: "Buy Value", align: "right", format: "currency" }]} />
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
