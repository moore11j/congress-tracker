import type { Metadata } from "next";
import type { ReactNode } from "react";
import Link from "next/link";
import { DataPanel, FilterLinks, MetricGrid, ProfilePageHeader, SectorStackedChart, formatCompactCurrency } from "@/components/profiles/ProfileLanding";
import { formatDateShort } from "@/lib/format";
import { getDepartmentsOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";
import { cardClassName, tickerLinkClassName } from "@/lib/styles";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/departments", {
  title: "Government Contracts & Department Spending | Walnut Markets",
  description: "Track department spending, contract awards, vendors, and public-company exposure.",
});

function selectedPeriod(sp: SearchParams) {
  const value = typeof sp.period === "string" ? sp.period : "365";
  return ["30", "90", "365"].includes(value) ? value : "365";
}

export default async function DepartmentsPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  const period = selectedPeriod(sp);
  const authState = await optionalPageAuthState();
  const data = await getDepartmentsOverview({ period_days: Number(period), authToken: authState.token });
  const comparisonStatus = data.comparison?.status ?? "ok";
  const comparisonLabel = data.comparison?.label || `previous ${data.period_days} days`;
  const comparisonPaused = comparisonStatus !== "ok";
  const activeDepartment = data.most_active_departments[0] ?? {};
  const largestAward = data.largest_recent_awards[0] ?? {};
  const growthLeader = data.fastest_growing_vendors[0] ?? {};

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="DEPARTMENTS"
        title="Government Contract Activity"
        subtitle="Track department spending, contract awards, vendors, and public-company exposure."
        actions={
          <FilterLinks
            label="Period"
            active={period}
            options={[
              { label: "TTM", value: "365", href: "/departments?period=365" },
              { label: "90D", value: "90", href: "/departments?period=90" },
              { label: "30D", value: "30", href: "/departments?period=30" },
            ]}
          />
        }
      />
      <section className={`${cardClassName} overflow-hidden p-0`}>
        <div className="border-b border-white/10 bg-slate-900/60 px-5 py-4">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-300">Last {data.period_days} Days</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Government contract snapshot</h2>
          <p className="mt-1 text-sm leading-6 text-slate-400">
            {comparisonPaused
              ? data.comparison?.message ?? "Comparison paused while the government-contract backfill catches up."
              : `Momentum metrics compare against the ${comparisonLabel}; future-dated awards are excluded from the current period.`}
          </p>
        </div>
        <div className="grid gap-px bg-white/10 md:grid-cols-3">
          <SnapshotTile label="Most Active Department" title={textValue(activeDepartment, "name")} detail={`${numberValue(activeDepartment, "contracts")} contracts`} href={hrefValue(activeDepartment)} />
          <SnapshotTile label="Largest Recent Award" title={textValue(largestAward, "company")} detail={formatCompactCurrency(numberOrNull(largestAward, "value"))} href={hrefValue(largestAward)} />
          <SnapshotTile label="Fastest Growing Vendor" title={textValue(growthLeader, "company")} detail={formatCompactCurrency(numberOrNull(growthLeader, "increase_value"))} href={hrefValue(growthLeader)} />
        </div>
      </section>
      <MetricGrid metrics={data.summary} comparisonLabel={comparisonLabel} />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel
          title="Top Departments by Contract Value"
          subtitle={comparisonPaused ? "Current period shown without prior-period deltas until ingest coverage is comparable." : `Current period compared with the ${comparisonLabel}.`}
          rows={data.top_departments}
          empty="No department contract data available for this period."
          cta={{ href: "/departments", label: "View all departments" }}
          columns={[
            { key: "name", label: "Department", format: "link" },
            { key: "contract_value", label: "Contract Value", align: "right", format: "currency" },
            { key: "previous_value", label: "Previous Period", align: "right", format: "currency" },
            { key: "change_pct", label: "Change", align: "right", format: "percent" },
            { key: "contracts", label: "Contracts", align: "right", format: "number" },
            { key: "top_vendor", label: "Top Vendor" },
          ]}
        />
        <DataPanel
          title="Top Vendors"
          rows={data.top_vendors}
          empty="No mapped public-company vendors found for this period."
          cta={{ href: "/feed?mode=government_contracts", label: "View all vendors" }}
          columns={[
            { key: "vendor", label: "Vendor", format: "link" },
            { key: "symbol", label: "Ticker" },
            { key: "contract_value", label: "Contract Value", align: "right", format: "currency" },
            { key: "contracts", label: "Contracts", align: "right", format: "number" },
            { key: "top_department", label: "Top Department" },
          ]}
        />
      </section>
      <SectorStackedChart
        title="Contract Value by Sector Over Time"
        rows={data.contract_value_over_time}
        note="Stacked quarterly values use mapped public-company vendor sectors. Unclassified vendor symbols are excluded until sector metadata is available."
      />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <RecordPanel title="Most Active Departments" rows={data.most_active_departments} empty="No department activity available.">
          {(row) => (
            <RecordLink href={hrefValue(row)} title={textValue(row, "name")}>
              <MetricLine label="Contracts" value={numberValue(row, "contracts")} />
              <MetricLine label="Value" value={formatCompactCurrency(numberOrNull(row, "contract_value"))} />
            </RecordLink>
          )}
        </RecordPanel>
        <RecordPanel title="Largest Recent Awards" rows={data.largest_recent_awards} empty="No recent contract awards available.">
          {(row) => (
            <RecordLink href={hrefValue(row)} title={textValue(row, "company")} eyebrow={textValue(row, "symbol")}>
              <MetricLine label="Department" value={textValue(row, "department")} />
              <MetricLine label="Value" value={formatCompactCurrency(numberOrNull(row, "value"))} />
              <MetricLine label="Date" value={formatDateShort(stringValue(row, "date"))} />
            </RecordLink>
          )}
        </RecordPanel>
      </section>
      <RecordPanel title="Fastest Growing Vendors" rows={data.fastest_growing_vendors} empty="No vendor growth data available.">
        {(row) => (
          <RecordLink href={hrefValue(row)} title={textValue(row, "company")} eyebrow={textValue(row, "symbol")}>
            <MetricLine label="Current" value={formatCompactCurrency(numberOrNull(row, "current_value"))} />
            <MetricLine label="Previous" value={formatCompactCurrency(numberOrNull(row, "previous_value"))} />
            <MetricLine label="Increase" value={formatCompactCurrency(numberOrNull(row, "increase_value"))} />
          </RecordLink>
        )}
      </RecordPanel>
    </div>
  );
}

function SnapshotTile({ label, title, detail, href }: { label: string; title: string; detail: string; href?: string | null }) {
  const content = (
    <>
      <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className="mt-2 truncate text-lg font-semibold text-white">{title}</p>
      <p className="mt-1 truncate text-sm text-slate-400">{detail}</p>
    </>
  );
  return href ? (
    <Link href={href} prefetch={false} className="min-w-0 bg-slate-950/35 p-4 transition hover:bg-slate-900/75">
      {content}
    </Link>
  ) : (
    <div className="min-w-0 bg-slate-950/35 p-4">{content}</div>
  );
}

function RecordPanel({
  title,
  rows,
  empty,
  children,
}: {
  title: string;
  rows: Array<Record<string, unknown>>;
  empty: string;
  children: (row: Record<string, unknown>) => ReactNode;
}) {
  return (
    <section className={`${cardClassName} min-w-0 p-0`}>
      <div className="border-b border-white/10 p-4">
        <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-white">{title}</h2>
      </div>
      {rows.length ? <div className="grid gap-px bg-white/5 md:grid-cols-2">{rows.map((row, index) => <div key={`${title}-${index}`}>{children(row)}</div>)}</div> : <div className="m-4 rounded-xl border border-dashed border-white/10 bg-slate-950/35 p-5 text-sm text-slate-400">{empty}</div>}
    </section>
  );
}

function RecordLink({ href, title, eyebrow, children }: { href?: string | null; title: string; eyebrow?: string; children: ReactNode }) {
  const content = (
    <>
      <div className="flex min-w-0 items-start justify-between gap-3">
        <p className="min-w-0 truncate text-sm font-semibold text-white">{title}</p>
        {eyebrow && eyebrow !== "-" ? <span className={tickerLinkClassName}>{eyebrow}</span> : null}
      </div>
      <div className="mt-3 space-y-2">{children}</div>
    </>
  );
  return href ? (
    <Link href={href} prefetch={false} className="block min-h-36 min-w-0 bg-slate-950/35 p-4 transition hover:bg-slate-900/75">
      {content}
    </Link>
  ) : (
    <div className="min-h-36 min-w-0 bg-slate-950/35 p-4">{content}</div>
  );
}

function MetricLine({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex min-w-0 items-center justify-between gap-3 text-xs">
      <span className="shrink-0 text-slate-500">{label}</span>
      <span className="min-w-0 truncate text-right font-semibold tabular-nums text-slate-200">{value}</span>
    </div>
  );
}

function textValue(row: Record<string, unknown>, key: string) {
  const value = row[key];
  return typeof value === "string" && value.trim() ? value : "-";
}

function stringValue(row: Record<string, unknown>, key: string) {
  const value = row[key];
  return typeof value === "string" ? value : null;
}

function hrefValue(row: Record<string, unknown>) {
  const value = row.href;
  return typeof value === "string" && value ? value : null;
}

function numberValue(row: Record<string, unknown>, key: string) {
  const value = numberOrNull(row, key);
  return typeof value === "number" ? value.toLocaleString() : "-";
}

function numberOrNull(row: Record<string, unknown>, key: string) {
  const value = row[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
