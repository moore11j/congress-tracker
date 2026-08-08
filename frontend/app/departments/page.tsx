import type { Metadata } from "next";
import { DataPanel, MetricGrid, ProfilePageHeader, TimeSeriesPanel } from "@/components/profiles/ProfileLanding";
import { getDepartmentsOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const metadata: Metadata = appPageMetadata("/departments", {
  title: "Government Contracts & Department Spending | Walnut Markets",
  description: "Track department spending, contract awards, vendors, and public-company exposure.",
});

export default async function DepartmentsPage() {
  const authState = await optionalPageAuthState();
  const data = await getDepartmentsOverview({ authToken: authState.token });

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="DEPARTMENTS"
        title="Government Contract Activity"
        subtitle="Track department spending, contract awards, vendors, and public-company exposure."
        actions={<div className="rounded-2xl border border-white/10 bg-slate-950/40 px-4 py-2 text-sm font-semibold text-slate-200">Fiscal Year / TTM</div>}
      />
      <MetricGrid metrics={data.summary} />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel
          title="Top Departments by Contract Value"
          rows={data.top_departments}
          empty="No department contract data available."
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
      <TimeSeriesPanel title="Contract Value Over Time" rows={data.contract_value_over_time} empty="No contract value trend available." />
      <section className="grid min-w-0 gap-3 xl:grid-cols-3">
        <DataPanel title="Largest Recent Awards" rows={data.largest_recent_awards} empty="No recent contract awards available." columns={[{ key: "company", label: "Vendor", format: "link" }, { key: "department", label: "Department" }, { key: "value", label: "Value", align: "right", format: "currency" }, { key: "date", label: "Date", align: "right", format: "date" }]} />
        <DataPanel title="Fastest Growing Vendors" rows={data.fastest_growing_vendors} empty="No vendor growth data available." columns={[{ key: "company", label: "Vendor", format: "link" }, { key: "current_value", label: "Current", align: "right", format: "currency" }, { key: "previous_value", label: "Previous", align: "right", format: "currency" }, { key: "increase_value", label: "Increase", align: "right", format: "currency" }]} />
        <DataPanel title="Most Active Departments" rows={data.most_active_departments} empty="No department activity available." columns={[{ key: "name", label: "Department", format: "link" }, { key: "contracts", label: "Contracts", align: "right", format: "number" }, { key: "contract_value", label: "Value", align: "right", format: "currency" }]} />
      </section>
    </div>
  );
}
