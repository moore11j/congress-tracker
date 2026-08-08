import type { Metadata } from "next";
import { DataPanel, LockedPanel, MetricGrid, ProfilePageHeader, SectorStackedChart } from "@/components/profiles/ProfileLanding";
import { getInstitutionsOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const metadata: Metadata = appPageMetadata("/institutions", {
  title: "Institutional Holdings & 13F Position Changes | Walnut Markets",
  description: "Track institutional portfolios, quarterly position changes, accumulation, and sector exposure.",
});

export default async function InstitutionsPage() {
  const authState = await optionalPageAuthState();
  const data = await getInstitutionsOverview({ authToken: authState.token });
  const period = data.report_year && data.report_quarter ? `Q${data.report_quarter} ${data.report_year}` : "Latest available 13F quarter";

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="INSTITUTIONS"
        title="Institutional Holdings"
        subtitle="Track institutional portfolios, quarterly position changes, accumulation, and sector exposure."
        actions={<div className="rounded-2xl border border-white/10 bg-slate-950/40 px-4 py-2 text-sm font-semibold text-slate-200">{period}</div>}
      />
      <MetricGrid metrics={data.summary} />
      {data.locked ? <LockedPanel message={data.message} /> : null}
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel
          title="Top Institutions by Portfolio Value"
          rows={data.top_institutions}
          empty={data.locked ? "Upgrade to Pro to view institution rankings." : "No institutional filings available for this quarter."}
          cta={{ href: "/feed?mode=institutional", label: "View all institutions" }}
          columns={[
            { key: "name", label: "Institution", format: "link" },
            { key: "portfolio_value", label: "Portfolio Value", align: "right", format: "currency" },
            { key: "previous_value", label: "Previous Quarter", align: "right", format: "currency" },
            { key: "qoq_change", label: "QoQ Change", align: "right", format: "currency" },
            { key: "positions", label: "Positions", align: "right", format: "number" },
            { key: "largest_holding", label: "Largest Holding" },
          ]}
        />
        <DataPanel
          title="Top Increased Positions"
          rows={data.position_changes}
          empty={data.locked ? "Upgrade to Pro to view position changes." : "No position-change data available for this quarter."}
          cta={{ href: "/signals?mode=institutional", label: "View all increased positions" }}
          columns={[
            { key: "symbol", label: "Ticker", format: "link" },
            { key: "company", label: "Company" },
            { key: "current_value", label: "Current Value", align: "right", format: "currency" },
            { key: "previous_value", label: "Previous Value", align: "right", format: "currency" },
            { key: "increase_value", label: "Increase", align: "right", format: "currency" },
            { key: "institution_count", label: "Institutions", align: "right", format: "number" },
          ]}
        />
      </section>
      <SectorStackedChart title="Sector Exposure Over Time" rows={data.sector_exposure} note="Institutional filings disclose quarter-end holdings and may not reflect real-time trading." />
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel title="Most Widely Held Stocks" rows={data.most_widely_held} empty="No widely held stock data available." columns={[{ key: "symbol", label: "Ticker", format: "link" }, { key: "company", label: "Company" }, { key: "holders", label: "Holders", align: "right", format: "number" }, { key: "value", label: "Reported Value", align: "right", format: "currency" }]} />
        <DataPanel title="Recent 13F Filings" rows={data.recent_filings} empty="No recent 13F filings available." columns={[{ key: "name", label: "Institution", format: "link" }, { key: "report_period", label: "Report Period" }, { key: "form_type", label: "Form" }, { key: "filing_date", label: "Filing Date", align: "right", format: "date" }]} />
      </section>
      <section className="grid min-w-0 gap-3 xl:grid-cols-2">
        <DataPanel title="Largest New Positions" rows={data.largest_new_positions} empty="No new positions available for this quarter." columns={[{ key: "symbol", label: "Ticker", format: "link" }, { key: "company", label: "Company" }, { key: "current_value", label: "Current Value", align: "right", format: "currency" }, { key: "institution_count", label: "Institutions", align: "right", format: "number" }]} />
        <DataPanel title="Largest Exits" rows={data.largest_exits} empty="No exits available for this quarter." columns={[{ key: "symbol", label: "Ticker", format: "link" }, { key: "company", label: "Company" }, { key: "previous_value", label: "Previous Value", align: "right", format: "currency" }, { key: "institution_count", label: "Institutions", align: "right", format: "number" }]} />
      </section>
    </div>
  );
}
