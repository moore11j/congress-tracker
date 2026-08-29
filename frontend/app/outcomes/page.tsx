import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { OutcomeLedgerClient } from "@/components/outcomes/OutcomeLedgerClient";
import { getOutcomeLedgerOverview } from "@/lib/api";

// Outcome data is provider-backed and can exceed the static build timeout.
// Render it at request time and retain the existing client fallback instead.
export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Outcome Ledger | Walnut Markets",
  description: "Track Walnut confirmation-score judgments as immutable point-in-time snapshots.",
  alternates: {
    canonical: "/outcomes",
  },
};

export default async function OutcomesPage() {
  if (process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0") notFound();
  try {
    const overview = await getOutcomeLedgerOverview({ limit: 100, horizons: "30D,7D" });
    return <OutcomeLedgerClient initialStatus={overview.status} initialSummary={overview.summaries["30D"] ?? null} initialSnapshots={overview.snapshots} />;
  } catch {
    return <OutcomeLedgerClient initialStatus={null} initialSummary={null} initialSnapshots={null} />;
  }
}
