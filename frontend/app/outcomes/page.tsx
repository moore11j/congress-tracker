import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { OutcomeLedgerClient } from "@/components/outcomes/OutcomeLedgerClient";
import { getOutcomeLedgerOverview, getOutcomeSnapshots } from "@/lib/api";

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

export default async function OutcomesPage({ searchParams }: { searchParams: Promise<{ ticker?: string }> }) {
  const requested = (await searchParams).ticker;
  const initialTicker = requested && /^[A-Za-z0-9.^-]{1,15}$/.test(requested) ? requested.toUpperCase() : undefined;
  if (process.env.NEXT_PUBLIC_OUTCOMES_LEDGER_ENABLED === "0") notFound();
  try {
    const overview = await getOutcomeLedgerOverview({ limit: 500, horizons: "30D,7D" });
    return <OutcomeLedgerClient key={initialTicker || "all"} initialTicker={initialTicker} initialStatus={overview.status} initialSummary={overview.summaries["30D"] ?? null} initialSnapshots={initialTicker ? await getOutcomeSnapshots({ ticker: initialTicker, limit: 500, horizon: "30D" }) : overview.snapshots} />;
  } catch {
    return <OutcomeLedgerClient key={initialTicker || "all"} initialTicker={initialTicker} initialStatus={null} initialSummary={null} initialSnapshots={null} />;
  }
}
