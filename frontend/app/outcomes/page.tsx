import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { OutcomeLedgerClient } from "@/components/outcomes/OutcomeLedgerClient";
import { getOutcomeLedgerStatus, getOutcomeSnapshots } from "@/lib/api";

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
    const [status, snapshots] = await Promise.all([
      getOutcomeLedgerStatus(),
      getOutcomeSnapshots({ limit: 25, calculation_type: "live" }),
    ]);
    return <OutcomeLedgerClient initialStatus={status} initialSnapshots={snapshots} />;
  } catch {
    return <OutcomeLedgerClient initialStatus={null} initialSnapshots={null} />;
  }
}
