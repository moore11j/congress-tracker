import type { Metadata } from "next";
import { InstitutionProfilesDashboard } from "@/components/profiles/InstitutionProfilesDashboard";
import { appPageMetadata } from "@/lib/marketingMetadata";

export const metadata: Metadata = appPageMetadata("/institutions", {
  title: "Institutional Holdings & 13F Position Changes | Walnut Markets",
  description: "Track institutional portfolios, quarterly position changes, accumulation, and sector exposure.",
});

export default function InstitutionsPage() {
  return <InstitutionProfilesDashboard />;
}
