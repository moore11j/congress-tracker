import type { Metadata } from "next";
import { EnhancedInstitutionDashboard } from "@/components/profiles/EnhancedProfileDashboards";
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
  return <EnhancedInstitutionDashboard data={data} period={period} />;
}
