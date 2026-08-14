import type { Metadata } from "next";
import { Suspense } from "react";
import { EnhancedInstitutionDashboard } from "@/components/profiles/EnhancedProfileDashboards";
import { ProfileDashboardSkeleton } from "@/components/profiles/ProfileDashboardSkeleton";
import { getInstitutionsOverview } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const metadata: Metadata = appPageMetadata("/institutions", {
  title: "Institutional Holdings & 13F Position Changes | Walnut Markets",
  description: "Track institutional portfolios, quarterly position changes, accumulation, and sector exposure.",
});

export default async function InstitutionsPage() {
  const authState = await optionalPageAuthState();
  return (
    <Suspense fallback={<ProfileDashboardSkeleton variant="institutions" />}>
      <InstitutionsDashboard authToken={authState.token} />
    </Suspense>
  );
}

async function InstitutionsDashboard({ authToken }: { authToken: string | null }) {
  const data = await getInstitutionsOverview({ authToken });
  const period = data.report_year && data.report_quarter ? `Q${data.report_quarter} ${data.report_year}` : "Latest available 13F quarter";
  return <EnhancedInstitutionDashboard data={data} period={period} />;
}
