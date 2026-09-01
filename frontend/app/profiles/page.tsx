import type { Metadata } from "next";
import { EnhancedProfilesOverview } from "@/components/profiles/EnhancedProfileDashboards";
import { ProfileDashboardUnavailable } from "@/components/profiles/ProfileDashboardUnavailable";
import { getProfilesSummary, type ProfileActivityItem } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const metadata: Metadata = appPageMetadata("/profiles", {
  title: "Investor & Government Profiles | Walnut Markets",
  description: "Explore Walnut Markets profiles for Congress, insiders, institutions, and government departments.",
});

export default async function ProfilesPage() {
  const authState = await optionalPageAuthState();
  const data = await getProfilesSummary({ authToken: authState.token, include_activity: true, activity_per_type: 5 }).catch(() =>
    getProfilesSummary({ include_activity: true, activity_per_type: 5 }).catch(() => null),
  );
  if (!data) return <ProfileDashboardUnavailable kind="profiles" />;
  return <EnhancedProfilesOverview data={{ ...data, activity: activityVisibleAcrossFilters(data.activity ?? []) }} />;
}

/**
 * LatestProfileActivity renders at most five records for All or for any one
 * activity-type tab. Keeping the first five records for every type preserves
 * every row that the existing UI can display while avoiding serializing the
 * remaining, unreachable activity rows into the React Server Component payload.
 */
function activityVisibleAcrossFilters(items: ProfileActivityItem[], perTypeLimit = 5): ProfileActivityItem[] {
  const counts = new Map<string, number>();
  return items.filter((item) => {
    // Do this before counting a type. Otherwise records the dashboard cannot
    // render usefully can consume the entire five-row allowance and leave the
    // activity widget with fewer current entries than are actually available.
    if (!item.profile || item.profile === "Profile unavailable") return false;
    const count = counts.get(item.type) ?? 0;
    if (count >= perTypeLimit) return false;
    counts.set(item.type, count + 1);
    return true;
  });
}
