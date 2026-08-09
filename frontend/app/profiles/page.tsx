import type { Metadata } from "next";
import { EnhancedProfilesOverview } from "@/components/profiles/EnhancedProfileDashboards";
import { getProfilesSummary } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const metadata: Metadata = appPageMetadata("/profiles", {
  title: "Investor & Government Profiles | Walnut Markets",
  description: "Explore Walnut Markets profiles for Congress, insiders, institutions, and government departments.",
});

export default async function ProfilesPage() {
  const authState = await optionalPageAuthState();
  const data = await getProfilesSummary({ authToken: authState.token, include_activity: true, activity_limit: 25 });
  return <EnhancedProfilesOverview data={data} />;
}
