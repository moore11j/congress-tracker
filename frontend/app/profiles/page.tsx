import type { Metadata } from "next";
import { ActivityFeed, ProfilePageHeader, SummaryCards } from "@/components/profiles/ProfileLanding";
import { getProfilesSummary } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/profiles", {
  title: "Investor & Government Profiles | Walnut Markets",
  description: "Explore Walnut Markets profiles for Congress, insiders, institutions, and government departments.",
});

function activityType(sp: SearchParams) {
  const value = typeof sp.type === "string" ? sp.type : "all";
  return ["all", "congress", "insiders", "institutions", "departments"].includes(value) ? value : "all";
}

export default async function ProfilesPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  const type = activityType(sp);
  const authState = await optionalPageAuthState();
  const data = await getProfilesSummary({ activity_type: type, activity_limit: 25, authToken: authState.token });

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="PROFILES"
        title="Follow the market's major players"
        subtitle="Track activity across Congress, corporate insiders, institutions, and government departments."
      />
      <SummaryCards cards={data.cards} />
      <ActivityFeed items={data.activity} activeType={type} />
    </div>
  );
}
