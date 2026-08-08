import type { Metadata } from "next";
import { ProfileDirectoryGrid, ProfilePageHeader, SummaryCards } from "@/components/profiles/ProfileLanding";
import { getProfilesSummary } from "@/lib/api";
import { appPageMetadata } from "@/lib/marketingMetadata";
import { optionalPageAuthState } from "@/lib/serverAuth";

export const metadata: Metadata = appPageMetadata("/profiles", {
  title: "Investor & Government Profiles | Walnut Markets",
  description: "Explore Walnut Markets profiles for Congress, insiders, institutions, and government departments.",
});

export default async function ProfilesPage() {
  const authState = await optionalPageAuthState();
  const data = await getProfilesSummary({ authToken: authState.token });

  return (
    <div className="min-w-0 space-y-6 overflow-x-hidden">
      <ProfilePageHeader
        eyebrow="PROFILES"
        title="Follow the market's major players"
        subtitle="Track activity across Congress, corporate insiders, institutions, and government departments."
      />
      <SummaryCards cards={data.cards} />
      <ProfileDirectoryGrid directories={data.directories} />
    </div>
  );
}
