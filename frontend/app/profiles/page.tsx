import type { Metadata } from "next";
import { ProfilesOverviewDashboard } from "@/components/profiles/ProfilesOverviewDashboard";
import { appPageMetadata } from "@/lib/marketingMetadata";

export const metadata: Metadata = appPageMetadata("/profiles", {
  title: "Investor & Government Profiles | Walnut Markets",
  description: "Explore Walnut Markets profiles for Congress, insiders, institutions, and government departments.",
});

export default function ProfilesPage() {
  return <ProfilesOverviewDashboard />;
}
