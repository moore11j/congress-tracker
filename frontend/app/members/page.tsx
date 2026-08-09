import type { Metadata } from "next";
import { CongressProfilesDashboard } from "@/components/profiles/CongressProfilesDashboard";
import type { Chamber } from "@/components/profiles/CongressProfilesDashboard";
import { appPageMetadata } from "@/lib/marketingMetadata";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/members", {
  title: "Congress Stock Trading & Member Portfolios | Walnut Markets",
  description: "Track disclosed trades, portfolio activity, and market positioning across members of Congress.",
});

function chamber(searchParams: SearchParams): Chamber {
  const value = typeof searchParams.chamber === "string" ? searchParams.chamber : "all";
  return value === "house" || value === "senate" ? value : "all";
}

export default async function MembersPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  return <CongressProfilesDashboard selectedChamber={chamber(sp)} />;
}
