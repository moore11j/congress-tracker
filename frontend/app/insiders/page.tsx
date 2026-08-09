import type { Metadata } from "next";
import { InsiderProfilesDashboard } from "@/components/profiles/InsiderProfilesDashboard";
import type { InsiderPeriod } from "@/components/profiles/InsiderProfilesDashboard";
import { appPageMetadata } from "@/lib/marketingMetadata";

type SearchParams = Record<string, string | string[] | undefined>;

export const metadata: Metadata = appPageMetadata("/insiders", {
  title: "Insider Trading Activity & Corporate Insider Purchases | Walnut Markets",
  description: "Track purchases and sales from executives, directors, and major shareholders.",
});

function period(searchParams: SearchParams): InsiderPeriod {
  return searchParams.period === "90" ? "90" : "ttm";
}

export default async function InsidersPage({ searchParams }: { searchParams?: Promise<SearchParams> }) {
  const sp = (await searchParams) ?? {};
  return <InsiderProfilesDashboard selectedPeriod={period(sp)} />;
}
