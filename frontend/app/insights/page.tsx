import { InsightsMarketSnapshotClient } from "@/components/insights/InsightsMarketSnapshotClient";
import { InsightsMacroPositioningPanel } from "@/components/insights/InsightsMacroPositioningPanel";
import { InsightsNewsClient } from "@/components/insights/InsightsNewsClient";
import { ResearchBriefsSection } from "@/components/insights/ResearchBriefsSection";
import { appPageMetadata } from "@/lib/marketingMetadata";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export const metadata = {
  ...appPageMetadata("/insights", {
    title: "Stock Market Insights & Research Briefs | Walnut Markets",
    description: "Market headlines, company-level news, macro context, and Walnut research briefs connected to a stock research workflow.",
  }),
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

export default async function InsightsPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const page = Math.max(Number.parseInt(one(sp, "page") || "0", 10) || 0, 0);
  const limit = 12;

  return (
    <div className="w-full max-w-[calc(100vw-2rem)] space-y-4 sm:max-w-[calc(100vw-3rem)] lg:max-w-none">
      <InsightsMarketSnapshotClient />
      <div className="grid gap-4 xl:grid-cols-[minmax(0,2.1fr)_minmax(22rem,0.9fr)]">
        <InsightsNewsClient page={page} limit={limit} />
        <InsightsMacroPositioningPanel />
      </div>
      <ResearchBriefsSection />
    </div>
  );
}
