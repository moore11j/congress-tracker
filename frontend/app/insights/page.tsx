import { InsightsMarketSnapshotClient } from "@/components/insights/InsightsMarketSnapshotClient";
import { InsightsMacroPositioningClient } from "@/components/insights/InsightsMacroPositioningClient";
import { InsightsNewsClient } from "@/components/insights/InsightsNewsClient";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export const metadata = {
  title: "Insights | Walnut Market Terminal",
  description: "Market headlines and company-level news connected to your intelligence workflow.",
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

export default async function InsightsPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const page = Math.max(Number.parseInt(one(sp, "page") || "0", 10) || 0, 0);
  const limit = 20;

  return (
    <div className="w-full max-w-[calc(100vw-2rem)] space-y-6 sm:max-w-[calc(100vw-3rem)] lg:max-w-none">
      <InsightsMarketSnapshotClient />
      <InsightsMacroPositioningClient />
      <InsightsNewsClient page={page} limit={limit} />
    </div>
  );
}
