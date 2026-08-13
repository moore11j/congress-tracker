import type { Metadata } from "next";
import Link from "next/link";
import { ResearchBriefsSection } from "@/components/insights/ResearchBriefsSection";
import { marketingPageMetadata } from "@/lib/marketingMetadata";

export const metadata: Metadata = marketingPageMetadata("/research", {
  title: "Research Briefs | Walnut Markets",
  description: "Browse Walnut Markets research briefs, company comparisons, market DD, and campaign analysis.",
});

export default function ResearchBriefsPage() {
  return (
    <div className="w-full max-w-[calc(100vw-2rem)] space-y-4 sm:max-w-[calc(100vw-3rem)] lg:max-w-none">
      <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4 shadow-[0_18px_60px_-42px_rgba(16,185,129,0.55)] sm:p-5">
        <Link
          href="/insights"
          className="inline-flex min-h-9 items-center rounded-md border border-white/10 px-3 py-1.5 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:text-white"
        >
          Back to Insights
        </Link>
        <div className="mt-5 max-w-3xl">
          <h1 className="text-2xl font-semibold tracking-tight text-white sm:text-3xl">Research Briefs</h1>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            All published Walnut research briefs, including company deep dives, comparison notes, market DD, and campaign analysis.
          </p>
        </div>
      </div>

      <ResearchBriefsSection mode="archive" />
    </div>
  );
}
