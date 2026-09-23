import type { Metadata } from "next";
import Link from "next/link";
import { ResearchBriefsSection } from "@/components/insights/ResearchBriefsSection";
import { appCanonicalUrl, marketingPageMetadata } from "@/lib/marketingMetadata";
import { researchArchivePage } from "@/lib/researchArchive";
import { notFound, redirect } from "next/navigation";

type Props = { searchParams?: Promise<Record<string, string | string[] | undefined>> };

export async function generateMetadata({ searchParams }: Props): Promise<Metadata> {
  const page = researchArchivePage((await searchParams)?.page);
  return marketingPageMetadata(`/research${page && page > 1 ? `?page=${page}` : ""}`, {
    title: `Research Briefs${page && page > 1 ? ` — Page ${page}` : ""} | Walnut Markets`,
    description: "Browse Walnut Markets research briefs, company comparisons, market DD, and campaign analysis.",
    ...(page === null ? { robots: { index: false, follow: true } } : {}),
  });
}

export default async function ResearchBriefsPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const page = researchArchivePage(sp.page);
  if (page === null) notFound();
  if (sp.page === "1") redirect("/research");
  return (
    <div className="w-full max-w-[calc(100vw-2rem)] space-y-4 sm:max-w-[calc(100vw-3rem)] lg:max-w-none">
      <div className="rounded-lg border border-white/10 bg-slate-950/55 p-4 shadow-[0_18px_60px_-42px_rgba(16,185,129,0.55)] sm:p-5">
        <Link
          href="https://app.walnutmarkets.com/insights"
          className="inline-flex min-h-9 items-center rounded-md border border-white/10 px-3 py-1.5 text-sm font-semibold text-slate-300 transition hover:border-white/20 hover:text-white"
        >
          Back to Insights
        </Link>
        <div className="mt-5 max-w-3xl">
          <h1 className="text-2xl font-semibold tracking-tight text-white sm:text-3xl">Research Briefs</h1>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            All published Walnut research briefs, including company deep dives, comparison notes, market DD, and campaign analysis.
          </p>
          <p className="mt-2 text-xs leading-5 text-slate-400">
            Learn how we use sources and AI, attribute research, and handle corrections in our{" "}
            <Link href={appCanonicalUrl("/editorial-policy")} className="text-emerald-200 hover:underline">Editorial Policy</Link>.
          </p>
        </div>
      </div>

      <ResearchBriefsSection mode="archive" page={page} />
    </div>
  );
}
