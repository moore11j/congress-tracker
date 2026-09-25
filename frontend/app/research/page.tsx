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
    description: "Read stock research, company comparisons, institutional ownership analysis, and government-contract research with source links and reporting dates.",
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
            Research company fundamentals, reported ownership changes, and government-contract exposure. Each brief should be read with its publication date, source documents, and data limitations in view.
          </p>
          <p className="mt-2 text-xs leading-5 text-slate-400">
            Learn how we use sources and AI, attribute research, and handle corrections in our{" "}
            <Link href={appCanonicalUrl("/editorial-policy")} className="text-emerald-200 hover:underline">Editorial Policy</Link>.
          </p>
        </div>
      </div>

      {page === 1 ? (
        <nav aria-label="Research by topic" className="grid gap-3 sm:grid-cols-2">
          <Link href="/government-contracts" className="rounded-lg border border-white/10 bg-slate-950/55 p-4 transition hover:border-emerald-300/40">
            <h2 className="font-semibold text-white">Government contracts and public companies</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Explore NASA, Defense, and Energy awards, recipient tickers, and how to interpret contract values.</p>
          </Link>
          <Link href="/institutional-filings" className="rounded-lg border border-white/10 bg-slate-950/55 p-4 transition hover:border-emerald-300/40">
            <h2 className="font-semibold text-white">Institutional ownership and 13F filings</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Understand reported holder changes, share counts, and the gap between quarter-end positions and filing dates.</p>
          </Link>
        </nav>
      ) : null}

      <ResearchBriefsSection mode="archive" page={page} />
    </div>
  );
}
