import Link from "next/link";
import type { ResearchBriefCard } from "@/lib/researchBriefs";
import { BRIEFS_PER_PAGE, loadResearchArchive, researchArchiveHref } from "@/lib/researchArchive";
import { marketingCanonicalUrl } from "@/lib/marketingMetadata";
import { notFound } from "next/navigation";

type ResearchBriefsSectionProps = {
  mode?: "preview" | "archive";
  page?: number;
};

function formatBriefDate(value: string): string {
  const date = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(date);
}

function judgmentClassName(judgment?: ResearchBriefCard["judgment"]): string {
  if (judgment === "bullish") return "text-emerald-300";
  if (judgment === "bearish") return "text-rose-300";
  if (judgment === "macro") return "text-cyan-300";
  if (judgment === "policy") return "text-amber-300";
  return "text-slate-300";
}

function briefKickerClassName(brief: ResearchBriefCard): string {
  return brief.premium ? "text-emerald-300" : judgmentClassName(brief.judgment);
}

function judgmentLabel(brief: ResearchBriefCard): string {
  if (brief.premium) return brief.requiredPlan === "pro" ? "Pro" : "Premium";
  if (brief.judgment === "bullish") return "Bullish";
  if (brief.judgment === "bearish") return "Bearish";
  if (brief.judgment === "macro") return "Macro";
  if (brief.judgment === "policy") return "Policy";
  if (brief.judgment === "mixed") return "Mixed";
  return brief.category;
}

function BriefVisual({ brief }: { brief: ResearchBriefCard }) {
  return (
    <div className="absolute inset-0 overflow-hidden">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_22%_12%,rgba(45,212,191,0.22),transparent_28%),linear-gradient(135deg,rgba(8,47,73,0.82),rgba(2,6,23,0.96)_58%,rgba(6,78,59,0.45))]" />
      <div className="absolute -right-8 top-6 h-36 w-36 rounded-full border border-emerald-300/15 bg-emerald-300/5" />
      <div className="absolute bottom-0 right-0 grid h-32 w-44 grid-cols-6 gap-1.5 p-5 opacity-40">
        {Array.from({ length: 30 }).map((_, index) => (
          <span
            key={index}
            className={`rounded-sm ${index % 5 === 0 || index % 7 === 0 ? "bg-emerald-300/80" : "bg-cyan-300/25"}`}
            style={{ height: `${18 + ((index * 11) % 54)}%`, alignSelf: "end" }}
          />
        ))}
      </div>
    </div>
  );
}

function BriefCard({ brief }: { brief: ResearchBriefCard }) {
  return (
    <Link
      href={marketingCanonicalUrl(brief.route)}
      prefetch={false}
      className="group relative flex min-h-[15rem] min-w-0 overflow-hidden rounded-lg border border-white/10 bg-slate-950/60 transition hover:border-emerald-300/35 hover:bg-slate-950/75 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/25"
    >
      <BriefVisual brief={brief} />
      <div className="relative z-10 flex w-full flex-col justify-between bg-gradient-to-r from-slate-950/94 via-slate-950/72 to-slate-950/20 p-4">
        <div>
          <span className={`text-[10px] font-semibold uppercase tracking-[0.14em] ${briefKickerClassName(brief)}`}>
            {judgmentLabel(brief)}
          </span>
          <h3 className="mt-4 max-w-[14rem] text-lg font-semibold leading-6 text-white transition group-hover:text-emerald-100">
            {brief.title}
          </h3>
          <p className="mt-3 max-w-[18rem] text-sm leading-6 text-slate-300 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">
            {brief.description}
          </p>
          <div className="mt-4 flex flex-wrap gap-2">
            {brief.premium ? (
              <span className="rounded-md border border-emerald-300/25 bg-emerald-300/10 px-2.5 py-1 text-xs font-semibold text-emerald-100">
                {brief.requiredPlan === "pro" ? "Pro" : "Premium"}
              </span>
            ) : null}
            {brief.tickers.map((ticker) => (
              <span key={ticker} className="rounded-md border border-white/10 bg-white/[0.04] px-2.5 py-1 text-xs font-semibold text-slate-200">
                {ticker}
              </span>
            ))}
          </div>
        </div>
        <div className="mt-5 flex items-center justify-between gap-3 text-xs text-slate-500">
          <span>
            {formatBriefDate(brief.publishedAt)} - {brief.readingMinutes} min read
          </span>
          <span className="font-semibold text-emerald-200 transition group-hover:text-emerald-100">Read brief</span>
        </div>
      </div>
    </Link>
  );
}

export async function ResearchBriefsSection({ mode = "preview", page = 1 }: ResearchBriefsSectionProps) {
  const isArchive = mode === "archive";
  const briefs = await loadResearchArchive();
  const totalPages = Math.max(1, Math.ceil(briefs.length / BRIEFS_PER_PAGE));
  if (!Number.isSafeInteger(page) || page < 1 || page > totalPages) notFound();
  const pageIndex = isArchive ? page - 1 : 0;
  const visibleBriefs = briefs.slice(pageIndex * BRIEFS_PER_PAGE, pageIndex * BRIEFS_PER_PAGE + BRIEFS_PER_PAGE);
  const canShowMore = pageIndex + 1 < totalPages;

  return (
    <section id="research-briefs" className="rounded-lg border border-white/10 bg-slate-950/55 p-4 shadow-[0_18px_60px_-42px_rgba(16,185,129,0.55)] sm:p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
          <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-100">Research Briefs</h2>
          <p className="text-sm text-slate-400">In-depth Walnut research and campaign analysis.</p>
        </div>
        {!isArchive ? (
          <Link
            href={researchArchiveHref(1)}
            className="inline-flex min-h-9 items-center rounded-md border border-white/10 px-3 py-1.5 text-sm font-semibold text-emerald-200 transition hover:border-emerald-300/35 hover:bg-emerald-300/10 hover:text-emerald-100"
          >
            Open Research Briefs -&gt;
          </Link>
        ) : null}
      </div>

      {briefs.length > 0 ? (
        <>
          {isArchive ? (
            <p className="mt-3 text-xs text-slate-500">
              Showing {pageIndex * BRIEFS_PER_PAGE + 1}-{Math.min((pageIndex + 1) * BRIEFS_PER_PAGE, briefs.length)} of {briefs.length} published briefs
            </p>
          ) : null}
          <div className={`mt-4 grid gap-4 ${visibleBriefs.length === 1 ? "max-w-[30rem]" : "sm:grid-cols-2 xl:grid-cols-3"}`}>
            {visibleBriefs.map((brief) => (
              <BriefCard key={brief.slug} brief={brief} />
            ))}
          </div>
          {briefs.length > BRIEFS_PER_PAGE ? (
            <nav aria-label="Research archive pagination" className="mt-5 flex flex-wrap items-center justify-center gap-3">
              {pageIndex > 0 ? (
                <Link
                  href={researchArchiveHref(page - 1)}
                  rel="prev"
                  className="inline-flex min-h-10 items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
                >
                  Previous
                </Link>
              ) : null}
              {canShowMore ? (
                <Link
                  href={researchArchiveHref(pageIndex + 2)}
                  rel="next"
                  className="inline-flex min-h-10 items-center justify-center rounded-lg border border-emerald-300/35 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
                >
                  More research briefs
                </Link>
              ) : null}
            </nav>
          ) : null}
        </>
      ) : (
        <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/45 px-4 py-5 text-sm text-slate-400">
          No research briefs are published yet.
        </div>
      )}
    </section>
  );
}
