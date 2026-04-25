import Link from "next/link";
import { MarketSnapshot } from "@/components/insights/MarketSnapshot";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { getInsightsMacroSnapshot, getInsightsNews } from "@/lib/api";
import { cardClassName } from "@/lib/styles";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export const metadata = {
  title: "Insights | Capitol Ledger",
  description: "Market headlines and company-level news connected to your intelligence workflow.",
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function pageHref(page: number): string {
  return page <= 0 ? "/insights" : `/insights?page=${page}`;
}

export default async function InsightsPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const page = Math.max(Number.parseInt(one(sp, "page") || "0", 10) || 0, 0);
  const limit = 20;

  const [snapshot, response] = await Promise.all([
    getInsightsMacroSnapshot().catch(() => ({
      indexes: [],
      treasury: [],
      economics: [],
      sector_performance: [],
      status: "unavailable" as const,
      generated_at: new Date().toISOString(),
    })),
    getInsightsNews({ page, limit }).catch(() => ({
      items: [],
      status: "unavailable" as const,
      message: "News data is unavailable from the current provider.",
      page,
      limit,
      has_next: false,
    })),
  ]);

  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insights</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Insights</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">
          Market headlines and company-level news connected to your intelligence workflow.
        </p>
      </section>

      <MarketSnapshot snapshot={snapshot} />

      <section className={cardClassName}>
        <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Market Headlines</p>
            <p className="mt-2 text-sm text-slate-400">A restrained market news feed built for discovery, not blog noise.</p>
          </div>
          <p className="text-xs text-slate-500">Page {response.page + 1}</p>
        </div>

        <NewsArticleList
          items={response.items}
          status={response.status}
          message={response.message}
          emptyMessage="No recent market news found."
          showImage
          compact={false}
        />

        <div className="mt-6 flex items-center justify-between gap-3">
          <Link
            href={pageHref(Math.max(page - 1, 0))}
            prefetch={false}
            aria-disabled={page === 0}
            className={`rounded-2xl border px-4 py-2 text-sm font-semibold ${
              page === 0
                ? "pointer-events-none border-white/10 bg-slate-950/40 text-slate-600"
                : "border-white/10 bg-slate-950/60 text-slate-200 hover:text-white"
            }`}
          >
            Previous
          </Link>
          <Link
            href={pageHref(page + 1)}
            prefetch={false}
            aria-disabled={!response.has_next}
            className={`rounded-2xl border px-4 py-2 text-sm font-semibold ${
              response.has_next
                ? "border-emerald-300/30 bg-emerald-400/10 text-emerald-100 hover:bg-emerald-400/15"
                : "pointer-events-none border-white/10 bg-slate-950/40 text-slate-600"
            }`}
          >
            Next
          </Link>
        </div>
      </section>
    </div>
  );
}
