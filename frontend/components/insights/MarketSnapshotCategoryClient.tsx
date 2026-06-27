"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { getInsightsCategoryNews, getInsightsMacroSnapshot, getInsightsOverview } from "@/lib/api";
import {
  applyInsightsOverview,
  deltaClassName,
  formatSnapshotUpdatedAt,
  marketSnapshotDetailRows,
  snapshotAsOf,
  type MarketSnapshotCategory,
  type MarketSnapshotCategorySlug,
} from "@/lib/marketSnapshot";
import type { InsightsNewsResponse, MacroSnapshotResponse } from "@/lib/types";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { cardClassName, ghostButtonClassName } from "@/lib/styles";

type Props = {
  category: MarketSnapshotCategory;
};

const EMPTY_SNAPSHOT: MacroSnapshotResponse = {
  world_indexes: [],
  indexes: [],
  treasury: [],
  economics: [],
  commodities: [],
  currencies: [],
  crypto: [],
  sector_performance: [],
  status: "loading",
  generated_at: new Date().toISOString(),
};

function CategorySkeleton({ category }: Props) {
  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">INSIGHTS</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">{category.title}</h1>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">{category.description}</p>
          </div>
          <div className="h-10 w-32 animate-pulse rounded-lg bg-white/10" />
        </div>
      </section>
      <section className={cardClassName}>
        <div className="space-y-3">
          {Array.from({ length: 7 }).map((_, index) => (
            <div key={index} className="grid gap-3 rounded-lg border border-white/10 bg-white/[0.03] p-4 sm:grid-cols-[minmax(0,1.5fr)_0.8fr_0.8fr_0.8fr]">
              <div className="h-5 animate-pulse rounded bg-white/10" />
              <div className="h-5 animate-pulse rounded bg-white/10" />
              <div className="h-5 animate-pulse rounded bg-white/10" />
              <div className="h-5 animate-pulse rounded bg-white/10" />
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function NewsSkeleton() {
  return (
    <div className="space-y-3">
      {Array.from({ length: 5 }).map((_, index) => (
        <div key={index} className="rounded-2xl border border-white/10 bg-slate-950/55 px-4 py-3">
          <div className="h-4 w-3/4 animate-pulse rounded bg-white/10" />
          <div className="mt-3 h-3 w-1/2 animate-pulse rounded bg-white/10" />
          <div className="mt-3 h-3 w-full animate-pulse rounded bg-white/10" />
        </div>
      ))}
    </div>
  );
}

function DesktopRows({ rows }: { rows: ReturnType<typeof marketSnapshotDetailRows> }) {
  return (
    <div className="hidden overflow-hidden rounded-xl border border-white/10 sm:block">
      <table className="w-full border-collapse text-left text-sm">
        <thead className="bg-slate-950/70 text-[11px] uppercase tracking-[0.18em] text-slate-500">
          <tr>
            <th className="px-4 py-3 font-semibold">Name</th>
            <th className="px-4 py-3 font-semibold">Symbol</th>
            <th className="px-4 py-3 text-right font-semibold">Latest Value</th>
            <th className="px-4 py-3 text-right font-semibold">Change</th>
            <th className="px-4 py-3 text-right font-semibold">Date</th>
            <th className="px-4 py-3 text-right font-semibold">Unit</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10">
          {rows.map((row) => (
            <tr key={row.id} className="bg-slate-950/35">
              <td className="px-4 py-4">
                <div className={`font-semibold ${row.unavailable ? "text-slate-400" : "text-slate-100"}`}>{row.name}</div>
              </td>
              <td className="px-4 py-4 font-mono text-xs text-slate-500">{row.symbol ?? "-"}</td>
              <td className={`px-4 py-4 text-right font-semibold tabular-nums ${row.unavailable ? "text-slate-500" : "text-slate-100"}`}>{row.valueText}</td>
              <td className={`px-4 py-4 text-right font-semibold tabular-nums ${deltaClassName(row.changeValue)}`}>{row.changeText}</td>
              <td className="px-4 py-4 text-right text-slate-400">{row.dateText}</td>
              <td className="px-4 py-4 text-right text-slate-500">{row.unitText ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MobileRows({ rows }: { rows: ReturnType<typeof marketSnapshotDetailRows> }) {
  return (
    <div className="space-y-3 sm:hidden">
      {rows.map((row) => (
        <article key={row.id} className="rounded-xl border border-white/10 bg-slate-950/45 p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <h2 className={`truncate text-sm font-semibold ${row.unavailable ? "text-slate-400" : "text-slate-100"}`}>{row.name}</h2>
              <p className="mt-1 font-mono text-xs text-slate-500">{row.symbol ?? row.unitText ?? "-"}</p>
            </div>
            <div className="shrink-0 text-right">
              <p className={`text-sm font-semibold tabular-nums ${row.unavailable ? "text-slate-500" : "text-slate-100"}`}>{row.valueText}</p>
              <p className={`mt-1 text-xs font-semibold tabular-nums ${deltaClassName(row.changeValue)}`}>{row.changeText}</p>
            </div>
          </div>
          <div className="mt-4 flex items-center justify-between gap-3 border-t border-white/10 pt-3 text-xs text-slate-500">
            <span>{row.dateText}</span>
            <span>{row.unitText ?? "Daily Change"}</span>
          </div>
        </article>
      ))}
    </div>
  );
}

export function MarketSnapshotCategoryClient({ category }: Props) {
  const [snapshot, setSnapshot] = useState<MacroSnapshotResponse | null>(null);
  const [news, setNews] = useState<InsightsNewsResponse | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    Promise.allSettled([
      getInsightsMacroSnapshot({ signal: controller.signal }),
      getInsightsOverview({ signal: controller.signal }),
    ])
      .then(([snapshotResult, overviewResult]) => {
        if (controller.signal.aborted) return;
        const base = snapshotResult.status === "fulfilled" ? snapshotResult.value : { ...EMPTY_SNAPSHOT, status: "unavailable" };
        const merged = overviewResult.status === "fulfilled" ? applyInsightsOverview(base, overviewResult.value) : base;
        setSnapshot(merged);
        setFailed(snapshotResult.status === "rejected" && overviewResult.status === "rejected");
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setSnapshot({ ...EMPTY_SNAPSHOT, status: "unavailable" });
          setFailed(true);
        }
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    setNews(null);
    getInsightsCategoryNews(category.slug, { page: 0, limit: 20, signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setNews(payload);
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setNews({
            items: [],
            status: "warming",
            message: "Headlines are warming. Check back shortly.",
            page: 0,
            limit: 20,
            has_next: false,
          });
        }
      });
    return () => controller.abort();
  }, [category.slug]);

  const rows = useMemo(
    () => (snapshot ? marketSnapshotDetailRows(snapshot, category.slug as MarketSnapshotCategorySlug) : []),
    [category.slug, snapshot],
  );

  if (!snapshot) return <CategorySkeleton category={category} />;

  const updatedLabel = formatSnapshotUpdatedAt(snapshotAsOf(snapshot));

  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">INSIGHTS</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">{category.title}</h1>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">{category.description}</p>
            {updatedLabel ? <p className="mt-3 text-xs text-slate-500">{updatedLabel}</p> : null}
          </div>
          <Link href="/insights" className={ghostButtonClassName}>
            Back to Insights
          </Link>
        </div>
      </section>

      {failed ? <div className="rounded-lg border border-rose-300/20 bg-rose-400/10 px-3 py-2 text-sm text-rose-100">Market snapshot is temporarily unavailable.</div> : null}

      <section className={cardClassName}>
        <div className="mb-5 flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-white">Expanded Snapshot</h2>
            <p className="mt-1 text-sm text-slate-400">{category.subtitle}</p>
          </div>
          <p className="text-xs text-slate-500">{rows.length} items</p>
        </div>

        {rows.length === 0 ? (
          <p className="rounded-xl border border-white/10 bg-slate-950/45 p-4 text-sm text-slate-400">No market snapshot data is available for this category right now.</p>
        ) : (
          <>
            <DesktopRows rows={rows} />
            <MobileRows rows={rows} />
          </>
        )}
      </section>

      <section className={cardClassName}>
        <div className="mb-5">
          <h2 className="text-lg font-semibold text-white">{category.title} Headlines</h2>
          <p className="mt-1 text-sm text-slate-400">Recent headlines connected to this market view.</p>
        </div>
        {news ? (
          <NewsArticleList
            items={news.items}
            status={news.status}
            message={news.message}
            emptyMessage={`No recent ${category.title.toLowerCase()} headlines found.`}
            showImage={false}
            compact
          />
        ) : (
          <NewsSkeleton />
        )}
      </section>
    </div>
  );
}
