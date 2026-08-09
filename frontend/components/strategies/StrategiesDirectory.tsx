"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import type { StrategyDefinitionPayload, StrategyListResponse, StrategyPerformanceSnapshot } from "@/lib/api";

type Props = {
  data: StrategyListResponse | null;
  category: string;
  period: string;
  sort: string;
};

const categories = [
  ["all", "Walnut Strategies"],
  ["congress", "Congress"],
  ["insider", "Insiders"],
  ["theme", "Themes"],
] as const;
const periods = ["30d", "1y", "2y", "3y", "max"] as const;
const sorts = [
  ["cagr", "Top CAGR"],
  ["return", "Top Return"],
  ["walnut_score", "Walnut Strategy Score"],
  ["alpha", "Best Excess vs SPY"],
  ["sharpe", "Highest Sharpe"],
  ["drawdown", "Lowest Drawdown"],
] as const;

function labelPeriod(value: string) {
  return { "30d": "30D", "1y": "1Y", "2y": "2Y", "3y": "3Y", max: "All" }[value] ?? value;
}

function formatPct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function formatNumber(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toFixed(2);
}

function statusLabel(status: string) {
  return status === "live" ? "Live" : status === "historical" ? "Historical" : status === "testing" ? "Testing" : status;
}

function performance(strategy: StrategyDefinitionPayload) {
  return strategy.performance as StrategyPerformanceSnapshot | null | undefined;
}

function MetricCard({ label, value, tone = "default" }: { label: string; value: string; tone?: "default" | "good" }) {
  return (
    <div className="min-w-0 rounded-lg border border-white/10 bg-slate-950/35 px-4 py-3">
      <div className="text-xs font-medium text-slate-400">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${tone === "good" ? "text-emerald-300" : "text-white"}`}>{value}</div>
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const isLive = status === "live";
  return (
    <span className={`inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${isLive ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100" : "border-sky-300/25 bg-sky-300/10 text-sky-100"}`}>
      {statusLabel(status)}
    </span>
  );
}

export function StrategiesDirectory({ data, category, period, sort }: Props) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const items = data?.items ?? [];
  const selected = items[0] ?? null;
  const returns = items.map((item) => performance(item)?.totalReturnPct).filter((value): value is number => typeof value === "number");
  const best = returns.length ? Math.max(...returns) : null;
  const beatingSpy = items.filter((item) => {
    const metrics = performance(item);
    return metrics?.totalReturnPct != null && metrics?.benchmarkReturnPct != null && metrics.totalReturnPct > metrics.benchmarkReturnPct;
  }).length;

  function setQuery(next: Record<string, string>) {
    const params = new URLSearchParams(searchParams.toString());
    Object.entries(next).forEach(([key, value]) => {
      if (!value || value === "all" || (key === "period" && value === "max")) params.delete(key);
      else params.set(key, value);
    });
    const query = params.toString();
    router.push(`/strategies${query ? `?${query}` : ""}`);
  }

  return (
    <div className="space-y-4 py-3 sm:py-5">
      <section className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Strategies</p>
          <h1 className="mt-2 text-3xl font-semibold text-white">Strategies</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">Track published Walnut model strategies across live and historical data.</p>
        </div>
        <Link href="/strategies/methodology" className="rounded-md border border-white/15 px-3 py-2 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:text-emerald-100">
          View methodology
        </Link>
      </section>

      <section className="rounded-lg border border-sky-300/20 bg-sky-300/[0.06] px-4 py-3 text-sm text-slate-300">
        <span className="font-semibold text-sky-100">Coverage note. </span>
        Institutional history is still backfilling. Published strategy rankings exclude incomplete institutional history and will expand as coverage improves.
      </section>

      <section className="flex min-w-0 overflow-x-auto rounded-lg border border-white/10 bg-slate-950/40 p-1 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
        {categories.map(([value, label]) => {
          const active = category === value || (!category && value === "all");
          return (
            <button key={value} type="button" onClick={() => setQuery({ category: value })} className={`shrink-0 rounded-md px-3 py-2 text-sm font-semibold ${active ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>
              {label}
            </button>
          );
        })}
      </section>

      <section className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        <label className="text-xs font-medium text-slate-400">Timeframe
          <select value={period} onChange={(event) => setQuery({ period: event.target.value })} className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50">
            {periods.map((value) => <option key={value} value={value}>{labelPeriod(value)}</option>)}
          </select>
        </label>
        <label className="text-xs font-medium text-slate-400">Benchmark
          <select disabled value="SPY" className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-300"><option>SPY</option></select>
        </label>
        <label className="text-xs font-medium text-slate-400">Status
          <select disabled value="published" className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-300"><option>Published</option></select>
        </label>
        <label className="text-xs font-medium text-slate-400">Sort
          <select value={sort} onChange={(event) => setQuery({ sort: event.target.value })} className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50">
            {sorts.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
          </select>
        </label>
      </section>

      <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <MetricCard label="Published strategies" value={String(items.length)} />
        <MetricCard label={`Best ${labelPeriod(period)} return`} value={formatPct(best)} tone="good" />
        <MetricCard label={`Median ${labelPeriod(period)} return`} value={returns.length ? formatPct([...returns].sort((a, b) => a - b)[Math.floor(returns.length / 2)]) : "--"} tone="good" />
        <MetricCard label="Beating SPY" value={items.length ? `${Math.round((beatingSpy / items.length) * 100)}%` : "--"} tone="good" />
        <MetricCard label="Live strategies" value={String(items.filter((item) => item.status === "live").length)} />
      </section>

      {items.length ? (
        <section className="grid gap-4 xl:grid-cols-[minmax(0,1.55fr)_minmax(19rem,0.85fr)]">
          <div className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/35">
            <div className="border-b border-white/10 px-4 py-3"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Top strategies</h2></div>
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="border-b border-white/10 bg-slate-950/65 text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-4 py-3">Strategy</th><th className="px-3 py-3">Type</th><th className="px-3 py-3">Return</th><th className="px-3 py-3">CAGR</th><th className="px-3 py-3">Excess vs SPY</th><th className="px-3 py-3">Max drawdown</th><th className="px-4 py-3">Status</th></tr></thead>
                <tbody className="divide-y divide-white/10">
                  {items.map((strategy) => {
                    const metrics = performance(strategy);
                    const positive = (metrics?.totalReturnPct ?? 0) >= 0;
                    return <tr key={strategy.slug} className="cursor-pointer transition hover:bg-white/[0.035]" onClick={() => router.push(`/strategies/${strategy.slug}`)}>
                      <td className="px-4 py-3"><div className="font-semibold text-white">{strategy.name}</div><div className="mt-1 max-w-xs truncate text-xs text-slate-500">{strategy.shortDescription ?? "Model strategy"}</div></td>
                      <td className="px-3 py-3 capitalize text-slate-300">{strategy.category.replace("_", " ")}</td>
                      <td className={`px-3 py-3 font-semibold tabular-nums ${positive ? "text-emerald-300" : "text-rose-300"}`}>{formatPct(metrics?.totalReturnPct)}</td>
                      <td className="px-3 py-3 tabular-nums text-slate-200">{formatPct(metrics?.cagrPct)}</td>
                      <td className="px-3 py-3 tabular-nums text-slate-200">{formatPct(metrics?.alphaCagrPct)}</td>
                      <td className="px-3 py-3 tabular-nums text-slate-200">{formatPct(metrics?.maxDrawdownPct)}</td>
                      <td className="px-4 py-3"><StatusPill status={strategy.status} /></td>
                    </tr>;
                  })}
                </tbody>
              </table>
            </div>
          </div>
          {selected ? <aside className="rounded-lg border border-white/10 bg-slate-950/35 p-4">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">Selected strategy</p>
            <h2 className="mt-2 text-xl font-semibold text-white">{selected.name}</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">{selected.shortDescription}</p>
            <dl className="mt-5 grid grid-cols-2 gap-x-4 gap-y-3 text-sm"><div><dt className="text-slate-500">Universe</dt><dd className="mt-1 text-slate-200">{String(selected.universe?.source ?? "Published universe")}</dd></div><div><dt className="text-slate-500">Benchmark</dt><dd className="mt-1 text-slate-200">{selected.latestRun?.benchmark ?? "SPY"}</dd></div><div><dt className="text-slate-500">Start date</dt><dd className="mt-1 text-slate-200">{selected.latestRun?.backtestStartDate ?? "--"}</dd></div><div><dt className="text-slate-500">Confidence</dt><dd className="mt-1 capitalize text-slate-200">{selected.dataQualityConfidence}</dd></div></dl>
            <Link href={`/strategies/${selected.slug}`} className="mt-5 inline-flex rounded-md bg-emerald-400/15 px-3 py-2 text-sm font-semibold text-emerald-100 ring-1 ring-emerald-300/30 transition hover:bg-emerald-400/25">View strategy</Link>
          </aside> : null}
        </section>
      ) : (
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1.55fr)_minmax(19rem,0.85fr)]">
          <div className="space-y-4">
            <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/35"><div className="flex items-center justify-between border-b border-white/10 px-4 py-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Top strategies</h2><p className="mt-1 text-xs text-slate-500">Published strategies ranked on compatible, stored performance periods.</p></div><span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold text-slate-400">0 published</span></div><div className="overflow-x-auto"><table className="min-w-full text-left text-sm"><thead className="border-b border-white/10 bg-slate-950/60 text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-4 py-3">Strategy</th><th className="px-3 py-3">Type</th><th className="px-3 py-3">Universe</th><th className="px-3 py-3">Return</th><th className="px-3 py-3">CAGR</th><th className="px-3 py-3">Excess vs SPY</th><th className="px-4 py-3">Status</th></tr></thead></table></div><div className="flex min-h-52 flex-col items-center justify-center px-6 py-8 text-center"><span className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-emerald-300/25 bg-emerald-300/[0.07] text-lg text-emerald-200">+</span><h3 className="mt-3 font-semibold text-white">No strategies are published</h3><p className="mt-2 max-w-md text-sm leading-6 text-slate-400">Walnut will add rows here only after a candidate has passed data-quality, reproducibility, and methodology review.</p><Link href="/strategies/methodology" className="mt-4 text-sm font-semibold text-emerald-300 hover:text-emerald-200">Review methodology</Link></div></section>
            <section className="grid gap-3 md:grid-cols-3"><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><span className="h-2 w-2 rounded-full bg-emerald-300" /><h2 className="mt-3 text-sm font-semibold text-white">Point-in-time inputs</h2><p className="mt-2 text-sm leading-6 text-slate-400">No current score is applied retroactively to a historical strategy.</p></div><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><span className="h-2 w-2 rounded-full bg-sky-300" /><h2 className="mt-3 text-sm font-semibold text-white">Comparable benchmarks</h2><p className="mt-2 text-sm leading-6 text-slate-400">Strategy and SPY results use the same stored date range.</p></div><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><span className="h-2 w-2 rounded-full bg-amber-300" /><h2 className="mt-3 text-sm font-semibold text-white">Institutional coverage</h2><p className="mt-2 text-sm leading-6 text-slate-400">13F-dependent strategies remain in review while history is backfilled.</p></div></section>
          </div>
          <aside className="space-y-4"><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><div className="flex items-center justify-between gap-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Performance vs SPY</h2><p className="mt-1 text-xs text-slate-500">Select a published strategy to compare stored performance.</p></div><span className="rounded-md border border-white/10 px-2 py-1 text-xs text-slate-500">Awaiting data</span></div><div className="mt-4 grid h-44 grid-cols-6 grid-rows-4 border-l border-b border-white/[0.08] bg-[linear-gradient(to_right,rgba(148,163,184,0.06)_1px,transparent_1px),linear-gradient(to_bottom,rgba(148,163,184,0.06)_1px,transparent_1px)] bg-[size:16.666%_25%]"><div className="col-span-full row-span-full flex items-center justify-center px-6 text-center text-sm leading-6 text-slate-500">A real strategy equity curve will appear here after publication.</div></div></section><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Publication standards</h2><ol className="mt-4 space-y-3 text-sm leading-6 text-slate-400"><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">1</span>Point-in-time rules and a reproducible run.</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">2</span>Adequate history, sample size, and data confidence.</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">3</span>Reviewed methodology, limitations, and current-holding freshness.</li></ol></section></aside>
        </div>
      )}
    </div>
  );
}
