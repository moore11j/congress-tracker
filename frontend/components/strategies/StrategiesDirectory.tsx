"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useMemo, useState } from "react";
import { BacktestChart } from "@/components/backtesting/BacktestChart";
import type { StrategyDefinitionPayload, StrategyDetailPayload, StrategyListResponse, StrategyPerformanceSnapshot } from "@/lib/api";
import { displayStrategyName, displayStrategyUniverse } from "@/lib/strategyPresentation";

type Props = {
  data: StrategyListResponse | null;
  featured: StrategyDetailPayload | null;
  category: string;
  period: string;
  sort: string;
};

const periods = ["30d", "1y", "2y", "3y", "max"] as const;
const sorts = [
  ["cagr", "Top CAGR"],
  ["return", "Top return"],
  ["walnut_score", "Walnut Strategy Score"],
  ["alpha", "Best excess vs SPY"],
  ["sharpe", "Highest Sharpe"],
  ["drawdown", "Lowest drawdown"],
] as const;

function labelPeriod(value: string) {
  return { "30d": "30D", "1y": "1Y", "2y": "2Y", "3y": "3Y", max: "All" }[value] ?? value;
}

function categoryLabel(value: string) {
  const labels: Record<string, string> = { congress: "Congress", insider: "Insiders", cross_source: "Cross-source", walnut: "Walnut", theme: "Themes" };
  return labels[value] ?? value.replaceAll("_", " ");
}

function formatPct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function formatDate(value: string | null | undefined) {
  if (!value) return "--";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString("en-US", { month: "short", year: "numeric" });
}

function performance(strategy: StrategyDefinitionPayload) {
  return strategy.performance as StrategyPerformanceSnapshot | null | undefined;
}

function validatedScore(strategy: StrategyDefinitionPayload) {
  const diagnostics = strategy.latestRun?.diagnostics;
  const validation = diagnostics?.validation as Record<string, unknown> | undefined;
  const score = validation?.walnut_strategy_score as Record<string, unknown> | undefined;
  return score?.score_version === "walnut_strategy_score_v2" && typeof score.score === "number" ? score.score : null;
}

function displayStatus(strategy: StrategyDefinitionPayload) {
  if (strategy.status === "live" || strategy.latestRun?.runType === "live") return "Live";
  if (strategy.status === "published") return "Backtested";
  return strategy.status.replaceAll("_", " ");
}

function ruleChips(strategy: StrategyDefinitionPayload) {
  const source = { ...(strategy.rule ?? {}), ...(strategy.parameters ?? {}) };
  return Object.entries(source)
    .filter(([, value]) => ["string", "number", "boolean"].includes(typeof value))
    .slice(0, 3)
    .map(([key, value]) => `${key.replaceAll("_", " ")}: ${String(value)}`);
}

function MetricCard({ label, value, tone = "default" }: { label: string; value: string; tone?: "default" | "good" }) {
  return <div className="min-w-0 rounded-lg border border-white/10 bg-slate-950/35 px-4 py-3"><div className="flex items-center gap-2"><span className={`h-7 w-7 rounded-full border ${tone === "good" ? "border-emerald-300/25 bg-emerald-300/[0.08]" : "border-slate-300/15 bg-white/[0.03]"}`} /><div className="text-xs font-medium text-slate-400">{label}</div></div><div className={`mt-2 text-2xl font-semibold tabular-nums ${tone === "good" ? "text-emerald-300" : "text-white"}`}>{value}</div></div>;
}

function StatusPill({ strategy }: { strategy: StrategyDefinitionPayload }) {
  const label = displayStatus(strategy);
  const live = label === "Live";
  return <span className={`inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${live ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100" : "border-sky-300/25 bg-sky-300/10 text-sky-100"}`}>{label}</span>;
}

function SortHeader({ label, value, active, onSelect }: { label: string; value: string; active: boolean; onSelect: (value: string) => void }) {
  return <button type="button" onClick={() => onSelect(value)} className={`inline-flex items-center gap-1 font-semibold transition hover:text-emerald-200 ${active ? "text-emerald-200" : "text-slate-500"}`}>{label}<span aria-hidden="true">{active ? "↓" : "↕"}</span></button>;
}

export function StrategiesDirectory({ data, featured, category, period, sort }: Props) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [showAll, setShowAll] = useState(false);
  const items = data?.items ?? [];
  const visibleItems = showAll ? items : items.slice(0, 6);
  const categoryCounts = useMemo(() => items.reduce<Record<string, number>>((counts, strategy) => {
    counts[strategy.category] = (counts[strategy.category] ?? 0) + 1;
    return counts;
  }, {}), [items]);
  const categoryOptions = ["all", ...Object.keys(categoryCounts).sort()];
  const selected = featured ?? (items[0] as StrategyDetailPayload | undefined) ?? null;
  const selectedMetrics = selected ? performance(selected) : null;
  const metrics = items.map(performance);
  const returns = metrics.map((item) => item?.totalReturnPct).filter((value): value is number => typeof value === "number");
  const cagrs = metrics.map((item) => item?.cagrPct).filter((value): value is number => typeof value === "number");
  const sortedReturns = [...returns].sort((left, right) => left - right);
  const medianReturn = sortedReturns.length ? sortedReturns[Math.floor(sortedReturns.length / 2)] : null;
  const beatingSpy = metrics.filter((item) => item?.totalReturnPct != null && item?.benchmarkReturnPct != null && item.totalReturnPct > item.benchmarkReturnPct).length;
  const liveCount = items.filter((item) => displayStatus(item) === "Live").length;
  const previews = selected ? [selected, ...items.filter((item) => item.slug !== selected.slug)].slice(0, 3) : items.slice(0, 3);
  const equityCurve = selected?.equityCurve ?? [];
  const baseStrategyValue = equityCurve[0]?.strategyValue ?? 1;
  const baseBenchmarkValue = equityCurve[0]?.benchmarkValue ?? baseStrategyValue;
  const timeline = equityCurve.map((point) => {
    const benchmarkValue = point.benchmarkValue ?? point.strategyValue;
    return {
      date: point.date,
      strategy_value: point.strategyValue,
      benchmark_value: benchmarkValue,
      strategy_return_pct: ((point.strategyValue / baseStrategyValue) - 1) * 100,
      benchmark_return_pct: ((benchmarkValue / baseBenchmarkValue) - 1) * 100,
      active_positions: point.activeHoldings ?? 0,
      invested_pct: point.activeHoldings ? 100 : 0,
      cash: 0,
      daily_return_pct: 0,
    };
  });
  const benchmark = selected?.latestRun?.benchmark ?? items.find((item) => item.latestRun?.benchmark)?.latestRun?.benchmark ?? "SPY";

  function setQuery(next: Record<string, string>) {
    const params = new URLSearchParams(searchParams.toString());
    if (Object.keys(next).some((key) => key !== "selected")) params.delete("selected");
    Object.entries(next).forEach(([key, value]) => {
      if (!value || value === "all" || (key === "period" && value === "max")) params.delete(key);
      else params.set(key, value);
    });
    const query = params.toString();
    router.push(`/strategies${query ? `?${query}` : ""}`);
  }

  return <div className="space-y-3 py-3 sm:py-5">
    <section className="flex flex-wrap items-start justify-between gap-4">
      <div><p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Strategies</p><h1 className="mt-2 text-3xl font-semibold text-white">Strategies</h1><p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">Track published Walnut and source-driven strategies using stored, reproducible performance records.</p></div>
      <div className="flex flex-wrap gap-2"><Link href="/backtesting" className="rounded-md bg-emerald-400/15 px-3 py-2 text-sm font-semibold text-emerald-50 ring-1 ring-emerald-300/30 transition hover:bg-emerald-400/25">Run backtest</Link><Link href="/strategies/methodology" className="rounded-md border border-white/15 px-3 py-2 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/40 hover:text-emerald-100">View methodology</Link></div>
    </section>

    <section className="rounded-lg border border-sky-300/20 bg-sky-300/[0.06] px-4 py-3 text-sm leading-6 text-slate-300"><span className="font-semibold text-sky-100">Catalog note. </span>Each row reflects persisted strategy definitions and performance snapshots. Coverage and data-quality labels on individual strategies identify datasets still expanding.</section>

    <section className="grid gap-2 lg:grid-cols-[minmax(0,1fr)_minmax(19rem,0.65fr)]">
      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        <label className="text-xs font-medium text-slate-400">Strategy type<select value={category} onChange={(event) => setQuery({ category: event.target.value })} className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"><option value="all">All</option>{categoryOptions.filter((value) => value !== "all").map((value) => <option key={value} value={value}>{categoryLabel(value)}</option>)}</select></label>
        <label className="text-xs font-medium text-slate-400">Timeframe<select value={period} onChange={(event) => setQuery({ period: event.target.value })} className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50">{periods.map((value) => <option key={value} value={value}>{labelPeriod(value)}</option>)}</select></label>
        <label className="text-xs font-medium text-slate-400">Benchmark<select disabled value={benchmark} className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-300"><option>{benchmark}</option></select></label>
        <label className="text-xs font-medium text-slate-400">Sort<select value={sort} onChange={(event) => setQuery({ sort: event.target.value })} className="mt-1 block w-full rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50">{sorts.map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
      </div>
      <nav aria-label="Strategy categories" className="flex min-w-0 overflow-x-auto rounded-lg border border-white/10 bg-slate-950/40 p-1 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">{categoryOptions.map((value) => { const active = category === value || (!category && value === "all"); const count = value === "all" ? items.length : categoryCounts[value]; return <button key={value} type="button" onClick={() => setQuery({ category: value })} className={`shrink-0 rounded-md px-3 py-2 text-sm font-semibold ${active ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>{value === "all" ? "All strategies" : categoryLabel(value)} <span className="ml-1 text-xs opacity-70">{count}</span></button>; })}</nav>
    </section>

    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5"><MetricCard label="Published strategies" value={String(items.length)} /><MetricCard label={`Best ${labelPeriod(period)} CAGR`} value={formatPct(cagrs.length ? Math.max(...cagrs) : null)} tone="good" /><MetricCard label={`Median ${labelPeriod(period)} return`} value={formatPct(medianReturn)} tone="good" /><MetricCard label={`Beating ${benchmark}`} value={items.length ? `${Math.round((beatingSpy / items.length) * 100)}%` : "--"} tone="good" /><MetricCard label="Live strategies" value={String(liveCount)} /></section>

    {items.length ? <div className="grid gap-4 xl:grid-cols-[minmax(0,1.65fr)_minmax(19rem,0.9fr)]">
      <div className="space-y-4">
        <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/35"><div className="flex items-center justify-between border-b border-white/10 px-4 py-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Top strategies</h2><p className="mt-1 text-xs text-slate-500">Ranked by the selected stored {labelPeriod(period)} performance metric.</p></div><span className="rounded-md border border-white/10 px-2 py-1 text-xs font-semibold text-slate-400">{items.length} published</span></div><div className="overflow-x-auto"><table className="min-w-full text-left text-sm"><thead className="border-b border-white/10 bg-slate-950/65 text-xs uppercase tracking-wide text-slate-500"><tr><th className="w-10 px-4 py-3">#</th><th className="px-3 py-3">Strategy</th><th className="px-3 py-3">Type</th><th className="px-3 py-3"><SortHeader label="Return" value="return" active={sort === "return"} onSelect={(value) => setQuery({ sort: value })} /></th><th className="px-3 py-3"><SortHeader label="CAGR" value="cagr" active={sort === "cagr"} onSelect={(value) => setQuery({ sort: value })} /></th><th className="px-3 py-3"><SortHeader label={`Excess vs ${benchmark}`} value="alpha" active={sort === "alpha"} onSelect={(value) => setQuery({ sort: value })} /></th><th className="px-3 py-3"><SortHeader label="Sharpe" value="sharpe" active={sort === "sharpe"} onSelect={(value) => setQuery({ sort: value })} /></th><th className="px-3 py-3"><SortHeader label="Drawdown" value="drawdown" active={sort === "drawdown"} onSelect={(value) => setQuery({ sort: value })} /></th><th className="px-4 py-3">Status</th></tr></thead><tbody className="divide-y divide-white/10">{visibleItems.map((strategy, index) => { const itemMetrics = performance(strategy); const positive = (itemMetrics?.totalReturnPct ?? 0) >= 0; const selectedRow = strategy.slug === selected?.slug; return <tr key={strategy.slug} className={selectedRow ? "bg-emerald-400/[0.08]" : "transition hover:bg-white/[0.035]"}><td className="px-4 py-3 tabular-nums text-slate-500">{index + 1}</td><td className="px-3 py-3"><button type="button" onClick={() => setQuery({ selected: strategy.slug })} className="text-left font-semibold text-white hover:text-emerald-200">{displayStrategyName(strategy.name)}</button><div className="mt-1 max-w-xs truncate text-xs text-slate-500">{strategy.shortDescription ?? strategy.walnutTake ?? "Published model strategy"}</div></td><td className="px-3 py-3"><span className="rounded-md border border-emerald-300/20 bg-emerald-300/[0.06] px-2 py-1 text-xs font-semibold capitalize text-emerald-100">{categoryLabel(strategy.category)}</span></td><td className={`px-3 py-3 font-semibold tabular-nums ${positive ? "text-emerald-300" : "text-rose-300"}`}>{formatPct(itemMetrics?.totalReturnPct)}</td><td className="px-3 py-3 tabular-nums text-emerald-300">{formatPct(itemMetrics?.cagrPct)}</td><td className="px-3 py-3 tabular-nums text-emerald-300">{formatPct(itemMetrics?.alphaCagrPct)}</td><td className="px-3 py-3 tabular-nums text-slate-200">{itemMetrics?.sharpe == null ? "--" : itemMetrics.sharpe.toFixed(2)}</td><td className="px-3 py-3 tabular-nums text-rose-300">{formatPct(itemMetrics?.maxDrawdownPct)}</td><td className="px-4 py-3"><StatusPill strategy={strategy} /></td></tr>; })}</tbody></table></div>{items.length > 6 ? <div className="flex items-center justify-between border-t border-white/10 px-4 py-3 text-xs text-slate-500"><span>Showing 1-{visibleItems.length} of {items.length} strategies</span><button type="button" onClick={() => setShowAll((value) => !value)} className="font-semibold text-emerald-300 hover:text-emerald-200">{showAll ? "Show fewer" : "View all strategies"}</button></div> : null}</section>

        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><div className="flex items-center justify-between gap-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Strategy preview</h2><p className="mt-1 text-xs text-slate-500">Top-ranked records and their stored rule inputs.</p></div><Link href="/backtesting" className="text-sm font-semibold text-emerald-300 hover:text-emerald-200">Open backtester</Link></div><div className="mt-4 grid gap-3 md:grid-cols-3">{previews.map((strategy) => <article key={strategy.slug} className="min-w-0 border border-white/10 bg-slate-950/45 p-4"><div className="flex items-center justify-between gap-2"><span className="h-9 w-9 rounded-full border border-emerald-300/25 bg-emerald-300/[0.08]" /><span className="text-xs capitalize text-slate-500">{categoryLabel(strategy.category)}</span></div><h3 className="mt-3 font-semibold text-white">{displayStrategyName(strategy.name)}</h3><p className="mt-2 min-h-12 text-sm leading-5 text-slate-400">{strategy.shortDescription ?? strategy.walnutTake ?? "Stored strategy definition."}</p><div className="mt-3 flex flex-wrap gap-1.5">{ruleChips(strategy).length ? ruleChips(strategy).map((chip) => <span key={chip} className="rounded border border-white/10 px-1.5 py-1 text-xs text-slate-400">{chip}</span>) : <span className="text-xs text-slate-500">Rule inputs not stored</span>}</div><Link href={`/strategies/${strategy.slug}`} className="mt-4 inline-flex text-sm font-semibold text-emerald-300 hover:text-emerald-200">View strategy</Link></article>)}</div></section>
      </div>

      <aside className="space-y-4"><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><div className="flex items-start justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-300">Selected strategy</p><h2 className="mt-2 text-xl font-semibold text-white">{selected ? displayStrategyName(selected.name) : "No strategy selected"}</h2><p className="mt-2 text-sm leading-6 text-slate-400">{selected?.shortDescription ?? selected?.walnutTake ?? "Choose a published strategy to inspect its stored record."}</p></div>{selected ? <StatusPill strategy={selected} /> : null}</div>{selected ? <dl className="mt-5 grid grid-cols-2 gap-x-5 gap-y-4 text-sm"><div><dt className="text-slate-500">Coverage</dt><dd className="mt-1 text-slate-200">{displayStrategyUniverse(selected)}</dd></div><div><dt className="text-slate-500">Benchmark</dt><dd className="mt-1 text-slate-200">{selected.latestRun?.benchmark ?? benchmark}</dd></div><div><dt className="text-slate-500">Start date</dt><dd className="mt-1 text-slate-200">{formatDate(selected.latestRun?.backtestStartDate)}</dd></div><div><dt className="text-slate-500">Availability</dt><dd className="mt-1 text-slate-200">{displayStatus(selected)}</dd></div></dl> : null}<Link href={selected ? `/strategies/${selected.slug}` : "/strategies/methodology"} className="mt-5 inline-flex rounded-md bg-emerald-400 px-3 py-2 text-sm font-bold text-slate-950 shadow-sm shadow-emerald-500/20 transition hover:bg-emerald-300">{selected ? "Open strategy" : "Review methodology"}</Link></section>

        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><div className="flex items-center justify-between gap-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Performance vs {selected?.latestRun?.benchmark ?? benchmark}</h2><p className="mt-1 text-xs text-slate-500">Stored curve from the selected reproducible run.</p></div><span className="rounded-md border border-emerald-300/20 bg-emerald-300/[0.06] px-2 py-1 text-xs font-semibold text-emerald-100">{labelPeriod(period)}</span></div>{timeline.length ? <div className="mt-4"><BacktestChart timeline={timeline} /></div> : <div className="mt-4 flex h-56 items-center justify-center border border-dashed border-white/10 px-6 text-center text-sm leading-6 text-slate-500">No stored equity curve is available for this strategy run.</div>}<div className="mt-4 grid grid-cols-3 gap-3 border-t border-white/10 pt-4 text-sm"><div><div className="text-xs text-slate-500">Return</div><div className="mt-1 font-semibold tabular-nums text-emerald-300">{formatPct(selectedMetrics?.totalReturnPct)}</div></div><div><div className="text-xs text-slate-500">CAGR</div><div className="mt-1 font-semibold tabular-nums text-emerald-300">{formatPct(selectedMetrics?.cagrPct)}</div></div><div><div className="text-xs text-slate-500">Drawdown</div><div className="mt-1 font-semibold tabular-nums text-rose-300">{formatPct(selectedMetrics?.maxDrawdownPct)}</div></div></div></section>

        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Research safeguards</h2><ul className="mt-4 space-y-3 text-sm leading-6 text-slate-400"><li>Published records retain their methodology and benchmark with the associated run.</li><li>Historical portfolio results remain separate from prospective strategy alerts.</li><li>Data-confidence labels identify coverage constraints before a strategy is compared.</li></ul></section></aside>
    </div> : <section className="rounded-lg border border-white/10 bg-slate-950/35 px-6 py-14 text-center"><h2 className="text-lg font-semibold text-white">No published strategies match these filters</h2><p className="mx-auto mt-2 max-w-lg text-sm leading-6 text-slate-400">Adjust the catalog filters or review methodology while new reproducible strategy runs are prepared for publication.</p><button type="button" onClick={() => setQuery({ category: "all", period: "max", sort: "cagr" })} className="mt-5 rounded-md border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100 hover:bg-emerald-300/10">Reset filters</button></section>}
  </div>;
}
