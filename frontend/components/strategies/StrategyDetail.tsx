import Link from "next/link";
import { BacktestChart } from "@/components/backtesting/BacktestChart";
import { StrategyFollowButton } from "@/components/strategies/StrategyFollowButton";
import type { StrategyDetailPayload } from "@/lib/api";

type Props = { strategy: StrategyDetailPayload; period: string };
type ScoreDetails = { score: number; rawScore: number | null; components: Record<string, unknown>; penalties: Array<Record<string, unknown>> };

const periodLabels: Record<string, string> = { "30d": "30D", "1y": "1Y", "2y": "2Y", "3y": "3Y", max: "All" };

function pct(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "--" : `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function currency(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "--" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
}

function metricTone(value: number | null | undefined) {
  return (value ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300";
}

function ruleChips(strategy: StrategyDetailPayload) {
  const rule = strategy.rule ?? {};
  const parameters = strategy.parameters ?? {};
  const universe = strategy.universe ?? {};
  return [
    rule.kind ? `Rule: ${String(rule.kind).replaceAll("_", " ")}` : null,
    rule.source ? `Source: ${String(rule.source)}` : null,
    rule.technical_rule ? `Technical: ${String(rule.technical_rule).replaceAll("_", " ")}` : null,
    parameters.hold_days ? `Hold ${String(parameters.hold_days)} days` : null,
    parameters.weighting ? `${String(parameters.weighting)} weight` : null,
    parameters.rebalance_frequency ? `Rebalance ${String(parameters.rebalance_frequency)}` : null,
    universe.source ? `Universe: ${String(universe.source)}` : null,
  ].filter((item): item is string => Boolean(item));
}

function validatedScoreDetails(strategy: StrategyDetailPayload): ScoreDetails | null {
  const validation = strategy.latestRun?.diagnostics?.validation as Record<string, unknown> | undefined;
  const score = validation?.walnut_strategy_score as Record<string, unknown> | undefined;
  if (score?.score_version !== "walnut_strategy_score_v2" || typeof score.score !== "number") return null;
  return {
    score: score.score,
    rawScore: typeof score.raw_score === "number" ? score.raw_score : null,
    components: (score.components as Record<string, unknown> | undefined) ?? {},
    penalties: Array.isArray(score.penalties) ? score.penalties.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object") : [],
  };
}

const scoreComponentLabels: Array<[string, string]> = [
  ["out_of_sample_cagr", "Out-of-sample return"],
  ["out_of_sample_alpha", "Out-of-sample alpha"],
  ["risk_adjusted_return", "Risk adjusted"],
  ["drawdown_control", "Drawdown control"],
  ["rolling_consistency", "Rolling consistency"],
  ["sample_size", "Evidence depth"],
];

export function StrategyDetail({ strategy, period }: Props) {
  const performance = strategy.performance;
  const run = strategy.latestRun;
  const curve = (strategy.equityCurve ?? []).map((point) => ({
    date: point.date,
    strategy_value: point.strategyValue,
    benchmark_value: point.benchmarkValue ?? point.strategyValue,
    strategy_return_pct: 0,
    benchmark_return_pct: 0,
    active_positions: point.activeHoldings ?? 0,
    invested_pct: 0,
    cash: 0,
    daily_return_pct: 0,
  }));
  const chips = ruleChips(strategy);
  const statusLive = strategy.status === "live";
  const canFollow = strategy.status === "published";
  const scoreDetails = validatedScoreDetails(strategy);

  return <div className="space-y-4 py-3 sm:py-5">
    <section className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0"><p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">{strategy.category}</p><h1 className="mt-2 text-3xl font-semibold text-white">{strategy.name}</h1><p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">{strategy.shortDescription ?? strategy.walnutTake ?? "Published Walnut model strategy."}</p></div>
      <div className="flex flex-wrap gap-2"><Link href="/strategies/methodology" className="rounded-md border border-white/15 px-3 py-2 text-sm font-semibold text-slate-100 hover:border-emerald-300/40">Methodology</Link>{canFollow ? <StrategyFollowButton slug={strategy.slug} compact /> : <span className="rounded-md border border-white/10 px-3 py-2 text-sm font-semibold text-slate-400">Historical strategy</span>}</div>
    </section>

    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
      {[[`${periodLabels[period] ?? period} return`, pct(performance?.totalReturnPct), metricTone(performance?.totalReturnPct)],["CAGR", pct(performance?.cagrPct), metricTone(performance?.cagrPct)],["Excess vs SPY", pct(performance?.alphaCagrPct), metricTone(performance?.alphaCagrPct)],["Max drawdown", pct(performance?.maxDrawdownPct), "text-rose-300"],["Win rate", pct(performance?.winRatePct), "text-emerald-300"],["Status", statusLive ? "Live" : "Historical", statusLive ? "text-emerald-300" : "text-sky-200"]].map(([label, value, tone]) => <div key={String(label)} className="rounded-lg border border-white/10 bg-slate-950/35 px-4 py-3"><div className="text-xs text-slate-400">{label}</div><div className={`mt-1 text-xl font-semibold tabular-nums ${tone}`}>{value}</div></div>)}
    </section>

    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.6fr)_minmax(19rem,0.8fr)]">
      <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><div className="flex flex-wrap items-center justify-between gap-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Performance vs SPY</h2><p className="mt-1 text-xs text-slate-500">Stored strategy and benchmark values from the selected run.</p></div><div className="flex rounded-md border border-white/10 p-1">{["30d", "1y", "2y", "3y", "max"].map((key) => <Link key={key} href={`/strategies/${strategy.slug}${key === "max" ? "" : `?period=${key}`}`} className={`rounded px-2 py-1 text-xs font-semibold ${period === key ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>{periodLabels[key]}</Link>)}</div></div><div className="mt-4">{strategy.access?.locked ? <div className="rounded-lg border border-dashed border-white/15 p-8 text-center text-sm text-slate-400">This strategy's detailed performance is available with {strategy.access.requiredTier} access.</div> : <BacktestChart timeline={curve} />}</div></section>
      <aside className="space-y-3"><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Follow this strategy</h2><p className="mt-2 text-sm leading-6 text-slate-400">{canFollow ? "Follow this strategy to opt in to model-trade update emails. Alerts begin only after an approved prospective version is active." : "This is a historical model strategy. Following opens only after a prospective evaluation schedule is operating."}</p><div className="mt-4 flex items-center justify-between border-y border-white/10 py-3"><span className="text-sm text-slate-300">Email alerts</span><span className={`rounded-full px-2 py-1 text-xs font-semibold ${canFollow ? "bg-emerald-300/10 text-emerald-100" : "bg-white/5 text-slate-500"}`}>{canFollow ? "Opt in" : "Unavailable"}</span></div>{canFollow ? <div className="mt-4"><StrategyFollowButton slug={strategy.slug} /></div> : null}</section><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Strategy snapshot</h2><dl className="mt-4 space-y-3 text-sm"><div className="flex justify-between gap-3"><dt className="text-slate-500">Benchmark</dt><dd className="text-right text-slate-200">{run?.benchmark ?? "SPY"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Backtest range</dt><dd className="text-right text-slate-200">{run?.backtestStartDate ?? "--"}<br />{run?.backtestEndDate ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Methodology</dt><dd className="text-right text-slate-200">{strategy.methodologyVersion}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Data confidence</dt><dd className="capitalize text-right text-slate-200">{strategy.dataQualityConfidence}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Last refreshed</dt><dd className="text-right text-slate-200">{run?.completedAt?.slice(0, 10) ?? "--"}</dd></div></dl><div className="mt-5 border-t border-white/10 pt-4"><div className="flex items-baseline justify-between gap-3"><h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Walnut Strategy Score</h3>{scoreDetails ? <span className="text-2xl font-semibold tabular-nums text-emerald-300">{scoreDetails.score.toFixed(1)}</span> : <span className="text-xs font-medium text-slate-500">Not normalized</span>}</div>{scoreDetails ? <><p className="mt-2 text-xs leading-5 text-slate-500">v2 combines out-of-sample results, risk, consistency, evidence depth, and explicit data-quality penalties.</p><div className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-xs">{scoreComponentLabels.map(([key, label]) => <div key={key} className="flex justify-between gap-2 text-slate-400"><span>{label}</span><span className="tabular-nums text-slate-200">{typeof scoreDetails.components[key] === "number" ? (scoreDetails.components[key] as number).toFixed(0) : "--"}</span></div>)}</div>{scoreDetails.penalties.length ? <div className="mt-3 flex flex-wrap gap-1.5">{scoreDetails.penalties.map((penalty) => <span key={String(penalty.reason)} className="rounded border border-amber-300/20 bg-amber-300/[0.06] px-1.5 py-1 text-xs text-amber-100">{String(penalty.reason ?? "risk flag").replaceAll("_", " ")}</span>)}</div> : null}</> : <p className="mt-2 text-xs leading-5 text-slate-500">Individual portfolio research is shown separately until it has the same validated score methodology.</p>}</div><div className="mt-5 border-t border-white/10 pt-4"><h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">API and automation</h3><p className="mt-2 text-sm leading-6 text-slate-500">Strategy events are being designed for future API and webhook delivery. Brokerage execution is not available.</p></div></section></aside>
    </div>

    <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Strategy rules</h2><div className="mt-3 flex flex-wrap gap-2">{chips.length ? chips.map((chip) => <span key={chip} className="rounded-md border border-emerald-300/20 bg-emerald-300/[0.06] px-2 py-1 text-xs font-semibold capitalize text-emerald-100">{chip}</span>) : <span className="text-sm text-slate-500">Rule detail is not yet stored for this strategy.</span>}</div><p className="mt-4 text-sm leading-6 text-slate-400">{strategy.methodology ?? "The methodology text for this strategy is still under review."}</p>{strategy.riskNotes?.length ? <div className="mt-3 flex flex-wrap gap-2">{strategy.riskNotes.map((note) => <span key={note} className="rounded-md border border-amber-300/20 bg-amber-300/[0.06] px-2 py-1 text-xs text-amber-100">{note.replaceAll("_", " ")}</span>)}</div> : null}</section>

    <section className="grid gap-4 lg:grid-cols-2"><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">How it works</h2><ol className="mt-4 space-y-3 text-sm leading-6 text-slate-400"><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">1</span>Walnut evaluates the defined universe on the strategy schedule.</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">2</span>Securities matching the preserved rules qualify for the model portfolio.</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">3</span>Live strategies will persist additions, removals, and rebalance events before delivery.</li></ol></div><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Risk and data</h2><dl className="mt-4 space-y-3 text-sm"><div className="flex justify-between gap-3"><dt className="text-slate-500">Signals</dt><dd className="text-right text-slate-200">{performance?.independentSignalCount ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Average holdings</dt><dd className="text-right text-slate-200">{performance?.avgHoldings?.toFixed(1) ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Turnover events</dt><dd className="text-right text-slate-200">{performance?.turnoverEvents ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Run timing</dt><dd className="max-w-52 text-right text-slate-200">{run?.executionTiming ?? "--"}</dd></div></dl></div></section>

    <section className="rounded-lg border border-white/10 bg-slate-950/35"><div className="border-b border-white/10 px-4 py-3"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Current positions</h2><p className="mt-1 text-xs text-slate-500">Model-portfolio holdings saved with the latest strategy run.</p></div>{strategy.access?.locked ? <p className="px-4 py-8 text-center text-sm text-slate-400">Position detail is available with {strategy.access.requiredTier} access.</p> : strategy.currentHoldings?.length ? <div className="overflow-x-auto"><table className="min-w-full text-left text-sm"><thead className="border-b border-white/10 text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-4 py-3">Ticker</th><th className="px-3 py-3">First added</th><th className="px-3 py-3">Reference price</th><th className="px-3 py-3">Return</th><th className="px-3 py-3">Weight</th><th className="px-4 py-3">Signals</th></tr></thead><tbody className="divide-y divide-white/10">{strategy.currentHoldings.map((holding) => <tr key={holding.symbol}><td className="px-4 py-3"><Link href={`/ticker/${holding.symbol}`} className="font-semibold text-white hover:text-emerald-200">{holding.symbol}</Link><div className="mt-0.5 text-xs text-slate-500">{holding.companyName}</div></td><td className="px-3 py-3 text-slate-300">{holding.entryDate ?? "--"}</td><td className="px-3 py-3 tabular-nums text-slate-300">{currency(holding.lastPrice)}</td><td className={`px-3 py-3 font-semibold tabular-nums ${metricTone(holding.returnPct)}`}>{pct(holding.returnPct)}</td><td className="px-3 py-3 tabular-nums text-slate-300">{pct(holding.weightPct)}</td><td className="px-4 py-3 text-slate-300">{holding.sourceSignalCount ?? "--"}</td></tr>)}</tbody></table></div> : <p className="px-4 py-8 text-center text-sm text-slate-400">No current holdings are stored for this historical strategy.</p>}</section>

    <section className="rounded-lg border border-dashed border-white/15 bg-slate-950/20 px-4 py-5"><h2 className="font-semibold text-white">Strategy trade ledger</h2><p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">Prospective trade events will be stored separately from historical backtests. This strategy does not yet have a live, auditable trade ledger, so Walnut does not present reconstructed history as live model trades.</p></section>
  </div>;
}
