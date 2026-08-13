import Link from "next/link";
import { BacktestChart } from "@/components/backtesting/BacktestChart";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { StrategyFollowButton } from "@/components/strategies/StrategyFollowButton";
import type { StrategyDetailPayload } from "@/lib/api";
import { displayStrategyName, displayStrategyUniverse } from "@/lib/strategyPresentation";

type Props = { strategy: StrategyDetailPayload; period: string };
type ScoreDetails = { score: number; components: Record<string, unknown>; penalties: Array<Record<string, unknown>> };

const periodLabels: Record<string, string> = { "30d": "30D", "1y": "1Y", "2y": "2Y", "3y": "3Y", max: "All" };
const scoreComponentLabels: Array<[string, string]> = [["out_of_sample_cagr", "Out-of-sample return"], ["out_of_sample_alpha", "Out-of-sample alpha"], ["risk_adjusted_return", "Risk adjusted"], ["drawdown_control", "Drawdown control"], ["rolling_consistency", "Rolling consistency"], ["sample_size", "Evidence depth"]];

function pct(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "--" : `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function currency(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "--" : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
}

function metricTone(value: number | null | undefined) {
  return (value ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300";
}

function formatDate(value: string | null | undefined) {
  if (!value) return "--";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function ruleChips(strategy: StrategyDetailPayload) {
  const rule = strategy.rule ?? {};
  const parameters = strategy.parameters ?? {};
  return [
    rule.technical_rule ? `Technical: ${String(rule.technical_rule).replaceAll("_", " ")}` : null,
    parameters.hold_days ? `Hold ${String(parameters.hold_days)} days` : null,
    parameters.weighting ? `${String(parameters.weighting)} weight` : null,
    parameters.rebalance_frequency ? `Rebalance ${String(parameters.rebalance_frequency)}` : null,
  ].filter((item): item is string => Boolean(item));
}

function validatedScoreDetails(strategy: StrategyDetailPayload): ScoreDetails | null {
  const validation = strategy.latestRun?.diagnostics?.validation as Record<string, unknown> | undefined;
  const score = validation?.walnut_strategy_score as Record<string, unknown> | undefined;
  if (score?.score_version !== "walnut_strategy_score_v2" || typeof score.score !== "number") return null;
  return { score: score.score, components: (score.components as Record<string, unknown> | undefined) ?? {}, penalties: Array.isArray(score.penalties) ? score.penalties.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object") : [] };
}

function MetricCard({ label, value, tone }: { label: string; value: string; tone: string }) {
  return <div className="min-w-0 rounded-lg border border-white/10 bg-slate-950/35 px-4 py-3"><div className="flex items-center gap-2"><span className={`h-7 w-7 rounded-full border ${tone.includes("rose") ? "border-rose-300/25 bg-rose-300/[0.07]" : "border-emerald-300/25 bg-emerald-300/[0.07]"}`} /><div className="text-xs text-slate-400">{label}</div></div><div className={`mt-2 text-xl font-semibold tabular-nums ${tone}`}>{value}</div></div>;
}

export function StrategyDetail({ strategy, period }: Props) {
  const performance = strategy.performance;
  const run = strategy.latestRun;
  const equityCurve = strategy.equityCurve ?? [];
  const baseStrategyValue = equityCurve[0]?.strategyValue ?? 1;
  const baseBenchmarkValue = equityCurve[0]?.benchmarkValue ?? baseStrategyValue;
  const curve = equityCurve.map((point) => {
    const benchmarkValue = point.benchmarkValue ?? point.strategyValue;
    return { date: point.date, strategy_value: point.strategyValue, benchmark_value: benchmarkValue, strategy_return_pct: ((point.strategyValue / baseStrategyValue) - 1) * 100, benchmark_return_pct: ((benchmarkValue / baseBenchmarkValue) - 1) * 100, active_positions: point.activeHoldings ?? 0, invested_pct: point.activeHoldings ? 100 : 0, cash: 0, daily_return_pct: 0 };
  });
  const chips = ruleChips(strategy);
  const isProspective = Boolean(strategy.prospectiveActive);
  const canViewCurrentHoldings = Boolean(strategy.strategyAccess?.canViewCurrentHoldings);
  const scoreDetails = validatedScoreDetails(strategy);
  const benchmark = run?.benchmark ?? "SPY";
  const metrics = [
    [`${periodLabels[period] ?? period} return`, pct(performance?.totalReturnPct), metricTone(performance?.totalReturnPct)],
    ["CAGR", pct(performance?.cagrPct), metricTone(performance?.cagrPct)],
    [`Excess vs ${benchmark}`, pct(performance?.alphaCagrPct), metricTone(performance?.alphaCagrPct)],
    ["Max drawdown", pct(performance?.maxDrawdownPct), "text-rose-300"],
    ["Win rate", pct(performance?.winRatePct), "text-emerald-300"],
    ["Strategy type", isProspective ? "Active monitoring" : "Historical research", isProspective ? "text-emerald-300" : "text-sky-200"],
  ];

  return <div className="space-y-3 py-3 sm:py-5">
    <section className="flex flex-wrap items-start justify-between gap-4"><div className="min-w-0"><p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">{strategy.category.replaceAll("_", " ")}</p><h1 className="mt-2 text-3xl font-semibold text-white">{displayStrategyName(strategy.name)}</h1><p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">{strategy.shortDescription ?? strategy.walnutTake ?? "Published Walnut model strategy."}</p></div><div className="flex flex-wrap gap-2"><Link href="/strategies" className="rounded-md border border-white/15 px-3 py-2 text-sm font-semibold text-slate-100 hover:border-emerald-300/40">Back to strategies</Link>{isProspective ? <StrategyFollowButton slug={strategy.slug} compact /> : null}</div></section>

    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">{metrics.map(([label, value, tone]) => <MetricCard key={label} label={label} value={value} tone={tone} />)}</section>

    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(20rem,0.83fr)]"><div className="space-y-4"><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><div className="flex flex-wrap items-center justify-between gap-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Performance vs {benchmark}</h2><p className="mt-1 text-xs text-slate-500">Stored strategy and benchmark values from the selected run.</p></div><div className="flex rounded-md border border-white/10 p-1">{["30d", "1y", "2y", "3y", "max"].map((key) => <Link key={key} href={`/strategies/${strategy.slug}${key === "max" ? "" : `?period=${key}`}`} className={`rounded px-2 py-1 text-xs font-semibold ${period === key ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>{periodLabels[key]}</Link>)}</div></div><div className="mt-4">{curve.length ? <BacktestChart timeline={curve} /> : <div className="flex h-64 items-center justify-center border border-dashed border-white/10 px-6 text-center text-sm text-slate-500">No stored equity curve is available for this strategy run.</div>}</div></section>

      <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Methodology</h2>{chips.length ? <div className="mt-3 flex flex-wrap gap-2">{chips.map((chip) => <span key={chip} className="rounded-md border border-emerald-300/20 bg-emerald-300/[0.06] px-2 py-1 text-xs font-semibold capitalize text-emerald-100">{chip}</span>)}</div> : null}<p className="mt-4 text-sm leading-6 text-slate-400">{strategy.methodology ?? "The methodology text for this strategy is still under review."}</p></section>

      <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/35"><div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 px-4 py-3"><div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Current model positions</h2><p className="mt-1 text-xs text-slate-500">Holdings persisted with the latest model run, not reconstructed live trades.</p></div>{canViewCurrentHoldings ? <span className="rounded-md border border-white/10 px-2 py-1 text-xs text-slate-400">{strategy.currentHoldings?.length ?? 0} positions</span> : null}</div>{canViewCurrentHoldings && strategy.currentHoldings?.length ? <div className="overflow-x-auto"><table className="min-w-full text-left text-sm"><thead className="border-b border-white/10 bg-slate-950/60 text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-4 py-3">Ticker</th><th className="px-3 py-3">First added</th><th className="px-3 py-3">Reference price</th><th className="px-3 py-3">Return</th><th className="px-3 py-3">Weight</th><th className="px-4 py-3">Signals</th></tr></thead><tbody className="divide-y divide-white/10">{strategy.currentHoldings.map((holding) => <tr key={holding.symbol}><td className="px-4 py-3"><Link href={`/ticker/${holding.symbol}`} className="font-semibold text-white hover:text-emerald-200">{holding.symbol}</Link><div className="mt-0.5 text-xs text-slate-500">{holding.companyName}</div></td><td className="px-3 py-3 text-slate-300">{formatDate(holding.entryDate)}</td><td className="px-3 py-3 tabular-nums text-slate-300">{currency(holding.lastPrice)}</td><td className={`px-3 py-3 font-semibold tabular-nums ${metricTone(holding.returnPct)}`}>{pct(holding.returnPct)}</td><td className="px-3 py-3 tabular-nums text-slate-300">{pct(holding.weightPct)}</td><td className="px-4 py-3 text-slate-300">{holding.sourceSignalCount ?? "--"}</td></tr>)}</tbody></table></div> : canViewCurrentHoldings ? <p className="px-4 py-8 text-center text-sm text-slate-400">No current holdings are stored for this strategy.</p> : <div className="px-4 py-5"><p className="text-sm font-semibold text-white">{strategy.currentHoldingsCount ?? 0} current positions</p><p className="mt-1 text-sm leading-6 text-slate-400">Unlock the complete current model portfolio and follow strategy changes.</p><div className="mt-4"><UpgradePrompt title="Unlock current strategy positions" body="Premium includes the complete current portfolio plus real-time strategy alerts." compact /></div></div>}</section>

      <section className="rounded-lg border border-dashed border-white/15 bg-slate-950/20 px-4 py-5"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Strategy activity</h2><p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">No prospective strategy events are stored yet. Walnut keeps historical backtest positions separate from future additions, exits, and rebalance events so historical research is not presented as a live trade ledger.</p></section></div>

      <aside className="space-y-3"><section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">{isProspective ? "Follow this strategy" : "About this portfolio"}</h2><p className="mt-2 text-sm leading-6 text-slate-400">{isProspective ? "Follow this strategy for daily updates after Walnut ingests newly available filings and evaluates the model." : "This page replicates past, publicly disclosed Congress trades. It does not currently monitor future disclosures or send trade alerts."}</p><div className="mt-4">{isProspective ? <StrategyFollowButton slug={strategy.slug} panel /> : <p className="rounded-md border border-white/10 px-3 py-3 text-sm text-slate-400">Email alerts are not active for historical research.</p>}</div></section>

        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">How it works</h2><ol className="mt-4 space-y-3 text-sm leading-6 text-slate-400"><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">1</span>Walnut evaluates the saved strategy definition on its specified schedule.</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">2</span>Eligible securities are persisted with the run and benchmark context shown here.</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">3</span>Future prospective events are recorded before any subscriber delivery.</li></ol></section>

        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Strategy snapshot</h2><dl className="mt-4 grid grid-cols-2 gap-x-4 gap-y-4 text-sm"><div><dt className="text-slate-500">Universe</dt><dd className="mt-1 break-words text-slate-200">{displayStrategyUniverse(strategy)}</dd></div><div><dt className="text-slate-500">Benchmark</dt><dd className="mt-1 text-slate-200">{benchmark}</dd></div><div><dt className="text-slate-500">Backtest start</dt><dd className="mt-1 text-slate-200">{formatDate(run?.backtestStartDate)}</dd></div><div><dt className="text-slate-500">Last refreshed</dt><dd className="mt-1 text-slate-200">{formatDate(run?.completedAt)}</dd></div><div><dt className="text-slate-500">Methodology</dt><dd className="mt-1 break-words text-slate-200">{strategy.methodologyVersion}</dd></div><div><dt className="text-slate-500">Confidence</dt><dd className="mt-1 capitalize text-slate-200">{strategy.dataQualityConfidence}</dd></div></dl>{scoreDetails ? <div className="mt-5 border-t border-white/10 pt-4"><div className="flex items-baseline justify-between gap-3"><h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Walnut Strategy Score</h3><span className="text-2xl font-semibold tabular-nums text-emerald-300">{scoreDetails.score.toFixed(1)}</span></div><div className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-xs">{scoreComponentLabels.map(([key, label]) => <div key={key} className="flex justify-between gap-2 text-slate-400"><span>{label}</span><span className="tabular-nums text-slate-200">{typeof scoreDetails.components[key] === "number" ? (scoreDetails.components[key] as number).toFixed(0) : "--"}</span></div>)}</div>{scoreDetails.penalties.length ? <div className="mt-3 flex flex-wrap gap-1.5">{scoreDetails.penalties.map((penalty) => <span key={String(penalty.reason)} className="rounded border border-amber-300/20 bg-amber-300/[0.06] px-1.5 py-1 text-xs text-amber-100">{String(penalty.reason ?? "risk flag").replaceAll("_", " ")}</span>)}</div> : null}</div> : null}</section>

        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Automation</h2><p className="mt-2 text-sm leading-6 text-slate-500">API, webhook, and brokerage execution remain unavailable until prospective strategy events are operating and reviewed.</p></section></aside>
    </div>
  </div>;
}
