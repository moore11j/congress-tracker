import Link from "next/link";
import { BacktestChart } from "@/components/backtesting/BacktestChart";
import { StrategyFollowButton } from "@/components/strategies/StrategyFollowButton";
import type { StrategyDetailPayload } from "@/lib/api";
import { displayStrategyName, displayStrategyUniverse } from "@/lib/strategyPresentation";

type PositionView = "current" | "history";

type TransactionHistoryRecord = {
  id: string;
  recordType: "model_trade" | "reconstructed_position";
  symbol: string | null;
  action: string | null;
  status: string | null;
  signalDate: string | null;
  effectiveDate: string | null;
  exitDate: string | null;
  entryPrice: number | null;
  exitPrice: number | null;
  returnPct: number | null;
  weightPct: number | null;
  sourceType: string | null;
  sourceReason: string | null;
  confidence: string | null;
  sourceDocumentId: string | null;
  sourceUrl: string | null;
};

type StrategyWithHistory = StrategyDetailPayload & {
  transactionHistory?: TransactionHistoryRecord[];
  transactionHistoryTotal?: number;
};

type Props = {
  strategy: StrategyDetailPayload;
  period: string;
  holdingsPage: number;
  holdingsPageSize: number;
  positionsView: PositionView;
  isAuthenticated: boolean;
};

const periodLabels: Record<string, string> = { "30d": "30D", "1y": "1Y", "2y": "2Y", "3y": "3Y", max: "All" };

function pct(value: number | null | undefined) {
  return value == null || !Number.isFinite(value) ? "--" : `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function currency(value: number | null | undefined) {
  return value == null || !Number.isFinite(value)
    ? "--"
    : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
}

function metricTone(value: number | null | undefined) {
  return (value ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300";
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

function label(value: string | null | undefined) {
  return value ? value.replaceAll("_", " ") : "--";
}

export function StrategyDetail({ strategy, period, holdingsPage, holdingsPageSize, positionsView, isAuthenticated }: Props) {
  const detail = strategy as StrategyWithHistory;
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
  const isProspective = Boolean(strategy.prospectiveActive);
  const holdingsTotal = Math.max(0, Number(strategy.currentHoldingsTotal ?? strategy.currentHoldings?.length ?? 0));
  const history = detail.transactionHistory ?? [];
  const historyTotal = Math.max(0, Number(detail.transactionHistoryTotal ?? history.length));
  const recordsTotal = positionsView === "history" ? historyTotal : holdingsTotal;
  const totalPages = Math.max(1, Math.ceil(recordsTotal / holdingsPageSize));
  const currentPage = Math.min(Math.max(holdingsPage, 1), totalPages);
  const firstRecordNumber = recordsTotal === 0 ? 0 : (currentPage - 1) * holdingsPageSize + 1;
  const lastRecordNumber = Math.min(currentPage * holdingsPageSize, recordsTotal);
  const isReconstructedHistory = history.some((record) => record.recordType === "reconstructed_position");
  const positionsHref = (page: number, view = positionsView) => {
    const params = new URLSearchParams({ holdings_page: String(page) });
    if (period !== "max") params.set("period", period);
    if (view === "history") params.set("positions", "history");
    return `/strategies/${strategy.slug}?${params.toString()}`;
  };
  const signInHref = `/login?return_to=${encodeURIComponent(positionsHref(1))}`;

  return <div className="space-y-4 py-3 sm:py-5">
    <section className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0">
        <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">{strategy.category}</p>
        <h1 className="mt-2 text-3xl font-semibold text-white">{displayStrategyName(strategy.name)}</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">{strategy.shortDescription ?? strategy.walnutTake ?? "Published Walnut model strategy."}</p>
      </div>
      <div className="flex flex-wrap gap-2">
        <Link href="/strategies" className="rounded-md border border-white/15 px-3 py-2 text-sm font-semibold text-slate-100 hover:border-emerald-300/40">Back to strategies</Link>
        {isProspective ? <StrategyFollowButton slug={strategy.slug} compact /> : <span className="rounded-md border border-sky-300/20 bg-sky-300/[0.06] px-3 py-2 text-sm font-semibold text-sky-100">Historical research</span>}
      </div>
    </section>

    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
      {[
        [`${periodLabels[period] ?? period} return`, pct(performance?.totalReturnPct), metricTone(performance?.totalReturnPct)],
        ["CAGR", pct(performance?.cagrPct), metricTone(performance?.cagrPct)],
        ["Excess vs SPY", pct(performance?.alphaCagrPct), metricTone(performance?.alphaCagrPct)],
        ["Max drawdown", pct(performance?.maxDrawdownPct), "text-rose-300"],
        ["Win rate", pct(performance?.winRatePct), "text-emerald-300"],
        ["Strategy type", isProspective ? "Active monitoring" : "Historical research", isProspective ? "text-emerald-300" : "text-sky-200"],
      ].map(([metricLabel, value, tone]) => <div key={String(metricLabel)} className="rounded-lg border border-white/10 bg-slate-950/35 px-4 py-3"><div className="text-xs text-slate-400">{metricLabel}</div><div className={`mt-1 text-xl font-semibold tabular-nums ${tone}`}>{value}</div></div>)}
    </section>

    <div className="grid gap-4 xl:grid-cols-[minmax(0,1.6fr)_minmax(19rem,0.8fr)]">
      <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Performance vs SPY</h2><p className="mt-1 text-xs text-slate-500">Stored strategy and benchmark values from the selected run.</p></div>
          <div className="flex rounded-md border border-white/10 p-1">{["30d", "1y", "2y", "3y", "max"].map((key) => <Link key={key} href={`/strategies/${strategy.slug}${key === "max" ? "" : `?period=${key}`}`} className={`rounded px-2 py-1 text-xs font-semibold ${period === key ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>{periodLabels[key]}</Link>)}</div>
        </div>
        <div className="mt-4">{strategy.access?.locked ? <div className="rounded-lg border border-dashed border-white/15 p-8 text-center text-sm text-slate-400">This strategy&apos;s detailed performance is available with {strategy.access.requiredTier} access.</div> : <BacktestChart timeline={curve} />}</div>
      </section>
      <aside className="space-y-3">
        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">{isProspective ? "Follow this strategy" : "About this portfolio"}</h2><p className="mt-2 text-sm leading-6 text-slate-400">{isProspective ? "Follow this strategy for daily updates after Walnut ingests newly available filings and evaluates the model." : "This page replicates past, publicly disclosed Congress trades. It does not currently monitor future disclosures or send trade alerts."}</p>{isProspective ? <div className="mt-4"><StrategyFollowButton slug={strategy.slug} panel /></div> : <div className="mt-4 flex items-center justify-between border-y border-white/10 py-3"><span className="text-sm text-slate-300">Email alerts</span><span className="rounded-full bg-white/5 px-2 py-1 text-xs font-semibold text-slate-500">Not active</span></div>}</section>
        <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Strategy snapshot</h2><dl className="mt-4 space-y-3 text-sm"><div className="flex justify-between gap-3"><dt className="text-slate-500">Universe</dt><dd className="text-right text-slate-200">{displayStrategyUniverse(strategy)}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Benchmark</dt><dd className="text-right text-slate-200">{run?.benchmark ?? "SPY"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Backtest range</dt><dd className="text-right text-slate-200">{run?.backtestStartDate ?? "--"}<br />{run?.backtestEndDate ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Data confidence</dt><dd className="capitalize text-right text-slate-200">{strategy.dataQualityConfidence}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Last refreshed</dt><dd className="text-right text-slate-200">{run?.completedAt?.slice(0, 10) ?? "--"}</dd></div></dl><div className="mt-5 border-t border-white/10 pt-4"><h3 className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">API and automation</h3><p className="mt-2 text-sm leading-6 text-slate-500">Brokerage execution is not available.</p></div></section>
      </aside>
    </div>

    <section className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Methodology</h2>{chips.length ? <div className="mt-3 flex flex-wrap gap-2">{chips.map((chip) => <span key={chip} className="rounded-md border border-emerald-300/20 bg-emerald-300/[0.06] px-2 py-1 text-xs font-semibold capitalize text-emerald-100">{chip}</span>)}</div> : null}<p className="mt-4 text-sm leading-6 text-slate-400">{strategy.methodology ?? "The methodology text for this strategy is still under review."}</p></section>

    <section className="grid gap-4 lg:grid-cols-2"><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">How it works</h2><ol className="mt-4 space-y-3 text-sm leading-6 text-slate-400"><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">1</span>{isProspective ? "Walnut evaluates the saved strategy after the daily data refresh." : "Walnut starts with the member's publicly disclosed stock purchases."}</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">2</span>{isProspective ? "Qualifying securities are saved as the current model portfolio." : "The replication enters only after the public disclosure becomes available."}</li><li><span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full border border-emerald-300/30 text-xs font-semibold text-emerald-200">3</span>{isProspective ? "Daily additions, exits, and rebalances are recorded before subscriber emails are sent." : "A matching reported sale closes the historical position."}</li></ol></div><div className="rounded-lg border border-white/10 bg-slate-950/35 p-4"><h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Risk and data</h2><dl className="mt-4 space-y-3 text-sm"><div className="flex justify-between gap-3"><dt className="text-slate-500">Signals</dt><dd className="text-right text-slate-200">{performance?.independentSignalCount ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Average holdings</dt><dd className="text-right text-slate-200">{performance?.avgHoldings?.toFixed(1) ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Turnover events</dt><dd className="text-right text-slate-200">{performance?.turnoverEvents ?? "--"}</dd></div><div className="flex justify-between gap-3"><dt className="text-slate-500">Execution timing</dt><dd className="max-w-52 text-right text-slate-200">{run?.executionTiming ?? "--"}</dd></div></dl></div></section>

    <section className="rounded-lg border border-white/10 bg-slate-950/35">
      <div className="flex flex-wrap items-end justify-between gap-3 border-b border-white/10 px-4 py-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-200">Portfolio records</h2>
            <div className="flex rounded-md border border-white/10 p-1">
              <Link href={positionsHref(1, "current")} className={`rounded px-2.5 py-1 text-xs font-semibold ${positionsView === "current" ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>Current positions</Link>
              <Link href={positionsHref(1, "history")} className={`rounded px-2.5 py-1 text-xs font-semibold ${positionsView === "history" ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"}`}>Transaction history</Link>
            </div>
          </div>
          <p className="mt-2 text-xs text-slate-500">{positionsView === "history" ? isReconstructedHistory ? "Historical disclosed positions used by this replication. These are not prospective Walnut trade alerts." : "Persisted model-trade and rebalance records from strategy evaluations." : "Model-portfolio holdings saved with the latest strategy run."}</p>
        </div>
        {!strategy.access?.locked && recordsTotal > 0 ? <span className="text-xs tabular-nums text-slate-500">Showing {firstRecordNumber}-{lastRecordNumber} of {recordsTotal}</span> : null}
      </div>
      {strategy.access?.locked ? <p className="px-4 py-8 text-center text-sm text-slate-400">Position detail is available with {strategy.access.requiredTier} access.</p> : positionsView === "current" ? holdingsTotal > 0 ? <>
        <div className="overflow-x-auto"><table className="min-w-full text-left text-sm"><thead className="border-b border-white/10 text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-4 py-3">Ticker</th><th className="px-3 py-3">First added</th><th className="px-3 py-3">Reference price</th><th className="px-3 py-3">Return</th><th className="px-3 py-3">Weight</th><th className="px-4 py-3">Signals</th></tr></thead><tbody className="divide-y divide-white/10">{(strategy.currentHoldings ?? []).map((holding) => <tr key={holding.symbol}><td className="px-4 py-3"><Link href={`/ticker/${holding.symbol}`} className="font-semibold text-white hover:text-emerald-200">{holding.symbol}</Link><div className="mt-0.5 text-xs text-slate-500">{holding.companyName}</div></td><td className="px-3 py-3 text-slate-300">{holding.entryDate ?? "--"}</td><td className="px-3 py-3 tabular-nums text-slate-300">{currency(holding.lastPrice)}</td><td className={`px-3 py-3 font-semibold tabular-nums ${metricTone(holding.returnPct)}`}>{pct(holding.returnPct)}</td><td className="px-3 py-3 tabular-nums text-slate-300">{pct(holding.weightPct)}</td><td className="px-4 py-3 text-slate-300">{holding.sourceSignalCount ?? "--"}</td></tr>)}</tbody></table></div>
      </> : <p className="px-4 py-8 text-center text-sm text-slate-400">No current holdings are stored for this historical strategy.</p> : historyTotal > 0 ? <>
        <div className="overflow-x-auto"><table className="min-w-full text-left text-sm"><thead className="border-b border-white/10 text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-4 py-3">Ticker</th><th className="px-3 py-3">Action</th><th className="px-3 py-3">Entered</th><th className="px-3 py-3">Exited</th><th className="px-3 py-3">Entry price</th><th className="px-3 py-3">Return</th><th className="px-4 py-3">Status</th></tr></thead><tbody className="divide-y divide-white/10">{history.map((record) => <tr key={record.id}><td className="px-4 py-3"><Link href={`/ticker/${record.symbol}`} className="font-semibold text-white hover:text-emerald-200">{record.symbol ?? "--"}</Link>{record.recordType === "reconstructed_position" ? <div className="mt-0.5 text-xs text-slate-500">Disclosed source position</div> : <div className="mt-0.5 text-xs text-slate-500">Model portfolio event</div>}</td><td className="px-3 py-3 capitalize text-slate-300">{label(record.action)}</td><td className="px-3 py-3 text-slate-300">{record.effectiveDate ?? record.signalDate ?? "--"}</td><td className="px-3 py-3 text-slate-300">{record.exitDate ?? "--"}</td><td className="px-3 py-3 tabular-nums text-slate-300">{currency(record.entryPrice)}</td><td className={`px-3 py-3 font-semibold tabular-nums ${metricTone(record.returnPct)}`}>{pct(record.returnPct)}</td><td className="px-4 py-3"><span className="rounded border border-white/10 px-2 py-1 text-xs capitalize text-slate-300">{label(record.status)}</span>{record.confidence ? <span className="ml-2 text-xs capitalize text-slate-500">{record.confidence}</span> : null}</td></tr>)}</tbody></table></div>
      </> : <p className="px-4 py-8 text-center text-sm text-slate-400">No transaction history is stored for this strategy yet.</p>}
      {!strategy.access?.locked && recordsTotal > 0 ? isAuthenticated && totalPages > 1 ? <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/10 px-4 py-3"><span className="text-xs text-slate-500">Page {currentPage} of {totalPages}</span><div className="flex items-center gap-2"><Link aria-disabled={currentPage === 1} href={positionsHref(Math.max(1, currentPage - 1))} className={`rounded-md border px-3 py-1.5 text-xs font-semibold ${currentPage === 1 ? "pointer-events-none border-white/5 text-slate-600" : "border-white/10 text-slate-200 hover:border-white/20 hover:text-white"}`}>Previous</Link><Link aria-disabled={currentPage === totalPages} href={positionsHref(Math.min(totalPages, currentPage + 1))} className={`rounded-md border px-3 py-1.5 text-xs font-semibold ${currentPage === totalPages ? "pointer-events-none border-white/5 text-slate-600" : "border-emerald-300/30 text-emerald-100 hover:border-emerald-300/60"}`}>Next</Link></div></div> : !isAuthenticated && recordsTotal > holdingsPageSize ? <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/10 px-4 py-3"><span className="text-xs text-slate-500">Sign in to browse the remaining {recordsTotal - holdingsPageSize} records.</span><Link href={signInHref} className="rounded-md border border-emerald-300/30 px-3 py-1.5 text-xs font-semibold text-emerald-100 hover:border-emerald-300/60">Sign in</Link></div> : null : null}
    </section>
  </div>;
}
