"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  getAdminStrategies,
  getAdminStrategy,
  type StrategyDefinitionPayload,
  type StrategyDetailPayload,
  type StrategyHolding,
  type StrategyPerformanceSnapshot,
} from "@/lib/api";
import type { AdminToastApi } from "@/components/admin/AdminToast";

const PERIODS = ["max", "30d", "1y", "2y", "3y"] as const;

type PeriodKey = (typeof PERIODS)[number];

function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function formatPct(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return `${formatNumber(value)}%`;
}

function statusMessage(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 403) return "Admin access required.";
  }
  return error instanceof Error ? error.message : "Unable to load strategies.";
}

function metricRows(performance?: StrategyPerformanceSnapshot | null) {
  return [
    ["Walnut Score", formatNumber(performance?.walnutStrategyScore)],
    ["CAGR", formatPct(performance?.cagrPct)],
    ["Return", formatPct(performance?.totalReturnPct)],
    ["SPY CAGR", formatPct(performance?.benchmarkCagrPct)],
    ["Alpha", formatPct(performance?.alphaCagrPct)],
    ["Sharpe", formatNumber(performance?.sharpe)],
    ["Max drawdown", formatPct(performance?.maxDrawdownPct)],
    ["Volatility", formatPct(performance?.annualizedVolatilityPct)],
    ["Win rate", formatPct(performance?.winRatePct)],
    ["Trades", formatNumber(performance?.tradeCount, 0)],
    ["Signals", formatNumber(performance?.independentSignalCount, 0)],
    ["Avg holdings", formatNumber(performance?.avgHoldings)],
  ] as const;
}

function JsonBlock({ value }: { value: unknown }) {
  return (
    <pre className="max-h-96 overflow-auto rounded-lg border border-white/10 bg-slate-950/70 p-3 text-xs leading-5 text-slate-300">
      {JSON.stringify(value ?? {}, null, 2)}
    </pre>
  );
}

function StrategyPill({ children, tone = "neutral" }: { children: string; tone?: "neutral" | "good" | "warn" }) {
  const classes =
    tone === "good"
      ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
      : tone === "warn"
        ? "border-amber-300/30 bg-amber-300/10 text-amber-100"
        : "border-white/10 bg-slate-950/60 text-slate-300";
  return <span className={`rounded-md border px-2 py-1 text-xs font-semibold ${classes}`}>{children}</span>;
}

function StrategyListItem({
  strategy,
  selected,
  onSelect,
}: {
  strategy: StrategyDefinitionPayload;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full rounded-lg border p-4 text-left transition ${
        selected ? "border-emerald-300/40 bg-emerald-300/10" : "border-white/10 bg-slate-950/40 hover:border-white/20"
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="truncate font-semibold text-white">{strategy.name}</h3>
          <p className="mt-1 truncate text-xs text-slate-500">{strategy.slug}</p>
        </div>
        <StrategyPill tone={strategy.status === "published" ? "good" : "warn"}>{strategy.status}</StrategyPill>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        <StrategyPill>{strategy.category}</StrategyPill>
        <StrategyPill>{strategy.dataQualityConfidence}</StrategyPill>
        <StrategyPill>{`Score ${formatNumber(strategy.latestRun?.walnutStrategyScore)}`}</StrategyPill>
      </div>
    </button>
  );
}

function HoldingsTable({ holdings }: { holdings: StrategyHolding[] }) {
  const rows = holdings.slice(0, 30);
  if (!rows.length) {
    return <p className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">No current holdings stored.</p>;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-white/10">
      <table className="min-w-full divide-y divide-white/10 text-sm">
        <thead className="bg-slate-950/70 text-left text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-3 py-2">Rank</th>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Weight</th>
            <th className="px-3 py-2">Entry</th>
            <th className="px-3 py-2">Return</th>
            <th className="px-3 py-2">Signals</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10 bg-slate-950/30">
          {rows.map((holding) => (
            <tr key={`${holding.rank ?? "x"}-${holding.symbol}`}>
              <td className="px-3 py-2 text-slate-400">{holding.rank ?? "n/a"}</td>
              <td className="px-3 py-2 font-semibold text-white">{holding.symbol}</td>
              <td className="px-3 py-2 text-slate-300">{formatPct(holding.weightPct)}</td>
              <td className="px-3 py-2 text-slate-400">{holding.entryDate ?? "n/a"}</td>
              <td className="px-3 py-2 text-slate-300">{formatPct(holding.returnPct)}</td>
              <td className="px-3 py-2 text-slate-400">{holding.sourceSignalCount ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function AdminStrategiesView({ showToast }: { showToast?: AdminToastApi["showToast"] }) {
  const [period, setPeriod] = useState<PeriodKey>("max");
  const [strategies, setStrategies] = useState<StrategyDefinitionPayload[]>([]);
  const [selectedSlug, setSelectedSlug] = useState<string | null>(null);
  const [detail, setDetail] = useState<StrategyDetailPayload | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const selected = useMemo(
    () => strategies.find((strategy) => strategy.slug === selectedSlug) ?? strategies[0] ?? null,
    [selectedSlug, strategies],
  );

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      setBusy(true);
      setStatus(null);
      try {
        const response = await getAdminStrategies({ period, sort: "walnut_score", signal: controller.signal });
        setStrategies(response.items);
        setSelectedSlug((current) => current ?? response.items[0]?.slug ?? null);
      } catch (error) {
        if (controller.signal.aborted) return;
        const message = statusMessage(error);
        setStatus(message);
        showToast?.({ message, tone: "error" });
      } finally {
        if (!controller.signal.aborted) setBusy(false);
      }
    }
    load();
    return () => controller.abort();
  }, [period, showToast]);

  useEffect(() => {
    if (!selected?.slug) {
      setDetail(null);
      return;
    }
    const controller = new AbortController();
    async function loadDetail() {
      setBusy(true);
      setStatus(null);
      try {
        setDetail(await getAdminStrategy(selected.slug, { period, equityLimit: 1500, signal: controller.signal }));
      } catch (error) {
        if (controller.signal.aborted) return;
        const message = statusMessage(error);
        setStatus(message);
        showToast?.({ message, tone: "error" });
      } finally {
        if (!controller.signal.aborted) setBusy(false);
      }
    }
    loadDetail();
    return () => controller.abort();
  }, [period, selected?.slug, showToast]);

  const activeDetail = detail?.slug === selected?.slug ? detail : null;
  const performance = activeDetail?.performance ?? selected?.performance ?? null;
  const run = activeDetail?.latestRun ?? selected?.latestRun ?? null;
  const diagnostics = run?.diagnostics as Record<string, unknown> | undefined;

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">Strategy drafts</h2>
            <p className="mt-2 text-sm text-slate-400">
              {strategies.length} persisted strategy {strategies.length === 1 ? "draft" : "drafts"} in review.
            </p>
          </div>
          <div className="inline-flex rounded-lg border border-white/10 bg-slate-950/50 p-1">
            {PERIODS.map((nextPeriod) => (
              <button
                key={nextPeriod}
                type="button"
                onClick={() => setPeriod(nextPeriod)}
                className={`rounded-md px-3 py-2 text-sm font-semibold ${
                  period === nextPeriod ? "bg-emerald-300/10 text-emerald-100" : "text-slate-300 hover:text-white"
                }`}
              >
                {nextPeriod}
              </button>
            ))}
          </div>
        </div>
        {status ? <p className="mt-3 text-sm text-amber-200">{status}</p> : null}
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(18rem,24rem)_minmax(0,1fr)]">
        <section className="space-y-3">
          {strategies.map((strategy) => (
            <StrategyListItem
              key={strategy.slug}
              strategy={strategy}
              selected={strategy.slug === selected?.slug}
              onSelect={() => setSelectedSlug(strategy.slug)}
            />
          ))}
          {!strategies.length && !busy ? (
            <p className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">No strategy drafts found.</p>
          ) : null}
        </section>

        <section className="min-w-0 space-y-5 rounded-lg border border-white/10 bg-slate-900/70 p-5">
          {selected ? (
            <>
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="min-w-0">
                  <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">{selected.category}</p>
                  <h2 className="mt-1 text-2xl font-semibold text-white">{selected.name}</h2>
                  <p className="mt-2 break-all text-xs text-slate-500">{selected.slug}</p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <StrategyPill tone={selected.status === "published" ? "good" : "warn"}>{selected.status}</StrategyPill>
                  <StrategyPill>{selected.dataQualityConfidence}</StrategyPill>
                  <StrategyPill>{selected.accessTier}</StrategyPill>
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                {metricRows(performance).map(([label, value]) => (
                  <div key={label} className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
                    <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
                    <div className="mt-1 text-lg font-semibold text-white">{value}</div>
                  </div>
                ))}
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                  <h3 className="font-semibold text-white">Methodology</h3>
                  <p className="mt-2 text-sm text-slate-300">{selected.methodology ?? "No methodology text stored."}</p>
                  <dl className="mt-4 space-y-2 text-sm">
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Backtest</dt>
                      <dd className="text-right text-slate-200">{run?.backtestStartDate ?? "n/a"} to {run?.backtestEndDate ?? "n/a"}</dd>
                    </div>
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Benchmark</dt>
                      <dd className="text-right text-slate-200">{run?.benchmark ?? "n/a"}</dd>
                    </div>
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Code version</dt>
                      <dd className="text-right text-slate-200">{run?.codeVersion ?? "n/a"}</dd>
                    </div>
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Run key</dt>
                      <dd className="max-w-52 break-all text-right text-slate-200">{run?.runKey ?? "n/a"}</dd>
                    </div>
                  </dl>
                </div>

                <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                  <h3 className="font-semibold text-white">Review flags</h3>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {(selected.riskNotes?.length ? selected.riskNotes : ["none"]).map((note) => (
                      <StrategyPill key={note} tone={note === "none" ? "good" : "warn"}>{note}</StrategyPill>
                    ))}
                  </div>
                  <p className="mt-4 text-sm text-slate-400">{selected.walnutTake ?? "No Walnut take stored."}</p>
                </div>
              </div>

              <div>
                <div className="mb-3 flex items-center justify-between gap-3">
                  <h3 className="font-semibold text-white">Current holdings</h3>
                  <span className="text-sm text-slate-500">{activeDetail?.currentHoldings?.length ?? 0} stored</span>
                </div>
                <HoldingsTable holdings={activeDetail?.currentHoldings ?? []} />
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <details className="rounded-lg border border-white/10 bg-slate-950/40 p-4" open>
                  <summary className="cursor-pointer font-semibold text-white">Rule and parameters</summary>
                  <div className="mt-3 grid gap-3">
                    <JsonBlock value={{ rule: selected.rule, parameters: selected.parameters, universe: selected.universe }} />
                  </div>
                </details>

                <details className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                  <summary className="cursor-pointer font-semibold text-white">Diagnostics</summary>
                  <div className="mt-3">
                    <JsonBlock value={diagnostics ?? {}} />
                  </div>
                </details>
              </div>
            </>
          ) : (
            <p className="text-sm text-slate-400">Select a strategy draft.</p>
          )}
        </section>
      </div>
    </div>
  );
}
