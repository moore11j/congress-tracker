"use client";

import { useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { BacktestChart } from "@/components/backtesting/BacktestChart";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import type { BacktestPresetsResponse, BacktestRunRequest, BacktestRunResponse, BacktestStrategyType } from "@/lib/api";
import { runBacktest } from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";
import { cardClassName, inputClassName, primaryButtonClassName, selectClassName } from "@/lib/styles";

type Props = {
  initialEntitlements: Entitlements;
  initialPresets: BacktestPresetsResponse;
  initialQuery?: {
    strategy?: string;
    watchlist_id?: string;
    saved_screen_id?: string;
    scope?: string;
    member_id?: string;
    insider_cik?: string;
  };
};

const strategyTabs: { key: BacktestStrategyType; label: string }[] = [
  { key: "watchlist", label: "Watchlist" },
  { key: "saved_screen", label: "Saved Screen" },
  { key: "congress", label: "Congress" },
  { key: "insider", label: "Insider" },
];

function normalizeStrategy(value: string | undefined, presets: BacktestPresetsResponse): BacktestStrategyType {
  return presets.strategy_types.some((item) => item.key === value) ? (value as BacktestStrategyType) : "watchlist";
}

function parsePositiveInt(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function shiftIsoDate(endDate: string, days: number) {
  const parsed = new Date(`${endDate}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) return endDate;
  parsed.setUTCDate(parsed.getUTCDate() - Math.max(days, 0));
  return parsed.toISOString().slice(0, 10);
}

function pct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(1)}%`;
}

function formatPrice(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
}

function formatDate(value: string) {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return value;
  return parsed.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function toneClass(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "text-white";
  if (value > 0) return "text-emerald-300";
  if (value < 0) return "text-rose-300";
  return "text-slate-200";
}

function summaryItems(result: BacktestRunResponse | null) {
  if (!result) return [];
  return [
    { label: "Total Return", value: pct(result.summary.strategy_return_pct), tone: toneClass(result.summary.strategy_return_pct) },
    { label: "S&P Return", value: pct(result.summary.benchmark_return_pct), tone: toneClass(result.summary.benchmark_return_pct) },
    { label: "Alpha", value: pct(result.summary.alpha_pct), tone: toneClass(result.summary.alpha_pct) },
    { label: "Win Rate", value: pct(result.summary.win_rate), tone: "text-white" },
    { label: "Max Drawdown", value: pct(result.summary.max_drawdown_pct), tone: "text-white" },
    { label: "Trades / Positions", value: `${result.summary.trade_count} / ${result.summary.positions_count}`, tone: "text-white" },
  ];
}

function extractErrorMessage(error: unknown) {
  if (error instanceof Error && error.message) {
    if (error.message.includes("premium_required")) return "Portfolio backtesting is currently a Premium feature.";
    return error.message.replace(/^HTTP \d+\s+\w+\s+/m, "").trim() || error.message;
  }
  return "Unable to run this backtest right now.";
}

function ResultSkeleton() {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
        {Array.from({ length: 6 }).map((_, index) => (
          <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
            <SkeletonBlock className="h-3 w-24" />
            <SkeletonBlock className="mt-3 h-7 w-20" />
          </div>
        ))}
      </div>
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
        <SkeletonBlock className="h-4 w-44" />
        <SkeletonBlock className="mt-3 h-[280px] w-full" />
      </div>
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
        <SkeletonBlock className="h-4 w-40" />
        <SkeletonTable columns={6} rows={5} />
      </div>
    </div>
  );
}

export function BacktestingWorkbench({ initialEntitlements, initialPresets, initialQuery }: Props) {
  const strategyFallback = initialPresets.watchlists.length > 0 ? "watchlist" : "congress";
  const defaultStrategy = normalizeStrategy(initialQuery?.strategy, initialPresets) || strategyFallback;
  const today = initialPresets.today || new Date().toISOString().slice(0, 10);

  const [strategy, setStrategy] = useState<BacktestStrategyType>(defaultStrategy);
  const [watchlistId, setWatchlistId] = useState<string>(
    initialQuery?.watchlist_id || String(initialPresets.watchlists[0]?.id ?? "")
  );
  const [savedScreenId, setSavedScreenId] = useState<string>(
    initialQuery?.saved_screen_id || String(initialPresets.saved_screens[0]?.id ?? "")
  );
  const [sourceScope, setSourceScope] = useState<string>(
    initialQuery?.scope || (defaultStrategy === "insider" ? "all_insiders" : "all_congress")
  );
  const [memberId, setMemberId] = useState<string>(initialQuery?.member_id || "");
  const [insiderCik, setInsiderCik] = useState<string>(initialQuery?.insider_cik || "");
  const [lookbackDays, setLookbackDays] = useState<number>(parsePositiveInt(undefined, initialPresets.defaults.lookback_days));
  const [holdDays, setHoldDays] = useState<30 | 60 | 90 | 180 | 365>(90);
  const [result, setResult] = useState<BacktestRunResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const canRun = initialPresets.access.can_run && initialEntitlements.features.includes("backtesting");
  const startDate = useMemo(() => shiftIsoDate(today, Math.max(lookbackDays - 1, 0)), [lookbackDays, today]);
  const summary = summaryItems(result);

  const payload = useMemo<BacktestRunRequest | null>(() => {
    const base: BacktestRunRequest = {
      strategy_type: strategy,
      start_date: startDate,
      end_date: today,
      hold_days: holdDays,
      rebalance: "on_signal",
      weighting: "equal",
      benchmark: "^GSPC",
    };
    if (strategy === "watchlist") {
      const id = Number(watchlistId);
      return Number.isFinite(id) && id > 0 ? { ...base, watchlist_id: id } : null;
    }
    if (strategy === "saved_screen") {
      const id = Number(savedScreenId);
      return Number.isFinite(id) && id > 0 ? { ...base, saved_screen_id: id } : null;
    }
    if (strategy === "congress") {
      if (sourceScope === "member" && !memberId.trim()) return null;
      return {
        ...base,
        source_scope: (sourceScope === "house" || sourceScope === "senate" || sourceScope === "member" ? sourceScope : "all_congress"),
        member_id: memberId.trim() || undefined,
      };
    }
    if (sourceScope === "insider" && !insiderCik.trim()) return null;
    return {
      ...base,
      source_scope: sourceScope === "insider" ? "insider" : "all_insiders",
      insider_cik: insiderCik.trim() || undefined,
    };
  }, [holdDays, insiderCik, memberId, savedScreenId, sourceScope, startDate, strategy, today, watchlistId]);

  const canSubmit = Boolean(payload) && !loading && canRun;

  async function handleRun() {
    if (!payload || loading || !canRun) return;
    setLoading(true);
    setError(null);
    try {
      const next = await runBacktest(payload);
      setResult(next);
    } catch (runError) {
      setError(extractErrorMessage(runError));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  function handleStrategyChange(nextStrategy: BacktestStrategyType) {
    setStrategy(nextStrategy);
    setError(null);
    if (nextStrategy === "watchlist" && !watchlistId) setWatchlistId(String(initialPresets.watchlists[0]?.id ?? ""));
    if (nextStrategy === "saved_screen" && !savedScreenId) setSavedScreenId(String(initialPresets.saved_screens[0]?.id ?? ""));
    if (nextStrategy === "congress") setSourceScope("all_congress");
    if (nextStrategy === "insider") setSourceScope("all_insiders");
  }

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[0.92fr_1.08fr]">
        <div className={`${cardClassName} space-y-5`}>
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Premium Research</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">Backtest Signals &amp; Portfolios</h1>
            <p className="mt-2 max-w-2xl text-sm text-slate-400">
              Test how Congress, insider, watchlist, and saved-screen strategies performed historically.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            {strategyTabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                onClick={() => handleStrategyChange(tab.key)}
                className={`rounded-2xl border px-4 py-2 text-sm font-semibold transition ${
                  strategy === tab.key
                    ? "border-emerald-300/40 bg-emerald-400/10 text-emerald-100"
                    : "border-white/10 bg-slate-950/50 text-slate-300 hover:border-white/20 hover:text-white"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            {strategy === "watchlist" ? (
              <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">
                Watchlist
                <select value={watchlistId} onChange={(event) => setWatchlistId(event.target.value)} className={selectClassName} disabled={!canRun}>
                  <option value="">{initialPresets.watchlists.length ? "Select a watchlist" : "No watchlists found"}</option>
                  {initialPresets.watchlists.map((watchlist) => (
                    <option key={watchlist.id} value={watchlist.id}>
                      {watchlist.name} · {watchlist.ticker_count} tickers
                    </option>
                  ))}
                </select>
              </label>
            ) : null}

            {strategy === "saved_screen" ? (
              <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">
                Saved Screen
                <select value={savedScreenId} onChange={(event) => setSavedScreenId(event.target.value)} className={selectClassName} disabled={!canRun}>
                  <option value="">{initialPresets.saved_screens.length ? "Select a saved screen" : "No saved screens found"}</option>
                  {initialPresets.saved_screens.map((screen) => (
                    <option key={screen.id} value={screen.id}>
                      {screen.name}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}

            {strategy === "congress" ? (
              <>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Scope
                  <select value={sourceScope} onChange={(event) => setSourceScope(event.target.value)} className={selectClassName} disabled={!canRun}>
                    {initialPresets.source_scopes.congress.map((scope) => (
                      <option key={scope.key} value={scope.key}>
                        {scope.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Member ID
                  <input
                    value={memberId}
                    onChange={(event) => setMemberId(event.target.value)}
                    className={inputClassName}
                    placeholder="M000355"
                    disabled={!canRun || sourceScope !== "member"}
                  />
                </label>
              </>
            ) : null}

            {strategy === "insider" ? (
              <>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Scope
                  <select value={sourceScope} onChange={(event) => setSourceScope(event.target.value)} className={selectClassName} disabled={!canRun}>
                    {initialPresets.source_scopes.insider.map((scope) => (
                      <option key={scope.key} value={scope.key}>
                        {scope.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
                  Insider CIK
                  <input
                    value={insiderCik}
                    onChange={(event) => setInsiderCik(event.target.value)}
                    className={inputClassName}
                    placeholder="0001234567"
                    disabled={!canRun || sourceScope !== "insider"}
                  />
                </label>
              </>
            ) : null}

            <div className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">
              Lookback
              <div className="flex flex-wrap gap-2">
                {initialPresets.lookback_options.map((option) => (
                  <button
                    key={option.days}
                    type="button"
                    onClick={() => setLookbackDays(option.days)}
                    className={`rounded-2xl border px-3 py-2 text-sm font-semibold normal-case transition ${
                      lookbackDays === option.days
                        ? "border-emerald-300/40 bg-emerald-400/10 text-emerald-100"
                        : "border-white/10 bg-slate-950/50 text-slate-300 hover:border-white/20 hover:text-white"
                    }`}
                    disabled={!canRun}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
            </div>

            <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
              Hold Period
              <select value={holdDays} onChange={(event) => setHoldDays(Number(event.target.value) as 30 | 60 | 90 | 180 | 365)} className={selectClassName} disabled={!canRun}>
                {initialPresets.hold_day_options.map((option) => (
                  <option key={option.days} value={option.days}>
                    {option.label} days
                  </option>
                ))}
              </select>
            </label>

            <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">
              Benchmark
              <select value="^GSPC" className={selectClassName} disabled={true}>
                <option value="^GSPC">S&amp;P 500</option>
              </select>
            </label>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/10 pt-4">
            <div className="text-sm text-slate-400">
              Window <span className="text-white">{startDate}</span> to <span className="text-white">{today}</span>
            </div>
            <button type="button" onClick={handleRun} disabled={!canSubmit} className={`${primaryButtonClassName} disabled:cursor-not-allowed disabled:opacity-60`}>
              {canRun ? (loading ? "Running..." : "Run backtest") : "Premium required"}
            </button>
          </div>

          {error ? <div className="rounded-2xl border border-rose-300/20 bg-rose-400/[0.07] px-4 py-3 text-sm text-rose-100">{error}</div> : null}
        </div>

        <div className={`${cardClassName} space-y-4`}>
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Results</p>
              <h2 className="mt-1 text-xl font-semibold text-white">Strategy vs S&amp;P 500</h2>
            </div>
            <span className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-300">
              {initialEntitlements.tier}
            </span>
          </div>

          {!canRun ? (
            <div className="space-y-4">
              <UpgradePrompt
                title="Unlock portfolio backtesting"
                body="Backtesting is included with Premium. Free users can preview the workflow here, but custom runs stay locked until upgrade."
              />
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {["Total Return", "S&P Return", "Alpha", "Win Rate", "Max Drawdown", "Trades / Positions"].map((label) => (
                  <div key={label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                    <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{label}</p>
                    <p className="mt-3 text-2xl font-semibold text-slate-300">Premium</p>
                  </div>
                ))}
              </div>
              <div className="rounded-2xl border border-white/10 bg-[#07111d] px-4 py-12 text-center text-sm text-slate-400">
                Strategy equity curve previews unlock after upgrade.
              </div>
            </div>
          ) : loading ? (
            <ResultSkeleton />
          ) : result ? (
            <div className="space-y-5">
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {summary.map((item) => (
                  <div key={item.label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
                    <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{item.label}</p>
                    <p className={`mt-3 text-2xl font-semibold ${item.tone}`}>{item.value}</p>
                  </div>
                ))}
              </div>

              <BacktestChart timeline={result.timeline} />

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-400">Assumptions</h3>
                <p className="mt-2 text-sm text-slate-300">
                  Backtests are hypothetical and based on disclosed historical data. Congress and insider trades may be reported after execution, so simulations use available disclosure/filing timing where possible. Results exclude taxes, transaction costs, and market impact.
                </p>
                <div className="mt-3 flex flex-wrap gap-2">
                  {result.assumptions.map((assumption) => (
                    <span key={assumption} className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-xs text-slate-300">
                      {assumption}
                    </span>
                  ))}
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-400">Positions</h3>
                  <span className="text-xs text-slate-500">{result.positions.length} rows</span>
                </div>
                {result.positions.length === 0 ? (
                  <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-center text-sm text-slate-400">
                    No simulated positions matched this configuration in the selected window.
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-white/10 text-sm">
                      <thead>
                        <tr className="text-left text-xs uppercase tracking-[0.18em] text-slate-500">
                          <th className="pb-3 pr-4 font-medium">Symbol</th>
                          <th className="pb-3 pr-4 font-medium">Entry</th>
                          <th className="pb-3 pr-4 font-medium">Exit</th>
                          <th className="pb-3 pr-4 font-medium">Entry Px</th>
                          <th className="pb-3 pr-4 font-medium">Exit Px</th>
                          <th className="pb-3 pr-4 font-medium">Return</th>
                          <th className="pb-3 font-medium">Source</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-white/5">
                        {result.positions.map((position) => (
                          <tr key={`${position.symbol}-${position.entry_date}-${position.source_event_id ?? "static"}`} className="text-slate-200">
                            <td className="py-3 pr-4 font-semibold text-white">{position.symbol}</td>
                            <td className="py-3 pr-4">{formatDate(position.entry_date)}</td>
                            <td className="py-3 pr-4">{formatDate(position.exit_date)}</td>
                            <td className="py-3 pr-4 tabular-nums">{formatPrice(position.entry_price)}</td>
                            <td className="py-3 pr-4 tabular-nums">{formatPrice(position.exit_price)}</td>
                            <td className={`py-3 pr-4 font-semibold tabular-nums ${toneClass(position.return_pct)}`}>{pct(position.return_pct)}</td>
                            <td className="py-3 text-slate-400">{position.source_label || (position.source_event_id ? `Event #${position.source_event_id}` : "Current universe")}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="rounded-2xl border border-dashed border-white/10 px-5 py-10 text-center">
                <h3 className="text-lg font-semibold text-white">Run a backtest to populate the panel</h3>
                <p className="mt-2 text-sm text-slate-400">
                  Equal-weight, daily-close, long-only results will appear here alongside the S&amp;P 500 benchmark.
                </p>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-400">Assumptions</h3>
                <p className="mt-2 text-sm text-slate-300">
                  Backtests are hypothetical and based on disclosed historical data. Congress and insider trades may be reported after execution, so simulations use available disclosure/filing timing where possible. Results exclude taxes, transaction costs, and market impact.
                </p>
              </div>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
