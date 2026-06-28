"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
  type CongressTraderLeaderboardChamber,
  type CongressTraderLeaderboardPerformanceModel,
  type CongressTraderLeaderboardPortfolioSort,
  type CongressTraderLeaderboardSort,
  type CongressTraderLeaderboardSourceMode,
  type CongressTraderLeaderboardTradeSort,
} from "@/lib/api";
import { cardClassName } from "@/lib/styles";

const LOOKBACK_OPTIONS = [30, 90, 180, 365, 1095] as const;
const TRADE_LOOKBACK_OPTIONS = [
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "180D", days: 180 },
  { label: "1Y", days: 365 },
  { label: "3Y", days: 1095 },
] as const;
const PORTFOLIO_LOOKBACK_OPTIONS = [
  { label: "30D", days: 30 },
  { label: "90D", days: 90 },
  { label: "180D", days: 180 },
  { label: "1Y", days: 365 },
  { label: "3Y", days: 1095 },
] as const;
const SOURCE_MODE_OPTIONS: CongressTraderLeaderboardSourceMode[] = ["congress", "insiders"];
const PERFORMANCE_MODEL_OPTIONS: CongressTraderLeaderboardPerformanceModel[] = ["outcomes", "portfolio"];
const INSIDER_PORTFOLIO_DISABLED_TITLE = "Portfolio Simulation is currently available for Congress only.";
const TRADE_SORT_OPTIONS: CongressTraderLeaderboardTradeSort[] = ["avg_alpha", "avg_return", "win_rate", "trade_count"];
const PORTFOLIO_SORT_OPTIONS: CongressTraderLeaderboardPortfolioSort[] = [
  "alpha_pct",
  "total_return_pct",
  "cagr_pct",
  "sharpe_ratio",
  "max_drawdown_pct",
  "win_rate_pct",
];
const LIMIT_OPTIONS = [10, 25, 50, 100] as const;
const LEADERBOARD_FILTER_PARAM_KEYS = [
  "lookback_days",
  "chamber",
  "source_mode",
  "performance_model",
  "mode",
  "sort",
  "min_trades",
  "limit",
] as const;

type LeaderboardFilters = {
  lookbackDays: number;
  chamber: CongressTraderLeaderboardChamber;
  sourceMode: CongressTraderLeaderboardSourceMode;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
  sort: CongressTraderLeaderboardSort;
  minTrades: number;
  limit: number;
};

function normalizePortfolioLookback(lookbackDays: number): number {
  return PORTFOLIO_LOOKBACK_OPTIONS.some((option) => option.days === lookbackDays) ? lookbackDays : 365;
}

function normalizeTradeLookback(lookbackDays: number): number {
  return LOOKBACK_OPTIONS.includes(lookbackDays as (typeof LOOKBACK_OPTIONS)[number]) ? lookbackDays : 365;
}

function normalizeFilters(filters: LeaderboardFilters): LeaderboardFilters {
  const performanceModel = filters.sourceMode === "congress" ? filters.performanceModel : "outcomes";
  const lookbackDays =
    performanceModel === "portfolio" ? normalizePortfolioLookback(filters.lookbackDays) : normalizeTradeLookback(filters.lookbackDays);
  const chamber = filters.sourceMode === "insiders" ? "all" : filters.chamber;
  let sort = filters.sort;
  if (performanceModel === "portfolio" && !PORTFOLIO_SORT_OPTIONS.includes(sort as CongressTraderLeaderboardPortfolioSort)) {
    sort = "alpha_pct";
  }
  if (performanceModel !== "portfolio" && !TRADE_SORT_OPTIONS.includes(sort as CongressTraderLeaderboardTradeSort)) {
    sort = "avg_alpha";
  }
  return { ...filters, lookbackDays, chamber, performanceModel, sort };
}

function filtersSignature(filters: LeaderboardFilters): string {
  return [
    filters.lookbackDays,
    filters.chamber,
    filters.sourceMode,
    filters.performanceModel,
    filters.sort,
    filters.minTrades,
    filters.limit,
  ].join("|");
}

function buildLeaderboardHref(pathname: string, searchParamsString: string, filters: LeaderboardFilters): string {
  const params = new URLSearchParams(searchParamsString);
  LEADERBOARD_FILTER_PARAM_KEYS.forEach((key) => params.delete(key));

  const nextFilters = normalizeFilters(filters);
  params.set("lookback_days", String(nextFilters.lookbackDays));
  if (nextFilters.performanceModel !== "portfolio") {
    params.set("chamber", nextFilters.sourceMode === "insiders" ? "all" : nextFilters.chamber);
  }
  params.set("source_mode", nextFilters.sourceMode);
  params.set("performance_model", nextFilters.performanceModel);
  if (nextFilters.performanceModel === "portfolio") {
    params.set("mode", "realistic_disclosure_lag");
  }
  params.set("sort", nextFilters.sort);
  if (nextFilters.performanceModel !== "portfolio") {
    params.set("min_trades", String(nextFilters.minTrades));
  }
  params.set("limit", String(nextFilters.limit));

  const nextSearch = params.toString();
  return `${pathname}${nextSearch ? `?${nextSearch}` : ""}`;
}

function pillClassName(active: boolean): string {
  return `rounded-full border px-3 py-1 text-xs font-semibold transition ${
    active
      ? "border-emerald-300/60 bg-emerald-500/20 text-emerald-100"
      : "border-white/15 bg-white/[0.03] text-slate-300 hover:bg-white/[0.06]"
  }`;
}

function disabledPillClassName(): string {
  return "cursor-not-allowed rounded-full border border-white/10 bg-white/[0.02] px-3 py-1 text-xs font-semibold text-slate-500 opacity-60";
}

export function CongressTraderLeaderboardFiltersClient({
  lookbackDays,
  chamber,
  sourceMode,
  performanceModel,
  sort,
  minTrades,
  limit,
}: LeaderboardFilters) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const initialFilters = useMemo(
    () => normalizeFilters({ lookbackDays, chamber, sourceMode, performanceModel, sort, minTrades, limit }),
    [chamber, limit, lookbackDays, minTrades, performanceModel, sort, sourceMode],
  );
  const initialFiltersKey = useMemo(() => filtersSignature(initialFilters), [initialFilters]);
  const [draftFilters, setDraftFilters] = useState<LeaderboardFilters>(() => initialFilters);
  const [appliedFilters, setAppliedFilters] = useState<LeaderboardFilters>(() => initialFilters);
  const draftFiltersKey = filtersSignature(normalizeFilters(draftFilters));
  const appliedFiltersKey = filtersSignature(appliedFilters);
  const hasPendingChanges = draftFiltersKey !== appliedFiltersKey;
  const draftIsInsiderMode = draftFilters.sourceMode === "insiders";
  const draftIsPortfolioMode = draftFilters.performanceModel === "portfolio";
  const applyButtonClassName = hasPendingChanges
    ? "inline-flex h-10 w-full items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20 sm:w-auto"
    : "inline-flex h-10 w-full cursor-not-allowed items-center justify-center rounded-2xl border border-slate-800 bg-slate-950/30 px-4 text-sm font-semibold text-slate-500 sm:w-auto";

  useEffect(() => {
    setDraftFilters(initialFilters);
    setAppliedFilters(initialFilters);
  }, [initialFilters, initialFiltersKey]);

  const updateDraftFilters = (patch: Partial<LeaderboardFilters>) => {
    setDraftFilters((current) => normalizeFilters({ ...current, ...patch }));
  };

  const applyFilters = () => {
    const nextFilters = normalizeFilters(draftFilters);
    const nextKey = filtersSignature(nextFilters);
    if (nextKey === appliedFiltersKey) return;
    setDraftFilters(nextFilters);
    setAppliedFilters(nextFilters);
    router.push(buildLeaderboardHref(pathname, searchParamsString, nextFilters), { scroll: false });
  };

  return (
    <div className={`${cardClassName} grid gap-3 md:grid-cols-[max-content_max-content_max-content_max-content]`}>
      <div className="space-y-1.5">
        <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Universe</div>
        <div className="flex flex-wrap items-center gap-1">
          {SOURCE_MODE_OPTIONS.map((option) => {
            const label = option === "congress" ? "Congress" : "Insiders";
            const active = draftFilters.sourceMode === option;
            const targetPerformanceModel =
              option === "insiders"
                ? "outcomes"
                : active
                  ? draftFilters.performanceModel
                  : "portfolio";
            const targetSort = targetPerformanceModel === "portfolio" ? "alpha_pct" : "avg_alpha";
            const targetChamber = option === "insiders" ? "all" : draftFilters.chamber;
            return (
              <button
                key={option}
                type="button"
                aria-pressed={active}
                onClick={() =>
                  updateDraftFilters({
                    chamber: targetChamber,
                    sourceMode: option,
                    performanceModel: targetPerformanceModel,
                    sort: active ? draftFilters.sort : targetSort,
                  })
                }
                className={pillClassName(active)}
              >
                {label}
              </button>
            );
          })}
        </div>
      </div>
      <div className="space-y-1.5">
        <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Performance Model</div>
        <div className="flex flex-wrap items-center gap-1">
          {PERFORMANCE_MODEL_OPTIONS.map((option) => {
            const label = option === "portfolio" ? "Portfolio Simulation" : "Trade Outcomes";
            const active = draftFilters.performanceModel === option;
            if (draftIsInsiderMode && option === "portfolio") {
              return (
                <button
                  key={option}
                  type="button"
                  disabled
                  aria-disabled="true"
                  title={INSIDER_PORTFOLIO_DISABLED_TITLE}
                  className={disabledPillClassName()}
                >
                  {label}
                </button>
              );
            }
            const targetSort = option === "portfolio" ? "alpha_pct" : "avg_alpha";
            const targetSourceMode = option === "portfolio" ? "congress" : draftFilters.sourceMode;
            const targetLookbackDays = option === "portfolio" ? 365 : normalizeTradeLookback(draftFilters.lookbackDays);
            return (
              <button
                key={option}
                type="button"
                aria-pressed={active}
                onClick={() =>
                  updateDraftFilters({
                    lookbackDays: targetLookbackDays,
                    sourceMode: targetSourceMode,
                    performanceModel: option,
                    sort: active ? draftFilters.sort : targetSort,
                    chamber: targetSourceMode === "insiders" ? "all" : draftFilters.chamber,
                  })
                }
                className={pillClassName(active)}
              >
                {label}
              </button>
            );
          })}
        </div>
      </div>
      {draftIsPortfolioMode ? (
        <div className="space-y-1.5">
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Simulation Window</div>
          <div className="flex flex-wrap items-center gap-1">
            {PORTFOLIO_LOOKBACK_OPTIONS.map((option) => {
              const active = draftFilters.lookbackDays === option.days;
              return (
                <button
                  key={option.days}
                  type="button"
                  aria-pressed={active}
                  onClick={() =>
                    updateDraftFilters({
                      lookbackDays: option.days,
                      chamber: "all",
                      sourceMode: "congress",
                      performanceModel: "portfolio",
                      sort: "alpha_pct",
                    })
                  }
                  className={pillClassName(active)}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>
      ) : (
        <div className="space-y-1.5">
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Trade Outcomes Window</div>
          <div className="flex flex-wrap items-center gap-1">
            {TRADE_LOOKBACK_OPTIONS.map((option) => {
              const active = draftFilters.lookbackDays === option.days;
              return (
                <button
                  key={option.days}
                  type="button"
                  aria-pressed={active}
                  onClick={() =>
                    updateDraftFilters({
                      lookbackDays: option.days,
                      performanceModel: "outcomes",
                    })
                  }
                  className={pillClassName(active)}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>
      )}
      <div className="space-y-1.5">
        <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Rows</div>
        <div className="flex flex-wrap items-center gap-1">
          {LIMIT_OPTIONS.map((option) => {
            const active = draftFilters.limit === option;
            return (
              <button
                key={option}
                type="button"
                aria-pressed={active}
                onClick={() => updateDraftFilters({ limit: option })}
                className={pillClassName(active)}
              >
                {option}
              </button>
            );
          })}
        </div>
      </div>
      <div className="pt-1 md:col-span-4">
        <button type="button" disabled={!hasPendingChanges} onClick={applyFilters} className={applyButtonClassName}>
          Apply filters
        </button>
      </div>
    </div>
  );
}
