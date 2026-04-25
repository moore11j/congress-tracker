"use client";

import { useEffect, useMemo, useState } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { BacktestChart } from "@/components/backtesting/BacktestChart";
import { CongressMemberAutosuggest } from "@/components/backtesting/CongressMemberAutosuggest";
import { TickerMultiAutosuggest } from "@/components/backtesting/TickerMultiAutosuggest";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import type {
  BacktestContributionFrequency,
  BacktestPresetsResponse,
  BacktestRebalancingFrequency,
  BacktestRunRequest,
  BacktestRunResponse,
  BacktestStrategyType,
  CongressTraderLeaderboardRow,
  MemberInsiderSuggestion,
  SignalItem,
  SymbolSuggestion,
} from "@/lib/api";
import { getCongressTraderLeaderboard, getSignalsAll, getTickerProfiles, runBacktest } from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";
import type { TickerProfilesMap } from "@/lib/types";
import { cardClassName, inputClassName, selectClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

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
    tickers?: string;
  };
};

type SummaryItem = { label: string; value: string; tone: string };
type BacktestingView = BacktestStrategyType | "signals";
type SignalPreset = "top" | "bullish" | "bearish" | "high_confirmation";
type CustomTickerRow = { symbol: string; allocationInput: string; name?: string | null };
type CongressStrategyKey =
  | "specific_member"
  | "top_house_alpha"
  | "top_senate_alpha"
  | "top_house_return"
  | "top_senate_return"
  | "top_house_win_rate"
  | "top_senate_win_rate"
  | "most_active_house"
  | "most_active_senate";

const strategyTabs: { key: BacktestingView; label: string }[] = [
  { key: "watchlist", label: "Watchlist" },
  { key: "saved_screen", label: "Screens" },
  { key: "signals", label: "Signals" },
  { key: "congress", label: "Congress" },
  { key: "insider", label: "Insider" },
  { key: "custom_tickers", label: "Custom" },
];

const ASSUMPTIONS_AND_NOTES = [
  "Backtests are hypothetical and based on disclosed historical data. Congress and insider trades may be reported after execution, so simulations use disclosure or filing timing where available.",
  "The portfolio uses a capital-constrained model. Total exposure is capped at 100%, with equal-weight allocations unless custom weights are provided.",
  "New positions are only entered on scheduled rebalance dates. Exited positions are sold at the close, and proceeds are returned to cash until the next rebalance.",
  "Performance metrics (returns, volatility, Sharpe ratio, CAGR) are time-weighted and exclude the impact of contributions.",
  "Daily close prices are used. Transaction costs, taxes, slippage, leverage, and shorting are not included in this version.",
] as const;

const SIGNAL_PRESET_OPTIONS: { key: SignalPreset; label: string }[] = [
  { key: "top", label: "Current Top Signals" },
  { key: "bullish", label: "Bullish Signals" },
  { key: "bearish", label: "Bearish Signals" },
  { key: "high_confirmation", label: "High Confirmation Signals" },
];

const SIGNAL_LIMIT_OPTIONS = [10, 25] as const;
const CONGRESS_COUNT_OPTIONS = [5, 10, 25] as const;
const CUSTOM_TICKER_LIMIT = 10;

const CONGRESS_STRATEGY_OPTIONS: {
  key: CongressStrategyKey;
  label: string;
  chamber?: "house" | "senate";
  sort?: "avg_alpha" | "avg_return" | "win_rate" | "trade_count";
}[] = [
  { key: "specific_member", label: "Specific Member" },
  { key: "top_house_alpha", label: "Top House by Alpha", chamber: "house", sort: "avg_alpha" },
  { key: "top_senate_alpha", label: "Top Senate by Alpha", chamber: "senate", sort: "avg_alpha" },
  { key: "top_house_return", label: "Top House by Return", chamber: "house", sort: "avg_return" },
  { key: "top_senate_return", label: "Top Senate by Return", chamber: "senate", sort: "avg_return" },
  { key: "top_house_win_rate", label: "Top House by Win Rate", chamber: "house", sort: "win_rate" },
  { key: "top_senate_win_rate", label: "Top Senate by Win Rate", chamber: "senate", sort: "win_rate" },
  { key: "most_active_house", label: "Most Active House Traders", chamber: "house", sort: "trade_count" },
  { key: "most_active_senate", label: "Most Active Senate Traders", chamber: "senate", sort: "trade_count" },
];

function normalizeStrategyView(value: string | undefined): BacktestingView {
  const normalized = (value ?? "").trim();
  return strategyTabs.some((item) => item.key === normalized) ? (normalized as BacktestingView) : "watchlist";
}

function normalizeTicker(value: string) {
  return value.trim().toUpperCase();
}

function parseTickerQuery(value: string | undefined) {
  const next: string[] = [];
  for (const part of (value ?? "").split(",")) {
    const ticker = normalizeTicker(part);
    if (!ticker || next.includes(ticker)) continue;
    next.push(ticker);
    if (next.length >= CUSTOM_TICKER_LIMIT) break;
  }
  return next;
}

function parsePositiveInt(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNumber(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function shiftIsoDate(endDate: string, days: number) {
  const parsed = new Date(`${endDate}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) return endDate;
  parsed.setUTCDate(parsed.getUTCDate() - Math.max(days, 0));
  return parsed.toISOString().slice(0, 10);
}

function pct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${value.toFixed(1)}%`;
}

function ratio(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "-";
  return value.toFixed(2);
}

function formatPrice(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
}

function formatInteger(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
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

function summaryGroups(result: BacktestRunResponse | null) {
  if (!result) return { primary: [] as SummaryItem[], secondary: [] as SummaryItem[] };
  return {
    primary: [
      { label: "Ending Balance", value: formatPrice(result.summary.ending_balance), tone: "text-white" },
      { label: "Net Profit", value: formatPrice(result.summary.net_profit), tone: toneClass(result.summary.net_profit) },
      { label: "Total Return", value: pct(result.summary.strategy_return_pct), tone: toneClass(result.summary.strategy_return_pct) },
      { label: "CAGR", value: pct(result.summary.cagr_pct), tone: toneClass(result.summary.cagr_pct) },
      { label: "Alpha", value: pct(result.summary.alpha_pct), tone: toneClass(result.summary.alpha_pct) },
      { label: "Sharpe Ratio", value: ratio(result.summary.sharpe_ratio), tone: "text-white" },
    ],
    secondary: [
      { label: "S&P Return", value: pct(result.summary.benchmark_return_pct), tone: toneClass(result.summary.benchmark_return_pct) },
      { label: "Max Drawdown", value: pct(result.summary.max_drawdown_pct), tone: "text-white" },
      { label: "Volatility", value: pct(result.summary.volatility_pct), tone: "text-white" },
      { label: "Position Win Rate", value: pct(result.summary.win_rate), tone: "text-white" },
      { label: "Trades / Positions", value: `${formatInteger(result.summary.trade_count)} / ${formatInteger(result.summary.positions_count)}`, tone: "text-white" },
    ],
  };
}

function extractErrorMessage(error: unknown) {
  if (error instanceof Error && error.message) {
    if (error.message.includes("premium_required")) return "Portfolio backtesting is currently a Premium feature.";
    return error.message.replace(/^HTTP \d+\s+\w+\s+/m, "").trim() || error.message;
  }
  return "Unable to run this backtest right now.";
}

function Spinner() {
  return (
    <svg className="h-4 w-4 animate-spin" viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <circle cx="12" cy="12" r="9" className="stroke-current opacity-25" strokeWidth="3" />
      <path d="M21 12a9 9 0 0 0-9-9" className="stroke-current" strokeWidth="3" strokeLinecap="round" />
    </svg>
  );
}

function LockIcon() {
  return (
    <svg className="h-4 w-4" viewBox="0 0 20 20" fill="none" aria-hidden="true">
      <path d="M6.75 8V6.75a3.25 3.25 0 1 1 6.5 0V8" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
      <rect x="4.5" y="8" width="11" height="8" rx="2" stroke="currentColor" strokeWidth="1.6" />
    </svg>
  );
}

function MetricCard({ item }: { item: SummaryItem }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3">
      <p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{item.label}</p>
      <p className={`mt-3 text-2xl font-semibold ${item.tone}`}>{item.value}</p>
    </div>
  );
}

function ResultSkeleton() {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 6 }).map((_, index) => <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3"><SkeletonBlock className="h-3 w-24" /><SkeletonBlock className="mt-3 h-7 w-24" /></div>)}
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        {Array.from({ length: 5 }).map((_, index) => <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3"><SkeletonBlock className="h-3 w-24" /><SkeletonBlock className="mt-3 h-7 w-20" /></div>)}
      </div>
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"><SkeletonBlock className="h-4 w-44" /><SkeletonBlock className="mt-3 h-[320px] w-full" /></div>
      <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"><SkeletonBlock className="h-4 w-40" /><SkeletonTable columns={6} rows={5} /></div>
    </div>
  );
}

function AssumptionsPanel({ skippedPositionsCount, priceFallbackPositionsCount }: { skippedPositionsCount?: number; priceFallbackPositionsCount?: number }) {
  const hasSkippedPositions = skippedPositionsCount != null && Number.isFinite(skippedPositionsCount) && skippedPositionsCount > 0;
  const hasFallbackUsage = priceFallbackPositionsCount != null && Number.isFinite(priceFallbackPositionsCount) && priceFallbackPositionsCount > 0;

  return (
    <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
      <h3 className="text-base font-semibold text-white">Assumptions &amp; Notes</h3>
      {hasFallbackUsage ? <div className="mt-3 rounded-lg border border-sky-200/10 bg-sky-300/[0.06] px-3 py-2.5 text-[15px] leading-relaxed text-sky-50/95">Price fallback used for {formatInteger(priceFallbackPositionsCount)} positions where exact-date closes were unavailable.</div> : null}
      {hasSkippedPositions ? <div className="mt-3 rounded-lg border border-amber-200/10 bg-amber-300/[0.06] px-3 py-2.5 text-[15px] leading-relaxed text-amber-50/95">{formatInteger(skippedPositionsCount)} positions were still skipped after bounded price fallback could not find a valid close.</div> : null}
      <ul className="mt-3 list-disc space-y-3 pl-5 text-[15px] leading-relaxed text-slate-300/90">
        {ASSUMPTIONS_AND_NOTES.map((assumption) => (
          <li key={assumption}>{assumption}</li>
        ))}
      </ul>
    </div>
  );
}

function equalWeightInputs(count: number) {
  if (count <= 0) return [];
  const base = Math.floor((10000 / count)) / 100;
  const values = Array.from({ length: count }, () => base);
  const allocatedWithoutLast = base * (count - 1);
  values[count - 1] = Number((100 - allocatedWithoutLast).toFixed(2));
  return values.map((value) => value.toFixed(2));
}

function buildCustomRows(symbols: SymbolSuggestion[], existingNames?: Map<string, string | null | undefined>) {
  const allocations = equalWeightInputs(symbols.length);
  return symbols.map((item, index) => ({
    symbol: normalizeTicker(item.symbol),
    name: item.name ?? existingNames?.get(normalizeTicker(item.symbol)) ?? null,
    allocationInput: allocations[index] ?? "0.00",
  }));
}

function parseAllocationInput(value: string) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : NaN;
}

function customAllocationSummary(rows: CustomTickerRow[]) {
  const values = rows.map((row) => parseAllocationInput(row.allocationInput));
  const hasInvalid = values.some((value) => !Number.isFinite(value) || value <= 0);
  const total = values.reduce((sum, value) => sum + (Number.isFinite(value) ? value : 0), 0);
  return { total, hasInvalid, isValidTotal: Math.abs(total - 100) < 0.02 };
}

function signalPresetParams(preset: SignalPreset) {
  if (preset === "bullish") return { sort: "confirmation" as const, confirmation_direction: "bullish" as const };
  if (preset === "bearish") return { sort: "confirmation" as const, confirmation_direction: "bearish" as const };
  if (preset === "high_confirmation") return { sort: "confirmation" as const, confirmation_band: "strong_plus" as const };
  return { sort: "smart" as const };
}

function dedupeSignalTickers(items: SignalItem[], limit: number) {
  const next: string[] = [];
  for (const item of items) {
    const symbol = normalizeTicker(item.symbol ?? "");
    if (!symbol || next.includes(symbol)) continue;
    next.push(symbol);
    if (next.length >= limit) break;
  }
  return next;
}

function congressStrategyConfig(strategy: CongressStrategyKey) {
  return CONGRESS_STRATEGY_OPTIONS.find((item) => item.key === strategy);
}

function dedupeLeaderboardMembers(rows: CongressTraderLeaderboardRow[], limit: number) {
  const memberIds: string[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const memberId = (row.member_id ?? "").trim().toUpperCase();
    if (!memberId || seen.has(memberId)) continue;
    seen.add(memberId);
    memberIds.push(memberId);
    if (memberIds.length >= limit) break;
  }
  return memberIds;
}

export function BacktestingWorkbench({ initialEntitlements, initialPresets, initialQuery }: Props) {
  const initialTickerRows = useMemo(
    () => buildCustomRows(parseTickerQuery(initialQuery?.tickers).map((symbol) => ({ symbol }))),
    [initialQuery?.tickers],
  );
  const strategyFallback: BacktestingView = initialPresets.watchlists.length > 0 ? "watchlist" : "congress";
  const initialView = initialTickerRows.length > 0 ? "custom_tickers" : normalizeStrategyView(initialQuery?.strategy) || strategyFallback;
  const today = initialPresets.today || new Date().toISOString().slice(0, 10);
  const canRun = initialPresets.access.can_run && initialEntitlements.features.includes("backtesting");

  const [view, setView] = useState<BacktestingView>(initialView);
  const [watchlistId, setWatchlistId] = useState<string>(initialQuery?.watchlist_id || String(initialPresets.watchlists[0]?.id ?? ""));
  const [savedScreenId, setSavedScreenId] = useState<string>(initialQuery?.saved_screen_id || String(initialPresets.saved_screens[0]?.id ?? ""));
  const [signalPreset, setSignalPreset] = useState<SignalPreset>("top");
  const [signalLimit, setSignalLimit] = useState<(typeof SIGNAL_LIMIT_OPTIONS)[number]>(25);
  const [signalTickers, setSignalTickers] = useState<string[]>([]);
  const [signalLoading, setSignalLoading] = useState(false);
  const [signalError, setSignalError] = useState<string | null>(null);
  const [congressStrategy, setCongressStrategy] = useState<CongressStrategyKey>(initialQuery?.scope === "member" ? "specific_member" : "top_house_alpha");
  const [congressCount, setCongressCount] = useState<(typeof CONGRESS_COUNT_OPTIONS)[number]>(10);
  const [memberId, setMemberId] = useState<string>(initialQuery?.member_id || "");
  const [selectedMember, setSelectedMember] = useState<MemberInsiderSuggestion | null>(
    initialQuery?.member_id
      ? { label: initialQuery.member_id, value: initialQuery.member_id, category: "congress", bioguide_id: initialQuery.member_id }
      : null,
  );
  const [leaderboardRows, setLeaderboardRows] = useState<CongressTraderLeaderboardRow[]>([]);
  const [leaderboardMemberIds, setLeaderboardMemberIds] = useState<string[]>([]);
  const [leaderboardLoading, setLeaderboardLoading] = useState(false);
  const [leaderboardError, setLeaderboardError] = useState<string | null>(null);
  const [insiderScope, setInsiderScope] = useState<string>("all_insiders");
  const [insiderCik, setInsiderCik] = useState<string>(initialQuery?.insider_cik || "");
  const [customRows, setCustomRows] = useState<CustomTickerRow[]>(initialTickerRows);
  const [tickerProfiles, setTickerProfiles] = useState<TickerProfilesMap>({});
  const [lookbackDays, setLookbackDays] = useState<number>(parsePositiveInt(undefined, initialPresets.defaults.lookback_days));
  const [holdDays, setHoldDays] = useState<30 | 60 | 90 | 180 | 365>(initialPresets.defaults.hold_days);
  const [startBalanceInput, setStartBalanceInput] = useState<string>(String(initialPresets.defaults.start_balance));
  const [contributionAmountInput, setContributionAmountInput] = useState<string>(String(initialPresets.defaults.contribution_amount));
  const [contributionFrequency, setContributionFrequency] = useState<BacktestContributionFrequency>(initialPresets.defaults.contribution_frequency);
  const [rebalancingFrequency, setRebalancingFrequency] = useState<BacktestRebalancingFrequency>(initialPresets.defaults.rebalancing_frequency);
  const [result, setResult] = useState<BacktestRunResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const startDate = useMemo(() => shiftIsoDate(today, Math.max(lookbackDays - 1, 0)), [lookbackDays, today]);
  const summary = summaryGroups(result);
  const customAllocationState = useMemo(() => customAllocationSummary(customRows), [customRows]);
  const selectedTickerSymbols = useMemo(() => customRows.map((row) => row.symbol), [customRows]);

  useEffect(() => {
    if (view !== "signals" || !canRun) return;
    let cancelled = false;
    setSignalLoading(true);
    setSignalError(null);
    getSignalsAll({ ...signalPresetParams(signalPreset), mode: "all", limit: signalLimit })
      .then((response) => {
        if (!cancelled) setSignalTickers(dedupeSignalTickers(response.items, signalLimit));
      })
      .catch((nextError) => {
        if (cancelled) return;
        setSignalTickers([]);
        setSignalError(nextError instanceof Error ? nextError.message : "Unable to load Signals.");
      })
      .finally(() => {
        if (!cancelled) setSignalLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [canRun, signalLimit, signalPreset, view]);

  useEffect(() => {
    const strategyConfig = congressStrategyConfig(congressStrategy);
    if (view !== "congress" || !canRun || !strategyConfig?.sort || !strategyConfig.chamber) return;
    let cancelled = false;
    setLeaderboardLoading(true);
    setLeaderboardError(null);
    getCongressTraderLeaderboard({
      lookback_days: lookbackDays,
      chamber: strategyConfig.chamber,
      source_mode: "congress",
      sort: strategyConfig.sort,
      min_trades: 1,
      limit: congressCount,
    })
      .then((response) => {
        if (cancelled) return;
        setLeaderboardRows(response.rows);
        setLeaderboardMemberIds(dedupeLeaderboardMembers(response.rows, congressCount));
      })
      .catch((nextError) => {
        if (cancelled) return;
        setLeaderboardRows([]);
        setLeaderboardMemberIds([]);
        setLeaderboardError(nextError instanceof Error ? nextError.message : "Unable to load the Congress leaderboard.");
      })
      .finally(() => {
        if (!cancelled) setLeaderboardLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [canRun, congressCount, congressStrategy, lookbackDays, view]);

  useEffect(() => {
    if (selectedTickerSymbols.length === 0) {
      setTickerProfiles({});
      return;
    }
    let cancelled = false;
    getTickerProfiles(selectedTickerSymbols)
      .then((profiles) => {
        if (!cancelled) setTickerProfiles(profiles);
      })
      .catch(() => {
        if (!cancelled) setTickerProfiles({});
      });
    return () => {
      cancelled = true;
    };
  }, [selectedTickerSymbols]);

  function setRowsEqualWeight(nextRows: CustomTickerRow[]) {
    const equalWeights = equalWeightInputs(nextRows.length);
    setCustomRows(nextRows.map((row, index) => ({ ...row, allocationInput: equalWeights[index] ?? row.allocationInput })));
  }

  function handleAddCustomSymbols(items: SymbolSuggestion[]) {
    const existingNames = new Map(customRows.map((row) => [row.symbol, row.name]));
    const nextSymbols = [...customRows.map((row) => ({ symbol: row.symbol, name: row.name })), ...items];
    const deduped: SymbolSuggestion[] = [];
    const seen = new Set<string>();
    for (const item of nextSymbols) {
      const symbol = normalizeTicker(item.symbol);
      if (!symbol || seen.has(symbol)) continue;
      seen.add(symbol);
      deduped.push({ symbol, name: item.name ?? existingNames.get(symbol) ?? null });
      if (deduped.length >= CUSTOM_TICKER_LIMIT) break;
    }
    setRowsEqualWeight(buildCustomRows(deduped, existingNames));
  }

  function handleRemoveCustomSymbol(symbol: string) {
    setRowsEqualWeight(customRows.filter((row) => row.symbol !== symbol));
  }

  function handleAllocationChange(symbol: string, value: string) {
    setCustomRows((current) => current.map((row) => (row.symbol === symbol ? { ...row, allocationInput: value } : row)));
  }

  const payload = useMemo<BacktestRunRequest | null>(() => {
    const startBalance = parseNumber(startBalanceInput, NaN);
    const contributionAmount = parseNumber(contributionAmountInput, NaN);
    if (!Number.isFinite(startBalance) || startBalance <= 0) return null;
    if (!Number.isFinite(contributionAmount) || contributionAmount < 0) return null;
    if (contributionAmount > 0 && contributionFrequency === "none") return null;

    const base: BacktestRunRequest = {
      strategy_type: "watchlist",
      start_date: startDate,
      end_date: today,
      hold_days: holdDays,
      start_balance: startBalance,
      contribution_amount: contributionAmount,
      contribution_frequency: contributionFrequency,
      rebalancing_frequency: rebalancingFrequency,
      max_position_weight: initialPresets.defaults.max_position_weight,
      weighting: "equal",
      benchmark: "^GSPC",
    };

    if (view === "watchlist") {
      const id = Number(watchlistId);
      return Number.isFinite(id) && id > 0 ? { ...base, strategy_type: "watchlist", watchlist_id: id } : null;
    }
    if (view === "saved_screen") {
      const id = Number(savedScreenId);
      return Number.isFinite(id) && id > 0 ? { ...base, strategy_type: "saved_screen", saved_screen_id: id } : null;
    }
    if (view === "signals") {
      return signalTickers.length > 0 ? { ...base, strategy_type: "custom_tickers", tickers: signalTickers, source_label: SIGNAL_PRESET_OPTIONS.find((option) => option.key === signalPreset)?.label ?? "Signals" } : null;
    }
    if (view === "congress") {
      if (congressStrategy === "specific_member") {
        return memberId.trim() ? { ...base, strategy_type: "congress", source_scope: "member", member_id: memberId.trim() } : null;
      }
      return leaderboardMemberIds.length > 0 ? { ...base, strategy_type: "congress", source_scope: "member_list", member_ids: leaderboardMemberIds } : null;
    }
    if (view === "insider") {
      if (insiderScope === "insider" && !insiderCik.trim()) return null;
      return { ...base, strategy_type: "insider", source_scope: insiderScope === "insider" ? "insider" : "all_insiders", insider_cik: insiderCik.trim() || undefined };
    }
    if (customRows.length === 0 || customAllocationState.hasInvalid || !customAllocationState.isValidTotal) return null;
    return {
      ...base,
      strategy_type: "custom_tickers",
      tickers: customRows.map((row) => ({ symbol: row.symbol, allocation_pct: parseAllocationInput(row.allocationInput) })),
    };
  }, [congressStrategy, contributionAmountInput, contributionFrequency, customAllocationState.hasInvalid, customAllocationState.isValidTotal, customRows, holdDays, initialPresets.defaults.max_position_weight, insiderCik, insiderScope, leaderboardMemberIds, memberId, rebalancingFrequency, savedScreenId, signalPreset, signalTickers, startBalanceInput, startDate, today, view, watchlistId]);

  const helperText =
    !canRun ? null
      : view === "signals" && signalLoading ? "Loading signal symbols"
      : view === "signals" && signalTickers.length === 0 ? "No signal tickers matched this preset"
      : view === "congress" && congressStrategy !== "specific_member" && leaderboardLoading ? "Loading leaderboard members"
      : view === "congress" && congressStrategy !== "specific_member" && leaderboardMemberIds.length === 0 ? "No leaderboard members matched this selection"
      : view === "custom_tickers" && customRows.length === 0 ? "Add at least one ticker"
      : view === "custom_tickers" && !customAllocationState.isValidTotal ? "Allocations must total 100%."
      : contributionFrequency === "none" && parseNumber(contributionAmountInput, 0) > 0 ? "Choose a contribution frequency when a contribution amount is set"
      : !payload ? "Select inputs to run backtest"
      : null;
  const buttonDisabled = loading || !canRun || !payload || signalLoading || leaderboardLoading;
  const premiumTooltip = "Backtesting is a Premium feature";

  async function handleRun() {
    if (!payload || loading || !canRun) return;
    setLoading(true);
    setError(null);
    try {
      setResult(await runBacktest(payload));
    } catch (runError) {
      setError(extractErrorMessage(runError));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }

  function handleViewChange(nextView: BacktestingView) {
    setView(nextView);
    setError(null);
    if (nextView === "watchlist" && !watchlistId) setWatchlistId(String(initialPresets.watchlists[0]?.id ?? ""));
    if (nextView === "saved_screen" && !savedScreenId) setSavedScreenId(String(initialPresets.saved_screens[0]?.id ?? ""));
  }

  return (
    <div className="space-y-8">
      <section className="grid gap-6 xl:grid-cols-[0.92fr_1.08fr]">
        <div className={`${cardClassName} space-y-5`}>
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Premium Research</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">Backtest Signals &amp; Portfolios</h1>
            <p className="mt-2 max-w-2xl text-sm text-slate-400">Run capital-constrained historical simulations across watchlists, screens, Signals, Congress filings, and insider disclosures.</p>
          </div>

          <div className="flex flex-wrap gap-2">
            {strategyTabs.map((tab) => (
              <button key={tab.key} type="button" onClick={() => handleViewChange(tab.key)} className={`rounded-2xl border px-4 py-2 text-sm font-semibold transition ${view === tab.key ? "border-emerald-300/40 bg-emerald-400/10 text-emerald-100" : "border-white/10 bg-slate-950/50 text-slate-300 hover:border-white/20 hover:text-white"}`}>
                {tab.label}
              </button>
            ))}
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            {view === "watchlist" ? <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">Watchlist<select value={watchlistId} onChange={(event) => setWatchlistId(event.target.value)} className={selectClassName} disabled={!canRun}><option value="">{initialPresets.watchlists.length ? "Select a watchlist" : "No watchlists found"}</option>{initialPresets.watchlists.map((watchlist) => <option key={watchlist.id} value={watchlist.id}>{watchlist.name} - {watchlist.ticker_count} tickers</option>)}</select></label> : null}
            {view === "saved_screen" ? <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">Screens<select value={savedScreenId} onChange={(event) => setSavedScreenId(event.target.value)} className={selectClassName} disabled={!canRun}><option value="">{initialPresets.saved_screens.length ? "Select a screen" : "No saved screens found"}</option>{initialPresets.saved_screens.map((screen) => <option key={screen.id} value={screen.id}>{screen.name}</option>)}</select></label> : null}

            {view === "signals" ? (
              <>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Signal Set<select value={signalPreset} onChange={(event) => setSignalPreset(event.target.value as SignalPreset)} className={selectClassName} disabled={!canRun}>{SIGNAL_PRESET_OPTIONS.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}</select></label>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Limit<select value={signalLimit} onChange={(event) => setSignalLimit(Number(event.target.value) as (typeof SIGNAL_LIMIT_OPTIONS)[number])} className={selectClassName} disabled={!canRun}>{SIGNAL_LIMIT_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}</select></label>
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 md:col-span-2">
                  <div className="flex items-center justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Signals</p><p className="mt-1 text-sm text-slate-400">Selecting a signal set imports its current symbols directly into the backtest run.</p></div>{signalLoading ? <Spinner /> : <span className="text-xs text-slate-500">{signalTickers.length} symbols</span>}</div>
                  {signalError ? <p className="mt-3 text-sm text-rose-200">{signalError}</p> : null}
                  {signalTickers.length > 0 ? <div className="mt-3 flex flex-wrap gap-2">{signalTickers.map((ticker) => <span key={ticker} className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-xs font-semibold text-slate-200">{ticker}</span>)}</div> : <p className="mt-3 text-sm text-slate-500">No signal symbols loaded yet.</p>}
                </div>
              </>
            ) : null}

            {view === "congress" ? (
              <>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">Congress Strategy<select value={congressStrategy} onChange={(event) => setCongressStrategy(event.target.value as CongressStrategyKey)} className={selectClassName} disabled={!canRun}>{CONGRESS_STRATEGY_OPTIONS.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}</select></label>
                {congressStrategy === "specific_member" ? <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">Member<CongressMemberAutosuggest value={selectedMember} fallbackLabel={memberId} disabled={!canRun} onChange={(nextSelection) => { setSelectedMember(nextSelection); setMemberId(nextSelection?.bioguide_id?.trim() ?? ""); }} /></label> : <>
                  <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Member Count<select value={congressCount} onChange={(event) => setCongressCount(Number(event.target.value) as (typeof CONGRESS_COUNT_OPTIONS)[number])} className={selectClassName} disabled={!canRun}><option value={5}>Top 5</option><option value={10}>Top 10</option><option value={25}>Top 25</option></select></label>
                  <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4"><div className="flex items-center justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Leaderboard Import</p><p className="mt-1 text-sm text-slate-400">Top-ranked members are resolved first, then their underlying Congress trades are used for the backtest.</p></div>{leaderboardLoading ? <Spinner /> : <span className="text-xs text-slate-500">{leaderboardMemberIds.length} members</span>}</div></div>
                  <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 md:col-span-2">
                    {leaderboardError ? <p className="text-sm text-rose-200">{leaderboardError}</p> : null}
                    {leaderboardRows.length > 0 ? <div className="grid gap-2">{leaderboardRows.slice(0, congressCount).map((row) => <div key={`${row.member_id}-${row.rank}`} className="flex items-center justify-between rounded-xl border border-white/10 bg-slate-950/40 px-3 py-2"><div><div className="text-sm font-semibold text-white">{row.member_name}</div><div className="text-xs text-slate-400">{[row.chamber, row.party, row.state].filter(Boolean).join(" • ")}</div></div><div className="text-right text-xs text-slate-400"><div>Rank #{row.rank}</div><div>{row.trade_count_total} trades</div></div></div>)}</div> : <p className="text-sm text-slate-500">No leaderboard members resolved yet.</p>}
                  </div>
                </>}
              </>
            ) : null}

            {view === "insider" ? (
              <>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Scope<select value={insiderScope} onChange={(event) => setInsiderScope(event.target.value)} className={selectClassName} disabled={!canRun}>{initialPresets.source_scopes.insider.map((scope) => <option key={scope.key} value={scope.key}>{scope.label}</option>)}</select></label>
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Insider CIK<input value={insiderCik} onChange={(event) => setInsiderCik(event.target.value)} className={inputClassName} placeholder="0001234567" disabled={!canRun || insiderScope !== "insider"} /></label>
              </>
            ) : null}

            {view === "custom_tickers" ? (
              <div className="grid gap-4 md:col-span-2">
                <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Tickers<TickerMultiAutosuggest selectedSymbols={selectedTickerSymbols} onAddSymbols={handleAddCustomSymbols} disabled={!canRun} limit={CUSTOM_TICKER_LIMIT} /><div className="text-xs font-normal normal-case text-slate-500">{customRows.length > 0 ? `${customRows.length}/${CUSTOM_TICKER_LIMIT} selected` : `Add up to ${CUSTOM_TICKER_LIMIT} tickers`}</div></label>
                <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Custom Allocations</p><p className="mt-1 text-sm text-slate-400">Manual portfolios default to equal weight, and you can override allocations as long as they total 100%.</p></div><button type="button" onClick={() => setRowsEqualWeight(customRows)} disabled={!canRun || customRows.length === 0} className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:cursor-not-allowed disabled:text-slate-500">Equal Weight</button></div>
                  {customRows.length === 0 ? <div className="mt-4 rounded-2xl border border-dashed border-white/10 px-4 py-8 text-center text-sm text-slate-400">Add tickers to build a custom portfolio.</div> : <div className="mt-4 space-y-3">{customRows.map((row) => { const profile = tickerProfiles[row.symbol]; const companyName = row.name ?? profile?.ticker?.name ?? null; return <div key={row.symbol} className="grid gap-3 rounded-2xl border border-white/10 bg-slate-950/35 px-4 py-3 md:grid-cols-[1.1fr_minmax(0,1fr)_140px_auto] md:items-center"><div><div className="font-semibold text-white">{row.symbol}</div><div className="text-xs text-slate-400">{companyName || "Company name unavailable"}</div></div><div className="text-xs uppercase tracking-[0.16em] text-slate-500">Target Allocation</div><div className="flex items-center gap-2"><input type="number" min="0" max="100" step="0.01" value={row.allocationInput} onChange={(event) => handleAllocationChange(row.symbol, event.target.value)} className={inputClassName} disabled={!canRun} /><span className="text-sm text-slate-400">%</span></div><button type="button" onClick={() => handleRemoveCustomSymbol(row.symbol)} disabled={!canRun} className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-rose-300/30 hover:text-rose-200 disabled:cursor-not-allowed disabled:text-slate-500">Remove</button></div>; })}</div>}
                  <div className="mt-4 flex flex-wrap items-center justify-between gap-2 text-sm"><span className="text-slate-400">Allocation Total</span><span className={customAllocationState.isValidTotal ? "font-semibold text-emerald-200" : "font-semibold text-amber-200"}>{customAllocationState.total.toFixed(2)}%</span></div>
                  {!customAllocationState.isValidTotal && customRows.length > 0 ? <p className="mt-2 text-sm text-amber-200">Allocations must total 100%.</p> : null}
                </div>
              </div>
            ) : null}

            <div className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400 md:col-span-2">Lookback<div className="flex flex-wrap gap-2">{initialPresets.lookback_options.map((option) => <button key={option.days} type="button" onClick={() => setLookbackDays(option.days)} className={`rounded-2xl border px-3 py-2 text-sm font-semibold normal-case transition ${lookbackDays === option.days ? "border-emerald-300/40 bg-emerald-400/10 text-emerald-100" : "border-white/10 bg-slate-950/50 text-slate-300 hover:border-white/20 hover:text-white"}`} disabled={!canRun}>{option.label}</button>)}</div></div>
            <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Hold Period<select value={holdDays} onChange={(event) => setHoldDays(Number(event.target.value) as 30 | 60 | 90 | 180 | 365)} className={selectClassName} disabled={!canRun}>{initialPresets.hold_day_options.map((option) => <option key={option.days} value={option.days}>{option.label} days</option>)}</select></label>
            <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Benchmark<select value="^GSPC" className={selectClassName} disabled={true}><option value="^GSPC">S&amp;P 500</option></select></label>
          </div>

          <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
            <div className="flex items-center justify-between gap-3"><div><p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Portfolio Settings</p><p className="mt-1 text-sm text-slate-400">Configure capital, deposits, and rebalance cadence for the simulated portfolio.</p></div><span className="rounded-full border border-white/10 bg-slate-950/70 px-3 py-1 text-[11px] uppercase tracking-[0.18em] text-slate-300">v1</span></div>
            <div className="mt-4 grid gap-4 md:grid-cols-2">
              <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Start Balance<input type="number" min="1" step="100" value={startBalanceInput} onChange={(event) => setStartBalanceInput(event.target.value)} className={inputClassName} disabled={!canRun} /></label>
              <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Contribution Amount<input type="number" min="0" step="100" value={contributionAmountInput} onChange={(event) => setContributionAmountInput(event.target.value)} className={inputClassName} disabled={!canRun} /></label>
              <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Contribution Frequency<select value={contributionFrequency} onChange={(event) => setContributionFrequency(event.target.value as BacktestContributionFrequency)} className={selectClassName} disabled={!canRun}>{initialPresets.contribution_frequency_options.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}</select></label>
              <label className="grid gap-1.5 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Rebalancing<select value={rebalancingFrequency} onChange={(event) => setRebalancingFrequency(event.target.value as BacktestRebalancingFrequency)} className={selectClassName} disabled={!canRun}>{initialPresets.rebalancing_frequency_options.map((option) => <option key={option.key} value={option.key}>{option.label}</option>)}</select></label>
            </div>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/10 pt-4">
            <div className="text-sm text-slate-400">Window <span className="text-white">{startDate}</span> to <span className="text-white">{today}</span></div>
            <div className="flex flex-col items-end gap-2">
              <button type="button" onClick={handleRun} disabled={buttonDisabled} title={!canRun ? premiumTooltip : undefined} aria-label={!canRun ? premiumTooltip : loading ? "Running backtest" : "Run Backtest"} className={`${subtlePrimaryButtonClassName} min-w-[172px] gap-2 disabled:cursor-not-allowed disabled:border-white/10 disabled:bg-slate-800/70 disabled:text-slate-400`}>{loading ? <><Spinner />Running...</> : !canRun ? <><LockIcon />Run Backtest</> : "Run Backtest"}</button>
              {helperText ? <div className="text-xs text-slate-500">{helperText}</div> : null}
              {!canRun ? <div className="text-xs text-slate-500">{premiumTooltip}</div> : null}
            </div>
          </div>
        </div>

        <div className={`${cardClassName} space-y-4`}>
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">Results</p>
              <h2 className="mt-1 text-xl font-semibold text-white">Strategy vs S&amp;P 500</h2>
              <p className="mt-2 max-w-2xl text-sm text-slate-400">This is a capital-constrained portfolio simulation. Individual trade returns may be large, but portfolio performance is based on actual allocated capital over time.</p>
              <p className="mt-2 max-w-2xl text-sm text-slate-400">Total exposure is capped at 100%, with equal-weight allocations unless custom weights are provided.</p>
            </div>
            <span className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-slate-300">{initialEntitlements.tier}</span>
          </div>

          {loading ? <ResultSkeleton /> : error ? (
            <div className="space-y-4">
              <div className="rounded-2xl border border-rose-300/20 bg-rose-400/[0.07] px-4 py-3 text-sm text-rose-100">{error}</div>
              <div className="rounded-2xl border border-dashed border-white/10 px-5 py-10 text-center"><h3 className="text-lg font-semibold text-white">Backtest run failed</h3><p className="mt-2 text-sm text-slate-400">Review the selected inputs and try again.</p></div>
            </div>
          ) : !canRun ? (
            <div className="space-y-4">
              <UpgradePrompt title="Unlock portfolio backtesting" body="Backtesting is included with Premium. Free users can preview the workflow here, but custom runs stay locked until upgrade." />
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">{["Ending Balance", "Net Profit", "Total Return", "CAGR", "Alpha", "Sharpe Ratio"].map((label) => <div key={label} className="rounded-2xl border border-white/10 bg-white/[0.03] px-4 py-3"><p className="text-[11px] uppercase tracking-[0.18em] text-slate-500">{label}</p><p className="mt-3 text-2xl font-semibold text-slate-300">Premium</p></div>)}</div>
              <div className="rounded-2xl border border-white/10 bg-[#07111d] px-4 py-12 text-center text-sm text-slate-400">Portfolio equity curve previews unlock after upgrade.</div>
            </div>
          ) : result ? (
            <div className="space-y-5">
              {result.positions.length === 0 ? <div className="rounded-2xl border border-dashed border-white/10 bg-white/[0.02] px-4 py-4 text-sm text-slate-300">No qualifying positions found for this strategy and date range.</div> : null}
              <div><p className="mb-3 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Primary Metrics</p><div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">{summary.primary.map((item) => <MetricCard key={item.label} item={item} />)}</div></div>
              <div><p className="mb-3 text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Secondary Metrics</p><div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">{summary.secondary.map((item) => <MetricCard key={item.label} item={item} />)}</div></div>
              <div className="flex flex-wrap gap-2 text-xs text-slate-300"><span className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1">Start {formatPrice(result.summary.start_balance)}</span><span className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1">Total Contributed {formatPrice(result.summary.total_contributions)}</span><span className="rounded-full border border-white/10 bg-slate-950/60 px-3 py-1">Benchmark End {formatPrice(result.summary.benchmark_ending_balance)}</span></div>
              {result.diagnostics ? <div className="space-y-1"><div className="text-sm text-slate-400">Avg Active Positions {result.diagnostics.average_active_positions.toFixed(1)} | Avg Invested {result.diagnostics.average_invested_pct.toFixed(1)}% | Max Position Weight {result.diagnostics.max_position_weight_observed.toFixed(1)}%</div><div className="text-xs text-slate-500">Max position weight reflects actual portfolio concentration during the simulation.</div></div> : null}
              <BacktestChart timeline={result.timeline} />
              <AssumptionsPanel skippedPositionsCount={result.summary.skipped_positions_count} priceFallbackPositionsCount={result.summary.price_fallback_positions_count} />
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="mb-3 flex items-center justify-between gap-3"><div><h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-400">Positions</h3><p className="mt-1 text-xs text-slate-500">Position returns are individual trade returns. Portfolio returns are capital-weighted and may be much lower than individual winners.</p></div><span className="text-xs text-slate-500">{result.positions.length} rows</span></div>
                {result.positions.length === 0 ? <div className="rounded-2xl border border-dashed border-white/10 px-4 py-8 text-center text-sm text-slate-400">No qualifying positions found for this strategy and date range.</div> : <div className="overflow-x-auto"><table className="min-w-full divide-y divide-white/10 text-sm"><thead><tr className="text-left text-xs uppercase tracking-[0.18em] text-slate-500"><th className="pb-3 pr-4 font-medium">Symbol</th><th className="pb-3 pr-4 font-medium">Entry</th><th className="pb-3 pr-4 font-medium">Exit</th><th className="pb-3 pr-4 font-medium">Entry Px</th><th className="pb-3 pr-4 font-medium">Exit Px</th><th className="pb-3 pr-4 font-medium">Return</th><th className="pb-3 font-medium">Source</th></tr></thead><tbody className="divide-y divide-white/5">{result.positions.map((position) => <tr key={`${position.symbol}-${position.entry_date}-${position.source_event_id ?? "static"}`} className="text-slate-200"><td className="py-3 pr-4 font-semibold text-white">{position.symbol}</td><td className="py-3 pr-4">{formatDate(position.entry_date)}</td><td className="py-3 pr-4">{formatDate(position.exit_date)}</td><td className="py-3 pr-4 tabular-nums">{formatPrice(position.entry_price)}</td><td className="py-3 pr-4 tabular-nums">{formatPrice(position.exit_price)}</td><td className={`py-3 pr-4 font-semibold tabular-nums ${toneClass(position.return_pct)}`}>{pct(position.return_pct)}</td><td className="py-3 text-slate-400">{position.source_label || (position.source_event_id ? `Event #${position.source_event_id}` : "Current universe")}{position.price_fallback_used ? <span className="ml-2 rounded-full border border-sky-300/20 bg-sky-300/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-sky-100">Fallback</span> : null}</td></tr>)}</tbody></table></div>}
              </div>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="rounded-2xl border border-dashed border-white/10 px-5 py-10 text-center"><h3 className="text-lg font-semibold text-white">Run a backtest to populate the panel</h3><p className="mt-2 text-sm text-slate-400">Capital-constrained portfolio results will appear here with dollar balances, time-weighted returns, and the S&amp;P 500 benchmark.</p></div>
              <AssumptionsPanel />
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
