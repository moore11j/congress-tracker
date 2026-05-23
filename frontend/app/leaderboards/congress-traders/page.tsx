import Link from "next/link";
import { Suspense } from "react";
import { CongressTraderLeaderboardClientResults } from "@/components/leaderboards/CongressTraderLeaderboardClientResults";
import { CongressTraderLeaderboardStatusState, CongressTraderLeaderboardTable } from "@/components/leaderboards/CongressTraderLeaderboardTable";
import { SkeletonBlock, SkeletonTable } from "@/components/ui/LoadingSkeleton";
import {
  ApiError,
  getCongressTraderLeaderboard,
  type CongressTraderLeaderboardChamber,
  type CongressTraderLeaderboardPerformanceModel,
  type CongressTraderLeaderboardPortfolioSort,
  type CongressTraderLeaderboardSort,
  type CongressTraderLeaderboardSourceMode,
  type CongressTraderLeaderboardTradeSort,
} from "@/lib/api";
import { buildReturnTo, requirePageAuthState } from "@/lib/serverAuth";
import { cardClassName, selectClassName } from "@/lib/styles";

type SearchParams = Record<string, string | string[] | undefined>;

const LOOKBACK_OPTIONS = [30, 90, 180, 365] as const;
const CHAMBER_OPTIONS: CongressTraderLeaderboardChamber[] = ["all", "house", "senate"];
const SOURCE_MODE_OPTIONS: CongressTraderLeaderboardSourceMode[] = ["congress", "insiders"];
const PERFORMANCE_MODEL_OPTIONS: CongressTraderLeaderboardPerformanceModel[] = ["outcomes", "portfolio"];
const TRADE_SORT_OPTIONS: CongressTraderLeaderboardTradeSort[] = ["avg_alpha", "avg_return", "win_rate", "trade_count"];
const PORTFOLIO_SORT_OPTIONS: CongressTraderLeaderboardPortfolioSort[] = [
  "alpha_pct",
  "total_return_pct",
  "cagr_pct",
  "sharpe_ratio",
  "max_drawdown_pct",
  "win_rate_pct",
];
const MIN_TRADE_OPTIONS = [1, 3, 5, 10] as const;
const LIMIT_OPTIONS = [10, 25, 50, 100] as const;

function getParam(sp: SearchParams, key: string): string {
  const value = sp[key];
  if (Array.isArray(value)) {
    for (let idx = value.length - 1; idx >= 0; idx -= 1) {
      const candidate = value[idx];
      if (typeof candidate === "string") return candidate;
    }
    return "";
  }
  return typeof value === "string" ? value : "";
}

function toPositiveInt(value: string, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function parseLookback(raw: string): number {
  const parsed = toPositiveInt(raw, 365);
  return LOOKBACK_OPTIONS.includes(parsed as (typeof LOOKBACK_OPTIONS)[number]) ? parsed : 365;
}

function parseChamber(raw: string): CongressTraderLeaderboardChamber {
  return CHAMBER_OPTIONS.includes(raw as CongressTraderLeaderboardChamber)
    ? (raw as CongressTraderLeaderboardChamber)
    : "all";
}

function parseSourceMode(raw: string): CongressTraderLeaderboardSourceMode {
  if (raw === "all") return "congress";
  return SOURCE_MODE_OPTIONS.includes(raw as CongressTraderLeaderboardSourceMode)
    ? (raw as CongressTraderLeaderboardSourceMode)
    : "congress";
}

function parsePerformanceModel(raw: string, sourceMode: CongressTraderLeaderboardSourceMode): CongressTraderLeaderboardPerformanceModel {
  if (sourceMode !== "congress") return "outcomes";
  const normalized = (raw || "outcomes").trim().toLowerCase();
  return normalized === "portfolio" ? "portfolio" : "outcomes";
}

function parseSort(raw: string, performanceModel: CongressTraderLeaderboardPerformanceModel): CongressTraderLeaderboardSort {
  if (performanceModel === "portfolio") {
    return PORTFOLIO_SORT_OPTIONS.includes(raw as CongressTraderLeaderboardPortfolioSort)
      ? (raw as CongressTraderLeaderboardPortfolioSort)
      : "alpha_pct";
  }
  return TRADE_SORT_OPTIONS.includes(raw as CongressTraderLeaderboardTradeSort)
    ? (raw as CongressTraderLeaderboardTradeSort)
    : "avg_alpha";
}

function parseMinTrades(raw: string): number {
  const parsed = toPositiveInt(raw, 3);
  return MIN_TRADE_OPTIONS.includes(parsed as (typeof MIN_TRADE_OPTIONS)[number]) ? parsed : 3;
}

function parseLimit(raw: string): number {
  const parsed = toPositiveInt(raw, 10);
  return LIMIT_OPTIONS.includes(parsed as (typeof LIMIT_OPTIONS)[number]) ? parsed : 10;
}

function buildUrl(params: {
  lookback_days: number;
  chamber: CongressTraderLeaderboardChamber;
  source_mode: CongressTraderLeaderboardSourceMode;
  performance_model?: CongressTraderLeaderboardPerformanceModel;
  sort: CongressTraderLeaderboardSort;
  min_trades: number;
  limit: number;
}) {
  const url = new URL("https://local/leaderboards/congress-traders");
  const performanceModel = params.source_mode === "congress" ? params.performance_model ?? "outcomes" : "outcomes";
  url.searchParams.set("lookback_days", String(performanceModel === "portfolio" ? 365 : params.lookback_days));
  url.searchParams.set("chamber", params.source_mode === "insiders" ? "all" : params.chamber);
  url.searchParams.set("source_mode", params.source_mode);
  if (performanceModel === "portfolio") url.searchParams.set("performance_model", "portfolio");
  url.searchParams.set("sort", params.sort);
  if (performanceModel !== "portfolio") url.searchParams.set("min_trades", String(params.min_trades));
  url.searchParams.set("limit", String(params.limit));
  return `${url.pathname}${url.search}`;
}

function buildSortHrefs(params: {
  lookback_days: number;
  chamber: CongressTraderLeaderboardChamber;
  source_mode: CongressTraderLeaderboardSourceMode;
  performance_model: CongressTraderLeaderboardPerformanceModel;
  min_trades: number;
  limit: number;
}) {
  const sortOptions =
    params.performance_model === "portfolio"
      ? PORTFOLIO_SORT_OPTIONS
      : TRADE_SORT_OPTIONS;
  return Object.fromEntries(
    sortOptions.map((sortOption) => [
      sortOption,
      buildUrl({
        ...params,
        sort: sortOption,
      }),
    ]),
  ) as Partial<Record<CongressTraderLeaderboardSort, string>>;
}

function pillClassName(active: boolean): string {
  return `rounded-full border px-3 py-1 text-xs font-semibold transition ${
    active
      ? "border-emerald-300/60 bg-emerald-500/20 text-emerald-100"
      : "border-white/15 bg-white/[0.03] text-slate-300 hover:bg-white/[0.06]"
  }`;
}

function LeaderboardResultsFallback() {
  return (
    <div className={`${cardClassName} min-h-[32rem] overflow-hidden p-4`} aria-live="polite" aria-busy="true">
      <div className="mb-4 flex items-center justify-between">
        <SkeletonBlock className="h-4 w-44" />
        <SkeletonBlock className="h-4 w-28" />
      </div>
      <SkeletonTable columns={8} rows={8} />
    </div>
  );
}

function cleanLeaderboardError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402 || error.status === 403) return "Premium access required.";
    return "Unable to load leaderboard.";
  }
  if (error instanceof Error && error.message.startsWith("Fetch failed for ")) return "Unable to load leaderboard.";
  return error instanceof Error ? error.message : "Unable to load leaderboard.";
}

async function LeaderboardResultsSection({
  lookbackDays,
  chamber,
  sourceMode,
  performanceModel,
  sort,
  minTrades,
  limit,
  isInsiderMode,
  authToken,
}: {
  lookbackDays: number;
  chamber: CongressTraderLeaderboardChamber;
  sourceMode: CongressTraderLeaderboardSourceMode;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
  sort: CongressTraderLeaderboardSort;
  minTrades: number;
  limit: number;
  isInsiderMode: boolean;
  authToken: string;
}) {
  let data = null;
  let errorMessage: string | null = null;
  const sortHrefs = buildSortHrefs({
    lookback_days: lookbackDays,
    chamber,
    source_mode: sourceMode,
    performance_model: performanceModel,
    min_trades: minTrades,
    limit,
  });

  if (!authToken) {
    return (
      <CongressTraderLeaderboardClientResults
        lookbackDays={lookbackDays}
        chamber={chamber}
        sourceMode={sourceMode}
        performanceModel={performanceModel}
        sort={sort}
        minTrades={minTrades}
        limit={limit}
        isInsiderMode={isInsiderMode}
        sortHrefs={sortHrefs}
      />
    );
  } else {
    try {
      data = await getCongressTraderLeaderboard({
        lookback_days: lookbackDays,
        chamber,
        source_mode: sourceMode,
        performance_model: performanceModel,
        mode: performanceModel === "portfolio" ? "realistic_disclosure_lag" : undefined,
        sort,
        min_trades: performanceModel === "portfolio" ? undefined : minTrades,
        limit,
        authToken,
      });
    } catch (error) {
      console.error("[leaderboards] fetch failed", error);
      errorMessage = cleanLeaderboardError(error);
    }
  }

  return (
    <div className={`${cardClassName} min-h-[32rem] overflow-hidden p-0`}>
      {errorMessage ? (
        <CongressTraderLeaderboardStatusState
          title={errorMessage === "Sign in required." ? "Sign in required" : errorMessage === "Premium access required." ? "Premium required" : "Leaderboard unavailable"}
          message={
            errorMessage === "Sign in required."
              ? "Log in to view trade leaderboards."
              : errorMessage === "Premium access required."
                ? "Leaderboards are included with Premium."
                : errorMessage
          }
          sort={sort}
          isInsiderMode={isInsiderMode}
          performanceModel={performanceModel}
          sortHrefs={sortHrefs}
        />
      ) : !data ? (
        <CongressTraderLeaderboardStatusState
          title="Loading leaderboard"
          message="Fetching the latest rankings."
          sort={sort}
          isInsiderMode={isInsiderMode}
          performanceModel={performanceModel}
          sortHrefs={sortHrefs}
        />
      ) : data.rows.length === 0 ? (
        <CongressTraderLeaderboardStatusState
          title="No results"
          message={
            performanceModel === "portfolio"
              ? "No portfolio simulations meet the data-quality threshold for this view yet."
              : "No members matched your current filters."
          }
          sort={sort}
          isInsiderMode={isInsiderMode}
          performanceModel={performanceModel}
          sortHrefs={sortHrefs}
        />
      ) : (
        <CongressTraderLeaderboardTable
          data={data}
          sort={sort}
          isInsiderMode={isInsiderMode}
          performanceModel={performanceModel}
          sortHrefs={sortHrefs}
        />
      )}
    </div>
  );
}

export default async function CongressTraderLeaderboardPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const authState = await requirePageAuthState(buildReturnTo("/leaderboards/congress-traders", sp));
  const authToken = authState.token;
  const sourceMode = parseSourceMode(getParam(sp, "source_mode"));
  const performanceModel = parsePerformanceModel(getParam(sp, "performance_model"), sourceMode);
  const lookbackDays = performanceModel === "portfolio" ? 365 : parseLookback(getParam(sp, "lookback_days"));
  const chamber = parseChamber(getParam(sp, "chamber"));
  const sort = parseSort(getParam(sp, "sort"), performanceModel);
  const minTrades = parseMinTrades(getParam(sp, "min_trades"));
  const limit = parseLimit(getParam(sp, "limit"));
  const isInsiderMode = sourceMode === "insiders";
  const isPortfolioMode = performanceModel === "portfolio";
  const leaderboardTitle = isInsiderMode
    ? "Insider Trade Leaderboard"
    : isPortfolioMode
      ? "Congress Portfolio Simulation Leaderboard"
      : "Congress Trade Leaderboard";
  const leaderboardDescription = isInsiderMode
    ? "Rankings compare insider trading performance by historical returns and alpha versus the S&P 500."
    : isPortfolioMode
      ? "Rankings compare 365D replicated congressional portfolios using realistic disclosure lag."
      : "Rankings compare congressional trading performance by historical returns and alpha versus the S&P 500.";
  const resultsKey = JSON.stringify({ lookbackDays, chamber, sourceMode, performanceModel, sort, minTrades, limit });

  return (
    <div className="space-y-6">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">LEADERBOARDS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Trade Leaderboards</h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-300/80">
          <span className="font-semibold text-slate-100">{leaderboardTitle}:</span> {leaderboardDescription}
        </p>
      </div>

      <form className={`${cardClassName} grid grid-cols-2 gap-3 ${isInsiderMode ? "md:grid-cols-4" : "md:grid-cols-5"}`}>
        <input type="hidden" name="source_mode" value={sourceMode} />
        <input type="hidden" name="performance_model" value={performanceModel === "portfolio" ? "portfolio" : "outcomes"} />
        <input type="hidden" name="sort" value={sort} />
        {isPortfolioMode ? (
          <>
            <input type="hidden" name="lookback_days" value="365" />
            <label className="text-xs text-slate-300">
              <span className="mb-1 block">Lookback</span>
              <div className={`${selectClassName} flex h-10 items-center`}>365D</div>
            </label>
          </>
        ) : (
          <label className="text-xs text-slate-300">
            <span className="mb-1 block">Lookback</span>
            <select className={selectClassName} name="lookback_days" defaultValue={String(lookbackDays)}>
              <option value="30">30D</option>
              <option value="90">90D</option>
              <option value="180">180D</option>
              <option value="365">365D</option>
            </select>
          </label>
        )}
        {!isInsiderMode ? (
          <label className="text-xs text-slate-300">
            <span className="mb-1 block">Chamber</span>
            <select className={selectClassName} name="chamber" defaultValue={chamber}>
              <option value="all">All</option>
              <option value="house">House</option>
              <option value="senate">Senate</option>
            </select>
          </label>
        ) : (
          <input type="hidden" name="chamber" value="all" />
        )}
        {isPortfolioMode ? (
          <input type="hidden" name="min_trades" value={String(minTrades)} />
        ) : (
          <label className="text-xs text-slate-300">
            <span className="mb-1 block">Min Trades</span>
            <select className={selectClassName} name="min_trades" defaultValue={String(minTrades)}>
              <option value="1">1</option>
              <option value="3">3</option>
              <option value="5">5</option>
              <option value="10">10</option>
            </select>
          </label>
        )}
        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Limit</span>
          <select className={selectClassName} name="limit" defaultValue={String(limit)}>
            {LIMIT_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <button type="submit" className="col-span-2 inline-flex h-10 items-center justify-center self-end rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 hover:bg-emerald-500/20 md:col-span-1">
          Apply
        </button>

        <div className={`col-span-2 mt-2 grid gap-3 border-t border-white/10 pt-3 ${isInsiderMode ? "md:col-span-4" : "md:col-span-5"} md:grid-cols-[max-content_max-content]`}>
          <div className="space-y-1.5">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Universe</div>
            <div className="flex flex-wrap items-center gap-1">
              {SOURCE_MODE_OPTIONS.map((option) => {
                const label = option === "congress" ? "Congress" : "Insiders";
                const active = sourceMode === option;
                const targetPerformanceModel = option === "congress" ? performanceModel : "outcomes";
                const targetSort = targetPerformanceModel === "portfolio" ? "alpha_pct" : "avg_alpha";
                const targetChamber = option === "insiders" ? "all" : chamber;
                return (
                  <Link
                    key={option}
                    href={buildUrl({
                      lookback_days: lookbackDays,
                      chamber: targetChamber,
                      source_mode: option,
                      performance_model: targetPerformanceModel,
                      sort: active ? sort : targetSort,
                      min_trades: minTrades,
                      limit,
                    })}
                    className={pillClassName(active)}
                  >
                    {label}
                  </Link>
                );
              })}
            </div>
          </div>
          <div className="space-y-1.5">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Performance Model</div>
            <div className="flex flex-wrap items-center gap-1">
              {PERFORMANCE_MODEL_OPTIONS.map((option) => {
                const label = option === "portfolio" ? "Portfolio Simulation" : "Trade Outcomes";
                const active = performanceModel === option;
                const targetSort = option === "portfolio" ? "alpha_pct" : "avg_alpha";
                const targetSourceMode = option === "portfolio" ? "congress" : sourceMode;
                return (
                  <Link
                    key={option}
                    href={buildUrl({
                      lookback_days: lookbackDays,
                      chamber,
                      source_mode: targetSourceMode,
                      performance_model: option,
                      sort: active ? sort : targetSort,
                      min_trades: minTrades,
                      limit,
                    })}
                    className={pillClassName(active)}
                  >
                    {label}
                  </Link>
                );
              })}
            </div>
          </div>
        </div>
      </form>

      <Suspense key={resultsKey} fallback={<LeaderboardResultsFallback />}>
        <LeaderboardResultsSection
          lookbackDays={lookbackDays}
          chamber={chamber}
          sourceMode={sourceMode}
          performanceModel={performanceModel}
          sort={sort}
          minTrades={minTrades}
          limit={limit}
          isInsiderMode={isInsiderMode}
          authToken={authToken}
        />
      </Suspense>

      <div className="text-xs text-slate-500">
        Quick links:{" "}
        <Link className="text-emerald-300 hover:underline" href={buildUrl({ lookback_days: 365, chamber: "all", source_mode: "congress", performance_model: "outcomes", sort: "avg_alpha", min_trades: 3, limit: 10 })}>
          default
        </Link>
        {" | "}
        <Link className="text-emerald-300 hover:underline" href={buildUrl({ lookback_days: 365, chamber: "all", source_mode: "congress", performance_model: "portfolio", sort: "alpha_pct", min_trades: 3, limit: 100 })}>
          portfolio simulation
        </Link>
        {" | "}
        <Link className="text-emerald-300 hover:underline" href={buildUrl({ lookback_days: 90, chamber: "senate", source_mode: "congress", performance_model: "outcomes", sort: "avg_return", min_trades: 1, limit: 50 })}>
          senate 90D return
        </Link>
      </div>
    </div>
  );
}
