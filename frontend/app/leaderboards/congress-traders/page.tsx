import Link from "next/link";
import { Badge } from "@/components/Badge";
import {
  getCongressTraderLeaderboard,
  type CongressTraderLeaderboardChamber,
  type CongressTraderLeaderboardSort,
} from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { cardClassName, selectClassName } from "@/lib/styles";
import { nameToSlug } from "@/lib/memberSlug";

type SearchParams = Record<string, string | string[] | undefined>;

const LOOKBACK_OPTIONS = [30, 90, 180, 365] as const;
const CHAMBER_OPTIONS: CongressTraderLeaderboardChamber[] = ["all", "house", "senate"];
const SORT_OPTIONS: CongressTraderLeaderboardSort[] = ["avg_alpha", "avg_return", "win_rate", "trade_count"];
const MIN_TRADE_OPTIONS = [1, 3, 5, 10] as const;
const LIMIT_OPTIONS = [10, 25, 50, 100] as const;

function getParam(sp: SearchParams, key: string): string {
  const value = sp[key];
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

function parseSort(raw: string): CongressTraderLeaderboardSort {
  return SORT_OPTIONS.includes(raw as CongressTraderLeaderboardSort)
    ? (raw as CongressTraderLeaderboardSort)
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

function sortedColumnClass(active: boolean): string {
  return active ? "border-l border-emerald-400/15 bg-emerald-500/[0.04]" : "";
}

function sortedHeaderClass(active: boolean): string {
  return active
    ? "border-l border-emerald-400/20 bg-emerald-500/[0.07] font-semibold text-emerald-100"
    : "text-slate-400";
}

function isSortColumn(sort: CongressTraderLeaderboardSort, column: CongressTraderLeaderboardSort): boolean {
  return sort === column;
}

function pct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

function pct0(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${Math.round(value * 100)}%`;
}

function signedPctTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-400";
  if (Math.abs(value) < 0.05) return "text-slate-300";
  return value > 0 ? "text-emerald-300" : "text-rose-300";
}

function winRateTone(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "text-slate-300";
  if (value >= 0.65) return "text-emerald-300";
  if (value <= 0.35) return "text-rose-300";
  return "text-slate-200";
}

function buildUrl(params: {
  lookback_days: number;
  chamber: CongressTraderLeaderboardChamber;
  sort: CongressTraderLeaderboardSort;
  min_trades: number;
  limit: number;
}) {
  const url = new URL("https://local/leaderboards/congress-traders");
  url.searchParams.set("lookback_days", String(params.lookback_days));
  url.searchParams.set("chamber", params.chamber);
  url.searchParams.set("sort", params.sort);
  url.searchParams.set("min_trades", String(params.min_trades));
  url.searchParams.set("limit", String(params.limit));
  return `${url.pathname}${url.search}`;
}

export default async function CongressTraderLeaderboardPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const sp = (await searchParams) ?? {};
  const lookbackDays = parseLookback(getParam(sp, "lookback_days"));
  const chamber = parseChamber(getParam(sp, "chamber"));
  const sort = parseSort(getParam(sp, "sort"));
  const minTrades = parseMinTrades(getParam(sp, "min_trades"));
  const limit = parseLimit(getParam(sp, "limit"));

  let data = null;
  let errorMessage: string | null = null;

  try {
    data = await getCongressTraderLeaderboard({
      lookback_days: lookbackDays,
      chamber,
      sort,
      min_trades: minTrades,
      limit,
    });
  } catch (error) {
    errorMessage = error instanceof Error ? error.message : "Unable to load leaderboard.";
  }

  return (
    <div className="space-y-6">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">LEADERBOARDS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Congress Trader Leaderboard</h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-300/80">
          Rankings compare members by historical trade performance, including returns and performance versus the S&amp;P 500.
        </p>
      </div>

      <form className={`${cardClassName} grid grid-cols-2 gap-3 md:grid-cols-5`}>
        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Lookback</span>
          <select className={selectClassName} name="lookback_days" defaultValue={String(lookbackDays)}>
            <option value="30">30D</option>
            <option value="90">90D</option>
            <option value="180">180D</option>
            <option value="365">365D</option>
          </select>
        </label>

        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Chamber</span>
          <select className={selectClassName} name="chamber" defaultValue={chamber}>
            <option value="all">All</option>
            <option value="house">House</option>
            <option value="senate">Senate</option>
          </select>
        </label>

        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Sort</span>
          <select className={selectClassName} name="sort" defaultValue={sort}>
            <option value="avg_alpha">Avg Alpha</option>
            <option value="avg_return">Avg Return</option>
            <option value="win_rate">Win Rate</option>
            <option value="trade_count">Trade Count</option>
          </select>
        </label>

        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Min Trades</span>
          <select className={selectClassName} name="min_trades" defaultValue={String(minTrades)}>
            <option value="1">1</option>
            <option value="3">3</option>
            <option value="5">5</option>
            <option value="10">10</option>
          </select>
        </label>

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

        <button
          type="submit"
          className="col-span-2 inline-flex h-10 items-center justify-center self-end rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 hover:bg-emerald-500/20 md:col-span-1"
        >
          Apply
        </button>
      </form>

      <div className={`${cardClassName} overflow-hidden p-0`}>
        {errorMessage ? (
          <div className="p-6 text-sm text-rose-200/90">{errorMessage}</div>
        ) : !data ? (
          <div className="p-8 text-center text-sm text-slate-300">Loading leaderboard…</div>
        ) : data.rows.length === 0 ? (
          <div className="p-8 text-center text-sm text-slate-300">No members matched your current filters.</div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm [font-variant-numeric:tabular-nums]">
                <thead className="border-b border-white/10 bg-slate-950/70 text-xs uppercase tracking-wide">
                  <tr>
                    <th className="px-4 py-3 text-slate-400">Rank</th>
                    <th className="px-4 py-3 text-slate-400">Member</th>
                    <th className="px-4 py-3 text-slate-400">Chamber</th>
                    <th className="px-4 py-3 text-slate-400">Party</th>
                    <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "trade_count"))}`}>
                      Trades{isSortColumn(sort, "trade_count") ? " ▾" : ""}
                    </th>
                    <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "avg_return"))}`}>
                      Avg Return{isSortColumn(sort, "avg_return") ? " ▾" : ""}
                    </th>
                    <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "avg_alpha"))}`}>
                      Avg Alpha{isSortColumn(sort, "avg_alpha") ? " ▾" : ""}
                    </th>
                    <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "win_rate"))}`}>
                      Win Rate{isSortColumn(sort, "win_rate") ? " ▾" : ""}
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-white/5">
                  {data.rows.map((row) => {
                    const chamber = chamberBadge(row.chamber);
                    const party = partyBadge(row.party);

                    return (
                    <tr key={row.member_id} className="text-slate-200 transition-colors hover:bg-slate-900/35">
                      <td className="px-4 py-3">
                        <span className="inline-flex min-w-11 items-center justify-center rounded-md border border-white/10 bg-white/[0.03] px-2 py-1 text-center font-semibold text-white">
                          #{row.rank}
                        </span>
                      </td>
                      <td className="px-4 py-3">
                        <Link
                          href={`/member/${row.member_id || nameToSlug(row.member_name)}`}
                          className="font-semibold text-slate-100 hover:text-emerald-200 hover:underline"
                        >
                          {row.member_name}
                        </Link>
                      </td>
                      <td className="px-4 py-3">
                        <span title={row.chamber ?? undefined}>
                          <Badge tone={chamber.tone} className="px-2 py-0.5 text-[10px]">
                            {chamber.label}
                          </Badge>
                        </span>
                      </td>
                      <td className="px-4 py-3">
                        <span title={row.party ?? undefined}>
                          <Badge tone={party.tone} className="px-2 py-0.5 text-[10px]">
                            {party.label}
                          </Badge>
                        </span>
                      </td>
                      <td className={`px-4 py-3 text-right text-slate-300 ${sortedColumnClass(isSortColumn(sort, "trade_count"))}`}>{row.trade_count_total}</td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.avg_return)} ${isSortColumn(sort, "avg_return") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "avg_return"))}`}>{pct(row.avg_return)}</td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.avg_alpha)} ${isSortColumn(sort, "avg_alpha") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "avg_alpha"))}`}>
                        {pct(row.avg_alpha)}
                      </td>
                      <td className={`px-4 py-3 text-right ${winRateTone(row.win_rate)} ${isSortColumn(sort, "win_rate") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "win_rate"))}`}>{pct0(row.win_rate)}</td>
                    </tr>
                  );
                  })}
                </tbody>
              </table>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 border-t border-white/10 bg-slate-950/60 px-4 py-3 text-xs text-slate-400">
              <div>
                Historical trade performance over the selected lookback period, compared against the S&amp;P 500.
              </div>
              <div>{data.rows.length} rows</div>
            </div>
          </>
        )}
      </div>

      <div className="text-xs text-slate-500">
        Quick links:{" "}
        <Link
          className="text-emerald-300 hover:underline"
          href={buildUrl({ lookback_days: 365, chamber: "all", sort: "avg_alpha", min_trades: 3, limit: 10 })}
        >
          default
        </Link>
        {" · "}
        <Link
          className="text-emerald-300 hover:underline"
          href={buildUrl({ lookback_days: 90, chamber: "senate", sort: "avg_return", min_trades: 1, limit: 50 })}
        >
          senate 90D return
        </Link>
      </div>
    </div>
  );
}
