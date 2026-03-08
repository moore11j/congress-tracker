import Link from "next/link";
import {
  getCongressTraderLeaderboard,
  type CongressTraderLeaderboardChamber,
  type CongressTraderLeaderboardSort,
} from "@/lib/api";
import { cardClassName, selectClassName } from "@/lib/styles";
import { nameToSlug } from "@/lib/memberSlug";

type SearchParams = Record<string, string | string[] | undefined>;

const LOOKBACK_OPTIONS = [90, 180, 365] as const;
const CHAMBER_OPTIONS: CongressTraderLeaderboardChamber[] = ["all", "house", "senate"];
const SORT_OPTIONS: CongressTraderLeaderboardSort[] = ["avg_alpha", "avg_return", "win_rate", "trade_count"];
const MIN_TRADE_OPTIONS = [1, 3, 5] as const;

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

function pct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

function pct0(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${Math.round(value * 100)}%`;
}

function chamberLabel(chamber: string | null | undefined): string {
  const normalized = (chamber ?? "").toLowerCase();
  if (normalized === "house") return "House";
  if (normalized === "senate") return "Senate";
  return "—";
}

function partyLabel(party: string | null | undefined): string {
  const normalized = (party ?? "").toUpperCase();
  if (!normalized) return "—";
  if (normalized === "D") return "D";
  if (normalized === "R") return "R";
  if (normalized === "I") return "I";
  return normalized;
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
  const limit = Math.min(toPositiveInt(getParam(sp, "limit"), 100), 250);

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
          Ranked using the same canonical member performance and alpha methodology used on each member profile.
        </p>
      </div>

      <form className={`${cardClassName} grid grid-cols-2 gap-3 md:grid-cols-5`}>
        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Lookback</span>
          <select className={selectClassName} name="lookback_days" defaultValue={String(lookbackDays)}>
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
          </select>
        </label>

        <label className="text-xs text-slate-300">
          <span className="mb-1 block">Limit</span>
          <input className={selectClassName} name="limit" defaultValue={String(limit)} inputMode="numeric" />
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
          <div className="p-6 text-sm text-rose-200">{errorMessage}</div>
        ) : !data ? (
          <div className="p-6 text-sm text-slate-300">Loading leaderboard…</div>
        ) : data.rows.length === 0 ? (
          <div className="p-6 text-sm text-slate-300">No members matched your current filters.</div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="min-w-full text-left text-sm">
                <thead className="border-b border-white/10 bg-slate-950/70 text-xs uppercase tracking-wide text-slate-400">
                  <tr>
                    <th className="px-4 py-3">Rank</th>
                    <th className="px-4 py-3">Member</th>
                    <th className="px-4 py-3">Chamber</th>
                    <th className="px-4 py-3">Party</th>
                    <th className="px-4 py-3">Trades</th>
                    <th className="px-4 py-3">Avg Return</th>
                    <th className="px-4 py-3">Avg Alpha</th>
                    <th className="px-4 py-3">Win Rate</th>
                  </tr>
                </thead>
                <tbody>
                  {data.rows.map((row) => (
                    <tr key={row.member_id} className="border-b border-white/5 text-slate-200">
                      <td className="px-4 py-3 font-semibold text-white">#{row.rank}</td>
                      <td className="px-4 py-3">
                        <Link href={`/member/${nameToSlug(row.member_name)}`} className="font-semibold text-emerald-200 hover:underline">
                          {row.member_name}
                        </Link>
                      </td>
                      <td className="px-4 py-3 text-slate-300">{chamberLabel(row.chamber)}</td>
                      <td className="px-4 py-3 text-slate-300">{partyLabel(row.party)}</td>
                      <td className="px-4 py-3 text-slate-300">{row.trade_count_total}</td>
                      <td className="px-4 py-3 text-slate-200">{pct(row.avg_return)}</td>
                      <td className="px-4 py-3 text-slate-200">{pct(row.avg_alpha)}</td>
                      <td className="px-4 py-3 text-slate-200">{pct0(row.win_rate)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="flex flex-wrap items-center justify-between gap-2 border-t border-white/10 bg-slate-950/60 px-4 py-3 text-xs text-slate-400">
              <div>
                Methodology: canonical member performance scoring, benchmark {data.benchmark_symbol}, lookback {data.lookback_days}d.
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
          href={buildUrl({ lookback_days: 365, chamber: "all", sort: "avg_alpha", min_trades: 3, limit: 100 })}
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
