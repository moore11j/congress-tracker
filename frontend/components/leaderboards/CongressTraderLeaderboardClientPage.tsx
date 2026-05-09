"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ApiError, getCongressTraderLeaderboard, getEntitlements, type CongressTraderLeaderboardRow } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, type Entitlements } from "@/lib/entitlements";
import { cardClassName, selectClassName } from "@/lib/styles";
import { memberHref } from "@/lib/memberSlug";

type ClientSearchParams = Record<string, string | undefined>;

function param(searchParams: ClientSearchParams, key: string, fallback: string) {
  return (searchParams[key] ?? fallback).trim() || fallback;
}

function cleanProtectedError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402) return "Premium access required.";
    return "Unable to load leaderboard.";
  }
  return error instanceof Error ? error.message : "Unable to load leaderboard.";
}

export function CongressTraderLeaderboardClientPage({ initialSearchParams }: { initialSearchParams: ClientSearchParams }) {
  const [entitlements, setEntitlements] = useState<Entitlements | null>(null);
  const [rows, setRows] = useState<CongressTraderLeaderboardRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const lookbackDays = Number(param(initialSearchParams, "lookback_days", "365")) || 365;
  const chamber = param(initialSearchParams, "chamber", "all") as any;
  const sourceMode = param(initialSearchParams, "source_mode", "congress") as any;
  const sort = param(initialSearchParams, "sort", "avg_alpha") as any;
  const minTrades = Number(param(initialSearchParams, "min_trades", "3")) || 3;
  const limit = Number(param(initialSearchParams, "limit", "10")) || 10;

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErrorMessage(null);
    getEntitlements()
      .then(async (nextEntitlements) => {
        if (cancelled) return;
        setEntitlements(nextEntitlements);
        if (!nextEntitlements.user) {
          setRows([]);
          setErrorMessage("Sign in required.");
          return;
        }
        if (!hasEntitlement(nextEntitlements, "leaderboards")) {
          setRows([]);
          setErrorMessage("Premium access required.");
          return;
        }
        const response = await getCongressTraderLeaderboard({
          lookback_days: lookbackDays,
          chamber,
          source_mode: sourceMode,
          sort,
          min_trades: minTrades,
          limit,
        });
        if (!cancelled) setRows(response.rows);
      })
      .catch((error) => {
        console.error("[leaderboards] protected client fetch failed", error);
        if (!cancelled) {
          setEntitlements(defaultEntitlements);
          setRows([]);
          setErrorMessage(cleanProtectedError(error));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [chamber, limit, lookbackDays, minTrades, sort, sourceMode]);

  const isSignInError = errorMessage === "Sign in required.";
  const isPremiumError = errorMessage === "Premium access required.";

  return (
    <div className="space-y-6">
      <div>
        <div className="text-xs tracking-[0.25em] text-emerald-300/70">LEADERBOARDS</div>
        <h1 className="mt-2 text-3xl font-semibold text-white">Trade Leaderboards</h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-300/80">
          Protected rankings load in your browser with your active session token.
        </p>
      </div>

      <form className={`${cardClassName} grid grid-cols-2 gap-3 md:grid-cols-5`}>
        <label className="text-xs text-slate-300"><span className="mb-1 block">Lookback</span><select className={selectClassName} name="lookback_days" defaultValue={String(lookbackDays)}><option value="30">30D</option><option value="90">90D</option><option value="180">180D</option><option value="365">365D</option></select></label>
        <label className="text-xs text-slate-300"><span className="mb-1 block">Chamber</span><select className={selectClassName} name="chamber" defaultValue={chamber}><option value="all">All</option><option value="house">House</option><option value="senate">Senate</option></select></label>
        <label className="text-xs text-slate-300"><span className="mb-1 block">Sort</span><select className={selectClassName} name="sort" defaultValue={sort}><option value="avg_alpha">Avg Alpha</option><option value="avg_return">Avg Return</option><option value="win_rate">Win Rate</option><option value="trade_count">Trade Count</option></select></label>
        <label className="text-xs text-slate-300"><span className="mb-1 block">Min Trades</span><select className={selectClassName} name="min_trades" defaultValue={String(minTrades)}><option value="1">1</option><option value="3">3</option><option value="5">5</option><option value="10">10</option></select></label>
        <button type="submit" className="inline-flex h-10 items-center justify-center self-end rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200">Apply</button>
      </form>

      {loading ? (
        <div className={`${cardClassName} min-h-[20rem] p-8 text-sm text-slate-300`}>Loading leaderboard...</div>
      ) : isSignInError ? (
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Sign in required</h2>
          <p className="mt-2 text-sm text-slate-400">Log in to view trade leaderboards.</p>
          <Link href="/login?return_to=%2Fleaderboards%2Fcongress-traders" className="mt-4 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100">Sign in</Link>
        </div>
      ) : isPremiumError ? (
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Premium required</h2>
          <p className="mt-2 text-sm text-slate-400">Leaderboards are included with Premium.</p>
          <Link href={entitlements?.upgrade_url ?? "/pricing"} className="mt-4 inline-flex rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100">View plans</Link>
        </div>
      ) : (
        <div className={`${cardClassName} min-h-[30rem] overflow-hidden p-0`}>
          {errorMessage ? (
            <div className="p-8 text-center text-sm text-slate-400">{errorMessage}</div>
          ) : (
            <table className="min-w-full text-left text-sm">
              <thead className="bg-slate-950/70 text-xs uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-4 py-3">Rank</th>
                  <th className="px-4 py-3">Member</th>
                  <th className="px-4 py-3 text-right">Trades</th>
                  <th className="px-4 py-3 text-right">Avg Alpha</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {rows.length === 0 ? (
                  <tr><td colSpan={4} className="px-4 py-10 text-center text-slate-400">No members matched your current filters.</td></tr>
                ) : (
                  rows.map((row) => (
                    <tr key={row.member_id} className="text-slate-200">
                      <td className="px-4 py-3 font-semibold text-white">#{row.rank}</td>
                      <td className="px-4 py-3"><Link href={memberHref({ slug: row.member_slug, name: row.member_name, memberId: row.member_id })} className="font-semibold text-slate-100 hover:underline">{row.member_name}</Link></td>
                      <td className="px-4 py-3 text-right">{row.trade_count_total}</td>
                      <td className="px-4 py-3 text-right">{row.avg_alpha == null ? "--" : `${row.avg_alpha.toFixed(1)}%`}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          )}
        </div>
      )}
    </div>
  );
}
