"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { Badge } from "@/components/Badge";
import {
  ApiError,
  getCongressTraderLeaderboard,
  type CongressTraderLeaderboardChamber,
  type CongressTraderLeaderboardResponse,
  type CongressTraderLeaderboardSort,
  type CongressTraderLeaderboardSourceMode,
} from "@/lib/api";
import { chamberBadge, partyBadge } from "@/lib/format";
import { insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { cardClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";

function cleanLeaderboardError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402 || error.status === 403) return "Premium access required.";
    return "Unable to load leaderboard.";
  }
  return error instanceof Error ? error.message : "Unable to load leaderboard.";
}

function pct(value: number | null | undefined, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(digits)}%`;
}

function pct0(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "--";
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

export function CongressTraderLeaderboardClientResults({
  lookbackDays,
  chamber,
  sourceMode,
  sort,
  minTrades,
  limit,
  isInsiderMode,
}: {
  lookbackDays: number;
  chamber: CongressTraderLeaderboardChamber;
  sourceMode: CongressTraderLeaderboardSourceMode;
  sort: CongressTraderLeaderboardSort;
  minTrades: number;
  limit: number;
  isInsiderMode: boolean;
}) {
  const [data, setData] = useState<CongressTraderLeaderboardResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setData(null);
    setErrorMessage(null);
    getCongressTraderLeaderboard({
      lookback_days: lookbackDays,
      chamber,
      source_mode: sourceMode,
      sort,
      min_trades: minTrades,
      limit,
    })
      .then((response) => {
        if (alive) setData(response);
      })
      .catch((error) => {
        console.error("[leaderboards] client fetch failed", error);
        if (alive) setErrorMessage(cleanLeaderboardError(error));
      });
    return () => {
      alive = false;
    };
  }, [lookbackDays, chamber, sourceMode, sort, minTrades, limit]);

  return (
    <div className={`${cardClassName} min-h-[32rem] overflow-hidden p-0`}>
      {errorMessage ? (
        <div className="p-6 text-sm text-slate-300">
          <p className="font-semibold text-white">
            {errorMessage === "Sign in required." ? "Sign in required" : errorMessage === "Premium access required." ? "Premium required" : "Leaderboard unavailable"}
          </p>
          <p className="mt-2 text-slate-400">
            {errorMessage === "Sign in required."
              ? "Log in to view trade leaderboards."
              : errorMessage === "Premium access required."
                ? "Leaderboards are included with Premium."
                : errorMessage}
          </p>
        </div>
      ) : !data ? (
        <div className="p-8 text-center text-sm text-slate-300">Loading leaderboard...</div>
      ) : data.rows.length === 0 ? (
        <div className="p-8 text-center text-sm text-slate-300">No members matched your current filters.</div>
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="min-w-full text-left text-sm [font-variant-numeric:tabular-nums]">
              <thead className="border-b border-white/10 bg-slate-950/70 text-xs uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-3 text-slate-400">Rank</th>
                  <th className="px-4 py-3 text-slate-400">{isInsiderMode ? "Insider" : "Member"}</th>
                  {isInsiderMode ? (
                    <>
                      <th className="px-4 py-3 text-slate-400">Ticker</th>
                      <th className="px-4 py-3 text-slate-400">Role</th>
                    </>
                  ) : (
                    <>
                      <th className="px-4 py-3 text-slate-400">Chamber</th>
                      <th className="px-4 py-3 text-slate-400">Party</th>
                    </>
                  )}
                  <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "trade_count"))}`}>Trades{isSortColumn(sort, "trade_count") ? " v" : ""}</th>
                  <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "avg_return"))}`}>Avg Return{isSortColumn(sort, "avg_return") ? " v" : ""}</th>
                  <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "avg_alpha"))}`}>Avg Alpha{isSortColumn(sort, "avg_alpha") ? " v" : ""}</th>
                  <th className={`px-4 py-3 text-right ${sortedHeaderClass(isSortColumn(sort, "win_rate"))}`}>Win Rate{isSortColumn(sort, "win_rate") ? " v" : ""}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/5">
                {data.rows.map((row) => {
                  const chamberBadgeValue = chamberBadge(row.chamber);
                  const party = partyBadge(row.party);
                  const roleCode = normalizeInsiderRoleBadge(row.role);
                  const roleTone = insiderRoleBadgeTone(roleCode);
                  const insiderLink = insiderHref(row.member_name, row.reporting_cik ?? row.member_id);
                  const memberLink = memberHref({ slug: row.member_slug, name: row.member_name, memberId: row.member_id });
                  const rowTicker = row.symbol ?? row.ticker ?? null;
                  const tickerLink = tickerHref(rowTicker);

                  return (
                    <tr key={`${row.rank}-${row.member_id}-${rowTicker ?? ""}`} className="text-slate-200 transition-colors hover:bg-slate-900/35">
                      <td className="px-4 py-3"><span className="inline-flex min-w-11 items-center justify-center rounded-md border border-white/10 bg-white/[0.03] px-2 py-1 text-center font-semibold text-white">#{row.rank}</span></td>
                      <td className="px-4 py-3">
                        {isInsiderMode ? (
                          <div className="min-w-[210px]">
                            {insiderLink ? <Link href={insiderLink} prefetch={false} className="font-semibold text-slate-100 hover:text-emerald-200 hover:underline">{row.member_name}</Link> : <span className="font-semibold text-slate-100">{row.member_name}</span>}
                            {row.company_name ? <div className="text-xs text-slate-400">{row.company_name}</div> : null}
                          </div>
                        ) : row.chamber ? (
                          <Link href={memberLink} prefetch={false} className="font-semibold text-slate-100 hover:text-emerald-200 hover:underline">{row.member_name}</Link>
                        ) : (
                          <span className="font-semibold text-slate-100">{row.member_name}</span>
                        )}
                      </td>
                      {isInsiderMode ? (
                        <>
                          <td className="px-4 py-3">
                            {rowTicker ? (
                              tickerLink ? <Link href={tickerLink} prefetch={false} className="font-mono text-xs font-semibold uppercase tracking-wide text-emerald-200 hover:text-emerald-100 hover:underline">{rowTicker}</Link> : <span className="font-mono text-xs uppercase tracking-wide text-slate-300">{rowTicker}</span>
                            ) : (
                              <span className="text-slate-500">--</span>
                            )}
                          </td>
                          <td className="px-4 py-3"><Badge tone={roleTone} className="px-2 py-0.5 text-[10px]">{roleCode}</Badge></td>
                        </>
                      ) : (
                        <>
                          <td className="px-4 py-3"><span title={row.chamber ?? undefined}><Badge tone={chamberBadgeValue.tone} className="px-2 py-0.5 text-[10px]">{chamberBadgeValue.label}</Badge></span></td>
                          <td className="px-4 py-3"><span title={row.party ?? undefined}><Badge tone={party.tone} className="px-2 py-0.5 text-[10px]">{party.label}</Badge></span></td>
                        </>
                      )}
                      <td className={`px-4 py-3 text-right text-slate-300 ${sortedColumnClass(isSortColumn(sort, "trade_count"))}`}>{row.trade_count_total}</td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.avg_return)} ${isSortColumn(sort, "avg_return") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "avg_return"))}`}>{pct(row.avg_return)}</td>
                      <td className={`px-4 py-3 text-right ${signedPctTone(row.avg_alpha)} ${isSortColumn(sort, "avg_alpha") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "avg_alpha"))}`}>{pct(row.avg_alpha)}</td>
                      <td className={`px-4 py-3 text-right ${winRateTone(row.win_rate)} ${isSortColumn(sort, "win_rate") ? "font-semibold" : ""} ${sortedColumnClass(isSortColumn(sort, "win_rate"))}`}>{pct0(row.win_rate)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className="flex flex-wrap items-center justify-between gap-2 border-t border-white/10 bg-slate-950/60 px-4 py-3 text-xs text-slate-400">
            <div>Historical trade performance over the selected lookback period, compared against the S&amp;P 500.</div>
            <div>{data.rows.length} rows</div>
          </div>
        </>
      )}
    </div>
  );
}
