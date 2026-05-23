"use client";

import { useEffect, useState } from "react";
import { CongressTraderLeaderboardStatusState, CongressTraderLeaderboardTable } from "@/components/leaderboards/CongressTraderLeaderboardTable";
import {
  ApiError,
  getCongressTraderLeaderboard,
  type CongressTraderLeaderboardChamber,
  type CongressTraderLeaderboardPerformanceModel,
  type CongressTraderLeaderboardResponse,
  type CongressTraderLeaderboardSort,
  type CongressTraderLeaderboardSourceMode,
} from "@/lib/api";
import { cardClassName } from "@/lib/styles";

function cleanLeaderboardError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402 || error.status === 403) return "Premium access required.";
    return "Unable to load leaderboard.";
  }
  if (error instanceof Error && error.message.startsWith("Fetch failed for ")) return "Unable to load leaderboard.";
  return error instanceof Error ? error.message : "Unable to load leaderboard.";
}

export function CongressTraderLeaderboardClientResults({
  lookbackDays,
  chamber,
  sourceMode,
  performanceModel,
  sort,
  minTrades,
  limit,
  isInsiderMode,
  sortHrefs,
}: {
  lookbackDays: number;
  chamber: CongressTraderLeaderboardChamber;
  sourceMode: CongressTraderLeaderboardSourceMode;
  performanceModel: CongressTraderLeaderboardPerformanceModel;
  sort: CongressTraderLeaderboardSort;
  minTrades: number;
  limit: number;
  isInsiderMode: boolean;
  sortHrefs?: Partial<Record<CongressTraderLeaderboardSort, string>>;
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
      performance_model: performanceModel,
      mode: performanceModel === "portfolio" ? "realistic_disclosure_lag" : undefined,
      sort,
      min_trades: performanceModel === "portfolio" ? undefined : minTrades,
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
  }, [lookbackDays, chamber, sourceMode, performanceModel, sort, minTrades, limit]);

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
