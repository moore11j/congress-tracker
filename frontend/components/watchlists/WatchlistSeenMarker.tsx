"use client";

import { useEffect } from "react";
import { markMonitoringSourceRead, markWatchlistSeen } from "@/lib/api";

export function WatchlistSeenMarker({ watchlistId }: { watchlistId: number }) {
  useEffect(() => {
    let cancelled = false;

    const markSeen = async () => {
      try {
        await markWatchlistSeen(watchlistId);
        await markMonitoringSourceRead(String(watchlistId));
        window.dispatchEvent(new Event("ct:monitoring-unread-updated"));
      } catch (error) {
        if (!cancelled) {
          console.warn("Unable to mark watchlist seen", error);
        }
      }
    };

    markSeen();

    return () => {
      cancelled = true;
    };
  }, [watchlistId]);

  return null;
}
