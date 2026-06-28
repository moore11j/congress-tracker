"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { NotificationPreferences } from "@/components/notifications/NotificationPreferences";
import { ConfirmationMonitoringPanel } from "@/components/watchlists/ConfirmationMonitoringRefreshButton";
import { WatchlistRecentActivity } from "@/components/watchlists/WatchlistRecentActivity";
import { WatchlistTickerManager } from "@/components/watchlists/WatchlistTickerManager";
import { cardClassName, ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import type { ConfirmationMonitoringEvent, FeedItem, WatchlistDetail } from "@/lib/types";
import type { WatchlistActivityState } from "@/lib/watchlistActivity";

type RecentActivityData = {
  items: FeedItem[];
  nextCursor: string | null;
  offset: number;
  hasMore: boolean;
};

type Props = {
  watchlist: WatchlistDetail;
  confirmationEvents: ConfirmationMonitoringEvent[];
  initialState: WatchlistActivityState;
  initialData: RecentActivityData;
};

const pendingWatchlistToastKey = "watchlist:create-toast";

export function WatchlistDetailContent({ watchlist, confirmationEvents, initialState, initialData }: Props) {
  const unseenCount = Math.max(Number(watchlist.unread_count ?? watchlist.unseen_count) || 0, 0);
  const [createToast, setCreateToast] = useState<string | null>(null);

  useEffect(() => {
    try {
      const message = window.sessionStorage.getItem(pendingWatchlistToastKey);
      if (message) {
        setCreateToast(message);
        window.sessionStorage.removeItem(pendingWatchlistToastKey);
      }
    } catch {
      // Storage can be unavailable; the detail page itself is still usable.
    }
  }, []);

  return (
    <div className="space-y-6">
      {createToast ? (
        <div
          role="alert"
          className="rounded-lg border border-rose-300/40 bg-rose-300/10 px-4 py-3 text-sm font-medium text-rose-100"
        >
          {createToast}
        </div>
      ) : null}

      <div className="grid w-full min-w-0 items-center gap-6 lg:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlist</p>
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="text-3xl font-semibold text-white">{watchlist.name ?? `Watchlist #${watchlist.watchlist_id}`}</h1>
            {unseenCount > 0 ? (
              <span className="rounded-lg border border-emerald-300/30 bg-emerald-300/15 px-2.5 py-1 text-xs font-semibold text-emerald-100">
                {unseenCount} new
              </span>
            ) : null}
          </div>
          <p className="text-sm text-slate-400">Monitor filings, insider trades, and unusual signals across saved tickers.</p>
        </div>
        <div className="flex w-full min-w-0 gap-2 lg:justify-end">
          <Link href={`/backtesting?strategy=watchlist&watchlist_id=${watchlist.watchlist_id}`} className={subtlePrimaryButtonClassName} prefetch={false}>
            Backtest this watchlist
          </Link>
          <Link href="/watchlists" className={ghostButtonClassName}>
            Back to watchlists
          </Link>
        </div>
      </div>

      <div className="grid w-full min-w-0 gap-6 lg:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <WatchlistTickerManager watchlistId={watchlist.watchlist_id} tickers={watchlist.tickers} />

        <section className={`${cardClassName} min-w-0 space-y-4`}>
          <NotificationPreferences
            sourceType="watchlist"
            sourceId={String(watchlist.watchlist_id)}
            sourceName={watchlist.name ?? `Watchlist #${watchlist.watchlist_id}`}
            useAccountEmailDestination={true}
            sourcePayload={{
              unseen_since: watchlist.unseen_since,
              last_seen_at: watchlist.last_seen_at,
            }}
          />

          <ConfirmationMonitoringPanel watchlistId={watchlist.watchlist_id} initialEvents={confirmationEvents} />

          <WatchlistRecentActivity
            watchlistId={watchlist.watchlist_id}
            tickerCount={watchlist.tickers.length}
            unseenCount={unseenCount}
            unseenSince={watchlist.unseen_since ?? ""}
            initialState={initialState}
            initialData={initialData}
          />
        </section>
      </div>
    </div>
  );
}
