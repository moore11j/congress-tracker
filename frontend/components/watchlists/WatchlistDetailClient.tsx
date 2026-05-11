"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  ApiError,
  getWatchlist,
  getWatchlistConfirmationEvents,
  getWatchlistEvents,
  getWatchlistSignals,
  hasClientAuthHint,
  type EventItem,
  type SignalItem,
} from "@/lib/api";
import { cardClassName, ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import type { ConfirmationMonitoringEvent, FeedItem, WatchlistDetail } from "@/lib/types";
import {
  eventToFeedItem,
  recentDaysToSince,
  signalToFeedItem,
  type WatchlistActivityState,
} from "@/lib/watchlistActivity";
import { WatchlistDetailContent } from "@/components/watchlists/WatchlistDetailContent";

type RecentActivityData = {
  items: FeedItem[];
  nextCursor: string | null;
  offset: number;
  hasMore: boolean;
};

type LoadState =
  | { status: "loading" }
  | {
      status: "ready";
      watchlist: WatchlistDetail;
      confirmationEvents: ConfirmationMonitoringEvent[];
      initialState: WatchlistActivityState;
      initialData: RecentActivityData;
    }
  | { status: "error"; code: number | null; message: string };

function statusCopy(code: number | null, watchlistId: number) {
  if (code === 401) {
    return {
      title: "Sign in to open this watchlist",
      body: "Your browser has a saved sign-in hint, but this session needs to be refreshed before loading the watchlist.",
      action: "Sign in",
      href: `/login?return_to=${encodeURIComponent(`/watchlists/${watchlistId}`)}`,
    };
  }
  if (code === 403) {
    return {
      title: "Access denied",
      body: "This watchlist belongs to another account or is no longer available to your session.",
      action: "Back to watchlists",
      href: "/watchlists",
    };
  }
  if (code === 404) {
    return {
      title: "Watchlist not found",
      body: "This watchlist may have been deleted or moved.",
      action: "Back to watchlists",
      href: "/watchlists",
    };
  }
  return {
    title: "Unable to load watchlist",
    body: "The page could not load this watchlist right now.",
    action: "Back to watchlists",
    href: "/watchlists",
  };
}

function DetailLoadingShell() {
  return (
    <div className="space-y-6" aria-busy="true">
      <div className="grid w-full min-w-0 items-center gap-6 lg:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <div className="min-w-0 space-y-3">
          <div className="h-3 w-24 rounded-full bg-emerald-300/20" />
          <div className="h-9 w-64 max-w-full rounded-lg bg-white/10" />
          <div className="h-4 w-80 max-w-full rounded-lg bg-white/5" />
        </div>
      </div>
      <div className="grid w-full min-w-0 gap-6 lg:grid-cols-[minmax(280px,360px)_minmax(0,1fr)]">
        <div className="h-64 rounded-3xl border border-white/10 bg-slate-900/70" />
        <div className={`${cardClassName} min-h-96`} />
      </div>
    </div>
  );
}

function DetailErrorState({ code, message, watchlistId }: { code: number | null; message: string; watchlistId: number }) {
  const copy = statusCopy(code, watchlistId);
  return (
    <div className={`${cardClassName} max-w-2xl space-y-4`}>
      <div>
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlist</p>
        <h1 className="mt-2 text-3xl font-semibold text-white">{copy.title}</h1>
        <p className="mt-2 text-sm text-slate-400">{copy.body}</p>
        {code === null ? <p className="mt-2 text-xs text-slate-500">{message}</p> : null}
      </div>
      <div className="flex flex-wrap gap-3">
        <Link href={copy.href} className={subtlePrimaryButtonClassName} prefetch={false}>
          {copy.action}
        </Link>
        <Link href="/watchlists" className={ghostButtonClassName} prefetch={false}>
          Watchlists
        </Link>
      </div>
    </div>
  );
}

export function WatchlistDetailClient({
  watchlistId,
  initialState,
  initialAuthPending = false,
}: {
  watchlistId: number;
  initialState: WatchlistActivityState;
  initialAuthPending?: boolean;
}) {
  const [state, setState] = useState<LoadState>({ status: "loading" });

  useEffect(() => {
    let cancelled = false;
    const likelyAuthenticated = initialAuthPending || hasClientAuthHint();

    async function load() {
      if (likelyAuthenticated) setState({ status: "loading" });
      try {
        const watchlist = await getWatchlist(watchlistId);
        const hydratedState = initialState.onlyNew
          ? { ...initialState, newSince: initialState.newSince || watchlist.unseen_since || "" }
          : initialState;
        const [confirmationEventsResponse, activity] = await Promise.all([
          getWatchlistConfirmationEvents(watchlistId, { limit: 5 }),
          hydratedState.mode === "signals"
            ? getWatchlistSignals(watchlistId, {
                mode: "all",
                sort: "smart",
                limit: hydratedState.limit,
                offset: 0,
              })
            : hydratedState.onlyNew && !hydratedState.newSince
              ? Promise.resolve({ items: [], next_cursor: null })
              : getWatchlistEvents(watchlistId, {
                mode: hydratedState.mode,
                since: hydratedState.onlyNew ? undefined : recentDaysToSince(hydratedState.recentDays),
                unread_only: hydratedState.onlyNew ? 1 : undefined,
                limit: hydratedState.limit,
              }),
        ]);
        const items =
          hydratedState.mode === "signals"
            ? (activity.items as SignalItem[]).map(signalToFeedItem)
            : (activity.items as EventItem[]).map(eventToFeedItem);

        if (!cancelled) {
          setState({
            status: "ready",
            watchlist,
            confirmationEvents: confirmationEventsResponse.items ?? [],
            initialState: hydratedState,
            initialData: {
              items,
              nextCursor: "next_cursor" in activity ? activity.next_cursor ?? null : null,
              offset: hydratedState.mode === "signals" ? items.length : 0,
              hasMore: hydratedState.mode === "signals" ? items.length === hydratedState.limit : Boolean("next_cursor" in activity && activity.next_cursor),
            },
          });
        }
      } catch (error) {
        if (cancelled) return;
        const code = error instanceof ApiError ? error.status : null;
        const message = error instanceof Error ? error.message : "Unable to load watchlist.";
        setState({ status: "error", code, message });
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, [initialAuthPending, initialState, watchlistId]);

  if (state.status === "loading") return <DetailLoadingShell />;
  if (state.status === "error") return <DetailErrorState code={state.code} message={state.message} watchlistId={watchlistId} />;
  return (
    <WatchlistDetailContent
      watchlist={state.watchlist}
      confirmationEvents={state.confirmationEvents}
      initialState={state.initialState}
      initialData={state.initialData}
    />
  );
}
