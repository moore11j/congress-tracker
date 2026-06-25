import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { WatchlistDetailClient } from "@/components/watchlists/WatchlistDetailClient";
import { WatchlistDetailContent } from "@/components/watchlists/WatchlistDetailContent";
import { getWatchlist, getWatchlistConfirmationEvents, getWatchlistEvents, getWatchlistSignals, type EventItem, type SignalItem } from "@/lib/api";
import { buildReturnTo, requirePageAuth } from "@/lib/serverAuth";
import { eventToFeedItem, getParam, parseMode, resolveWatchlistEventSince, signalToFeedItem, type WatchlistActivityState } from "@/lib/watchlistActivity";

type Props = {
  params: Promise<{ id: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export const dynamic = "force-dynamic";

export default async function WatchlistDetailPage({ params, searchParams }: Props) {
  const { id } = await params;
  const watchlistId = Number(id);
  const sp = (await searchParams) ?? {};
  const returnTo = buildReturnTo(`/watchlists/${id}`, sp);
  const authToken = await requirePageAuth(returnTo);

  const mode = parseMode(getParam(sp, "mode"));
  const recentDays = getParam(sp, "recent_days") || "30";
  const cursor = getParam(sp, "cursor");
  const offset = Number(getParam(sp, "offset") || "0");
  const limit = getParam(sp, "limit") || "25";
  const numericLimit = Math.min(Math.max(Number(limit) || 25, 1), 100);
  const initialState: WatchlistActivityState = {
    mode,
    recentDays,
    limit: numericLimit,
    onlyNew: getParam(sp, "only_new") === "1" && mode !== "signals",
    newSince: getParam(sp, "new_since") || "",
  };

  if (!authToken) {
    return (
      <VerifiedSessionGuard returnTo={returnTo}>
        <WatchlistDetailClient watchlistId={watchlistId} initialState={initialState} initialAuthPending />
      </VerifiedSessionGuard>
    );
  }

  const watchlist = await getWatchlist(watchlistId, authToken).catch(() => null);
  if (!watchlist) {
    return (
      <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
        <WatchlistDetailClient watchlistId={watchlistId} initialState={initialState} />
      </VerifiedSessionGuard>
    );
  }

  const confirmationEventsResponse = await getWatchlistConfirmationEvents(watchlistId, { limit: 5, authToken }).catch(() => ({ items: [] }));
  const confirmationEvents = confirmationEventsResponse.items ?? [];
  const hydratedState = initialState.onlyNew
    ? { ...initialState, newSince: initialState.newSince || watchlist.unseen_since || "" }
    : initialState;
  const activity =
    mode === "signals"
      ? await getWatchlistSignals(watchlistId, {
          mode: "all",
          sort: "smart",
          limit: numericLimit,
          offset: Number.isFinite(offset) ? offset : 0,
          authToken,
        })
      : hydratedState.onlyNew && !hydratedState.newSince
      ? { items: [], next_cursor: null }
      : await getWatchlistEvents(watchlistId, {
          mode,
          recent_days: Number(recentDays),
          since: resolveWatchlistEventSince(hydratedState),
          unread_only: hydratedState.onlyNew ? 1 : undefined,
          cursor: cursor || undefined,
          limit: numericLimit,
          authToken,
          source: "WatchlistPage",
        });

  const items =
    mode === "signals"
      ? (activity.items as SignalItem[]).map(signalToFeedItem)
      : (activity.items as EventItem[]).map(eventToFeedItem);

  return (
    <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
      <WatchlistDetailContent
        watchlist={watchlist}
        confirmationEvents={confirmationEvents}
        initialState={hydratedState}
        initialData={{
          items,
          nextCursor: "next_cursor" in activity ? activity.next_cursor ?? null : null,
          offset: mode === "signals" ? (Number.isFinite(offset) ? offset : 0) + items.length : 0,
          hasMore: mode === "signals" ? items.length === numericLimit : Boolean("next_cursor" in activity && activity.next_cursor),
        }}
      />
    </VerifiedSessionGuard>
  );
}
