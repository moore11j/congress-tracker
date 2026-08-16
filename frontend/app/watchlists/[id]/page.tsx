import nextDynamic from "next/dynamic";
import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { WatchlistDetailContent } from "@/components/watchlists/WatchlistDetailContent";
import { getWatchlist } from "@/lib/api";
import { buildReturnTo, requirePageAuth } from "@/lib/serverAuth";
import { getParam, parseMode, type WatchlistActivityState } from "@/lib/watchlistActivity";

const WatchlistDetailClient = nextDynamic(
  () => import("@/components/watchlists/WatchlistDetailClient").then((module) => module.WatchlistDetailClient),
);

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
  const offset = Number(getParam(sp, "offset") || "0");
  const limit = getParam(sp, "limit") || "20";
  const numericLimit = Math.min(Math.max(Number(limit) || 20, 1), 20);
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

  const hydratedState = initialState.onlyNew
    ? { ...initialState, newSince: initialState.newSince || watchlist.unseen_since || "" }
    : initialState;
  return (
    <VerifiedSessionGuard returnTo={returnTo} initiallyAuthorized={Boolean(authToken)}>
      <WatchlistDetailContent
        watchlist={watchlist}
        confirmationEvents={[]}
        initialState={hydratedState}
        canViewPremiumMetrics={false}
        initialData={{
          // Activity is hydrated in the client so a temporarily saturated API pool
          // never holds the entire watchlist page hostage.
          items: [],
          nextCursor: null,
          offset: mode === "signals" && Number.isFinite(offset) ? offset : 0,
          hasMore: false,
        }}
      />
    </VerifiedSessionGuard>
  );
}
