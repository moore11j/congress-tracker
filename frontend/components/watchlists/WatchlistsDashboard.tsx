"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { WatchlistCreateForm } from "@/components/watchlists/WatchlistCreateForm";
import { WatchlistList } from "@/components/watchlists/WatchlistList";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { addToWatchlist, getEntitlements, hasClientAuthHint, listWatchlists } from "@/lib/api";
import { defaultEntitlements, type Entitlements } from "@/lib/entitlements";
import { cardClassName, ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";
import { normalizeTickerSymbol } from "@/lib/ticker";
import { nextDefaultWatchlistName } from "@/lib/watchlistNames";

type Props = {
  initialWatchlists: WatchlistSummary[];
  initialAuthPending?: boolean;
};

type PendingTickerIntent = {
  symbol: string;
  returnTo: string | null;
};

const pendingTickerIntentMaxAgeMs = 15 * 60 * 1000;
const pendingWatchlistToastKey = "watchlist:create-toast";

function safeInternalReturnTo(value: string | null) {
  if (!value || !value.startsWith("/") || value.startsWith("//")) return null;
  return value;
}

function pendingTickerIntentFromSearchParams(searchParams: URLSearchParams): PendingTickerIntent | null {
  if (searchParams.get("create") !== "1" || searchParams.get("intent") !== "addTicker") return null;
  const symbol = normalizeTickerSymbol(searchParams.get("symbol"));
  if (!symbol) return null;

  const createdAt = Number(searchParams.get("createdAt") ?? 0);
  if (Number.isFinite(createdAt) && createdAt > 0 && Date.now() - createdAt > pendingTickerIntentMaxAgeMs) {
    return null;
  }

  return {
    symbol,
    returnTo: safeInternalReturnTo(searchParams.get("returnTo")),
  };
}

function rememberWatchlistToast(message: string) {
  try {
    window.sessionStorage.setItem(pendingWatchlistToastKey, message);
  } catch {
    // Ignore private browsing or storage denial; the detail page still loads.
  }
}

function WatchlistsSkeleton() {
  return (
    <div className="space-y-5" aria-busy="true" aria-live="polite">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <SkeletonBlock className="h-10 w-40 rounded-2xl" />
        <SkeletonBlock className="h-10 w-32 rounded-2xl" />
      </div>
      <div className={cardClassName}>
        <SkeletonBlock className="h-5 w-44" />
        <SkeletonBlock className="mt-3 h-4 w-full max-w-md" />
        <div className="mt-4 space-y-3">
          {Array.from({ length: 3 }).map((_, index) => (
            <div key={index} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
              <SkeletonBlock className="h-4 w-36" />
              <SkeletonBlock className="mt-3 h-3 w-56" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function WatchlistsDashboard({ initialWatchlists, initialAuthPending = false }: Props) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [watchlists, setWatchlists] = useState(initialWatchlists);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoading, setEntitlementsLoading] = useState(initialAuthPending);
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const searchParamsString = searchParams.toString();
  const pendingTickerIntent = useMemo(
    () => pendingTickerIntentFromSearchParams(new URLSearchParams(searchParamsString)),
    [searchParamsString],
  );
  const defaultName = useMemo(() => nextDefaultWatchlistName(watchlists), [watchlists]);

  useEffect(() => {
    let cancelled = false;
    const likelyAuthenticated = initialAuthPending || hasClientAuthHint();
    setEntitlementsLoading(likelyAuthenticated);
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      })
      .finally(() => {
        if (!cancelled) setEntitlementsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [initialAuthPending]);

  useEffect(() => {
    let cancelled = false;
    listWatchlists()
      .then((next) => {
        if (!cancelled) setWatchlists(next);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (pendingTickerIntent) {
      setIsCreateOpen(true);
    }
  }, [pendingTickerIntent]);

  const refreshWatchlists = async () => {
    const next = await listWatchlists();
    setWatchlists(next);
  };

  const handleCreated = async (created: WatchlistSummary) => {
    if (!pendingTickerIntent) {
      await refreshWatchlists();
      setIsCreateOpen(false);
      return;
    }

    try {
      await addToWatchlist(created.id, pendingTickerIntent.symbol);
    } catch {
      rememberWatchlistToast(`Watchlist created, but we couldn't add ${pendingTickerIntent.symbol}. Please try again.`);
    }

    setIsCreateOpen(false);
    router.push(`/watchlists/${created.id}`);
  };

  const cancelPendingIntent = () => {
    router.push(pendingTickerIntent?.returnTo ?? "/watchlists");
  };

  const closeCreateModal = () => {
    setIsCreateOpen(false);
    if (pendingTickerIntent) {
      cancelPendingIntent();
    }
  };

  if (entitlementsLoading) {
    return <WatchlistsSkeleton />;
  }

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <button
          type="button"
          className={subtlePrimaryButtonClassName}
          onClick={() => setIsCreateOpen(true)}
        >
          Create watchlist
        </button>
        <Link href="/?mode=all" className={ghostButtonClassName}>
          Back to feed
        </Link>
      </div>
      <WatchlistCreateForm
        open={isCreateOpen}
        onClose={closeCreateModal}
        onCreated={handleCreated}
        watchlistCount={watchlists.length}
        entitlements={entitlements}
        defaultName={defaultName}
        pendingTickerSymbol={pendingTickerIntent?.symbol}
      />
      <div className={cardClassName}>
        <h2 className="text-lg font-semibold text-white">Your watchlists</h2>
        <p className="mt-1 text-sm text-slate-400">
          Open a list to see recent activity across its tickers.
        </p>
        <div className="mt-4">
          <WatchlistList items={watchlists} />
        </div>
      </div>
    </div>
  );
}
