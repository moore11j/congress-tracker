"use client";

import { useEffect, useState } from "react";
import { WatchlistCreateForm } from "@/components/watchlists/WatchlistCreateForm";
import { WatchlistList } from "@/components/watchlists/WatchlistList";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { getEntitlements, hasClientAuthHint, listWatchlists } from "@/lib/api";
import { defaultEntitlements, type Entitlements } from "@/lib/entitlements";
import { cardClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  initialWatchlists: WatchlistSummary[];
  initialAuthPending?: boolean;
};

function WatchlistsSkeleton() {
  return (
    <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]" aria-busy="true" aria-live="polite">
      <div className="rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card">
        <SkeletonBlock className="h-5 w-40" />
        <SkeletonBlock className="mt-3 h-4 w-full max-w-sm" />
        <SkeletonBlock className="mt-5 h-11 w-full" />
        <SkeletonBlock className="mt-3 h-10 w-36" />
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
  const [watchlists, setWatchlists] = useState(initialWatchlists);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoading, setEntitlementsLoading] = useState(initialAuthPending);

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

  const refreshWatchlists = async () => {
    const next = await listWatchlists();
    setWatchlists(next);
  };

  if (entitlementsLoading) {
    return <WatchlistsSkeleton />;
  }

  return (
    <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
      <WatchlistCreateForm
        onCreated={refreshWatchlists}
        watchlistCount={watchlists.length}
        entitlements={entitlements}
      />
      <div className={cardClassName}>
        <h2 className="text-lg font-semibold text-white">Existing watchlists</h2>
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
