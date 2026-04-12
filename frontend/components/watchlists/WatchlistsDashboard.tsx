"use client";

import { useEffect, useState } from "react";
import { WatchlistCreateForm } from "@/components/watchlists/WatchlistCreateForm";
import { WatchlistList } from "@/components/watchlists/WatchlistList";
import { getEntitlements, listWatchlists } from "@/lib/api";
import { defaultEntitlements, type Entitlements } from "@/lib/entitlements";
import { cardClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  initialWatchlists: WatchlistSummary[];
};

export function WatchlistsDashboard({ initialWatchlists }: Props) {
  const [watchlists, setWatchlists] = useState(initialWatchlists);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);

  useEffect(() => {
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const refreshWatchlists = async () => {
    const next = await listWatchlists();
    setWatchlists(next);
  };

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
