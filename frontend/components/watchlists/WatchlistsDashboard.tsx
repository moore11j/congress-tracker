"use client";

import { useState } from "react";
import { WatchlistCreateForm } from "@/components/watchlists/WatchlistCreateForm";
import { WatchlistList } from "@/components/watchlists/WatchlistList";
import { cardClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  initialWatchlists: WatchlistSummary[];
};

export function WatchlistsDashboard({ initialWatchlists }: Props) {
  const [watchlists, setWatchlists] = useState(initialWatchlists);

  return (
    <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
      <WatchlistCreateForm
        onCreated={(watchlist) => {
          setWatchlists((current) => {
            if (current.some((item) => item.id === watchlist.id)) return current;
            return [...current, watchlist];
          });
        }}
      />
      <div className={cardClassName}>
        <h2 className="text-lg font-semibold text-white">Existing watchlists</h2>
        <div className="mt-4">
          <WatchlistList items={watchlists} />
        </div>
      </div>
    </div>
  );
}
