"use client";

import { useState } from "react";
import { WatchlistCreateForm } from "@/components/watchlists/WatchlistCreateForm";
import { WatchlistList } from "@/components/watchlists/WatchlistList";
import { listWatchlists } from "@/lib/api";
import { cardClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  initialWatchlists: WatchlistSummary[];
};

export function WatchlistsDashboard({ initialWatchlists }: Props) {
  const [watchlists, setWatchlists] = useState(initialWatchlists);

  const refreshWatchlists = async () => {
    const next = await listWatchlists();
    setWatchlists(next);
  };

  return (
    <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
      <WatchlistCreateForm onCreated={refreshWatchlists} />
      <div className={cardClassName}>
        <h2 className="text-lg font-semibold text-white">Existing watchlists</h2>
        <div className="mt-4">
          <WatchlistList items={watchlists} />
        </div>
      </div>
    </div>
  );
}
