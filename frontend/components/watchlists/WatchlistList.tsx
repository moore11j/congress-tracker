"use client";

import Link from "next/link";
import { useState, useTransition } from "react";
import { deleteWatchlist } from "@/lib/api";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  items: WatchlistSummary[];
};

export function WatchlistList({ items }: Props) {
  const [watchlists, setWatchlists] = useState(items);
  const [isPending, startTransition] = useTransition();

  const handleDelete = (id: number) => {
    if (!window.confirm("Delete this watchlist? This cannot be undone.")) return;

    startTransition(async () => {
      try {
        await deleteWatchlist(id);
        setWatchlists((current) => current.filter((watchlist) => watchlist.id !== id));
      } catch (err) {
        console.error(err);
        window.alert(err instanceof Error ? err.message : "Unable to delete watchlist.");
      }
    });
  };

  if (watchlists.length === 0) {
    return <p className="text-sm text-slate-400">No watchlists yet. Create one to start tracking tickers.</p>;
  }

  return (
    <div className="space-y-3">
      {watchlists.map((watchlist) => (
        <div
          key={watchlist.id}
          className="group flex items-center justify-between gap-4 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-200 transition hover:border-emerald-400/40"
        >
          <Link href={`/watchlists/${watchlist.id}`} className="flex-1">
            <div className="flex items-center justify-between gap-3">
              <span className="font-medium text-slate-100">{watchlist.name}</span>
              <span className="text-xs text-slate-400">#{watchlist.id}</span>
            </div>
          </Link>
          <button
            type="button"
            aria-label={`Delete watchlist ${watchlist.name}`}
            onClick={() => handleDelete(watchlist.id)}
            disabled={isPending}
            className="text-slate-500 opacity-0 transition-opacity group-hover:opacity-100 hover:text-rose-400"
          >
            ✕
          </button>
        </div>
      ))}
    </div>
  );
}
