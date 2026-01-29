"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState, useTransition } from "react";
import { deleteWatchlist } from "@/lib/api";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  items: WatchlistSummary[];
};

export function WatchlistList({ items }: Props) {
  const router = useRouter();
  const [watchlists, setWatchlists] = useState(items);
  const [isPending, startTransition] = useTransition();
  const [pendingDelete, setPendingDelete] = useState<WatchlistSummary | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setWatchlists(items);
    if (pendingDelete && !items.some((watchlist) => watchlist.id === pendingDelete.id)) {
      setPendingDelete(null);
    }
  }, [items, pendingDelete]);

  const handleDeleteRequest = (watchlist: WatchlistSummary) => {
    setError(null);
    setPendingDelete(watchlist);
  };

  const handleDeleteConfirm = () => {
    if (!pendingDelete) return;

    startTransition(async () => {
      try {
        await deleteWatchlist(pendingDelete.id);
        setWatchlists((current) => current.filter((watchlist) => watchlist.id !== pendingDelete.id));
        setError(null);
        setPendingDelete(null);
        router.refresh();
      } catch (err) {
        console.error(err);
        setError(err instanceof Error ? err.message : "Unable to delete watchlist.");
      }
    });
  };

  if (watchlists.length === 0) {
    return (
      <div className="space-y-2">
        {error ? <p className="text-sm text-rose-300">{error}</p> : null}
        <p className="text-sm text-slate-400">No watchlists yet. Create one to start tracking tickers.</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {error ? <p className="text-sm text-rose-300">{error}</p> : null}
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
            onClick={() => handleDeleteRequest(watchlist)}
            disabled={isPending}
            className="text-slate-500 opacity-0 transition-opacity group-hover:opacity-100 hover:text-rose-400"
          >
            ✕
          </button>
        </div>
      ))}
      {pendingDelete ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="delete-watchlist-title"
        >
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-6 text-slate-100 shadow-xl">
            <h2 id="delete-watchlist-title" className="text-lg font-semibold">
              Delete watchlist?
            </h2>
            <p className="mt-2 text-sm text-slate-300">
              This will permanently remove <span className="font-medium text-white">{pendingDelete.name}</span> and all
              of its tickers.
            </p>
            {error ? <p className="mt-3 text-sm text-rose-300">{error}</p> : null}
            <div className="mt-6 flex flex-wrap justify-end gap-3">
              <button
                type="button"
                className="rounded-full border border-white/10 px-4 py-2 text-sm text-slate-200 hover:border-white/30"
                onClick={() => {
                  setPendingDelete(null);
                  setError(null);
                }}
                disabled={isPending}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-full bg-rose-500 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-400 disabled:opacity-60"
                onClick={handleDeleteConfirm}
                disabled={isPending}
              >
                {isPending ? "Deleting..." : "Delete watchlist"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
