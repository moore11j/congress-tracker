"use client";

import Link from "next/link";
import { useEffect, useState, useTransition } from "react";
import { deleteWatchlist, listWatchlists, renameWatchlist } from "@/lib/api";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  items: WatchlistSummary[];
};

export function WatchlistList({ items }: Props) {
  const [watchlists, setWatchlists] = useState(items);
  const [isPending, startTransition] = useTransition();
  const [pendingDelete, setPendingDelete] = useState<WatchlistSummary | null>(null);
  const [renameTarget, setRenameTarget] = useState<WatchlistSummary | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setWatchlists(items);
  }, [items]);

  const refreshWatchlists = async () => {
    const next = await listWatchlists();
    setWatchlists(next);
  };

  const handleDeleteConfirm = () => {
    if (!pendingDelete) return;

    startTransition(async () => {
      try {
        await deleteWatchlist(pendingDelete.id);
        await refreshWatchlists();
        setError(null);
        setPendingDelete(null);
      } catch (err) {
        console.error(err);
        setError(err instanceof Error ? err.message : "Unable to delete watchlist.");
      }
    });
  };

  const handleRename = () => {
    if (!renameTarget) return;
    const trimmed = renameValue.trim();
    if (!trimmed) {
      setError("Enter a watchlist name.");
      return;
    }

    startTransition(async () => {
      try {
        await renameWatchlist(renameTarget.id, trimmed);
        await refreshWatchlists();
        setRenameTarget(null);
        setRenameValue("");
        setError(null);
      } catch (err) {
        console.error(err);
        setError(err instanceof Error ? err.message : "Unable to rename watchlist.");
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
          <div className="flex items-center gap-2">
            <button
              type="button"
              aria-label={`Rename watchlist ${watchlist.name}`}
              onClick={() => {
                setRenameTarget(watchlist);
                setRenameValue(watchlist.name);
                setError(null);
              }}
              disabled={isPending}
              className="text-slate-500 opacity-0 transition-opacity group-hover:opacity-100 hover:text-emerald-300"
            >
              Rename
            </button>
            <button
              type="button"
              aria-label={`Delete watchlist ${watchlist.name}`}
              onClick={() => {
                setError(null);
                setPendingDelete(watchlist);
              }}
              disabled={isPending}
              className="text-slate-500 opacity-0 transition-opacity group-hover:opacity-100 hover:text-rose-400"
            >
              ✕
            </button>
          </div>
        </div>
      ))}
      {renameTarget ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true">
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-6 text-slate-100 shadow-xl">
            <h2 className="text-lg font-semibold">Rename watchlist</h2>
            <input
              value={renameValue}
              onChange={(event) => setRenameValue(event.target.value)}
              className="mt-3 w-full rounded-full border border-white/10 bg-slate-950 px-4 py-2 text-sm text-slate-100"
            />
            <div className="mt-6 flex flex-wrap justify-end gap-3">
              <button
                type="button"
                className="rounded-full border border-white/10 px-4 py-2 text-sm text-slate-200 hover:border-white/30"
                onClick={() => {
                  setRenameTarget(null);
                  setRenameValue("");
                  setError(null);
                }}
                disabled={isPending}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-full bg-emerald-500 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-400 disabled:opacity-60"
                onClick={handleRename}
                disabled={isPending}
              >
                {isPending ? "Renaming..." : "Rename watchlist"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
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
