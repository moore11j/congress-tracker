"use client";

import Link from "next/link";
import { useEffect, useState, useTransition } from "react";
import { WalnutConfirmDialog } from "@/components/ui/WalnutConfirmDialog";
import { deleteWatchlist, listWatchlists, renameWatchlist } from "@/lib/api";
import type { WatchlistSummary } from "@/lib/types";
import { compactInteractiveSurfaceClassName, compactInteractiveTitleClassName } from "@/lib/styles";

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

  useEffect(() => {
    const handleUnreadUpdated = () => {
      refreshWatchlists().catch(() => {});
    };
    window.addEventListener("ct:monitoring-unread-updated", handleUnreadUpdated);
    return () => window.removeEventListener("ct:monitoring-unread-updated", handleUnreadUpdated);
  }, []);

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
        <p className="text-sm text-slate-400">
          No watchlists yet. Create one for a theme, sector, or research idea, then add tickers to monitor new filings and signals.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {error ? <p className="text-sm text-rose-300">{error}</p> : null}
      {watchlists.map((watchlist) => {
        const unreadCount = Math.max(Number(watchlist.unread_count ?? watchlist.unseen_count) || 0, 0);
        return (
        <div
          key={watchlist.id}
          className={`${compactInteractiveSurfaceClassName} flex items-center justify-between gap-4 rounded-2xl px-4 py-3 text-sm`}
        >
          <Link href={`/watchlists/${watchlist.id}`} prefetch={false} className="flex-1">
            <div className="flex items-center gap-3">
              <span className="text-xs text-slate-500">#{watchlist.id}</span>
              <span className={`font-medium ${compactInteractiveTitleClassName}`}>{watchlist.name}</span>
              {unreadCount > 0 ? (
                <span className="rounded-lg border border-emerald-300/30 bg-emerald-300/15 px-2 py-0.5 text-xs font-semibold text-emerald-100">
                  {unreadCount} new
                </span>
              ) : null}
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
        );
      })}
      <WalnutConfirmDialog
        open={Boolean(renameTarget)}
        eyebrow="Rename watchlist"
        title="Rename watchlist"
        description="Update the saved name without changing any tickers or monitoring settings."
        confirmLabel={isPending ? "Renaming..." : "Rename watchlist"}
        tone="success"
        isBusy={isPending}
        onClose={() => {
          setRenameTarget(null);
          setRenameValue("");
          setError(null);
        }}
        onConfirm={handleRename}
      >
        <label className="block text-sm">
          <span className="block font-medium text-slate-200">Name</span>
          <input
            value={renameValue}
            onChange={(event) => setRenameValue(event.target.value)}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none transition focus:border-emerald-300/50"
          />
        </label>
        {error ? <p className="mt-3 text-sm text-rose-300">{error}</p> : null}
      </WalnutConfirmDialog>
      <WalnutConfirmDialog
        open={Boolean(pendingDelete)}
        eyebrow="Delete watchlist"
        title="Delete watchlist?"
        description={
          <>
            This will permanently remove <span className="font-medium text-white">{pendingDelete?.name}</span> and all
            of its tickers.
          </>
        }
        confirmLabel={isPending ? "Deleting..." : "Delete watchlist"}
        tone="danger"
        isBusy={isPending}
        onClose={() => {
          setPendingDelete(null);
          setError(null);
        }}
        onConfirm={handleDeleteConfirm}
      >
        {error ? <p className="text-sm text-rose-300">{error}</p> : null}
      </WalnutConfirmDialog>
    </div>
  );
}
