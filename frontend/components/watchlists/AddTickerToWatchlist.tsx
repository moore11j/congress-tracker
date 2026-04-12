"use client";

import Link from "next/link";
import { useEffect, useMemo, useState, useTransition } from "react";
import { addToWatchlist, listWatchlists } from "@/lib/api";
import type { WatchlistSummary } from "@/lib/types";
import { ghostButtonClassName, primaryButtonClassName, selectClassName } from "@/lib/styles";

export function AddTickerToWatchlist({ symbol }: { symbol: string }) {
  const [watchlists, setWatchlists] = useState<WatchlistSummary[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [status, setStatus] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    let cancelled = false;
    listWatchlists()
      .then((items) => {
        if (cancelled) return;
        setWatchlists(items);
        setSelectedId((current) => current || (items[0] ? String(items[0].id) : ""));
      })
      .catch(() => {
        if (!cancelled) setStatus("Unable to load watchlists.");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const selectedName = useMemo(
    () => watchlists.find((watchlist) => String(watchlist.id) === selectedId)?.name,
    [selectedId, watchlists],
  );

  const handleAdd = () => {
    const id = Number(selectedId);
    if (!Number.isFinite(id)) return;

    setStatus(null);
    startTransition(async () => {
      try {
        await addToWatchlist(id, symbol);
        setStatus(`${symbol} added to ${selectedName ?? "watchlist"}.`);
      } catch (err) {
        const message = err instanceof Error ? err.message : "";
        if (message.includes("Ticker not found") || message.includes("HTTP 404")) {
          setStatus("We couldn't find that ticker. Check the symbol and try again.");
        } else {
          setStatus("Unable to add ticker right now.");
        }
      }
    });
  };

  if (watchlists.length === 0) {
    return (
      <Link href="/watchlists" prefetch={false} className={ghostButtonClassName}>
        Create watchlist
      </Link>
    );
  }

  return (
    <div className="flex flex-wrap items-center justify-end gap-2">
      <select
        value={selectedId}
        onChange={(event) => setSelectedId(event.target.value)}
        className={`${selectClassName} w-auto min-w-[160px] rounded-lg py-1.5`}
        aria-label="Choose watchlist"
      >
        {watchlists.map((watchlist) => (
          <option key={watchlist.id} value={watchlist.id}>
            {watchlist.name}
          </option>
        ))}
      </select>
      <button type="button" onClick={handleAdd} disabled={isPending || !selectedId} className={`${primaryButtonClassName} rounded-lg py-1.5`}>
        {isPending ? "Adding..." : "Add ticker"}
      </button>
      {status ? <p className="basis-full text-right text-xs text-slate-400">{status}</p> : null}
    </div>
  );
}
