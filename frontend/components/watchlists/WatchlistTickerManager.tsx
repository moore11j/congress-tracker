"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { addToWatchlist, removeFromWatchlist } from "@/lib/api";
import { ghostButtonClassName, inputClassName, primaryButtonClassName } from "@/lib/styles";

type Ticker = { symbol: string; name: string };

export function WatchlistTickerManager({ watchlistId, tickers }: { watchlistId: number; tickers: Ticker[] }) {
  const [symbol, setSymbol] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const router = useRouter();

  const handleAdd = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = symbol.trim().toUpperCase();
    if (!trimmed) {
      setError("Enter a ticker symbol.");
      return;
    }

    setError(null);
    startTransition(async () => {
      try {
        await addToWatchlist(watchlistId, trimmed);
        setSymbol("");
        router.refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to add ticker.");
      }
    });
  };

  const handleRemove = (tickerSymbol: string) => {
    setError(null);
    startTransition(async () => {
      try {
        await removeFromWatchlist(watchlistId, tickerSymbol);
        router.refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to remove ticker.");
      }
    });
  };

  return (
    <div className="rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card">
      <div className="flex flex-col gap-3 border-b border-white/10 pb-4">
        <h2 className="text-lg font-semibold text-white">Tickers in this watchlist</h2>
        <p className="text-sm text-slate-400">Add or remove symbols to tailor this list.</p>
        <form onSubmit={handleAdd} className="flex flex-col gap-3 sm:flex-row sm:items-center">
          <input
            value={symbol}
            onChange={(event) => setSymbol(event.target.value)}
            placeholder="Add ticker symbol"
            className={inputClassName}
          />
          <button type="submit" className={primaryButtonClassName} disabled={isPending}>
            {isPending ? "Updating..." : "Add"}
          </button>
        </form>
        {error ? <p className="text-sm text-rose-300">{error}</p> : null}
      </div>
      <div className="mt-4 flex flex-col gap-3">
        {tickers.length === 0 ? (
          <p className="text-sm text-slate-400">No tickers yet. Add one to start tracking trades.</p>
        ) : (
          tickers.map((ticker) => (
            <div key={ticker.symbol} className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
              <div>
                <Link href={`/ticker/${ticker.symbol}`} className="text-sm font-semibold text-emerald-200 hover:text-emerald-100">
                  {ticker.symbol}
                </Link>
                <div className="text-xs text-slate-400">{ticker.name}</div>
              </div>
              <button
                type="button"
                className={ghostButtonClassName}
                onClick={() => handleRemove(ticker.symbol)}
                disabled={isPending}
              >
                Remove
              </button>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
