"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { addToWatchlist, removeFromWatchlist } from "@/lib/api";
import { WatchlistTickerAutocomplete } from "@/components/watchlists/WatchlistTickerAutocomplete";
import { ghostButtonClassName, primaryButtonClassName, tickerLinkClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";

type Ticker = { symbol: string; name: string };

export function WatchlistTickerManager({ watchlistId, tickers }: { watchlistId: number; tickers: Ticker[] }) {
  const [symbol, setSymbol] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const router = useRouter();

  const cleanAddError = (err: unknown) => {
    const message = err instanceof Error ? err.message : "";
    if (message.includes("Ticker not found") || message.includes("HTTP 404")) {
      return "We couldn't find that ticker. Check the symbol and try again.";
    }
    if (message.includes("HTTP 422")) {
      return "Enter a valid ticker symbol.";
    }
    return "Unable to add ticker right now.";
  };

  const addSymbol = (rawSymbol: string) => {
    const trimmed = rawSymbol.trim().toUpperCase();
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
        setError(cleanAddError(err));
      }
    });
  };

  const handleAdd = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    addSymbol(symbol);
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
        <p className="text-sm text-slate-400">Add symbols to shape the monitoring feed for this research theme.</p>
        <form onSubmit={handleAdd} className="flex flex-col gap-3 sm:flex-row sm:items-center">
          <WatchlistTickerAutocomplete
            value={symbol}
            onChange={setSymbol}
            onSelect={addSymbol}
            disabled={isPending}
          />
          <button type="submit" className={primaryButtonClassName} disabled={isPending}>
            {isPending ? "Updating..." : "Add"}
          </button>
        </form>
        {error ? <p className="text-sm text-rose-300">{error}</p> : null}
      </div>
      <div className="mt-4 flex flex-col gap-3">
        {tickers.length === 0 ? (
          <p className="text-sm text-slate-400">No tickers yet. Add a symbol to start tracking filings, insider trades, and signals.</p>
        ) : (
          tickers.map((ticker) => (
            <div key={ticker.symbol} className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
              <div>
                {tickerHref(ticker.symbol) ? (
                  <Link href={tickerHref(ticker.symbol)!} prefetch={false} className={tickerLinkClassName}>
                    {ticker.symbol}
                  </Link>
                ) : (
                  <span className="text-sm font-semibold text-slate-200">{ticker.symbol}</span>
                )}
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
