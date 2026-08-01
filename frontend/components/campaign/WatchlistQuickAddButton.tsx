"use client";

import { useState, useTransition } from "react";
import { addToWatchlist, createWatchlist, listWatchlists } from "@/lib/api";
import type { WatchlistSummary } from "@/lib/types";
import { trackCampaignEvent } from "@/components/campaign/CampaignAnalytics";
import type { CampaignProperties } from "@/lib/campaignAttribution";
import { normalizeTickerSymbol } from "@/lib/ticker";

type Props = {
  symbols: string[];
  label: string;
  eventName: string;
  properties?: CampaignProperties;
  className?: string;
};

function normalizedSymbolValue(symbol: string | null | undefined) {
  return normalizeTickerSymbol(symbol) ?? "";
}

function watchlistHasSymbol(watchlist: WatchlistSummary, symbol: string) {
  const normalized = normalizedSymbolValue(symbol);
  return (watchlist.symbols ?? []).some((item) => normalizedSymbolValue(item) === normalized);
}

function cleanError(err: unknown) {
  const message = err instanceof Error ? err.message : "";
  if (message.includes("premium_required") || message.includes("Free accounts") || message.includes("Free watchlists")) {
    return "This watchlist limit is active. Open Pricing to compare plans.";
  }
  if (message.includes("HTTP 401") || message.includes("HTTP 403")) return "Sign in again to update your watchlist.";
  if (message.includes("Ticker not found") || message.includes("HTTP 404")) return "One of these tickers could not be found.";
  return "Unable to update your watchlist right now.";
}

export function WatchlistQuickAddButton({ symbols, label, eventName, properties, className }: Props) {
  const [status, setStatus] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const normalizedSymbols = symbols.map(normalizedSymbolValue).filter(Boolean);

  const handleClick = () => {
    trackCampaignEvent(eventName, properties);
    if (!normalizedSymbols.length) {
      setStatus("No ticker available to add.");
      return;
    }
    setStatus(null);
    startTransition(async () => {
      try {
        const watchlists = await listWatchlists();
        const target = watchlists[0] ?? await createWatchlist("Research Watchlist");
        const toAdd = normalizedSymbols.filter((symbol) => !watchlistHasSymbol(target, symbol));
        for (const symbol of toAdd) {
          await addToWatchlist(target.id, symbol);
        }
        setStatus(
          toAdd.length === 0
            ? `${normalizedSymbols.join(" and ")} already saved in ${target.name}.`
            : `Saved ${normalizedSymbols.join(" and ")} to ${target.name}.`,
        );
      } catch (err) {
        setStatus(cleanError(err));
      }
    });
  };

  return (
    <div className="min-w-0">
      <button type="button" onClick={handleClick} disabled={isPending} className={className} aria-busy={isPending}>
        {isPending ? "Saving..." : label}
      </button>
      {status ? <p className="mt-2 text-xs leading-5 text-slate-400">{status}</p> : null}
    </div>
  );
}
