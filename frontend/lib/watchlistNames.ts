import type { WatchlistSummary } from "@/lib/types";

const defaultWatchlistNamePattern = /^watchlist\s+(\d+)$/i;

export function nextDefaultWatchlistName(watchlists: Pick<WatchlistSummary, "name">[]) {
  const usedNumbers = new Set<number>();
  for (const watchlist of watchlists) {
    const match = watchlist.name.trim().match(defaultWatchlistNamePattern);
    if (!match) continue;
    const value = Number.parseInt(match[1], 10);
    if (Number.isInteger(value) && value > 0) usedNumbers.add(value);
  }

  let nextNumber = 1;
  while (usedNumbers.has(nextNumber)) nextNumber += 1;
  return `Watchlist ${nextNumber}`;
}
