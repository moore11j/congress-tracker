"use client";

import { useEffect, useRef, useState, useTransition } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { createWatchlist } from "@/lib/api";
import { formatInteger } from "@/lib/accountDisplay";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { inputClassName, subtlePrimaryButtonClassName } from "@/lib/styles";
import type { WatchlistSummary } from "@/lib/types";

type Props = {
  onCreated?: (created: WatchlistSummary) => Promise<void> | void;
  onCancelPendingIntent?: () => void;
  watchlistCount: number;
  entitlements?: Entitlements;
  defaultName: string;
  pendingTickerSymbol?: string | null;
};

export function WatchlistCreateForm({
  onCreated,
  onCancelPendingIntent,
  watchlistCount,
  entitlements = defaultEntitlements,
  defaultName,
  pendingTickerSymbol = null,
}: Props) {
  const [name, setName] = useState(defaultName);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const lastDefaultNameRef = useRef(defaultName);

  useEffect(() => {
    setName((current) => {
      const shouldUseNextDefault = !current.trim() || current === lastDefaultNameRef.current;
      lastDefaultNameRef.current = defaultName;
      return shouldUseNextDefault ? defaultName : current;
    });
  }, [defaultName]);

  const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) {
      setError("Enter a watchlist name.");
      return;
    }

    const limit = limitFor(entitlements, "watchlists");
    if (!hasEntitlement(entitlements, "watchlists")) {
      setError("Watchlist creation is currently a Premium feature.");
      return;
    }
    if (watchlistCount >= limit) {
      setError(`Free accounts can keep ${formatInteger(limit)} watchlists. Upgrade to create more.`);
      return;
    }

    setError(null);
    startTransition(async () => {
      try {
        const created = await createWatchlist(trimmed);
        await onCreated?.(created);
        setName(defaultName);
      } catch (err) {
        const message = err instanceof Error ? err.message : "";
        setError(
          message.includes("premium_required") || message.includes("Free accounts")
            ? `Free accounts can keep ${formatInteger(limitFor(entitlements, "watchlists"))} watchlists. Upgrade to create more.`
            : message || "Unable to create watchlist.",
        );
      }
    });
  };

  return (
    <form onSubmit={handleSubmit} className="flex flex-col gap-3 rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card">
      <div>
        <h2 className="text-lg font-semibold text-white">Name your watchlist</h2>
        <p className="text-sm text-slate-400">Organize tickers you want to monitor closely.</p>
      </div>
      <input
        value={name}
        onChange={(event) => setName(event.target.value)}
        placeholder="e.g. Election Cycle Momentum"
        className={inputClassName}
      />
      {pendingTickerSymbol ? (
        <p className="text-sm text-emerald-100">Creating this watchlist will add {pendingTickerSymbol}.</p>
      ) : null}
      {error ? <p className="text-sm text-rose-300">{error}</p> : null}
      {!hasEntitlement(entitlements, "watchlists") || watchlistCount >= limitFor(entitlements, "watchlists") ? (
        <UpgradePrompt
          title="More watchlists are a Premium workflow"
          body={
            hasEntitlement(entitlements, "watchlists")
              ? `Free includes ${formatInteger(limitFor(entitlements, "watchlists"))} watchlists so the core monitoring flow stays useful.`
              : "Watchlist creation is currently a Premium feature."
          }
          compact={true}
        />
      ) : null}
      <div className="flex flex-wrap gap-3">
        <button
          type="submit"
          className={subtlePrimaryButtonClassName}
          disabled={isPending}
        >
          {isPending ? "Creating..." : "Create watchlist"}
        </button>
        {pendingTickerSymbol && onCancelPendingIntent ? (
          <button
            type="button"
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
            disabled={isPending}
            onClick={onCancelPendingIntent}
          >
            Cancel
          </button>
        ) : null}
      </div>
    </form>
  );
}
