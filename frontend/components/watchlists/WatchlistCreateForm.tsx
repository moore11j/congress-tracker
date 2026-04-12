"use client";

import { useState, useTransition } from "react";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { createWatchlist } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, limitFor, type Entitlements } from "@/lib/entitlements";
import { inputClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

type Props = {
  onCreated?: () => Promise<void> | void;
  watchlistCount: number;
  entitlements?: Entitlements;
};

export function WatchlistCreateForm({ onCreated, watchlistCount, entitlements = defaultEntitlements }: Props) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

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
      setError(`Free accounts can keep ${limit} watchlists. Upgrade to create more.`);
      return;
    }

    setError(null);
    startTransition(async () => {
      try {
        await createWatchlist(trimmed);
        await onCreated?.();
        setName("");
      } catch (err) {
        const message = err instanceof Error ? err.message : "";
        setError(
          message.includes("premium_required") || message.includes("Free accounts")
            ? `Free accounts can keep ${limitFor(entitlements, "watchlists")} watchlists. Upgrade to create more.`
            : message || "Unable to create watchlist.",
        );
      }
    });
  };

  return (
    <form onSubmit={handleSubmit} className="flex flex-col gap-3 rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card">
      <div>
        <h2 className="text-lg font-semibold text-white">Create a watchlist</h2>
        <p className="text-sm text-slate-400">Organize tickers you want to monitor closely.</p>
      </div>
      <input
        value={name}
        onChange={(event) => setName(event.target.value)}
        placeholder="e.g. Election Cycle Momentum"
        className={inputClassName}
      />
      {error ? <p className="text-sm text-rose-300">{error}</p> : null}
      {!hasEntitlement(entitlements, "watchlists") || watchlistCount >= limitFor(entitlements, "watchlists") ? (
        <UpgradePrompt
          title="More watchlists are a Premium workflow"
          body={
            hasEntitlement(entitlements, "watchlists")
              ? `Free includes ${limitFor(entitlements, "watchlists")} watchlists so the core monitoring flow stays useful.`
              : "Watchlist creation is currently a Premium feature."
          }
          compact={true}
        />
      ) : null}
      <button
        type="submit"
        className={subtlePrimaryButtonClassName}
        disabled={isPending}
      >
        {isPending ? "Creating..." : "Create watchlist"}
      </button>
    </form>
  );
}
