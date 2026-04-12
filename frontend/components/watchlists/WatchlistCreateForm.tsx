"use client";

import { useState, useTransition } from "react";
import { createWatchlist } from "@/lib/api";
import { inputClassName } from "@/lib/styles";

type Props = {
  onCreated?: () => Promise<void> | void;
};

export function WatchlistCreateForm({ onCreated }: Props) {
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

    setError(null);
    startTransition(async () => {
      try {
        await createWatchlist(trimmed);
        await onCreated?.();
        setName("");
      } catch (err) {
        setError(err instanceof Error ? err.message : "Unable to create watchlist.");
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
      <button
        type="submit"
        className="inline-flex h-10 items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20 disabled:opacity-60"
        disabled={isPending}
      >
        {isPending ? "Creating..." : "Create watchlist"}
      </button>
    </form>
  );
}
