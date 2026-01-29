"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import { createWatchlist } from "@/lib/api";
import { inputClassName, primaryButtonClassName } from "@/lib/styles";

export function WatchlistCreateForm() {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const router = useRouter();

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
        setName("");
        router.replace("/watchlists");
        router.refresh();
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
      <button type="submit" className={primaryButtonClassName} disabled={isPending}>
        {isPending ? "Creating..." : "Create watchlist"}
      </button>
    </form>
  );
}
