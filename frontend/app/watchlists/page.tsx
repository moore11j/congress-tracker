import Link from "next/link";
import { listWatchlists } from "@/lib/api";
import { cardClassName, ghostButtonClassName } from "@/lib/styles";
import { WatchlistCreateForm } from "@/components/watchlists/WatchlistCreateForm";

export default async function WatchlistsPage() {
  const watchlists = await listWatchlists();

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlists</p>
          <h1 className="text-3xl font-semibold text-white">Your curated tickers.</h1>
          <p className="text-sm text-slate-400">Create thematic lists and monitor their latest trades.</p>
        </div>
        <Link href="/" className={ghostButtonClassName}>
          Back to feed
        </Link>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
        <WatchlistCreateForm />
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Existing watchlists</h2>
          <div className="mt-4 space-y-3">
            {watchlists.length === 0 ? (
              <p className="text-sm text-slate-400">No watchlists yet. Create one to start tracking tickers.</p>
            ) : (
              watchlists.map((watchlist) => (
                <Link
                  key={watchlist.id}
                  href={`/watchlists/${watchlist.id}`}
                  className="flex items-center justify-between rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-200 hover:border-emerald-400/40"
                >
                  <span>{watchlist.name}</span>
                  <span className="text-xs text-slate-400">#{watchlist.id}</span>
                </Link>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
