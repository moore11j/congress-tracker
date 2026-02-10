import Link from "next/link";
import { listWatchlists } from "@/lib/api";
import { ghostButtonClassName } from "@/lib/styles";
import { WatchlistsDashboard } from "@/components/watchlists/WatchlistsDashboard";

export const dynamic = "force-dynamic";

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

      <WatchlistsDashboard initialWatchlists={watchlists} />
    </div>
  );
}
