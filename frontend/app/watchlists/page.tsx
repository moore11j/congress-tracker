import { VerifiedSessionGuard } from "@/components/auth/VerifiedSessionGuard";
import { requirePageAuth } from "@/lib/serverAuth";
import { WatchlistsDashboard } from "@/components/watchlists/WatchlistsDashboard";

export const dynamic = "force-dynamic";

export default async function WatchlistsPage() {
  const authToken = await requirePageAuth("/watchlists");

  return (
    <VerifiedSessionGuard returnTo="/watchlists" initiallyAuthorized={Boolean(authToken)}>
      <div className="space-y-5">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlists</p>
          <h1 className="text-3xl font-semibold text-white">Monitor tickers</h1>
          <p className="mt-1 max-w-2xl text-sm text-slate-400">
            Create a list, add symbols, and open it to review recent filings, insiders, and signals.
          </p>
        </div>

        <WatchlistsDashboard initialWatchlists={[]} initialAuthPending={!authToken} />
      </div>
    </VerifiedSessionGuard>
  );
}
