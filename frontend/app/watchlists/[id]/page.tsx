import Link from "next/link";
import { FeedList } from "@/components/feed/FeedList";
import { WatchlistTickerManager } from "@/components/watchlists/WatchlistTickerManager";
import { getWatchlist, getWatchlistFeed } from "@/lib/api";
import { cardClassName, ghostButtonClassName, primaryButtonClassName, selectClassName } from "@/lib/styles";

function getParam(sp: Record<string, string | string[] | undefined>, key: string) {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

type Props = {
  params: Promise<{ id: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function WatchlistDetailPage({ params, searchParams }: Props) {
  const { id } = await params;
  const watchlistId = Number(id);
  const sp = (await searchParams) ?? {};
  const whale = getParam(sp, "whale");
  const recentDays = getParam(sp, "recent_days");
  const cursor = getParam(sp, "cursor");
  const limit = getParam(sp, "limit") || "50";

  const [watchlist, feed] = await Promise.all([
    getWatchlist(watchlistId),
    getWatchlistFeed(watchlistId, {
      whale: whale || undefined,
      recent_days: recentDays || undefined,
      cursor: cursor || undefined,
      limit,
    }),
  ]);

  const nextParams = new URLSearchParams();
  if (whale) nextParams.set("whale", whale);
  if (recentDays) nextParams.set("recent_days", recentDays);
  nextParams.set("limit", limit);
  if (feed.next_cursor) nextParams.set("cursor", feed.next_cursor);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Watchlist</p>
          <h1 className="text-3xl font-semibold text-white">Watchlist #{watchlist.watchlist_id}</h1>
          <p className="text-sm text-slate-400">Monitor trades tied to your selected tickers.</p>
        </div>
        <Link href="/watchlists" className={ghostButtonClassName}>
          Back to watchlists
        </Link>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1.1fr_1.4fr]">
        <WatchlistTickerManager watchlistId={watchlist.watchlist_id} tickers={watchlist.tickers} />
        <div className={cardClassName}>
          <h2 className="text-lg font-semibold text-white">Watchlist feed filters</h2>
          <form method="get" className="mt-4 grid gap-4">
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
              <select name="recent_days" defaultValue={recentDays} className={selectClassName}>
                <option value="">Anytime</option>
                <option value="7">Last 7 days</option>
                <option value="30">Last 30 days</option>
                <option value="90">Last 90 days</option>
              </select>
            </div>
            <div className="flex items-center gap-2">
              <input
                id="whale"
                name="whale"
                type="checkbox"
                value="1"
                defaultChecked={whale === "1"}
                className="h-4 w-4 rounded border-white/30 bg-slate-900 text-emerald-300 focus:ring-emerald-400"
              />
              <label htmlFor="whale" className="text-sm text-slate-300">
                Whale trades only (>$250k)
              </label>
            </div>
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Per page</label>
              <select name="limit" defaultValue={limit} className={selectClassName}>
                <option value="25">25</option>
                <option value="50">50</option>
                <option value="100">100</option>
              </select>
            </div>
            <input type="hidden" name="cursor" value="" />
            <div className="flex flex-wrap gap-3">
              <button type="submit" className={primaryButtonClassName}>
                Apply filters
              </button>
              <Link href={`/watchlists/${watchlistId}`} className={ghostButtonClassName}>
                Clear
              </Link>
            </div>
          </form>
        </div>
      </div>

      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-white">Watchlist trades</h2>
          <p className="text-sm text-slate-400">Showing {feed.items.length} transactions.</p>
        </div>
        <FeedList items={feed.items} />
        <div className="flex items-center justify-between gap-4">
          <span className="text-xs text-slate-500">Only tickers from this watchlist are included.</span>
          {feed.next_cursor ? (
            <Link href={`/watchlists/${watchlistId}?${nextParams.toString()}`} className={primaryButtonClassName}>
              Load more
            </Link>
          ) : (
            <span className="text-sm text-slate-500">No more results.</span>
          )}
        </div>
      </section>
    </div>
  );
}
