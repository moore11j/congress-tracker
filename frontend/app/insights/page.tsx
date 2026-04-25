import Link from "next/link";
import { NewsArticleList } from "@/components/insights/NewsArticleList";
import { getInsightsNews } from "@/lib/api";
import { cardClassName, inputClassName } from "@/lib/styles";
import { optionalPageAuthToken } from "@/lib/serverAuth";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export const metadata = {
  title: "Insights | Capitol Ledger",
  description: "Market headlines and company-level news connected to your intelligence workflow.",
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function hrefFor(category: string, ticker: string) {
  const params = new URLSearchParams();
  if (category && category !== "all") params.set("category", category);
  if (ticker) params.set("ticker", ticker);
  const query = params.toString();
  return query ? `/insights?${query}` : "/insights";
}

export default async function InsightsPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const authToken = await optionalPageAuthToken();
  const ticker = one(sp, "ticker").trim().toUpperCase();
  const requestedCategory = one(sp, "category").trim().toLowerCase();
  const category = requestedCategory === "market" || requestedCategory === "stock" || requestedCategory === "watchlist"
    ? requestedCategory
    : "all";

  const response = await getInsightsNews({
    category: ticker && category === "market" ? "stock" : (category as "all" | "market" | "stock" | "watchlist"),
    tickers: ticker || undefined,
    limit: 25,
    authToken,
  }).catch(
    (): Awaited<ReturnType<typeof getInsightsNews>> => ({
      items: [],
      status: "unavailable",
      message: "News is unavailable under the current data plan.",
      total: 0,
      offset: 0,
      limit: 25,
    }),
  );

  return (
    <div className="space-y-6">
      <section className={cardClassName}>
        <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Insights</p>
        <h1 className="mt-3 text-3xl font-semibold text-white">Insights</h1>
        <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-400">
          Market headlines and company-level news connected to your intelligence workflow.
        </p>

        <div className="mt-6 flex flex-wrap gap-2">
          {[
            { key: "all", label: "All", disabled: false },
            { key: "market", label: "Market", disabled: false },
            { key: "watchlist", label: "Watchlist", disabled: !authToken },
          ].map((filter) =>
            filter.disabled ? (
              <span
                key={filter.key}
                className="rounded-full border border-white/10 bg-slate-950/50 px-3 py-1.5 text-xs font-semibold text-slate-500"
              >
                {filter.label} soon
              </span>
            ) : (
              <Link
                key={filter.key}
                href={hrefFor(filter.key, ticker)}
                prefetch={false}
                className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${
                  category === filter.key
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-200"
                    : "border-white/10 bg-slate-950/50 text-slate-300"
                }`}
              >
                {filter.label}
              </Link>
            ),
          )}
          <span className="rounded-full border border-white/10 bg-slate-950/50 px-3 py-1.5 text-xs font-semibold text-slate-500">
            Congress-linked soon
          </span>
          <span className="rounded-full border border-white/10 bg-slate-950/50 px-3 py-1.5 text-xs font-semibold text-slate-500">
            Insider-linked soon
          </span>
        </div>

        <form action="/insights" className="mt-5 grid gap-3 md:grid-cols-[minmax(0,18rem)_auto] md:items-end">
          <div>
            <label htmlFor="ticker" className="block text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
              Ticker Filter
            </label>
            <input
              id="ticker"
              name="ticker"
              defaultValue={ticker}
              placeholder="AAPL"
              className={`mt-2 ${inputClassName}`}
            />
            {category !== "all" ? <input type="hidden" name="category" value={category} /> : null}
          </div>
          <button
            type="submit"
            className="inline-flex h-11 items-center justify-center rounded-2xl border border-emerald-300/30 bg-emerald-400/10 px-4 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-400/15"
          >
            Apply filter
          </button>
        </form>
      </section>

      <section className={cardClassName}>
        <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">News Feed</p>
            <p className="mt-2 text-sm text-slate-400">
              {ticker ? `Showing articles linked to ${ticker}.` : "Showing the latest discovery feed."}
            </p>
          </div>
          <p className="text-xs text-slate-500">{response.total ?? response.items.length} articles</p>
        </div>
        <NewsArticleList
          items={response.items}
          status={response.status}
          message={response.message}
          emptyMessage={ticker ? "No recent news found for this ticker." : "No recent market news found."}
        />
      </section>
    </div>
  );
}
