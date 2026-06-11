import Link from "next/link";
import { searchSuggest, type SearchSuggestResult } from "@/lib/api";
import { isHighConfidenceSearchResult, routeForSearchResult } from "@/lib/searchNavigation";

export const dynamic = "force-dynamic";

type Props = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

const KIND_ORDER: SearchSuggestResult["kind"][] = ["ticker", "member", "insider", "agency"];

const KIND_LABELS: Record<SearchSuggestResult["kind"], string> = {
  agency: "Departments",
  ticker: "Tickers",
  member: "Members",
  insider: "Insiders",
};

const TYPE_LABELS: Record<SearchSuggestResult["kind"], string> = {
  agency: "Department",
  ticker: "Ticker",
  member: "Member",
  insider: "Insider",
};

function one(sp: Record<string, string | string[] | undefined>, key: string): string {
  const value = sp[key];
  return typeof value === "string" ? value : "";
}

function dedupeResults(results: SearchSuggestResult[]): SearchSuggestResult[] {
  const seen = new Set<string>();
  const deduped: SearchSuggestResult[] = [];
  for (const result of results) {
    if (!result.href || !result.label) continue;
    const key = `${result.kind}:${result.id || result.href}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(result);
  }
  return deduped;
}

function groupedResults(results: SearchSuggestResult[]) {
  return KIND_ORDER
    .map((kind) => ({ kind, items: results.filter((result) => result.kind === kind) }))
    .filter((group) => group.items.length > 0);
}

export default async function SearchPage({ searchParams }: Props) {
  const sp = (await searchParams) ?? {};
  const query = one(sp, "q").trim();
  const response = query
    ? await searchSuggest(query, 20, { source: "SearchPage" }).catch(() => ({ items: [] as SearchSuggestResult[] }))
    : { items: [] as SearchSuggestResult[] };
  const results = dedupeResults(response.items ?? []);
  const groups = groupedResults(results);
  const topResult = results[0];
  const showDidYouMean = Boolean(query && topResult && !isHighConfidenceSearchResult(topResult, query));

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto flex w-full max-w-5xl flex-col gap-8 px-4 py-10 sm:px-6 lg:px-8">
        <section className="border-b border-white/10 pb-6">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-200/80">Search</p>
          <h1 className="mt-3 text-3xl font-semibold text-white">Search Walnut</h1>
          <form action="/search" className="mt-6 flex flex-col gap-3 sm:flex-row">
            <input
              name="q"
              defaultValue={query}
              placeholder="Search tickers, companies, members, insiders, departments..."
              className="min-h-11 flex-1 rounded-lg border border-white/10 bg-slate-900 px-4 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-emerald-300/60"
            />
            <button type="submit" className="rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
              Search
            </button>
          </form>
        </section>

        {!query ? (
          <section className="py-10 text-sm text-slate-400">Enter a company, ticker, member, insider, or department to search Walnut.</section>
        ) : null}

        {query && showDidYouMean && topResult ? (
          <section className="rounded-lg border border-emerald-300/25 bg-emerald-300/[0.06] p-4">
            <p className="text-sm text-emerald-100">
              Did you mean{" "}
              <Link href={routeForSearchResult(topResult)} className="font-semibold underline decoration-emerald-200/50 underline-offset-4">
                {topResult.kind === "ticker" && topResult.symbol ? topResult.symbol : topResult.label}
              </Link>
              ?
            </p>
            {topResult.subtitle ? <p className="mt-1 text-xs text-emerald-100/65">{topResult.subtitle}</p> : null}
          </section>
        ) : null}

        {query && results.length === 0 ? (
          <section className="rounded-lg border border-white/10 bg-slate-900/70 p-6">
            <h2 className="text-lg font-semibold text-white">No exact matches for {query}</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Try a company name, ticker, member name, insider name, or department.</p>
          </section>
        ) : null}

        {groups.length > 0 ? (
          <section className="space-y-6">
            {groups.map((group) => (
              <div key={group.kind}>
                <h2 className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-500">{KIND_LABELS[group.kind]}</h2>
                <div className="mt-3 divide-y divide-white/10 overflow-hidden rounded-lg border border-white/10 bg-slate-900/70">
                  {group.items.map((result) => (
                    <Link
                      key={`${result.kind}:${result.id}:${result.href}`}
                      href={routeForSearchResult(result)}
                      className="grid gap-3 px-4 py-4 transition hover:bg-white/[0.04] sm:grid-cols-[1fr_auto]"
                    >
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-semibold text-white">{result.label}</span>
                        <span className="mt-1 block truncate text-xs text-slate-500">{result.subtitle || TYPE_LABELS[result.kind]}</span>
                      </span>
                      <span className="self-center rounded border border-white/10 bg-white/[0.035] px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-300">
                        {TYPE_LABELS[result.kind]}
                      </span>
                    </Link>
                  ))}
                </div>
              </div>
            ))}
          </section>
        ) : null}
      </div>
    </main>
  );
}
