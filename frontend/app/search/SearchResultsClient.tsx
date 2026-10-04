"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState, type FormEvent } from "react";
import { searchSuggest, type SearchSuggestResult } from "@/lib/api";
import { isHighConfidenceSearchResult, routeForSearchResult, searchResultsHref } from "@/lib/searchNavigation";

const KIND_LABELS: Record<SearchSuggestResult["kind"], string> = {
  agency: "Departments",
  ticker: "Tickers",
  institution: "Institutions",
  member: "Members",
  insider: "Insiders",
  event: "Events",
};

const TYPE_LABELS: Record<SearchSuggestResult["kind"], string> = {
  agency: "Department",
  ticker: "Ticker",
  institution: "Institution",
  member: "Member",
  insider: "Insider",
  event: "Event",
};

function dedupeResults(results: SearchSuggestResult[]): SearchSuggestResult[] {
  const seen = new Set<string>();
  const deduped: SearchSuggestResult[] = [];
  for (const result of results) {
    if (!result.href || !result.label) continue;
    const key = result.href || `${result.kind}:${result.id}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(result);
  }
  return deduped;
}

function groupedResults(results: SearchSuggestResult[]) {
  const groups: Array<{ kind: SearchSuggestResult["kind"]; items: SearchSuggestResult[] }> = [];
  for (const result of results) {
    let group = groups.find((item) => item.kind === result.kind);
    if (!group) {
      group = { kind: result.kind, items: [] };
      groups.push(group);
    }
    group.items.push(result);
  }
  return groups;
}

export function SearchResultsClient({ initialQuery }: { initialQuery: string }) {
  const router = useRouter();
  const [inputValue, setInputValue] = useState(initialQuery);
  const [results, setResults] = useState<SearchSuggestResult[]>([]);
  const [loading, setLoading] = useState(Boolean(initialQuery));
  const [deepSettled, setDeepSettled] = useState(!initialQuery);
  const [error, setError] = useState(false);
  const [retry, setRetry] = useState(0);
  const query = initialQuery.trim();

  useEffect(() => {
    setInputValue(initialQuery);
  }, [initialQuery]);

  useEffect(() => {
    if (!query) {
      setResults([]);
      setLoading(false);
      setDeepSettled(true);
      setError(false);
      return;
    }

    const controller = new AbortController();
    let active = true;
    let timedOut = false;
    const timeout = window.setTimeout(() => {
      timedOut = true;
      controller.abort();
      if (!active) return;
      setLoading(false);
      setDeepSettled(true);
      setError(true);
    }, 8000);
    setLoading(true);
    setDeepSettled(false);
    setError(false);
    setResults([]);

    searchSuggest(query, 20, { signal: controller.signal, source: "SearchPageClient" })
      .catch((requestError) => {
        if (controller.signal.aborted) throw requestError;
        // Mobile networks/privacy filters may block the direct API host.
        return searchSuggest(query, 20, { signal: controller.signal, source: "SearchPageClientRetry", sameOrigin: true });
      })
      .then((response) => {
        if (!active) return;
        const fastResults = dedupeResults(response.items ?? []);
        setResults(fastResults);
        setError(false);
        setLoading(false);
        if (isHighConfidenceSearchResult(fastResults[0], query)) return undefined;
        return searchSuggest(query, 20, { signal: controller.signal, source: "SearchPageClientDeep", mode: "deep" });
      })
      .then((response) => {
        if (!active || !response) return;
        setResults((current) => dedupeResults([...(response.items ?? []), ...current]));
        setError(false);
      })
      .catch((requestError) => {
        if (!active) return;
        if (requestError instanceof Error && requestError.name === "AbortError" && !timedOut) return;
        setResults((current) => current);
        setError(true);
      })
      .finally(() => {
        window.clearTimeout(timeout);
        if (active && (!controller.signal.aborted || timedOut)) setLoading(false);
        if (active) setDeepSettled(true);
      });

    return () => {
      active = false;
      window.clearTimeout(timeout);
      controller.abort();
    };
  }, [query, retry]);

  const groups = useMemo(() => groupedResults(results), [results]);
  const topResult = results[0];
  const showDidYouMean = Boolean(query && topResult && !isHighConfidenceSearchResult(topResult, query));

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextQuery = inputValue.trim();
    if (nextQuery === query) {
      setRetry((value) => value + 1);
      return;
    }
    router.push(nextQuery ? searchResultsHref(nextQuery) : "/search");
  }

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto flex w-full max-w-5xl flex-col gap-8 px-4 py-10 sm:px-6 lg:px-8">
        <section className="border-b border-white/10 pb-6">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-emerald-200/80">Search</p>
          <h1 className="mt-3 text-3xl font-semibold text-white">Search Walnut</h1>
          <form onSubmit={submit} className="mt-6 flex flex-col gap-3 sm:flex-row">
            <input
              name="q"
              type="search"
              aria-label="Search Walnut"
              enterKeyHint="search"
              value={inputValue}
              onChange={(event) => setInputValue(event.target.value)}
              placeholder="Search tickers, companies, institutions, members, insiders, departments..."
              className="min-h-11 flex-1 rounded-lg border border-white/10 bg-slate-900 px-4 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-emerald-300/60"
            />
            <button type="submit" className="rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
              Search
            </button>
          </form>
        </section>

        {!query ? <section className="py-10 text-sm text-slate-400">Enter a company, ticker, institution, member, insider, or department to search Walnut.</section> : null}
        {query && loading && results.length === 0 ? <section className="py-10 text-sm text-slate-400">Searching...</section> : null}
        {query && error && results.length === 0 ? (
          <section role="status" className="rounded-lg border border-white/10 bg-slate-900/70 p-4 text-sm text-slate-300">
            Search couldn’t finish loading.{" "}
            <button type="button" className="text-emerald-200 underline underline-offset-4" onClick={() => setRetry((value) => value + 1)}>Try again</button>
          </section>
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

        {query && deepSettled && !loading && !error && results.length === 0 ? (
          <section className="rounded-lg border border-white/10 bg-slate-900/70 p-6">
            <h2 className="text-lg font-semibold text-white">No exact matches for {query}</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Try a company name, ticker, institution, member name, insider name, or department.</p>
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
