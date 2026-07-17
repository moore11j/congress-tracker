"use client";

import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { type SearchSuggestResult } from "@/lib/api";
import { useFastSearchSuggest } from "@/hooks/useFastSearchSuggest";
import { isHighConfidenceSearchResult, routeForSearchResult, searchResultsHref } from "@/lib/searchNavigation";

type LandingSearchProps = {
  appUrl: string;
};

const resultLimit = 6;
const minQueryLength = 2;

function absoluteAppHref(appUrl: string, route: string) {
  if (route.startsWith("http")) return route;
  return `${appUrl}${route.startsWith("/") ? route : `/${route}`}`;
}

function typeLabel(kind: SearchSuggestResult["kind"]) {
  if (kind === "agency") return "Department";
  if (kind === "ticker") return "Ticker";
  if (kind === "member") return "Member";
  return "Insider";
}

function SearchIcon() {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true" className="h-5 w-5 text-slate-500" fill="none">
      <path d="m14.2 14.2 3.1 3.1" stroke="currentColor" strokeLinecap="round" strokeWidth="1.8" />
      <circle cx="8.8" cy="8.8" r="5.7" stroke="currentColor" strokeWidth="1.8" />
    </svg>
  );
}

export function LandingSearch({ appUrl }: LandingSearchProps) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  const trimmedQuery = query.trim();
  const suggest = useFastSearchSuggest(trimmedQuery, { limit: resultLimit, minLength: minQueryLength, source: "LandingSearch" });
  const results = suggest.results.filter((result) => result.href && result.label);
  const bestResult = useMemo(() => {
    const exactTicker = results.find((result) => result.kind === "ticker" && result.symbol?.toUpperCase() === trimmedQuery.toUpperCase());
    return exactTicker ?? results[0];
  }, [results, trimmedQuery]);

  useEffect(() => {
    if (trimmedQuery.length < minQueryLength) {
      setOpen(false);
      return;
    }
    if (suggest.settled || results.length > 0) setOpen(true);
  }, [results.length, suggest.settled, trimmedQuery]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (rootRef.current?.contains(target)) return;
      setOpen(false);
    };

    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!trimmedQuery) {
      window.location.href = appUrl;
      return;
    }
    if (bestResult && isHighConfidenceSearchResult(bestResult, trimmedQuery)) {
      window.location.href = absoluteAppHref(appUrl, routeForSearchResult(bestResult));
      return;
    }
    window.location.href = absoluteAppHref(appUrl, searchResultsHref(trimmedQuery));
  }

  return (
    <div ref={rootRef} className="relative mt-8 max-w-2xl">
      <form onSubmit={submit} className="grid gap-3 rounded-lg border border-white/10 bg-slate-950/80 p-2 shadow-2xl shadow-black/25 sm:grid-cols-[1fr_auto]">
        <label className="flex min-w-0 items-center gap-3 rounded-md bg-white/[0.035] px-3 py-3">
          <SearchIcon />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onFocus={() => {
              if (trimmedQuery.length >= minQueryLength) setOpen(true);
            }}
            placeholder="Search tickers, members, insiders, departments..."
            className="min-w-0 flex-1 bg-transparent text-sm text-white outline-none placeholder:text-slate-500/40"
            aria-label="Search Walnut Market Terminal"
          />
        </label>
        <button type="submit" className="rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
          Search
        </button>
      </form>

      {open ? (
        <div className="absolute left-0 right-0 top-[calc(100%+0.5rem)] z-30 overflow-hidden rounded-lg border border-white/10 bg-slate-950 shadow-2xl shadow-black/40">
          {suggest.loading && results.length === 0 ? <p className="px-4 py-3 text-sm text-slate-400">Searching...</p> : null}
          {!suggest.loading && !suggest.error && suggest.settled && results.length === 0 ? <p className="px-4 py-3 text-sm text-slate-400">No matches found</p> : null}
          {!suggest.loading && results.length > 0 ? (
            <div className="divide-y divide-white/10">
              {results.map((result) => (
                <a
                  key={`${result.kind}:${result.id}:${result.href}`}
                  href={absoluteAppHref(appUrl, routeForSearchResult(result))}
                  className="flex items-center justify-between gap-4 px-4 py-3 text-sm transition hover:bg-white/[0.04]"
                >
                  <span className="min-w-0">
                    <span className="block truncate font-semibold text-white">{result.label}</span>
                    <span className="mt-1 block truncate text-xs text-slate-400">{result.subtitle || typeLabel(result.kind)}</span>
                  </span>
                  <span className="shrink-0 rounded border border-white/10 bg-white/[0.035] px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-300">
                    {typeLabel(result.kind)}
                  </span>
                </a>
              ))}
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
