"use client";

import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { globalSearch, type GlobalSearchResult } from "@/lib/api";
import { memberHref } from "@/lib/memberSlug";

type LandingSearchProps = {
  appUrl: string;
};

const resultLimit = 6;
const minQueryLength = 2;
const debounceMs = 300;

function absoluteAppHref(appUrl: string, route: string) {
  if (route.startsWith("http")) return route;
  return `${appUrl}${route.startsWith("/") ? route : `/${route}`}`;
}

function routeForResult(result: GlobalSearchResult) {
  if (result.type === "member") return memberHref({ name: result.label, memberId: result.id });
  return result.route;
}

function typeLabel(type: GlobalSearchResult["type"]) {
  if (type === "government_agency") return "Department";
  if (type === "ticker") return "Ticker";
  if (type === "member") return "Member";
  return "Insider";
}

function isTickerLikeQuery(value: string) {
  return /^[A-Za-z][A-Za-z0-9.-]{0,9}$/.test(value.trim());
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
  const [results, setResults] = useState<GlobalSearchResult[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [failed, setFailed] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const requestIdRef = useRef(0);

  const trimmedQuery = query.trim();
  const bestResult = useMemo(() => {
    const exactTicker = results.find((result) => result.type === "ticker" && result.symbol?.toUpperCase() === trimmedQuery.toUpperCase());
    return exactTicker ?? results[0];
  }, [results, trimmedQuery]);

  useEffect(() => {
    abortRef.current?.abort();
    if (trimmedQuery.length < minQueryLength) {
      setResults([]);
      setOpen(false);
      setLoading(false);
      setFailed(false);
      return;
    }

    const requestId = requestIdRef.current + 1;
    requestIdRef.current = requestId;
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);
    setFailed(false);

    const timeout = window.setTimeout(async () => {
      try {
        const response = await globalSearch(trimmedQuery, resultLimit, { signal: controller.signal });
        if (requestIdRef.current !== requestId) return;
        const nextResults = Array.isArray(response.results) ? response.results.filter((result) => result.route && result.label) : [];
        setResults(nextResults);
        setOpen(true);
      } catch (error) {
        if (error instanceof Error && error.name === "AbortError") return;
        if (requestIdRef.current !== requestId) return;
        setResults([]);
        setOpen(true);
        setFailed(true);
      } finally {
        if (requestIdRef.current === requestId) setLoading(false);
      }
    }, debounceMs);

    return () => {
      window.clearTimeout(timeout);
      abortRef.current?.abort();
    };
  }, [trimmedQuery]);

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
    if (bestResult) {
      window.location.href = absoluteAppHref(appUrl, routeForResult(bestResult));
      return;
    }
    if (isTickerLikeQuery(trimmedQuery)) {
      window.location.href = absoluteAppHref(appUrl, `/ticker/${encodeURIComponent(trimmedQuery.toUpperCase())}`);
      return;
    }
    window.location.href = appUrl;
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
            className="min-w-0 flex-1 bg-transparent text-sm text-white outline-none placeholder:text-slate-500"
            aria-label="Search Walnut Market Terminal"
          />
        </label>
        <button type="submit" className="rounded-lg bg-emerald-300 px-5 py-3 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
          Launch Terminal
        </button>
      </form>

      {open ? (
        <div className="absolute left-0 right-0 top-[calc(100%+0.5rem)] z-30 overflow-hidden rounded-lg border border-white/10 bg-slate-950 shadow-2xl shadow-black/40">
          {loading ? <p className="px-4 py-3 text-sm text-slate-400">Searching...</p> : null}
          {!loading && failed ? <p className="px-4 py-3 text-sm text-slate-400">Search is busy, try again. Press enter for exact tickers.</p> : null}
          {!loading && !failed && results.length === 0 ? <p className="px-4 py-3 text-sm text-slate-400">Press enter to launch the terminal.</p> : null}
          {!loading && !failed && results.length > 0 ? (
            <div className="divide-y divide-white/10">
              {results.map((result) => (
                <a
                  key={`${result.type}:${result.id}:${result.route}`}
                  href={absoluteAppHref(appUrl, routeForResult(result))}
                  className="flex items-center justify-between gap-4 px-4 py-3 text-sm transition hover:bg-white/[0.04]"
                >
                  <span className="min-w-0">
                    <span className="block truncate font-semibold text-white">{result.label}</span>
                    <span className="mt-1 block truncate text-xs text-slate-500">{result.subtitle || typeLabel(result.type)}</span>
                  </span>
                  <span className="shrink-0 rounded border border-white/10 bg-white/[0.035] px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-300">
                    {typeLabel(result.type)}
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
