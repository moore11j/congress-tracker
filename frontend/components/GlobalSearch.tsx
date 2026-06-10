"use client";

import { useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { searchSuggest, type SearchSuggestResult } from "@/lib/api";
import { useFastSearchSuggest } from "@/hooks/useFastSearchSuggest";
import { memberHref } from "@/lib/memberSlug";

const MIN_QUERY_LENGTH = 2;
const RESULT_LIMIT = 8;
const RECENT_SEARCH_RESULTS_KEY = "walnut:globalSearch:recentResults";
const MAX_RECENT_SEARCH_RESULTS = 12;

const CATEGORY_LABELS: Record<SearchSuggestResult["kind"], string> = {
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

function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tagName = target.tagName.toLowerCase();
  return tagName === "input" || tagName === "textarea" || target.isContentEditable;
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

function isSearchSuggestKind(value: unknown): value is SearchSuggestResult["kind"] {
  return value === "agency" || value === "ticker" || value === "member" || value === "insider";
}

function readRecentSearchResults(): SearchSuggestResult[] {
  if (typeof window === "undefined") return [];
  try {
    const parsed = JSON.parse(window.localStorage.getItem(RECENT_SEARCH_RESULTS_KEY) || "[]");
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter((item): item is SearchSuggestResult => (
        item &&
        typeof item === "object" &&
        isSearchSuggestKind((item as SearchSuggestResult).kind) &&
        typeof (item as SearchSuggestResult).id === "string" &&
        typeof (item as SearchSuggestResult).label === "string" &&
        typeof (item as SearchSuggestResult).href === "string"
      ))
      .slice(0, MAX_RECENT_SEARCH_RESULTS);
  } catch {
    return [];
  }
}

function rememberSearchResult(result: SearchSuggestResult) {
  if (typeof window === "undefined") return;
  const next = dedupeResults([result, ...readRecentSearchResults()]).slice(0, MAX_RECENT_SEARCH_RESULTS);
  try {
    window.localStorage.setItem(RECENT_SEARCH_RESULTS_KEY, JSON.stringify(next));
  } catch {
    // Local storage can be disabled; search still works without this memory layer.
  }
}

function recentSearchMatches(query: string): SearchSuggestResult[] {
  const key = query.trim().toLowerCase();
  if (key.length < MIN_QUERY_LENGTH) return [];
  return readRecentSearchResults()
    .filter((result) => {
      const values = [result.symbol, result.label, result.id].filter(Boolean).map((value) => String(value).toLowerCase());
      return values.some((value) => value === key || value.startsWith(key) || value.includes(key));
    })
    .slice(0, RESULT_LIMIT);
}

function warmPrefixesForResult(result: SearchSuggestResult): string[] {
  const raw = result.symbol || result.label || result.id;
  const compact = raw.trim().toLowerCase().replace(/[^a-z0-9. -]/g, "");
  if (compact.length < MIN_QUERY_LENGTH) return [];
  return Array.from(new Set([compact.slice(0, 2), compact.slice(0, 3), compact.split(/\s+/)[0]].filter((value) => value.length >= MIN_QUERY_LENGTH)));
}

function prefetchSearchPrefixes(prefixes: string[]) {
  if (typeof window === "undefined") return;
  const unique = Array.from(new Set(prefixes.map((value) => value.trim().toLowerCase()).filter((value) => value.length >= MIN_QUERY_LENGTH))).slice(0, 8);
  if (unique.length === 0) return;
  window.setTimeout(() => {
    unique.forEach((prefix) => {
      void searchSuggest(prefix, RESULT_LIMIT, { source: "GlobalSearchPrefetch" }).catch(() => undefined);
    });
  }, 250);
}

function groupedResults(results: SearchSuggestResult[]) {
  return (["agency", "ticker", "member", "insider"] as const)
    .map((kind) => ({
      kind,
      items: results.filter((result) => result.kind === kind),
    }))
    .filter((group) => group.items.length > 0);
}

function isTickerLikeQuery(value: string) {
  return /^[A-Za-z][A-Za-z0-9.-]{0,9}$/.test(value.trim());
}

function SearchIcon({ className = "h-4 w-4" }: { className?: string }) {
  return (
    <svg viewBox="0 0 20 20" aria-hidden="true" className={className} fill="none">
      <path d="m14.2 14.2 3.1 3.1" stroke="currentColor" strokeLinecap="round" strokeWidth="1.8" />
      <circle cx="8.8" cy="8.8" r="5.7" stroke="currentColor" strokeWidth="1.8" />
    </svg>
  );
}

export function GlobalSearch() {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const mobileInputRef = useRef<HTMLInputElement | null>(null);

  const trimmedQuery = query.trim();
  const suggest = useFastSearchSuggest(trimmedQuery, { limit: RESULT_LIMIT, minLength: MIN_QUERY_LENGTH, source: "GlobalSearch" });
  const recentResults = useMemo(() => recentSearchMatches(trimmedQuery), [trimmedQuery]);
  const results = useMemo(() => dedupeResults([...suggest.results, ...recentResults]), [recentResults, suggest.results]);
  const showPanel = open && trimmedQuery.length >= MIN_QUERY_LENGTH;
  const groups = useMemo(() => groupedResults(results), [results]);

  useEffect(() => {
    prefetchSearchPrefixes(readRecentSearchResults().flatMap(warmPrefixesForResult));
  }, []);

  useEffect(() => {
    if (trimmedQuery.length < MIN_QUERY_LENGTH) {
      setOpen(false);
      setHighlightedIndex(-1);
      return;
    }
    if (suggest.settled || results.length > 0) setOpen(true);
    setHighlightedIndex(results.length > 0 ? 0 : -1);
  }, [results.length, suggest.settled, trimmedQuery]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (rootRef.current?.contains(target)) return;
      setOpen(false);
      setMobileOpen(false);
    };

    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey || isEditableTarget(event.target)) return;
      event.preventDefault();
      setMobileOpen(true);
      setOpen(true);
      window.setTimeout(() => {
        const input = window.matchMedia("(min-width: 768px)").matches ? inputRef.current : mobileInputRef.current;
        input?.focus();
      }, 0);
    };

    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  useEffect(() => {
    if (!mobileOpen) return;
    window.setTimeout(() => mobileInputRef.current?.focus(), 0);
  }, [mobileOpen]);

  function closeSearch() {
    setOpen(false);
    setMobileOpen(false);
    setHighlightedIndex(-1);
  }

  function choose(result: SearchSuggestResult | undefined) {
    if (!result) return;
    const route = result?.kind === "member"
      ? memberHref({ name: result.label, memberId: result.id })
      : result?.href;
    if (!route) return;
    rememberSearchResult({ ...result, href: route });
    prefetchSearchPrefixes(warmPrefixesForResult(result));
    closeSearch();
    setQuery("");
    router.push(route);
  }

  function bestEnterResult(): SearchSuggestResult | undefined {
    const exactTicker = results.find((result) => result.kind === "ticker" && result.symbol?.toUpperCase() === trimmedQuery.toUpperCase());
    if (exactTicker) return exactTicker;
    if (highlightedIndex >= 0 && highlightedIndex < results.length) return results[highlightedIndex];
    return results[0];
  }

  function handleKeyDown(event: ReactKeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeSearch();
      return;
    }

    if (!showPanel && event.key !== "Enter") return;

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setOpen(true);
      setHighlightedIndex((current) => (results.length === 0 ? -1 : (current + 1) % results.length));
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setOpen(true);
      setHighlightedIndex((current) => (results.length === 0 ? -1 : current <= 0 ? results.length - 1 : current - 1));
      return;
    }

    if (event.key === "Enter") {
      const target = bestEnterResult();
      if (target) {
        event.preventDefault();
        choose(target);
        return;
      }
      if (isTickerLikeQuery(trimmedQuery)) {
        event.preventDefault();
        const ticker = trimmedQuery.toUpperCase();
        rememberSearchResult({
          kind: "ticker",
          id: ticker,
          symbol: ticker,
          label: ticker,
          subtitle: "Ticker",
          href: `/ticker/${encodeURIComponent(ticker)}`,
        });
        closeSearch();
        setQuery("");
        router.push(`/ticker/${encodeURIComponent(ticker)}`);
        return;
      }
    }
  }

  function renderPanel() {
    if (!showPanel) return null;
    return (
      <div className="absolute left-0 right-0 top-full z-[1300] mt-2 overflow-hidden rounded-lg border border-white/15 bg-slate-950/95 shadow-2xl shadow-black/45 backdrop-blur">
        {suggest.loading && results.length === 0 ? <div className="px-3 py-3 text-sm text-slate-400">Searching...</div> : null}
        {!suggest.loading && suggest.error ? <div className="px-3 py-3 text-sm text-rose-200">Search is busy, try again.</div> : null}
        {!suggest.loading && !suggest.error && suggest.settled && results.length === 0 ? <div className="px-3 py-3 text-sm text-slate-400">No matches found</div> : null}
        {!suggest.error && groups.length > 0 ? (
          <div className="max-h-[26rem] overflow-y-auto py-2">
            {groups.map((group) => (
              <div key={group.kind} className="py-1">
                <div className="px-3 pb-1 text-[0.65rem] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  {CATEGORY_LABELS[group.kind]}
                </div>
                {group.items.map((result) => {
                  const resultIndex = results.indexOf(result);
                  const selected = resultIndex === highlightedIndex;
                  return (
                    <button
                      key={`${result.kind}-${result.id}-${result.href}`}
                      type="button"
                      role="option"
                      aria-selected={selected}
                      className={`grid w-full grid-cols-[1fr_auto] gap-3 px-3 py-2.5 text-left transition ${
                        selected ? "bg-emerald-400/10 text-emerald-100" : "text-slate-200 hover:bg-white/[0.06]"
                      }`}
                      onMouseEnter={() => setHighlightedIndex(resultIndex)}
                      onMouseDown={(event) => event.preventDefault()}
                      onClick={() => choose(result)}
                    >
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-semibold text-white">{result.label}</span>
                        {result.subtitle ? <span className="mt-0.5 block truncate text-xs text-slate-400">{result.subtitle}</span> : null}
                      </span>
                      <span className="self-center rounded border border-white/10 px-1.5 py-0.5 text-[0.62rem] font-semibold uppercase tracking-[0.12em] text-slate-500">
                        {TYPE_LABELS[result.kind]}
                      </span>
                    </button>
                  );
                })}
              </div>
            ))}
          </div>
        ) : null}
      </div>
    );
  }

  const input = (
    <div className="relative">
      <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-500" />
      <input
        ref={inputRef}
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        onFocus={() => {
          if (trimmedQuery.length >= MIN_QUERY_LENGTH) setOpen(true);
        }}
        onKeyDown={handleKeyDown}
        placeholder="Search tickers, departments, members, insiders..."
        className="h-9 w-full rounded-lg border border-white/10 bg-slate-950/70 pl-9 pr-10 text-sm text-slate-100 outline-none transition placeholder:text-slate-500/40 focus:border-emerald-300/55 focus:bg-slate-950 focus:shadow-[0_0_0_1px_rgba(52,211,153,0.22)]"
        autoComplete="off"
        role="combobox"
        aria-expanded={showPanel}
        aria-label="Global search"
      />
      <span className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 rounded border border-white/10 bg-white/[0.03] px-1.5 py-0.5 text-[0.65rem] font-semibold text-slate-500">
        /
      </span>
      {renderPanel()}
    </div>
  );

  return (
    <div ref={rootRef} className="relative z-[1300]">
      <div className="hidden w-64 lg:block xl:w-80">{input}</div>
      <button
        type="button"
        className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-white/10 bg-slate-950/70 text-slate-300 transition hover:border-emerald-300/40 hover:text-emerald-100 lg:hidden"
        onClick={() => {
          setMobileOpen(true);
          setOpen(true);
        }}
        aria-label="Open search"
      >
        <SearchIcon />
      </button>

      {mobileOpen ? (
        <div className="fixed inset-x-3 top-3 z-[1400] lg:hidden">
          <div className="relative rounded-lg border border-white/15 bg-slate-950/98 p-2 shadow-2xl shadow-black/50">
            <SearchIcon className="pointer-events-none absolute left-5 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-500" />
            <input
              ref={mobileInputRef}
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Search tickers, departments, members, insiders..."
              className="h-10 w-full rounded-lg border border-white/10 bg-slate-950 pl-9 pr-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-500/40 focus:border-emerald-300/55"
              autoComplete="off"
              role="combobox"
              aria-expanded={showPanel}
              aria-label="Global search"
            />
            {renderPanel()}
          </div>
        </div>
      ) : null}
    </div>
  );
}
