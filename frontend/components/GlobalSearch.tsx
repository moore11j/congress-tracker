"use client";

import { useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from "react";
import { globalSearch, type GlobalSearchResult } from "@/lib/api";

const MIN_QUERY_LENGTH = 1;
const DEBOUNCE_MS = 240;
const RESULT_LIMIT = 8;

const CATEGORY_LABELS: Record<GlobalSearchResult["type"], string> = {
  government_agency: "Departments",
  ticker: "Tickers",
  member: "Members",
  insider: "Insiders",
};

const TYPE_LABELS: Record<GlobalSearchResult["type"], string> = {
  government_agency: "Department",
  ticker: "Ticker",
  member: "Member",
  insider: "Insider",
};

function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tagName = target.tagName.toLowerCase();
  return tagName === "input" || tagName === "textarea" || target.isContentEditable;
}

function dedupeResults(results: GlobalSearchResult[]): GlobalSearchResult[] {
  const seen = new Set<string>();
  const deduped: GlobalSearchResult[] = [];
  for (const result of results) {
    if (!result.route || !result.label) continue;
    const key = `${result.type}:${result.id || result.route}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(result);
  }
  return deduped;
}

function groupedResults(results: GlobalSearchResult[]) {
  return (["government_agency", "ticker", "member", "insider"] as const)
    .map((type) => ({
      type,
      items: results.filter((result) => result.type === type),
    }))
    .filter((group) => group.items.length > 0);
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
  const [results, setResults] = useState<GlobalSearchResult[]>([]);
  const [open, setOpen] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const mobileInputRef = useRef<HTMLInputElement | null>(null);
  const debounceRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);

  const trimmedQuery = query.trim();
  const showPanel = open && trimmedQuery.length >= MIN_QUERY_LENGTH;
  const groups = useMemo(() => groupedResults(results), [results]);

  useEffect(() => {
    if (debounceRef.current) window.clearTimeout(debounceRef.current);

    if (trimmedQuery.length < MIN_QUERY_LENGTH) {
      setResults([]);
      setOpen(false);
      setLoading(false);
      setError(false);
      setHighlightedIndex(-1);
      return;
    }

    debounceRef.current = window.setTimeout(async () => {
      const requestId = requestIdRef.current + 1;
      requestIdRef.current = requestId;
      setLoading(true);
      setError(false);

      try {
        const response = await globalSearch(trimmedQuery, RESULT_LIMIT);
        if (requestIdRef.current !== requestId) return;
        const nextResults = dedupeResults(Array.isArray(response.results) ? response.results : []);
        setResults(nextResults);
        setHighlightedIndex(nextResults.length > 0 ? 0 : -1);
        setOpen(true);
      } catch {
        if (requestIdRef.current !== requestId) return;
        setResults([]);
        setHighlightedIndex(-1);
        setOpen(true);
        setError(true);
      } finally {
        if (requestIdRef.current === requestId) setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
  }, [trimmedQuery]);

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

  function choose(result: GlobalSearchResult | undefined) {
    if (!result?.route) return;
    closeSearch();
    setQuery("");
    setResults([]);
    router.push(result.route);
  }

  function bestEnterResult(): GlobalSearchResult | undefined {
    const exactTicker = results.find((result) => result.type === "ticker" && result.symbol?.toUpperCase() === trimmedQuery.toUpperCase());
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
      }
    }
  }

  function renderPanel() {
    if (!showPanel) return null;
    return (
      <div className="absolute left-0 right-0 top-full z-[1300] mt-2 overflow-hidden rounded-lg border border-white/15 bg-slate-950/95 shadow-2xl shadow-black/45 backdrop-blur">
        {loading && results.length === 0 ? <div className="px-3 py-3 text-sm text-slate-400">Searching...</div> : null}
        {!loading && error ? <div className="px-3 py-3 text-sm text-rose-200">Search temporarily unavailable.</div> : null}
        {!loading && !error && results.length === 0 ? <div className="px-3 py-3 text-sm text-slate-400">No matches found</div> : null}
        {!error && groups.length > 0 ? (
          <div className="max-h-[26rem] overflow-y-auto py-2">
            {groups.map((group) => (
              <div key={group.type} className="py-1">
                <div className="px-3 pb-1 text-[0.65rem] font-semibold uppercase tracking-[0.16em] text-slate-500">
                  {CATEGORY_LABELS[group.type]}
                </div>
                {group.items.map((result) => {
                  const resultIndex = results.indexOf(result);
                  const selected = resultIndex === highlightedIndex;
                  return (
                    <button
                      key={`${result.type}-${result.id}-${result.route}`}
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
                        {TYPE_LABELS[result.type]}
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
        className="h-9 w-full rounded-lg border border-white/10 bg-slate-950/70 pl-9 pr-10 text-sm text-slate-100 outline-none transition placeholder:text-slate-500 focus:border-emerald-300/55 focus:bg-slate-950 focus:shadow-[0_0_0_1px_rgba(52,211,153,0.22)]"
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
              className="h-10 w-full rounded-lg border border-white/10 bg-slate-950 pl-9 pr-3 text-sm text-slate-100 outline-none transition placeholder:text-slate-500 focus:border-emerald-300/55"
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
