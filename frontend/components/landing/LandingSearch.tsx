"use client";

import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import { getEntitlements, type SearchSuggestResult } from "@/lib/api";
import { defaultEntitlements, type Entitlements } from "@/lib/entitlements";
import { recordGoogleAnalyticsEvent } from "@/lib/googleAnalytics";
import { hasPrivacyConsent } from "@/lib/privacyConsent";
import { useFastSearchSuggest } from "@/hooks/useFastSearchSuggest";
import { isHighConfidenceSearchResult, routeForSearchResult, searchResultsHref } from "@/lib/searchNavigation";

type LandingSearchProps = {
  appUrl: string;
  buttonLabel?: string;
  buttonOutside?: boolean;
  className?: string;
  featuredSuggestion?: SearchSuggestResult;
  placeholder?: string;
  reassuranceCopy?: string;
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
  if (kind === "institution") return "Institution";
  if (kind === "event") return "Event";
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

function planForAnalytics(entitlements: Entitlements) {
  return entitlements.effective_tier ?? entitlements.plan ?? entitlements.tier ?? "free";
}

export function LandingSearch({
  appUrl,
  buttonLabel = "Search",
  buttonOutside = false,
  className = "",
  featuredSuggestion,
  placeholder = "Search tickers, companies, Congress members, insiders, institutions, departments...",
  reassuranceCopy,
}: LandingSearchProps) {
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const rootRef = useRef<HTMLDivElement | null>(null);
  const homepageViewTrackedRef = useRef(false);
  const searchFocusTrackedRef = useRef(false);
  const searchInputTrackedRef = useRef(false);

  const trimmedQuery = query.trim();
  const suggest = useFastSearchSuggest(trimmedQuery, { limit: resultLimit, minLength: minQueryLength, source: "LandingSearch" });
  const results = useMemo(() => {
    const remoteResults = suggest.results.filter((result) => result.href && result.label);
    if (!featuredSuggestion || trimmedQuery.length < minQueryLength) return remoteResults;
    const queryKey = trimmedQuery.toLowerCase();
    const featuredSymbol = (featuredSuggestion.symbol || featuredSuggestion.id).toLowerCase();
    const featuredLabel = featuredSuggestion.label.toLowerCase();
    const shouldShowFeatured = featuredSymbol.startsWith(queryKey) || featuredLabel.includes(queryKey);
    if (!shouldShowFeatured) return remoteResults;
    const withoutDuplicate = remoteResults.filter((result) => {
      const resultSymbol = (result.symbol || result.id).toLowerCase();
      return result.kind !== featuredSuggestion.kind || resultSymbol !== featuredSymbol;
    });
    return [featuredSuggestion, ...withoutDuplicate].slice(0, resultLimit);
  }, [featuredSuggestion, suggest.results, trimmedQuery]);
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

  const track = (eventName: string, ticker?: string | null) => {
    if (!hasPrivacyConsent("analytics")) return;
    recordGoogleAnalyticsEvent(eventName, {
      pathname: window.location.pathname,
      ticker: ticker || undefined,
      source_page_type: "homepage",
      auth_state: entitlements.user ? "authenticated" : "anonymous",
      plan: planForAnalytics(entitlements),
    });
  };

  useEffect(() => {
    let cancelled = false;
    void getEntitlements()
      .then((nextEntitlements) => {
        if (cancelled) return;
        setEntitlements(nextEntitlements);
        if (!homepageViewTrackedRef.current) {
          homepageViewTrackedRef.current = true;
          if (hasPrivacyConsent("analytics")) {
            recordGoogleAnalyticsEvent("homepage_view", {
              pathname: window.location.pathname,
              source_page_type: "homepage",
              auth_state: nextEntitlements.user ? "authenticated" : "anonymous",
              plan: planForAnalytics(nextEntitlements),
            });
          }
        }
      })
      .catch(() => {
        if (!cancelled && !homepageViewTrackedRef.current) {
          homepageViewTrackedRef.current = true;
          track("homepage_view");
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    track("homepage_research_stock_click", bestResult?.kind === "ticker" ? bestResult.symbol : null);
    if (!trimmedQuery) {
      window.location.href = appUrl;
      return;
    }
    if (bestResult && isHighConfidenceSearchResult(bestResult, trimmedQuery)) {
      if (bestResult.kind === "ticker") track("homepage_ticker_selected", bestResult.symbol);
      window.location.href = absoluteAppHref(appUrl, routeForSearchResult(bestResult));
      return;
    }
    window.location.href = absoluteAppHref(appUrl, searchResultsHref(trimmedQuery));
  }

  const formClassName = buttonOutside
    ? "grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-stretch"
    : "grid gap-3 rounded-lg border border-emerald-300/25 bg-slate-950/80 p-2 shadow-2xl shadow-black/25 ring-1 ring-white/10 sm:grid-cols-[1fr_auto]";
  const inputShellClassName = buttonOutside
    ? "flex min-w-0 items-center gap-3 rounded-lg border border-emerald-300/25 bg-slate-950/80 px-4 py-3 shadow-2xl shadow-black/25 ring-1 ring-white/10"
    : "flex min-w-0 items-center gap-3 rounded-md bg-white/[0.035] px-3 py-3";
  const buttonClassName = buttonOutside
    ? "rounded-lg bg-emerald-300 px-6 py-3 text-sm font-medium text-slate-950 shadow-lg shadow-emerald-950/30 transition hover:bg-emerald-200"
    : "rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-5 py-3 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15";

  return (
    <div ref={rootRef} className={`relative z-[80] w-full ${reassuranceCopy ? "sm:pb-5 " : ""}${className || "mx-auto mt-4 max-w-2xl sm:mt-8"}`}>
      <form onSubmit={submit} className={formClassName}>
        <label className={inputShellClassName}>
          <SearchIcon />
          <input
            value={query}
            onChange={(event) => {
              setQuery(event.target.value);
              if (event.target.value.trim() && !searchInputTrackedRef.current) {
                searchInputTrackedRef.current = true;
                track("homepage_search_input");
              }
            }}
            onFocus={() => {
              if (!searchFocusTrackedRef.current) {
                searchFocusTrackedRef.current = true;
                track("homepage_search_focus");
              }
              if (trimmedQuery.length >= minQueryLength) setOpen(true);
            }}
            placeholder={placeholder}
            className="min-w-0 flex-1 bg-transparent text-sm text-white outline-none placeholder:text-slate-500/40"
            aria-label="Search Walnut Market Terminal"
          />
        </label>
        <div className="flex min-w-0 flex-col items-stretch sm:relative">
          <button type="submit" className={`${buttonClassName} w-full`}>
            {buttonLabel}
          </button>
          {reassuranceCopy ? <p className="mt-1.5 whitespace-nowrap text-center text-xs font-medium text-slate-500 sm:absolute sm:right-0 sm:top-full sm:text-right">{reassuranceCopy}</p> : null}
        </div>
      </form>

      {open ? (
        <div className="absolute left-0 right-0 top-[calc(100%+0.5rem)] z-[1400] overflow-hidden rounded-lg border border-white/10 bg-slate-950 shadow-2xl shadow-black/40">
          {suggest.loading && results.length === 0 ? <p className="px-4 py-3 text-sm text-slate-400">Searching...</p> : null}
          {!suggest.loading && !suggest.error && suggest.settled && results.length === 0 ? <p className="px-4 py-3 text-sm text-slate-400">No matches found</p> : null}
          {results.length > 0 ? (
            <div className="divide-y divide-white/10">
              {results.map((result) => (
                <a
                  key={`${result.kind}:${result.id}:${result.href}`}
                  href={absoluteAppHref(appUrl, routeForSearchResult(result))}
                  onClick={() => {
                    if (result.kind === "ticker") track("homepage_ticker_selected", result.symbol);
                  }}
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
