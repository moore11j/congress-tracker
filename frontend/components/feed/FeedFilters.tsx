"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import type { ChangeEvent, KeyboardEvent } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { cardClassName, ghostButtonClassName, inputClassName, selectClassName } from "@/lib/styles";
import { FilterPill } from "@/components/ui/FilterPill";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { suggestSymbols, type SymbolSuggestion } from "@/lib/api";
import { FeedMountLogger } from "@/components/feed/FeedMountLogger";
import type { EventItem } from "@/lib/api";

const debounceMs = 350;
const symbolSuggestDebounceMs = 200;
const filtersSessionKey = "ct:feedFilters";

type FeedMode = "congress" | "insider" | "government_contracts" | "all";
type WhaleMode = "off" | "500k" | "1m" | "5m";

function parseFeedMode(value: string): FeedMode {
  if (value === "congress" || value === "insider" || value === "government_contracts" || value === "all") return value;
  return "all";
}

type FilterState = {
  feedMode: FeedMode;
  symbol: string;
  minAmount: string;
  maxAmount: string;
  recentDays: string;
  member: string;
  chamber: string;
  party: string;
  tradeType: string;
  role: string;
  department: string;
  whale: WhaleMode;
};

const departmentOptions = [
  ["", "Any"],
  ["Department of Defense", "Department of Defense"],
  ["Department of Health and Human Services", "Department of Health and Human Services"],
  ["Department of Agriculture", "Department of Agriculture"],
  ["Department of Energy", "Department of Energy"],
  ["Department of Homeland Security", "Department of Homeland Security"],
  ["Department of Veterans Affairs", "Department of Veterans Affairs"],
  ["National Aeronautics and Space Administration", "National Aeronautics and Space Administration"],
  ["General Services Administration", "General Services Administration"],
  ["Department of Transportation", "Department of Transportation"],
  ["Department of Justice", "Department of Justice"],
  ["Other", "Other"],
] as const;

type FeedFiltersProps = {
  events?: EventItem[];
  resultsCount?: number;
  debugLifecycle?: boolean;
};

function filtersEqual(a: FilterState, b: FilterState): boolean {
  return (
    a.feedMode === b.feedMode &&
    a.symbol === b.symbol &&
    a.minAmount === b.minAmount &&
    a.maxAmount === b.maxAmount &&
    a.recentDays === b.recentDays &&
    a.member === b.member &&
    a.chamber === b.chamber &&
    a.party === b.party &&
    a.tradeType === b.tradeType &&
    a.role === b.role &&
    a.department === b.department &&
    a.whale === b.whale
  );
}

function normalizeWhaleMode(value: string): WhaleMode {
  if (value === "500k" || value === "1m" || value === "5m") return value;
  return "off";
}

function normalizeValue(value: string | null): string {
  return (value ?? "").trim();
}

function normalizeTradeType(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (!normalized) return "";
  if (normalized === "purchase" || normalized === "p-purchase") return "purchase";
  if (normalized === "sale" || normalized === "s-sale") return "sale";
  return "";
}

function clearHiddenFilters(mode: FeedMode, next: FilterState): FilterState {
  if (mode === "congress") {
    return { ...next, role: "" };
  }
  if (mode === "insider") {
    return { ...next, member: "", chamber: "", party: "" };
  }
  return {
    ...next,
    member: "",
    chamber: "",
    party: "",
    role: "",
  };
}

function isActive(value: string): boolean {
  return value.trim().length > 0;
}

function controlClassName(baseClassName: string, value: string): string {
  return isActive(value) ? `${baseClassName} border-emerald-500/40 bg-slate-950/40` : baseClassName;
}

function hasUrlManagedParams(params: URLSearchParams): boolean {
  const managedKeys = [
    "mode",
    "tape",
    "symbol",
    "min_amount",
    "max_amount",
    "recent_days",
    "member",
    "chamber",
    "party",
    "trade_type",
    "role",
    "department",
    "whale",
  ] as const;

  return managedKeys.some((key) => normalizeValue(params.get(key)).length > 0);
}

function parseStoredFilters(rawValue: string | null): Partial<FilterState> | null {
  if (!rawValue) return null;

  try {
    const parsed = JSON.parse(rawValue) as Partial<FilterState>;
    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch {
    return null;
  }
}

export function FeedFilters({ events = [], resultsCount, debugLifecycle = false }: FeedFiltersProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const [symbolSuggestions, setSymbolSuggestions] = useState<SymbolSuggestion[]>([]);
  const [isSuggestingSymbol, setIsSuggestingSymbol] = useState(false);
  const [highlightedSymbolSuggestionIndex, setHighlightedSymbolSuggestionIndex] = useState(-1);
  const [showSymbolSuggestions, setShowSymbolSuggestions] = useState(false);
  const [memberSuggestions, setMemberSuggestions] = useState<string[]>([]);
  const [showMemberSuggestions, setShowMemberSuggestions] = useState(false);
  const [highlightedMemberSuggestionIndex, setHighlightedMemberSuggestionIndex] = useState(-1);
  const suggestionsRequestRef = useRef(0);
  const symbolFieldRef = useRef<HTMLDivElement | null>(null);
  const memberFieldRef = useRef<HTMLDivElement | null>(null);
  const debugInteractivity = searchParams.get("debug_interactivity") === "1";
  const debugFeedSync = searchParams.get("debug_feed_sync") === "1";
  const debugDisableFilterSync = searchParams.get("debug_disable_filter_sync") === "1";
  const debugDisableFilterPopovers = searchParams.get("debug_disable_filter_popovers") === "1";
  const debugDisableFilterListeners = searchParams.get("debug_disable_filter_listeners") === "1";
  const debugStaticFilterShell = searchParams.get("debug_static_filter_shell") === "1";
  const debugDisableFilterPills = searchParams.get("debug_disable_filter_pills") === "1";
  const debugFilterTextOnly = searchParams.get("debug_filter_text_only") === "1";
  const debugFilterNoCardShell = searchParams.get("debug_filter_no_card_shell") === "1";
  const debugFilterNoPillComponent = searchParams.get("debug_filter_no_pill_component") === "1";
  const debugFilterNoControls = searchParams.get("debug_filter_no_controls") === "1";
  const debugFilterMinimalBox = searchParams.get("debug_filter_minimal_box") === "1";
  const debugFilters =
    debugInteractivity ||
    debugFeedSync ||
    debugDisableFilterSync ||
    debugDisableFilterPopovers ||
    debugDisableFilterListeners ||
    debugStaticFilterShell ||
    debugDisableFilterPills ||
    debugFilterTextOnly ||
    debugFilterNoCardShell ||
    debugFilterNoPillComponent ||
    debugFilterNoControls ||
    debugFilterMinimalBox;
  const debounceHandleRef = useRef<number | null>(null);
  const pendingNavigationRef = useRef(false);
  const lastRequestedSearchRef = useRef<string | null>(null);

  const logFeedSync = (message: string, detail?: Record<string, unknown>) => {
    if (!debugFeedSync) return;
    if (detail) {
      console.log(`[feed-sync] ${message}`, detail);
      return;
    }
    console.log(`[feed-sync] ${message}`);
  };

  const clearDebouncedSync = (reason: string) => {
    if (debounceHandleRef.current !== null) {
      window.clearTimeout(debounceHandleRef.current);
      debounceHandleRef.current = null;
      logFeedSync("debounce cancelled", { reason });
    }
  };

  const logFilterDebug = (message: string, detail?: Record<string, unknown>) => {
    if (!debugFilters) return;
    if (detail) {
      console.log(`[feed-filters-debug] ${message}`, detail);
      return;
    }
    console.log(`[feed-filters-debug] ${message}`);
  };

  const initialFilters = useMemo<FilterState>(() => {
    const params = new URLSearchParams(searchParamsString);
    const stored =
      typeof window !== "undefined" && !hasUrlManagedParams(params)
        ? parseStoredFilters(window.sessionStorage.getItem(filtersSessionKey))
        : null;
    const explicitMode = normalizeValue(params.get("mode")) || normalizeValue(params.get("tape"));
    const storedMode = normalizeValue(stored?.feedMode ?? "") || normalizeValue((stored as { tape?: string } | null)?.tape ?? "");
    const mode = parseFeedMode(explicitMode || storedMode);
    const tradeType = normalizeTradeType(
      normalizeValue(searchParams.get("trade_type")) || normalizeValue(stored?.tradeType ?? "")
    );

    return {
      feedMode: mode,
      symbol: normalizeValue(params.get("symbol")) || normalizeValue(stored?.symbol ?? ""),
      minAmount: normalizeValue(params.get("min_amount")) || normalizeValue(stored?.minAmount ?? ""),
      maxAmount: normalizeValue(params.get("max_amount")) || normalizeValue(stored?.maxAmount ?? ""),
      recentDays: normalizeValue(params.get("recent_days")) || normalizeValue(stored?.recentDays ?? ""),
      member: normalizeValue(params.get("member")) || normalizeValue(stored?.member ?? ""),
      chamber: normalizeValue(params.get("chamber")) || normalizeValue(stored?.chamber ?? ""),
      party: normalizeValue(params.get("party")) || normalizeValue(stored?.party ?? ""),
      tradeType,
      role: normalizeValue(params.get("role")) || normalizeValue(stored?.role ?? ""),
      department: normalizeValue(params.get("department")) || normalizeValue(stored?.department ?? ""),
      whale: normalizeWhaleMode(normalizeValue(params.get("whale")) || normalizeValue(stored?.whale ?? "off")),
    };
  }, [searchParamsString]);

  const members = useMemo(() => {
    const set = new Set<string>();
    events.forEach((event) => {
      if (event.event_type !== "congress_trade") return;
      const name = (event.payload?.member?.name ?? event.payload?.member_name ?? "").toString().trim();
      if (name) set.add(name);
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b));
  }, [events]);

  const [filters, setFilters] = useState<FilterState>(initialFilters);

  useEffect(() => {
    logFilterDebug("mount", {
      debugDisableFilterSync,
      debugDisableFilterPopovers,
      debugDisableFilterListeners,
      debugStaticFilterShell,
      debugDisableFilterPills,
      debugFilterTextOnly,
      debugFilterNoCardShell,
      debugFilterNoPillComponent,
      debugFilterNoControls,
      debugFilterMinimalBox,
    });
    return () => logFilterDebug("unmount");
  }, []);

  useEffect(() => {
    setFilters((current) => {
      const changed = !filtersEqual(current, initialFilters);
      logFeedSync("initial filters sync", {
        currentFilters: current,
        initialFilters,
        changed,
      });
      if (!changed) return current;
      return initialFilters;
    });
  }, [initialFilters]);

  useEffect(() => {
    setShowSymbolSuggestions(false);
    setShowMemberSuggestions(false);
    setHighlightedSymbolSuggestionIndex(-1);
    setHighlightedMemberSuggestionIndex(-1);
  }, [pathname, searchParamsString]);

  useEffect(() => {
    if (debugDisableFilterPopovers || debugStaticFilterShell) {
      setSymbolSuggestions([]);
      setHighlightedSymbolSuggestionIndex(-1);
      return;
    }
    const prefix = filters.symbol.trim();
    if (!prefix) {
      setSymbolSuggestions([]);
      setHighlightedSymbolSuggestionIndex(-1);
      return;
    }

    const requestId = suggestionsRequestRef.current + 1;
    suggestionsRequestRef.current = requestId;

    const handle = window.setTimeout(async () => {
      setIsSuggestingSymbol(true);
      try {
        const response = await suggestSymbols(prefix, filters.feedMode, 10);
        if (suggestionsRequestRef.current !== requestId) return;
        setSymbolSuggestions(response.items);
        setHighlightedSymbolSuggestionIndex(response.items.length > 0 ? 0 : -1);
      } catch {
        if (suggestionsRequestRef.current !== requestId) return;
        setSymbolSuggestions([]);
        setHighlightedSymbolSuggestionIndex(-1);
      } finally {
        if (suggestionsRequestRef.current === requestId) {
          setIsSuggestingSymbol(false);
        }
      }
    }, symbolSuggestDebounceMs);

    return () => window.clearTimeout(handle);
  }, [debugDisableFilterPopovers, debugStaticFilterShell, filters.symbol, filters.feedMode]);

  useEffect(() => {
    if (debugDisableFilterPopovers || debugStaticFilterShell) {
      setMemberSuggestions([]);
      setHighlightedMemberSuggestionIndex(-1);
      return;
    }
    const memberPrefix = filters.member.trim().toLowerCase();
    if (!memberPrefix) {
      setMemberSuggestions([]);
      setHighlightedMemberSuggestionIndex(-1);
      return;
    }

    const suggestions = members
      .filter((name) => name.toLowerCase().includes(memberPrefix))
      .slice(0, 10);
    setMemberSuggestions(suggestions);
    setHighlightedMemberSuggestionIndex(suggestions.length > 0 ? 0 : -1);
  }, [debugDisableFilterPopovers, debugStaticFilterShell, filters.member, members]);

  const buildParams = (nextFilters: FilterState) => {
    const params = new URLSearchParams(searchParamsString);
    const managedKeys = [
      "mode",
    "tape",
      "symbol",
      "min_amount",
      "max_amount",
      "recent_days",
      "member",
      "chamber",
      "party",
      "trade_type",
      "role",
      "department",
      "whale",
    ] as const;

    managedKeys.forEach((key) => params.delete(key));

    params.set("mode", nextFilters.feedMode);
    params.delete("tape");
    if (nextFilters.symbol) params.set("symbol", nextFilters.symbol);
    if (nextFilters.minAmount) params.set("min_amount", nextFilters.minAmount);
    if (nextFilters.maxAmount) params.set("max_amount", nextFilters.maxAmount);
    if (nextFilters.recentDays) params.set("recent_days", nextFilters.recentDays);

    if (nextFilters.tradeType) params.set("trade_type", nextFilters.tradeType);

    if (nextFilters.feedMode === "congress") {
      if (nextFilters.member) params.set("member", nextFilters.member);
      if (nextFilters.chamber) params.set("chamber", nextFilters.chamber);
      if (nextFilters.party) params.set("party", nextFilters.party);
    }

    if (nextFilters.feedMode === "insider") {
      if (nextFilters.role) params.set("role", nextFilters.role);
    }

    if (nextFilters.feedMode === "government_contracts" && nextFilters.department) {
      params.set("department", nextFilters.department);
    }

    params.set("whale", nextFilters.whale);

    return params;
  };

  useEffect(() => {
    if (debugDisableFilterSync || debugStaticFilterShell) return;
    if (pendingNavigationRef.current && lastRequestedSearchRef.current === searchParamsString) {
      pendingNavigationRef.current = false;
      logFeedSync("replace settled via searchParams catch-up", {
        search: searchParamsString,
      });
    }
    clearDebouncedSync("pathname/searchParams changed");
  }, [debugDisableFilterSync, debugStaticFilterShell, pathname, searchParamsString]);

  useEffect(() => {
    if (debugDisableFilterSync || debugStaticFilterShell) return;
    clearDebouncedSync("filters changed");

    if (filtersEqual(filters, initialFilters)) return;
    if (pendingNavigationRef.current) {
      logFeedSync("skipped debounce schedule (navigation pending)", {
        reason: "debounced-filters-change",
        pendingSearch: lastRequestedSearchRef.current,
      });
      return;
    }

    logFeedSync("debounce scheduled", { reason: "debounced-filters-change", delayMs: debounceMs });
    debounceHandleRef.current = window.setTimeout(() => {
      debounceHandleRef.current = null;
      if (pendingNavigationRef.current) {
        logFeedSync("skipped router.replace (navigation pending)", {
          reason: "debounced-filters-change",
          pendingSearch: lastRequestedSearchRef.current,
        });
        return;
      }

      const params = buildParams(filters);
      params.delete("cursor");
      params.delete("cursor_stack");
      params.delete("page");
      params.delete("offset");
      const hash = typeof window !== "undefined" ? window.location.hash : "";
      const nextSearch = params.toString();
      const currentSearch = searchParamsString;
      const currentUrl = `${pathname}${currentSearch ? `?${currentSearch}` : ""}${hash}`;
      const nextUrl = `${pathname}${nextSearch ? `?${nextSearch}` : ""}${hash}`;

      if (nextSearch === currentSearch) {
        logFeedSync("skipped router.replace (already equivalent)", {
          reason: "debounced-filters-change",
          current: currentUrl,
          next: nextUrl,
        });
        return;
      }

      pendingNavigationRef.current = true;
      lastRequestedSearchRef.current = nextSearch;
      logFeedSync("replace starts", {
        reason: "debounced-filters-change",
        current: currentUrl,
        next: nextUrl,
      });
      logFilterDebug("router.replace from FeedFilters", {
        reason: "debounced-filters-change",
        nextUrl,
      });
      router.replace(nextUrl, { scroll: false });
    }, debounceMs);

    return () => clearDebouncedSync("effect cleanup");
  }, [debugDisableFilterSync, debugStaticFilterShell, filters, initialFilters, pathname, router, searchParamsString]);

  useEffect(() => {
    if (debugDisableFilterSync || debugStaticFilterShell) return;
    const handle = window.setTimeout(() => {
      window.sessionStorage.setItem(filtersSessionKey, JSON.stringify(filters));
    }, 100);

    return () => window.clearTimeout(handle);
  }, [debugDisableFilterSync, debugStaticFilterShell, filters]);

  useEffect(() => {
    if (debugDisableFilterPopovers || debugDisableFilterListeners || debugStaticFilterShell) {
      logFilterDebug("document pointerdown listener skipped", {
        debugDisableFilterPopovers,
        debugDisableFilterListeners,
        debugStaticFilterShell,
      });
      return;
    }
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (debugInteractivity) {
        const path = event.composedPath().map((entry) => {
          if (!(entry instanceof Element)) return String(entry);
          const id = entry.id ? `#${entry.id}` : "";
          const className = entry.className && typeof entry.className === "string" ? `.${entry.className.trim().replace(/\s+/g, ".")}` : "";
          return `${entry.tagName.toLowerCase()}${id}${className}`;
        });
        logFilterDebug("pointerdown capture", {
          target:
            target instanceof Element
              ? {
                  tag: target.tagName.toLowerCase(),
                  id: target.id || null,
                  className: target.className || null,
                }
              : { value: String(target) },
          path,
        });
      }

      if (showSymbolSuggestions && symbolFieldRef.current && !symbolFieldRef.current.contains(target)) {
        logFilterDebug("autosuggest close", { type: "symbol", reason: "outside-pointerdown" });
        setShowSymbolSuggestions(false);
        setHighlightedSymbolSuggestionIndex(-1);
      }
      if (showMemberSuggestions && memberFieldRef.current && !memberFieldRef.current.contains(target)) {
        logFilterDebug("autosuggest close", { type: "member", reason: "outside-pointerdown" });
        setShowMemberSuggestions(false);
        setHighlightedMemberSuggestionIndex(-1);
      }
    };

    logFilterDebug("document pointerdown listener register", { capture: true });
    document.addEventListener("pointerdown", onPointerDown, true);
    return () => {
      logFilterDebug("document pointerdown listener cleanup", { capture: true });
      document.removeEventListener("pointerdown", onPointerDown, true);
    };
  }, [debugDisableFilterListeners, debugDisableFilterPopovers, debugInteractivity, debugStaticFilterShell, showMemberSuggestions, showSymbolSuggestions]);

  useEffect(() => {
    if (!debugInteractivity) return;

    const logLargeLayers = () => {
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const offenders = Array.from(document.querySelectorAll<HTMLElement>("body *"))
        .map((el) => {
          const style = window.getComputedStyle(el);
          if (!(style.position === "fixed" || style.position === "absolute")) return null;
          const z = Number.parseInt(style.zIndex || "0", 10);
          if (Number.isNaN(z) || z <= 0) return null;
          const rect = el.getBoundingClientRect();
          const coverage = (Math.max(0, rect.width) * Math.max(0, rect.height)) / (vw * vh);
          if (coverage < 0.65) return null;
          return {
            tag: el.tagName.toLowerCase(),
            id: el.id || null,
            className: el.className || null,
            zIndex: style.zIndex,
            position: style.position,
            coverage: Number(coverage.toFixed(2)),
            pointerEvents: style.pointerEvents,
          };
        })
        .filter(Boolean);

      if (offenders.length > 0) {
        console.warn("[feed-debug] large overlay candidates", offenders);
      }
    };

    const onClickCapture = (event: MouseEvent) => {
      const el = document.elementFromPoint(event.clientX, event.clientY);
      if (!el) return;
      const style = window.getComputedStyle(el);
      console.log("[feed-debug] click target", {
        x: event.clientX,
        y: event.clientY,
        tag: el.tagName.toLowerCase(),
        id: (el as HTMLElement).id || null,
        className: (el as HTMLElement).className || null,
        zIndex: style.zIndex,
        pointerEvents: style.pointerEvents,
        position: style.position,
      });
    };

    logLargeLayers();
    document.addEventListener("click", onClickCapture, true);
    window.addEventListener("resize", logLargeLayers);
    return () => {
      document.removeEventListener("click", onClickCapture, true);
      window.removeEventListener("resize", logLargeLayers);
    };
  }, [debugInteractivity]);

  const update =
    (key: keyof FilterState) => (event: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
      const value = event.target.value;
      setFilters((current) => ({ ...current, [key]: value }));
    };

  const setMode = (mode: FeedMode) => {
    if (debugDisableFilterPills || debugStaticFilterShell) return;
    setFilters((current) => clearHiddenFilters(mode, { ...current, feedMode: mode }));
  };

  const onReset = () => {
    setFilters({
      feedMode: "all",
      symbol: "",
      minAmount: "",
      maxAmount: "",
      recentDays: "",
      member: "",
      chamber: "",
      party: "",
      tradeType: "",
      role: "",
      department: "",
      whale: "off",
    });
    setShowSymbolSuggestions(false);
    setShowMemberSuggestions(false);
  };

  const selectSymbolSuggestion = (suggestion: SymbolSuggestion) => {
    setFilters((current) => ({ ...current, symbol: suggestion.symbol }));
    setShowSymbolSuggestions(false);
    setHighlightedSymbolSuggestionIndex(-1);
    logFilterDebug("autosuggest close", { type: "symbol", reason: "select" });

    if (debugDisableFilterSync || debugStaticFilterShell) {
      return;
    }

    const params = buildParams({ ...filters, symbol: suggestion.symbol });
    const hash = typeof window !== "undefined" ? window.location.hash : "";
    params.delete("offset");
    const nextSearch = params.toString();
    const currentSearch = searchParamsString;
    const currentUrl = `${pathname}${currentSearch ? `?${currentSearch}` : ""}${hash}`;
    const nextUrl = `${pathname}${nextSearch ? `?${nextSearch}` : ""}${hash}`;

    if (nextSearch === currentSearch) {
      logFeedSync("skipped router.replace (already equivalent)", {
        reason: "symbol-suggestion-select",
        current: currentUrl,
        next: nextUrl,
      });
      return;
    }

    if (pendingNavigationRef.current) {
      logFeedSync("skipped router.replace (navigation pending)", {
        reason: "symbol-suggestion-select",
        pendingSearch: lastRequestedSearchRef.current,
      });
      return;
    }

    pendingNavigationRef.current = true;
    lastRequestedSearchRef.current = nextSearch;
    logFeedSync("replace starts", {
      reason: "symbol-suggestion-select",
      current: currentUrl,
      next: nextUrl,
    });
    logFilterDebug("router.replace from FeedFilters", {
      reason: "symbol-suggestion-select",
      nextUrl,
    });
    router.replace(nextUrl, { scroll: false });
  };

  const onSymbolKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (!showSymbolSuggestions || symbolSuggestions.length === 0) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlightedSymbolSuggestionIndex((current) => (current + 1) % symbolSuggestions.length);
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlightedSymbolSuggestionIndex((current) => (current <= 0 ? symbolSuggestions.length - 1 : current - 1));
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      const index = highlightedSymbolSuggestionIndex >= 0 ? highlightedSymbolSuggestionIndex : 0;
      const suggestion = symbolSuggestions[index];
      if (suggestion) {
        selectSymbolSuggestion(suggestion);
      }
      return;
    }

    if (event.key === "Escape") {
      setShowSymbolSuggestions(false);
      setHighlightedSymbolSuggestionIndex(-1);
    }
  };

  const selectMemberSuggestion = (member: string) => {
    setFilters((current) => ({ ...current, member }));
    setShowMemberSuggestions(false);
    setHighlightedMemberSuggestionIndex(-1);
    logFilterDebug("autosuggest close", { type: "member", reason: "select" });
  };

  const useMinimalPills = debugDisableFilterPills || debugFilterNoPillComponent;
  const wrapperClassName = debugFilterNoCardShell ? "space-y-4" : `${cardClassName} space-y-4`;
  const renderPill = (
    key: string,
    label: string,
    active: boolean,
    onClick?: () => void,
  ) => {
    if (!useMinimalPills) {
      return (
        <FilterPill key={key} active={active} onClick={onClick}>
          {label}
        </FilterPill>
      );
    }

    if (onClick) {
      return (
        <button
          key={key}
          type="button"
          onClick={onClick}
          className={`rounded border px-2 py-1 text-xs ${active ? "border-emerald-500 text-emerald-300" : "border-slate-700 text-slate-300"}`}
        >
          {label}
        </button>
      );
    }

    return (
      <span key={key} className={`rounded border px-2 py-1 text-xs ${active ? "border-emerald-500 text-emerald-300" : "border-slate-700 text-slate-300"}`}>
        {label}
      </span>
    );
  };

  if (debugStaticFilterShell) {
    return (
      <>
        <FeedMountLogger name="FeedFilters" enabled={debugLifecycle} detail={{ resultsCount: resultsCount ?? null }} />
        <section className={wrapperClassName}>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-lg font-semibold text-white">Feed mode & filters</h2>
              <p className="text-sm text-slate-400">debug_static_filter_shell=1 (static diagnostic shell)</p>
            </div>
            <button type="button" disabled className={ghostButtonClassName}>
              Reset
            </button>
          </div>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex flex-wrap gap-2">
              {renderPill("all", "All", true)}
              {renderPill("congress", "Congress", false)}
              {renderPill("insider", "Insider", false)}
              {renderPill("government_contracts", "Government Contracts", false)}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Whale mode</span>
              {renderPill("off", "Off", true)}
              {renderPill("500k", "$500K+", false)}
              {renderPill("1m", "$1M+", false)}
              {renderPill("5m", "$5M+", false)}
            </div>
          </div>
          {debugFilterNoControls ? (
            <div className="text-sm text-slate-400">debug_filter_no_controls=1 (labels/text only)</div>
          ) : (
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
                <input className={inputClassName} value="" readOnly placeholder="NVDA" />
              </div>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
                <input className={inputClassName} value="" readOnly placeholder="250000" />
              </div>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
                <select className={selectClassName} value="" disabled>
                  <option value="">Anytime</option>
                </select>
              </div>
            </div>
          )}
        </section>
      </>
    );
  }

  const onMemberKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (!showMemberSuggestions || memberSuggestions.length === 0) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      setHighlightedMemberSuggestionIndex((current) => (current + 1) % memberSuggestions.length);
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      setHighlightedMemberSuggestionIndex((current) => (current <= 0 ? memberSuggestions.length - 1 : current - 1));
      return;
    }

    if (event.key === "Enter") {
      event.preventDefault();
      const index = highlightedMemberSuggestionIndex >= 0 ? highlightedMemberSuggestionIndex : 0;
      const suggestion = memberSuggestions[index];
      if (suggestion) {
        selectMemberSuggestion(suggestion);
      }
      return;
    }

    if (event.key === "Escape") {
      setShowMemberSuggestions(false);
      setHighlightedMemberSuggestionIndex(-1);
    }
  };

  if (debugFilterMinimalBox) {
    return (
      <>
        <FeedMountLogger name="FeedFilters" enabled={debugLifecycle} detail={{ resultsCount: resultsCount ?? null }} />
        <div className="border border-slate-700 p-3 text-sm text-slate-300">debug_filter_minimal_box=1</div>
      </>
    );
  }

  if (debugFilterTextOnly) {
    return (
      <>
        <FeedMountLogger name="FeedFilters" enabled={debugLifecycle} detail={{ resultsCount: resultsCount ?? null }} />
        <section>
          <p className="text-sm text-slate-300">debug_filter_text_only=1</p>
          <p className="text-sm text-slate-400">Feed mode and filter diagnostics (plain text only).</p>
        </section>
      </>
    );
  }

  return (
    <>
      <FeedMountLogger name="FeedFilters" enabled={debugLifecycle} detail={{ resultsCount: resultsCount ?? null }} />

    <section className={wrapperClassName}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Feed mode & filters</h2>
          {typeof resultsCount === "number" ? (
            <p className="text-sm text-slate-400">{resultsCount} results in current view.</p>
          ) : null}
        </div>
        <button type="button" onClick={onReset} className={ghostButtonClassName}>
          Reset
        </button>
      </div>

      <SavedViewsBar
        surface="feed"
        restoreOnLoad={true}
        defaultParams={{ mode: filters.feedMode }}
        paramKeys={[
          "mode",
          "symbol",
          "min_amount",
          "max_amount",
          "recent_days",
          "member",
          "chamber",
          "party",
          "trade_type",
          "role",
          "department",
          "ownership",
          "whale",
          "limit",
          "page_size",
        ]}
      />

      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap gap-2">
          {([
            ["all", "All"],
            ["congress", "Congress"],
            ["insider", "Insider"],
            ["government_contracts", "Government Contracts"],
          ] as const).map(([value, label]) => (
            renderPill(value, label, filters.feedMode === value, () => setMode(value))
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Whale mode</span>
          {([
            ["off", "Off"],
            ["500k", "$500K+"],
            ["1m", "$1M+"],
            ["5m", "$5M+"],
          ] as const).map(([value, label]) => (
            renderPill(
              value,
              label,
              filters.whale === value,
              debugDisableFilterPills ? undefined : () => setFilters((current) => ({ ...current, whale: value }))
            )
          ))}
        </div>
      </div>

      {debugFilterNoControls ? (
        <div className="grid gap-2.5 md:grid-cols-2">
          <div className="text-sm text-slate-300">debug_filter_no_controls=1</div>
          <div className="text-sm text-slate-400">Symbol / Min amount / Recent days</div>
        </div>
      ) : (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <div className="relative" ref={symbolFieldRef}>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
            <input
              className={controlClassName(inputClassName, filters.symbol)}
              value={filters.symbol}
              onChange={update("symbol")}
              onFocus={
                debugDisableFilterPopovers
                  ? undefined
                  : () => {
                      logFilterDebug("autosuggest open", { type: "symbol", reason: "focus" });
                      setShowSymbolSuggestions(true);
                    }
              }
              onBlur={
                debugDisableFilterPopovers
                  ? undefined
                  : () =>
                      window.setTimeout(() => {
                        logFilterDebug("autosuggest close", { type: "symbol", reason: "blur" });
                        setShowSymbolSuggestions(false);
                      }, 120)
              }
              onKeyDown={onSymbolKeyDown}
              placeholder="NVDA"
              autoComplete="off"
            />
            {!debugDisableFilterPopovers && showSymbolSuggestions && (symbolSuggestions.length > 0 || isSuggestingSymbol) ? (
              <div className="pointer-events-none absolute left-0 top-full z-20 mt-1 w-full">
                <div className="pointer-events-auto max-h-52 overflow-y-auto rounded-md border border-slate-700 bg-slate-900 shadow-xl">
                {isSuggestingSymbol && symbolSuggestions.length === 0 ? (
                  <div className="px-3 py-2 text-sm text-slate-400">Loading…</div>
                ) : (
                  symbolSuggestions.map((suggestion, index) => (
                    <button
                      key={suggestion.symbol}
                      type="button"
                      className={`w-full px-3 py-2 text-left text-sm ${index === highlightedSymbolSuggestionIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"}`}
                      onMouseDown={(event) => event.preventDefault()}
                      onClick={() => selectSymbolSuggestion(suggestion)}
                    >
                      <div className="font-medium text-white">{suggestion.symbol}</div>
                      {suggestion.name ? <div className="text-xs text-slate-400">{suggestion.name}</div> : null}
                    </button>
                  ))
                )}
                </div>
              </div>
            ) : null}
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
            <input className={controlClassName(inputClassName, filters.minAmount)} value={filters.minAmount} onChange={update("minAmount")} placeholder="250000" />
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Max amount</label>
            <input className={controlClassName(inputClassName, filters.maxAmount)} value={filters.maxAmount} onChange={update("maxAmount")} placeholder="5000000" />
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
            <select className={controlClassName(selectClassName, filters.recentDays)} value={filters.recentDays} onChange={update("recentDays")}>
              <option value="">Anytime</option>
              <option value="1">1 day</option>
              <option value="7">7 days</option>
              <option value="30">30 days</option>
              <option value="90">90 days</option>
            </select>
          </div>
        </div>
      )}

      {filters.feedMode === "congress" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
          {debugFilterNoControls ? (
            <div className="text-sm text-slate-400">Congress fields: Member / Chamber / Party / Trade Type</div>
          ) : (
            <>
              <div className="relative" ref={memberFieldRef}>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
                <input
                  className={controlClassName(inputClassName, filters.member)}
                  value={filters.member}
                  onChange={update("member")}
                  onFocus={
                    debugDisableFilterPopovers
                      ? undefined
                      : () => {
                          logFilterDebug("autosuggest open", { type: "member", reason: "focus" });
                          setShowMemberSuggestions(true);
                        }
                  }
                  onBlur={
                    debugDisableFilterPopovers
                      ? undefined
                      : () =>
                          window.setTimeout(() => {
                            logFilterDebug("autosuggest close", { type: "member", reason: "blur" });
                            setShowMemberSuggestions(false);
                          }, 120)
                  }
                  onKeyDown={onMemberKeyDown}
                  placeholder="Pelosi"
                  autoComplete="off"
                />
                {!debugDisableFilterPopovers && showMemberSuggestions && memberSuggestions.length > 0 ? (
                  <div className="pointer-events-none absolute left-0 top-full z-20 mt-1 w-full">
                    <div className="pointer-events-auto max-h-52 overflow-y-auto rounded-md border border-slate-700 bg-slate-900 shadow-xl">
                      {memberSuggestions.map((member, index) => (
                        <button
                          key={`${member}-${index}`}
                          type="button"
                          className={`w-full px-3 py-2 text-left text-sm ${index === highlightedMemberSuggestionIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"}`}
                          onMouseDown={(event) => event.preventDefault()}
                          onClick={() => selectMemberSuggestion(member)}
                        >
                          {member}
                        </button>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
                <select className={controlClassName(selectClassName, filters.chamber)} value={filters.chamber} onChange={update("chamber")}>
                  <option value="">All chambers</option>
                  <option value="house">House</option>
                  <option value="senate">Senate</option>
                </select>
              </div>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Party</label>
                <select className={controlClassName(selectClassName, filters.party)} value={filters.party} onChange={update("party")}>
                  <option value="">All parties</option>
                  <option value="democrat">Democrat</option>
                  <option value="republican">Republican</option>
                  <option value="independent">Independent</option>
                </select>
              </div>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
                <select className={controlClassName(selectClassName, filters.tradeType)} value={filters.tradeType} onChange={update("tradeType")}>
                  <option value="">All types</option>
                  <option value="purchase">Purchase</option>
                  <option value="sale">Sale</option>
                </select>
              </div>
            </>
          )}
        </div>
      ) : null}

      {filters.feedMode === "insider" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
          {debugFilterNoControls ? (
            <div className="text-sm text-slate-400">Insider fields: Trade Type / Role</div>
          ) : (
            <>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
                <select className={controlClassName(selectClassName, filters.tradeType)} value={filters.tradeType} onChange={update("tradeType")}>
                  <option value="">All types</option>
                  <option value="purchase">Purchase</option>
                  <option value="sale">Sale</option>
                </select>
              </div>
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Role</label>
                <input className={controlClassName(inputClassName, filters.role)} value={filters.role} onChange={update("role")} placeholder="CEO" />
              </div>
            </>
          )}
        </div>
      ) : null}

      {filters.feedMode === "all" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
          {debugFilterNoControls ? (
            <div className="text-sm text-slate-400">All mode fields: Trade Type</div>
          ) : (
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
              <select className={controlClassName(selectClassName, filters.tradeType)} value={filters.tradeType} onChange={update("tradeType")}>
                <option value="">All types</option>
                <option value="purchase">Purchase</option>
                <option value="sale">Sale</option>
              </select>
            </div>
          )}
        </div>
      ) : null}

      {filters.feedMode === "government_contracts" ? (
        <div className="grid gap-3 md:grid-cols-2 border-t border-slate-800 pt-4">
          {debugFilterNoControls ? (
            <div className="text-sm text-slate-400">Government Contract fields: Department</div>
          ) : (
            <div>
              <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Department</label>
              <select className={controlClassName(selectClassName, filters.department)} value={filters.department} onChange={update("department")}>
                {departmentOptions.map(([value, label]) => (
                  <option key={value || "any"} value={value}>
                    {label}
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>
      ) : null}
    </section>
    </>
  );
}
