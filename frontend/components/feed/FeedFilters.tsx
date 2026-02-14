"use client";

import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import type { ChangeEvent, KeyboardEvent } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { cardClassName, ghostButtonClassName, inputClassName, pillClassName, selectClassName } from "@/lib/styles";
import { suggestSymbols } from "@/lib/api";
import type { EventItem } from "@/lib/api";

const debounceMs = 350;
const symbolSuggestDebounceMs = 200;
const filtersSessionKey = "ct:feedFilters";

type FeedMode = "congress" | "insider" | "all";

type FilterState = {
  tape: FeedMode;
  symbol: string;
  minAmount: string;
  recentDays: string;
  member: string;
  chamber: string;
  party: string;
  tradeType: string;
  role: string;
};

type FeedFiltersProps = {
  events: EventItem[];
  resultsCount: number;
};

function filtersEqual(a: FilterState, b: FilterState): boolean {
  return (
    a.tape === b.tape &&
    a.symbol === b.symbol &&
    a.minAmount === b.minAmount &&
    a.recentDays === b.recentDays &&
    a.member === b.member &&
    a.chamber === b.chamber &&
    a.party === b.party &&
    a.tradeType === b.tradeType &&
    a.role === b.role
  );
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
    "tape",
    "symbol",
    "min_amount",
    "recent_days",
    "member",
    "chamber",
    "party",
    "trade_type",
    "transaction_type",
    "role",
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

export function FeedFilters({ events, resultsCount }: FeedFiltersProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [symbolSuggestions, setSymbolSuggestions] = useState<string[]>([]);
  const [isSuggestingSymbol, setIsSuggestingSymbol] = useState(false);
  const [highlightedSymbolSuggestionIndex, setHighlightedSymbolSuggestionIndex] = useState(-1);
  const [showSymbolSuggestions, setShowSymbolSuggestions] = useState(false);
  const [memberSuggestions, setMemberSuggestions] = useState<string[]>([]);
  const [showMemberSuggestions, setShowMemberSuggestions] = useState(false);
  const [highlightedMemberSuggestionIndex, setHighlightedMemberSuggestionIndex] = useState(-1);
  const suggestionsRequestRef = useRef(0);

  const initialFilters = useMemo<FilterState>(() => {
    const params = new URLSearchParams(searchParams.toString());
    const stored =
      typeof window !== "undefined" && !hasUrlManagedParams(params)
        ? parseStoredFilters(window.sessionStorage.getItem(filtersSessionKey))
        : null;
    const tape = normalizeValue(searchParams.get("tape"));
    const storedTape = normalizeValue(stored?.tape ?? "");
    const tapeValue = tape || storedTape;
    const mode: FeedMode = tapeValue === "congress" || tapeValue === "insider" || tapeValue === "all" ? tapeValue : "all";
    const tradeType = normalizeTradeType(
      normalizeValue(searchParams.get("trade_type")) ||
        normalizeValue(searchParams.get("transaction_type")) ||
        normalizeValue(stored?.tradeType ?? "")
    );

    return {
      tape: mode,
      symbol: normalizeValue(searchParams.get("symbol")) || normalizeValue(stored?.symbol ?? ""),
      minAmount: normalizeValue(searchParams.get("min_amount")) || normalizeValue(stored?.minAmount ?? ""),
      recentDays: normalizeValue(searchParams.get("recent_days")) || normalizeValue(stored?.recentDays ?? ""),
      member: normalizeValue(searchParams.get("member")) || normalizeValue(stored?.member ?? ""),
      chamber: normalizeValue(searchParams.get("chamber")) || normalizeValue(stored?.chamber ?? ""),
      party: normalizeValue(searchParams.get("party")) || normalizeValue(stored?.party ?? ""),
      tradeType,
      role: normalizeValue(searchParams.get("role")) || normalizeValue(stored?.role ?? ""),
    };
  }, [searchParams]);

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
    setFilters(initialFilters);
  }, [initialFilters]);

  useEffect(() => {
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
        const response = await suggestSymbols(prefix, filters.tape, 10);
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
  }, [filters.symbol, filters.tape]);

  useEffect(() => {
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
  }, [filters.member, members]);

  const buildParams = (nextFilters: FilterState) => {
    const params = new URLSearchParams(searchParams.toString());
    const managedKeys = [
      "tape",
      "symbol",
      "min_amount",
      "recent_days",
      "member",
      "chamber",
      "party",
      "trade_type",
      "transaction_type",
      "role",
    ] as const;

    managedKeys.forEach((key) => params.delete(key));

    params.set("tape", nextFilters.tape);
    if (nextFilters.symbol) params.set("symbol", nextFilters.symbol);
    if (nextFilters.minAmount) params.set("min_amount", nextFilters.minAmount);
    if (nextFilters.recentDays) params.set("recent_days", nextFilters.recentDays);

    if (nextFilters.tape === "congress") {
      if (nextFilters.member) params.set("member", nextFilters.member);
      if (nextFilters.chamber) params.set("chamber", nextFilters.chamber);
      if (nextFilters.party) params.set("party", nextFilters.party);
      if (nextFilters.tradeType) params.set("trade_type", nextFilters.tradeType);
    }

    if (nextFilters.tape === "insider") {
      if (nextFilters.tradeType) {
        const insiderTradeType = nextFilters.tradeType === "purchase" ? "p-purchase" : "s-sale";
        params.set("trade_type", insiderTradeType);
      }
      if (nextFilters.role) params.set("role", nextFilters.role);
    }

    return params;
  };

  useEffect(() => {
    const handle = window.setTimeout(() => {
      if (filtersEqual(filters, initialFilters)) return;

      const params = buildParams(filters);
      params.delete("cursor");
      params.delete("cursor_stack");
      const hash = typeof window !== "undefined" ? window.location.hash : "";
      startTransition(() => router.replace(`/?${params.toString()}${hash}`, { scroll: false }));
    }, debounceMs);
    return () => window.clearTimeout(handle);
  }, [filters, initialFilters, router, startTransition]);

  useEffect(() => {
    const handle = window.setTimeout(() => {
      window.sessionStorage.setItem(filtersSessionKey, JSON.stringify(filters));
    }, 100);

    return () => window.clearTimeout(handle);
  }, [filters]);

  const update =
    (key: keyof FilterState) => (event: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
      const value = event.target.value;
      setFilters((current) => ({ ...current, [key]: value }));
    };

  const setMode = (mode: FeedMode) => {
    setFilters((current) => clearHiddenFilters(mode, { ...current, tape: mode }));
  };

  const onReset = () => {
    setFilters({
      tape: "all",
      symbol: "",
      minAmount: "",
      recentDays: "",
      member: "",
      chamber: "",
      party: "",
      tradeType: "",
      role: "",
    });
    setShowSymbolSuggestions(false);
    setShowMemberSuggestions(false);
  };

  const selectSymbolSuggestion = (symbol: string) => {
    setFilters((current) => ({ ...current, symbol }));
    setShowSymbolSuggestions(false);
    setHighlightedSymbolSuggestionIndex(-1);

    const params = buildParams({ ...filters, symbol });
    const hash = typeof window !== "undefined" ? window.location.hash : "";
    startTransition(() => router.replace(`/?${params.toString()}${hash}`));
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
  };

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

  return (
    <section className={`${cardClassName} space-y-4`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Feed mode & filters</h2>
          <p className="text-sm text-slate-400">{resultsCount} results in current view.</p>
        </div>
        <button type="button" onClick={onReset} className={ghostButtonClassName} disabled={isPending}>
          Reset
        </button>
      </div>

      <div className="flex flex-wrap gap-2">
        {([
          ["congress", "Congress"],
          ["insider", "Insider"],
          ["all", "All"],
        ] as const).map(([value, label]) => (
          <button
            key={value}
            type="button"
            className={`${pillClassName} ${filters.tape === value ? "border-emerald-500/60 text-emerald-200" : ""}`}
            onClick={() => setMode(value)}
          >
            {label}
          </button>
        ))}
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="relative">
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
          <input
            className={controlClassName(inputClassName, filters.symbol)}
            value={filters.symbol}
            onChange={update("symbol")}
            onFocus={() => setShowSymbolSuggestions(true)}
            onBlur={() => window.setTimeout(() => setShowSymbolSuggestions(false), 120)}
            onKeyDown={onSymbolKeyDown}
            placeholder="NVDA"
            autoComplete="off"
          />
          {showSymbolSuggestions && (symbolSuggestions.length > 0 || isSuggestingSymbol) ? (
            <div className="absolute z-20 mt-1 max-h-52 w-full overflow-y-auto rounded-md border border-slate-700 bg-slate-900 shadow-xl">
              {isSuggestingSymbol && symbolSuggestions.length === 0 ? (
                <div className="px-3 py-2 text-sm text-slate-400">Loading…</div>
              ) : (
                symbolSuggestions.map((symbol, index) => (
                  <button
                    key={symbol}
                    type="button"
                    className={`w-full px-3 py-2 text-left text-sm ${index === highlightedSymbolSuggestionIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"}`}
                    onMouseDown={(event) => event.preventDefault()}
                    onClick={() => selectSymbolSuggestion(symbol)}
                  >
                    {symbol}
                  </button>
                ))
              )}
            </div>
          ) : null}
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
          <input className={controlClassName(inputClassName, filters.minAmount)} value={filters.minAmount} onChange={update("minAmount")} placeholder="250000" />
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

      {filters.tape === "congress" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
          <div className="relative">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
            <input
              className={controlClassName(inputClassName, filters.member)}
              value={filters.member}
              onChange={update("member")}
              onFocus={() => setShowMemberSuggestions(true)}
              onBlur={() => window.setTimeout(() => setShowMemberSuggestions(false), 120)}
              onKeyDown={onMemberKeyDown}
              placeholder="Pelosi"
              autoComplete="off"
            />
            {showMemberSuggestions && memberSuggestions.length > 0 ? (
              <div className="absolute z-20 mt-1 max-h-52 w-full overflow-y-auto rounded-md border border-slate-700 bg-slate-900 shadow-xl">
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
              <option value="other">Other</option>
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
        </div>
      ) : null}

      {filters.tape === "insider" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
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
        </div>
      ) : null}

      {filters.tape === "all" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
            <select className={controlClassName(selectClassName, filters.tradeType)} value={filters.tradeType} onChange={update("tradeType")}>
              <option value="">All types</option>
              <option value="purchase">Purchase</option>
              <option value="sale">Sale</option>
            </select>
          </div>
        </div>
      ) : null}
    </section>
  );
}
