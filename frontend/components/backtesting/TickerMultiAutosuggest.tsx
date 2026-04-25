"use client";

import { suggestSymbols, type SymbolSuggestion } from "@/lib/api";
import { useEffect, useRef, useState } from "react";

const MIN_QUERY_LENGTH = 1;
const DEBOUNCE_MS = 200;

type Props = {
  selectedSymbols: string[];
  onAddSymbols: (symbols: SymbolSuggestion[]) => void;
  disabled?: boolean;
  limit?: number;
};

function normalizeTicker(value: string) {
  return value.trim().toUpperCase();
}

export function TickerMultiAutosuggest({ selectedSymbols, onAddSymbols, disabled = false, limit = 10 }: Props) {
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<SymbolSuggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const debounceRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);

  const selectedSet = new Set(selectedSymbols.map((symbol) => normalizeTicker(symbol)));
  const selectedKey = selectedSymbols.map((symbol) => normalizeTicker(symbol)).join("|");
  const canAddMore = selectedSymbols.length < limit;

  useEffect(() => {
    const trimmed = query.trim();
    if (debounceRef.current) window.clearTimeout(debounceRef.current);

    if (!trimmed || trimmed.length < MIN_QUERY_LENGTH || !canAddMore) {
      setSuggestions([]);
      setOpen(false);
      setLoading(false);
      setHighlightedIndex(-1);
      return;
    }

    debounceRef.current = window.setTimeout(async () => {
      const requestId = requestIdRef.current + 1;
      requestIdRef.current = requestId;
      setLoading(true);

      try {
        const response = await suggestSymbols(trimmed, "all", 10);
        if (requestIdRef.current !== requestId) return;
        const next = response.items.filter((item) => !selectedSet.has(normalizeTicker(item.symbol)));
        setSuggestions(next);
        setHighlightedIndex(next.length > 0 ? 0 : -1);
        setOpen(next.length > 0);
      } catch {
        if (requestIdRef.current !== requestId) return;
        setSuggestions([]);
        setOpen(false);
        setHighlightedIndex(-1);
      } finally {
        if (requestIdRef.current === requestId) setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
  }, [canAddMore, query, selectedKey]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (!rootRef.current?.contains(target)) setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  function commitSymbols(items: SymbolSuggestion[]) {
    const next: SymbolSuggestion[] = [];
    const seen = new Set(selectedSet);
    for (const item of items) {
      const symbol = normalizeTicker(item.symbol);
      if (!symbol || seen.has(symbol)) continue;
      seen.add(symbol);
      next.push({ ...item, symbol });
      if (selectedSymbols.length + next.length >= limit) break;
    }
    if (next.length === 0) return;
    onAddSymbols(next);
    setQuery("");
    setSuggestions([]);
    setOpen(false);
    setHighlightedIndex(-1);
    inputRef.current?.focus();
  }

  function selectSuggestion(suggestion: SymbolSuggestion) {
    commitSymbols([suggestion]);
  }

  function commitTypedValue(rawValue: string) {
    const parsed = rawValue
      .split(/[\s,]+/)
      .map((value) => normalizeTicker(value))
      .filter(Boolean)
      .map((symbol) => ({ symbol }));
    if (parsed.length === 0) return false;
    commitSymbols(parsed);
    return true;
  }

  return (
    <div ref={rootRef} className="relative">
      <input
        ref={inputRef}
        value={query}
        onChange={(event) => setQuery(event.target.value.toUpperCase())}
        onFocus={() => {
          if (suggestions.length > 0) setOpen(true);
        }}
        onPaste={(event) => {
          if (!commitTypedValue(event.clipboardData.getData("text"))) return;
          event.preventDefault();
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            setOpen(false);
            return;
          }
          if ((event.key === "Enter" || event.key === ",") && commitTypedValue(query)) {
            event.preventDefault();
            return;
          }
          if (!open || suggestions.length === 0) return;
          if (event.key === "ArrowDown") {
            event.preventDefault();
            setHighlightedIndex((current) => (current + 1) % suggestions.length);
            return;
          }
          if (event.key === "ArrowUp") {
            event.preventDefault();
            setHighlightedIndex((current) => (current <= 0 ? suggestions.length - 1 : current - 1));
            return;
          }
          if (event.key === "Enter" && highlightedIndex >= 0) {
            event.preventDefault();
            selectSuggestion(suggestions[highlightedIndex]);
          }
        }}
        placeholder={canAddMore ? "Search or paste tickers" : `Up to ${limit} tickers`}
        className="h-11 w-full rounded-2xl border border-white/10 bg-slate-950/50 px-3 text-sm text-white outline-none transition placeholder:text-slate-500 focus:border-white/20 disabled:cursor-not-allowed disabled:text-slate-500"
        disabled={disabled || !canAddMore}
        autoComplete="off"
        aria-label="Tickers"
      />

      {open || (loading && query.trim().length >= MIN_QUERY_LENGTH) ? (
        <div className="absolute z-20 mt-1 w-full overflow-hidden rounded-xl border border-white/15 bg-slate-950/95 shadow-xl shadow-black/30">
          {loading && suggestions.length === 0 ? <div className="px-3 py-2 text-sm text-slate-400">Searching...</div> : null}
          {!loading
            ? suggestions.map((suggestion, index) => (
                <button
                  key={`${suggestion.symbol}-${index}`}
                  type="button"
                  role="option"
                  aria-selected={index === highlightedIndex}
                  className={`block w-full px-3 py-2 text-left text-sm ${
                    index === highlightedIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"
                  }`}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => selectSuggestion(suggestion)}
                >
                  <div className="font-medium text-white">{suggestion.symbol}</div>
                  {suggestion.name ? <div className="text-xs text-slate-400">{suggestion.name}</div> : null}
                </button>
              ))
            : null}
        </div>
      ) : null}
    </div>
  );
}
