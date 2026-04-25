"use client";

import { suggestSymbols, type SymbolSuggestion } from "@/lib/api";
import { useEffect, useRef, useState } from "react";

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 200;

type Props = {
  tickers: string[];
  onChange: (tickers: string[]) => void;
  disabled?: boolean;
  limit?: number;
};

function normalizeTicker(value: string) {
  return value.trim().toUpperCase();
}

function mergeTickers(current: string[], nextValues: string[], limit: number) {
  const merged = [...current];
  for (const value of nextValues) {
    const symbol = normalizeTicker(value);
    if (!symbol || merged.includes(symbol)) continue;
    if (merged.length >= limit) break;
    merged.push(symbol);
  }
  return merged;
}

export function TickerMultiAutosuggest({ tickers, onChange, disabled = false, limit = 25 }: Props) {
  const [query, setQuery] = useState("");
  const [suggestions, setSuggestions] = useState<SymbolSuggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const debounceRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);

  const canAddMore = tickers.length < limit;
  const hasSuggestions = suggestions.length > 0;

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
        const next = response.items.filter((item) => !tickers.includes(item.symbol));
        setSuggestions(next);
        setHighlightedIndex(next.length > 0 ? 0 : -1);
        setOpen(next.length > 0);
      } catch {
        if (requestIdRef.current !== requestId) return;
        setSuggestions([]);
        setHighlightedIndex(-1);
        setOpen(false);
      } finally {
        if (requestIdRef.current === requestId) setLoading(false);
      }
    }, DEBOUNCE_MS);

    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
  }, [canAddMore, query, tickers]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (!rootRef.current?.contains(target)) setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  function commitSymbols(values: string[]) {
    const next = mergeTickers(tickers, values, limit);
    if (next.length === tickers.length) return;
    onChange(next);
    setQuery("");
    setSuggestions([]);
    setOpen(false);
    setHighlightedIndex(-1);
  }

  function selectSuggestion(suggestion: SymbolSuggestion) {
    commitSymbols([suggestion.symbol]);
    inputRef.current?.focus();
  }

  function removeTicker(symbol: string) {
    onChange(tickers.filter((ticker) => ticker !== symbol));
    inputRef.current?.focus();
  }

  function handlePaste(rawValue: string) {
    const tokens = rawValue
      .split(/[\s,]+/)
      .map((value) => value.trim())
      .filter(Boolean);
    if (tokens.length < 2) return false;
    commitSymbols(tokens);
    return true;
  }

  return (
    <div ref={rootRef} className="relative">
      <div className="rounded-2xl border border-white/10 bg-slate-950/50 px-3 py-2 focus-within:border-white/20">
        <div className="flex flex-wrap items-center gap-2">
          {tickers.map((ticker) => (
            <span key={ticker} className="inline-flex items-center gap-1 rounded-md border border-white/10 bg-white/[0.04] px-2 py-1 text-sm text-slate-200">
              <span>{ticker}</span>
              <button
                type="button"
                onClick={() => removeTicker(ticker)}
                className="text-slate-400 transition hover:text-white"
                aria-label={`Remove ${ticker}`}
                disabled={disabled}
              >
                x
              </button>
            </span>
          ))}
          <input
            ref={inputRef}
            value={query}
            onChange={(event) => setQuery(event.target.value.toUpperCase())}
            onFocus={() => {
              if (hasSuggestions) setOpen(true);
            }}
            onPaste={(event) => {
              if (!handlePaste(event.clipboardData.getData("text"))) return;
              event.preventDefault();
            }}
            onKeyDown={(event) => {
              if (event.key === "Backspace" && !query && tickers.length > 0) {
                onChange(tickers.slice(0, -1));
                return;
              }
              if (event.key === "Escape") {
                setOpen(false);
                return;
              }
              if ((event.key === "," || event.key === "Enter") && handlePaste(query)) {
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
            placeholder={canAddMore ? "Search tickers" : `Up to ${limit} tickers`}
            className="min-w-[10rem] flex-1 bg-transparent py-1 text-sm text-white outline-none placeholder:text-slate-500 disabled:cursor-not-allowed"
            disabled={disabled || !canAddMore}
            autoComplete="off"
            aria-label="Tickers"
          />
        </div>
      </div>

      {open || (loading && query.trim().length >= MIN_QUERY_LENGTH) ? (
        <div className="absolute z-20 mt-1 w-full overflow-hidden rounded-xl border border-white/15 bg-slate-950/95 shadow-xl shadow-black/30">
          {loading && !hasSuggestions ? <div className="px-3 py-2 text-sm text-slate-400">Searching...</div> : null}
          {!loading && hasSuggestions
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
