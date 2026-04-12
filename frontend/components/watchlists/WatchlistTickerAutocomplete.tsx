"use client";

import { useEffect, useRef, useState } from "react";
import { suggestSymbols } from "@/lib/api";
import { inputClassName } from "@/lib/styles";

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 200;

type Props = {
  value: string;
  onChange: (value: string) => void;
  onSelect: (value: string) => void;
  disabled?: boolean;
};

export function WatchlistTickerAutocomplete({ value, onChange, onSelect, disabled = false }: Props) {
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const debounceRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const query = value.trim();
    if (debounceRef.current) window.clearTimeout(debounceRef.current);

    if (query.length < MIN_QUERY_LENGTH) {
      setSuggestions([]);
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
        const response = await suggestSymbols(query, "all", 10);
        if (requestIdRef.current !== requestId) return;
        const next = Array.isArray(response.items) ? response.items : [];
        setSuggestions(next);
        setHighlightedIndex(next.length > 0 ? 0 : -1);
        setOpen(true);
      } catch {
        if (requestIdRef.current !== requestId) return;
        setSuggestions([]);
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
  }, [value]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (!rootRef.current?.contains(target)) setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  const choose = (symbol: string) => {
    onChange(symbol);
    setOpen(false);
    setSuggestions([]);
    setHighlightedIndex(-1);
    onSelect(symbol);
  };

  const showPanel = open && value.trim().length >= MIN_QUERY_LENGTH;

  return (
    <div ref={rootRef} className="relative min-w-[12rem] flex-1">
      <input
        value={value}
        onChange={(event) => onChange(event.target.value.toUpperCase())}
        onFocus={() => {
          if (value.trim().length >= MIN_QUERY_LENGTH) setOpen(true);
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            setOpen(false);
            return;
          }
          if (!showPanel || suggestions.length === 0) return;
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
            choose(suggestions[highlightedIndex]);
          }
        }}
        placeholder="Add ticker symbol"
        className={inputClassName}
        disabled={disabled}
        autoComplete="off"
        role="combobox"
        aria-expanded={showPanel}
        aria-label="Add ticker symbol"
      />

      {showPanel ? (
        <div className="absolute z-30 mt-1 w-full overflow-hidden rounded-xl border border-white/15 bg-slate-950/95 shadow-xl shadow-black/30">
          {loading ? <div className="px-3 py-2 text-sm text-slate-400">Searching...</div> : null}
          {!loading && error ? <div className="px-3 py-2 text-sm text-rose-300">Unable to load suggestions.</div> : null}
          {!loading && !error && suggestions.length === 0 ? (
            <div className="px-3 py-2 text-sm text-slate-400">No matching ticker suggestions.</div>
          ) : null}
          {suggestions.map((symbol, index) => (
            <button
              key={`${symbol}-${index}`}
              type="button"
              role="option"
              aria-selected={index === highlightedIndex}
              className={`block w-full px-3 py-2 text-left text-sm ${
                index === highlightedIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"
              }`}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => choose(symbol)}
            >
              {symbol}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}
