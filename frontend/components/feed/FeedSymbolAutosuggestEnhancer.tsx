"use client";

import { suggestSymbols } from "@/lib/api";
import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";

type FeedSymbolAutosuggestEnhancerProps = {
  formId: string;
  inputName: string;
  mode: "congress" | "insider" | "all";
};

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 200;

export function FeedSymbolAutosuggestEnhancer({ formId, inputName, mode }: FeedSymbolAutosuggestEnhancerProps) {
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const [loading, setLoading] = useState(false);

  const hostRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const formRef = useRef<HTMLFormElement | null>(null);
  const debounceRef = useRef<number | null>(null);
  const blurTimeoutRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);
  const suggestionsRef = useRef<string[]>([]);
  const highlightedIndexRef = useRef(-1);
  const openRef = useRef(false);

  const hasSuggestions = suggestions.length > 0;

  useEffect(() => {
    suggestionsRef.current = suggestions;
  }, [suggestions]);

  useEffect(() => {
    highlightedIndexRef.current = highlightedIndex;
  }, [highlightedIndex]);

  useEffect(() => {
    openRef.current = open;
  }, [open]);
  const listboxId = useMemo(() => `${formId}-${inputName}-autosuggest`, [formId, inputName]);

  useEffect(() => {
    const form = document.getElementById(formId);
    if (!(form instanceof HTMLFormElement)) return;

    const input = form.elements.namedItem(inputName);
    if (!(input instanceof HTMLInputElement)) return;

    formRef.current = form;
    inputRef.current = input;

    const clearDropdown = () => {
      setOpen(false);
      setSuggestions([]);
      setHighlightedIndex(-1);
      setLoading(false);
    };

    const querySuggestions = async (rawValue: string) => {
      const query = rawValue.trim();
      if (query.length < MIN_QUERY_LENGTH) {
        clearDropdown();
        return;
      }

      const requestId = requestIdRef.current + 1;
      requestIdRef.current = requestId;
      setLoading(true);

      try {
        const response = await suggestSymbols(query, mode, 10);
        if (requestIdRef.current !== requestId) return;

        const next = Array.isArray(response.items) ? response.items : [];
        setSuggestions(next);
        setHighlightedIndex(next.length > 0 ? 0 : -1);
        setOpen(next.length > 0);
      } catch {
        if (requestIdRef.current !== requestId) return;
        setSuggestions([]);
        setHighlightedIndex(-1);
        setOpen(false);
      } finally {
        if (requestIdRef.current === requestId) {
          setLoading(false);
        }
      }
    };

    const onInput = () => {
      const value = input.value;
      if (debounceRef.current) window.clearTimeout(debounceRef.current);

      if (value.trim().length < MIN_QUERY_LENGTH) {
        clearDropdown();
        return;
      }

      debounceRef.current = window.setTimeout(() => {
        void querySuggestions(value);
      }, DEBOUNCE_MS);
    };

    const selectSuggestion = (symbol: string) => {
      input.value = symbol;
      clearDropdown();
      form.requestSubmit();
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        clearDropdown();
        return;
      }

      const currentOpen = openRef.current;
      const currentSuggestions = suggestionsRef.current;
      if (!currentOpen || currentSuggestions.length === 0) return;

      if (event.key === "ArrowDown") {
        event.preventDefault();
        setHighlightedIndex((current) => (current + 1) % currentSuggestions.length);
        return;
      }

      if (event.key === "ArrowUp") {
        event.preventDefault();
        setHighlightedIndex((current) => (current <= 0 ? currentSuggestions.length - 1 : current - 1));
        return;
      }

      if (event.key === "Enter") {
        const index = highlightedIndexRef.current >= 0 ? highlightedIndexRef.current : -1;
        if (index < 0 || index >= currentSuggestions.length) return;
        event.preventDefault();
        selectSuggestion(currentSuggestions[index]);
      }
    };

    const onBlur = () => {
      if (blurTimeoutRef.current) window.clearTimeout(blurTimeoutRef.current);
      blurTimeoutRef.current = window.setTimeout(() => {
        setOpen(false);
      }, 120);
    };

    const onFocus = () => {
      if (blurTimeoutRef.current) {
        window.clearTimeout(blurTimeoutRef.current);
        blurTimeoutRef.current = null;
      }
      if (suggestionsRef.current.length > 0) {
        setOpen(true);
      }
    };

    const onOutsidePointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      const clickedInput = input.contains(target);
      const clickedMenu = hostRef.current?.contains(target) ?? false;
      if (!clickedInput && !clickedMenu) {
        setOpen(false);
      }
    };

    input.addEventListener("input", onInput);
    input.addEventListener("keydown", onKeyDown);
    input.addEventListener("blur", onBlur);
    input.addEventListener("focus", onFocus);
    document.addEventListener("pointerdown", onOutsidePointerDown);

    return () => {
      input.removeEventListener("input", onInput);
      input.removeEventListener("keydown", onKeyDown);
      input.removeEventListener("blur", onBlur);
      input.removeEventListener("focus", onFocus);
      document.removeEventListener("pointerdown", onOutsidePointerDown);
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
      if (blurTimeoutRef.current) window.clearTimeout(blurTimeoutRef.current);
    };
  }, [formId, inputName, mode]);

  const onSuggestionMouseDown = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
  };

  const onSuggestionClick = (symbol: string) => {
    const input = inputRef.current;
    const form = formRef.current;
    if (!input || !form) return;

    input.value = symbol;
    setOpen(false);
    setSuggestions([]);
    setHighlightedIndex(-1);
    form.requestSubmit();
  };

  if (!open || (!hasSuggestions && !loading)) return <div ref={hostRef} className="relative" />;

  return (
    <div ref={hostRef} className="relative">
      <div
        id={listboxId}
        role="listbox"
        className="absolute z-20 mt-1 w-full overflow-hidden rounded-xl border border-white/15 bg-slate-950/95 shadow-xl shadow-black/30"
      >
        {loading && !hasSuggestions ? <div className="px-3 py-2 text-sm text-slate-400">Searching…</div> : null}
        {suggestions.map((symbol, index) => (
          <button
            key={`${symbol}-${index}`}
            type="button"
            role="option"
            aria-selected={index === highlightedIndex}
            className={`block w-full px-3 py-2 text-left text-sm ${
              index === highlightedIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"
            }`}
            onMouseDown={onSuggestionMouseDown}
            onClick={() => onSuggestionClick(symbol)}
          >
            {symbol}
          </button>
        ))}
      </div>
    </div>
  );
}
