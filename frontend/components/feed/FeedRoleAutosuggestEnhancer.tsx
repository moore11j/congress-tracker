"use client";

import { suggestRoles } from "@/lib/api";
import { useEffect, useMemo, useRef, useState, type MouseEvent } from "react";

type FeedRoleAutosuggestEnhancerProps = {
  formId: string;
  inputName: string;
};

const MIN_QUERY_LENGTH = 1;
const DEBOUNCE_MS = 160;

export function FeedRoleAutosuggestEnhancer({ formId, inputName }: FeedRoleAutosuggestEnhancerProps) {
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const hostRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const formRef = useRef<HTMLFormElement | null>(null);
  const debounceRef = useRef<number | null>(null);
  const suggestionsRef = useRef<string[]>([]);
  const highlightedIndexRef = useRef(-1);
  const openRef = useRef(false);

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
    };

    const querySuggestions = async (rawValue: string) => {
      const query = rawValue.trim();
      if (query.length < MIN_QUERY_LENGTH) {
        clearDropdown();
        return;
      }
      try {
        const response = await suggestRoles(query, 10);
        const next = Array.isArray(response.items) ? response.items : [];
        setSuggestions(next);
        setHighlightedIndex(next.length > 0 ? 0 : -1);
        setOpen(next.length > 0);
      } catch {
        clearDropdown();
      }
    };

    const selectSuggestion = (suggestion: string) => {
      input.value = suggestion;
      clearDropdown();
      form.requestSubmit();
    };

    const onInput = () => {
      const value = input.value;
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
      debounceRef.current = window.setTimeout(() => {
        void querySuggestions(value);
      }, DEBOUNCE_MS);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        clearDropdown();
        return;
      }
      const currentSuggestions = suggestionsRef.current;
      if (!openRef.current || currentSuggestions.length === 0) return;
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
        const index = highlightedIndexRef.current;
        if (index >= 0 && index < currentSuggestions.length) {
          event.preventDefault();
          selectSuggestion(currentSuggestions[index]);
        }
      }
    };

    const onFocus = () => {
      if (suggestionsRef.current.length > 0) setOpen(true);
    };

    const onOutsidePointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (input.contains(target) || hostRef.current?.contains(target)) return;
      setOpen(false);
    };

    input.addEventListener("input", onInput);
    input.addEventListener("keydown", onKeyDown);
    input.addEventListener("focus", onFocus);
    document.addEventListener("pointerdown", onOutsidePointerDown);

    return () => {
      input.removeEventListener("input", onInput);
      input.removeEventListener("keydown", onKeyDown);
      input.removeEventListener("focus", onFocus);
      document.removeEventListener("pointerdown", onOutsidePointerDown);
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
  }, [formId, inputName]);

  const onSuggestionMouseDown = (event: MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
  };

  const onSuggestionClick = (suggestion: string) => {
    const input = inputRef.current;
    const form = formRef.current;
    if (!input || !form) return;
    input.value = suggestion;
    setOpen(false);
    setSuggestions([]);
    setHighlightedIndex(-1);
    form.requestSubmit();
  };

  if (!open || suggestions.length === 0) return <div ref={hostRef} className="relative z-[120]" />;

  return (
    <div ref={hostRef} className="relative z-[120]">
      <div
        id={listboxId}
        role="listbox"
        className="absolute z-[1200] mt-1 w-full overflow-hidden rounded-xl border border-white/15 bg-slate-950/95 shadow-xl shadow-black/30"
      >
        {suggestions.map((suggestion, index) => (
          <button
            key={suggestion}
            type="button"
            role="option"
            aria-selected={index === highlightedIndex}
            className={`block w-full px-3 py-2 text-left text-sm ${
              index === highlightedIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"
            }`}
            onMouseDown={onSuggestionMouseDown}
            onClick={() => onSuggestionClick(suggestion)}
          >
            <span className="block truncate">{suggestion}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
