"use client";

import { suggestMemberInsiders, type MemberInsiderSuggestion } from "@/lib/api";
import { useEffect, useRef, useState } from "react";

const MIN_QUERY_LENGTH = 2;
const DEBOUNCE_MS = 200;

type Props = {
  value: MemberInsiderSuggestion | null;
  onChange: (value: MemberInsiderSuggestion | null) => void;
  disabled?: boolean;
  fallbackLabel?: string;
  category?: "congress" | "insider";
  onQueryChange?: (value: string) => void;
};

function isLegacyAlias(bioguideId?: string | null) {
  return (bioguideId ?? "").trim().toUpperCase().startsWith("FMP_");
}

function formatSuggestionLabel(suggestion: MemberInsiderSuggestion | null, fallbackLabel?: string) {
  if (suggestion?.label?.trim()) return suggestion.label.trim();
  if (suggestion?.value?.trim()) return suggestion.value.trim();
  return fallbackLabel?.trim() ?? "";
}

function formatInputLabel(suggestion: MemberInsiderSuggestion | null, fallbackLabel?: string) {
  if (suggestion?.category === "insider" && suggestion.value?.trim()) {
    const cik = suggestion.reporting_cik?.trim();
    return cik ? `${suggestion.value.trim()} (${cik})` : suggestion.value.trim();
  }
  return formatSuggestionLabel(suggestion, fallbackLabel);
}

function selectionKey(suggestion: MemberInsiderSuggestion | null, category: "congress" | "insider") {
  if (category === "insider") return suggestion?.reporting_cik?.trim() ?? "";
  return suggestion?.bioguide_id?.trim() ?? "";
}

function dedupeSuggestions(items: MemberInsiderSuggestion[], category: "congress" | "insider") {
  const deduped = new Map<string, MemberInsiderSuggestion>();
  for (const item of items) {
    if (category === "insider") {
      const reportingCik = (item.reporting_cik ?? "").trim();
      if (!reportingCik) continue;
      if (!deduped.has(reportingCik)) {
        deduped.set(reportingCik, {
          ...item,
          reporting_cik: reportingCik,
          label: formatSuggestionLabel(item),
        });
      }
      continue;
    }

    const memberId = (item.bioguide_id ?? "").trim().toUpperCase();
    if (!memberId) continue;
    const dedupeKey = `${formatSuggestionLabel(item).trim().toLowerCase()}|${(item.chamber ?? "").trim().toLowerCase()}`;
    const existing = deduped.get(dedupeKey);
    if (!existing || (isLegacyAlias(existing.bioguide_id) && !isLegacyAlias(item.bioguide_id))) {
      deduped.set(dedupeKey, {
        ...item,
        bioguide_id: memberId,
        label: formatSuggestionLabel(item),
      });
    }
  }
  return Array.from(deduped.values());
}

function suggestionSubtitle(suggestion: MemberInsiderSuggestion, category: "congress" | "insider") {
  if (category === "insider") {
    return [
      suggestion.reporting_cik ? `CIK ${suggestion.reporting_cik}` : null,
      suggestion.symbol,
      suggestion.company_name,
      suggestion.role,
    ].filter(Boolean).join(" - ");
  }
  return [
    suggestion.chamber === "house" ? "House" : suggestion.chamber === "senate" ? "Senate" : suggestion.chamber,
    suggestion.party,
    suggestion.state,
  ].filter(Boolean).join(" - ");
}

export function CongressMemberAutosuggest({ value, onChange, disabled = false, fallbackLabel, category = "congress", onQueryChange }: Props) {
  const [query, setQuery] = useState(() => formatInputLabel(value, fallbackLabel));
  const [suggestions, setSuggestions] = useState<MemberInsiderSuggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(-1);

  const rootRef = useRef<HTMLDivElement | null>(null);
  const debounceRef = useRef<number | null>(null);
  const requestIdRef = useRef(0);
  const activeSelectionKey = selectionKey(value, category);
  const placeholder = category === "insider" ? "Search by insider name or CIK" : "Search members by name";
  const ariaLabel = category === "insider" ? "Insider" : "Member";

  useEffect(() => {
    const nextLabel = formatInputLabel(value, fallbackLabel);
    if (activeSelectionKey || nextLabel) {
      setQuery(nextLabel);
    }
  }, [activeSelectionKey, fallbackLabel, value]);

  useEffect(() => {
    const trimmed = query.trim();
    if (debounceRef.current) window.clearTimeout(debounceRef.current);

    if (!trimmed || trimmed.length < MIN_QUERY_LENGTH || activeSelectionKey) {
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
        const response = await suggestMemberInsiders(trimmed, 10);
        if (requestIdRef.current !== requestId) return;
        const next = dedupeSuggestions(response.items.filter((item) => item.category === category), category);
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
  }, [activeSelectionKey, category, query]);

  useEffect(() => {
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (!rootRef.current?.contains(target)) setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    return () => document.removeEventListener("pointerdown", onPointerDown);
  }, []);

  function selectSuggestion(suggestion: MemberInsiderSuggestion) {
    onChange(suggestion);
    setQuery(formatInputLabel(suggestion));
    setSuggestions([]);
    setOpen(false);
    setHighlightedIndex(-1);
  }

  return (
    <div ref={rootRef} className="relative">
      <input
        value={query}
        onChange={(event) => {
          if (value) onChange(null);
          const nextQuery = event.target.value;
          onQueryChange?.(nextQuery);
          setQuery(nextQuery);
        }}
        onFocus={() => {
          if (suggestions.length > 0) setOpen(true);
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            setOpen(false);
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
        placeholder={placeholder}
        className="h-11 w-full rounded-2xl border border-white/10 bg-slate-950/50 px-3 text-sm text-white outline-none transition placeholder:text-slate-500/40 focus:border-white/20 disabled:cursor-not-allowed disabled:text-slate-500"
        disabled={disabled}
        autoComplete="off"
        aria-label={ariaLabel}
      />

      {open || (loading && query.trim().length >= MIN_QUERY_LENGTH) ? (
        <div className="absolute z-20 mt-1 w-full overflow-hidden rounded-xl border border-white/15 bg-slate-950/95 shadow-xl shadow-black/30">
          {loading && suggestions.length === 0 ? <div className="px-3 py-2 text-sm text-slate-400">Searching...</div> : null}
          {!loading
            ? suggestions.map((suggestion, index) => (
                <button
                  key={`${suggestion.bioguide_id ?? suggestion.value}-${index}`}
                  type="button"
                  role="option"
                  aria-selected={index === highlightedIndex}
                  className={`block w-full px-3 py-2 text-left text-sm ${
                    index === highlightedIndex ? "bg-slate-800 text-emerald-200" : "text-slate-200 hover:bg-slate-800"
                  }`}
                  onMouseDown={(event) => event.preventDefault()}
                  onClick={() => selectSuggestion(suggestion)}
                >
                  <div className="font-medium text-white">{formatSuggestionLabel(suggestion)}</div>
                  {suggestionSubtitle(suggestion, category) ? <div className="text-xs text-slate-400">{suggestionSubtitle(suggestion, category)}</div> : null}
                </button>
              ))
            : null}
        </div>
      ) : null}
    </div>
  );
}
