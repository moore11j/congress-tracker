"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { suggestSymbols, type SymbolSuggestion } from "@/lib/api";

type Props = {
  leftSymbol: string;
  rightSymbol: string;
};

function cleanSymbol(value: string) {
  return value.trim().toUpperCase().replace(/\./g, "-");
}

function suggestionName(item: SymbolSuggestion) {
  return (item.name || item.label || "").trim();
}

function isPlaceholderTickerSuggestion(item: SymbolSuggestion) {
  const symbol = cleanSymbol(item.symbol);
  return Boolean(symbol && suggestionName(item).toUpperCase() === `TICKER: ${symbol}`);
}

function bestSuggestionForQuery(items: SymbolSuggestion[], query: string) {
  const normalized = cleanSymbol(query);
  const queryText = query.trim().toLowerCase();
  const realItems = items.filter((item) => !isPlaceholderTickerSuggestion(item));
  return (
    realItems.find((item) => cleanSymbol(item.symbol) === normalized) ||
    realItems.find((item) => suggestionName(item).toLowerCase() === queryText) ||
    realItems.find((item) => suggestionName(item).toLowerCase().startsWith(queryText)) ||
    realItems[0] ||
    null
  );
}

function canCommitRawTicker(query: string) {
  const raw = query.trim();
  const symbol = cleanSymbol(raw);
  return Boolean(symbol && (symbol.length <= 4 || /[.$/-]/.test(raw)));
}

function SuggestInput({
  label,
  value,
  otherValue,
  onCommit,
}: {
  label: string;
  value: string;
  otherValue: string;
  onCommit: (symbol: string) => void;
}) {
  const [query, setQuery] = useState(value);
  const [items, setItems] = useState<SymbolSuggestion[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [resolving, setResolving] = useState(false);
  const [active, setActive] = useState(false);
  const requestId = useRef(0);
  const normalized = cleanSymbol(query);

  useEffect(() => {
    setQuery(value);
    setItems([]);
    setOpen(false);
    setActive(false);
  }, [value]);

  useEffect(() => {
    const trimmed = query.trim();
    requestId.current += 1;
    const currentRequest = requestId.current;
    if (!active || trimmed.length < 1) {
      setItems([]);
      setOpen(false);
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    suggestSymbols(trimmed, "all", 8, { signal: controller.signal, source: "PeerCompareSelector" })
      .then((response) => {
        if (requestId.current !== currentRequest) return;
        const nextItems = (response.items || []).filter((item) => item.type !== "government_agency");
        setItems(nextItems);
        setOpen(nextItems.length > 0);
      })
      .catch(() => {
        if (requestId.current === currentRequest) setItems([]);
      })
      .finally(() => {
        if (requestId.current === currentRequest) setLoading(false);
      });
    return () => controller.abort();
  }, [active, query]);

  function commit(symbol: string) {
    const next = cleanSymbol(symbol);
    if (!next || next === cleanSymbol(otherValue)) return;
    setOpen(false);
    onCommit(next);
  }

  async function commitQuery() {
    const trimmed = query.trim();
    if (!trimmed) return;

    const currentBest = bestSuggestionForQuery(items, trimmed);
    if (currentBest) {
      commit(currentBest.symbol);
      return;
    }

    setResolving(true);
    try {
      const response = await suggestSymbols(trimmed, "all", 8, { source: "PeerCompareSelectorCommit" });
      const nextItems = (response.items || []).filter((item) => item.type !== "government_agency");
      setItems(nextItems);
      setOpen(nextItems.length > 0);
      const nextBest = bestSuggestionForQuery(nextItems, trimmed);
      if (nextBest) {
        commit(nextBest.symbol);
        return;
      }
    } catch {
      setItems([]);
    } finally {
      setResolving(false);
    }

    if (canCommitRawTicker(trimmed)) commit(normalized);
  }

  return (
    <label className="relative block min-w-0 text-xs font-semibold uppercase tracking-[0.22em] text-slate-400">
      {label}
      <input
        value={query}
        onChange={(event) => {
          setActive(true);
          setQuery(event.target.value);
        }}
        onFocus={() => {
          setActive(true);
          setOpen(items.length > 0);
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            void commitQuery();
          }
        }}
        className="mt-2 h-11 w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 text-sm font-semibold tracking-normal text-white outline-none transition focus:border-cyan-300/60"
        placeholder="Search ticker or company"
      />
      {open ? (
        <div className="absolute z-40 mt-2 max-h-72 w-full overflow-auto rounded-lg border border-white/10 bg-slate-950/95 p-1 shadow-2xl shadow-black/50">
          {(loading || resolving) && items.length === 0 ? <div className="px-3 py-2 text-sm normal-case tracking-normal text-slate-400">Searching...</div> : null}
          {!loading && !resolving && items.length === 0 ? <div className="px-3 py-2 text-sm normal-case tracking-normal text-slate-400">No matching tickers.</div> : null}
          {items.map((item) => (
            <button
              key={`${item.symbol}-${item.id ?? item.name ?? ""}`}
              type="button"
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => commit(item.symbol)}
              className="grid w-full gap-0.5 rounded-md px-3 py-2 text-left normal-case tracking-normal text-slate-300 hover:bg-cyan-300/10"
            >
              <span className="font-semibold text-white">{item.symbol}</span>
              {item.name ? <span className="truncate text-xs text-slate-400">{item.name}</span> : null}
            </button>
          ))}
        </div>
      ) : null}
    </label>
  );
}

export function PeerCompareSelector({ leftSymbol, rightSymbol }: Props) {
  const router = useRouter();
  const left = useMemo(() => {
    const symbol = cleanSymbol(leftSymbol);
    return symbol === "_" ? "" : symbol;
  }, [leftSymbol]);
  const right = useMemo(() => {
    const symbol = cleanSymbol(rightSymbol);
    return symbol === "_" ? "" : symbol;
  }, [rightSymbol]);

  function navigate(nextLeft: string, nextRight: string) {
    const normalizedLeft = cleanSymbol(nextLeft);
    const normalizedRight = cleanSymbol(nextRight);
    if (!normalizedLeft || !normalizedRight || normalizedLeft === normalizedRight) return;
    router.push(`/compare/${encodeURIComponent(normalizedLeft)}/${encodeURIComponent(normalizedRight)}`);
  }

  return (
    <div className="grid gap-3 rounded-lg border border-white/10 bg-slate-950/55 p-3 sm:grid-cols-[1fr_auto_1fr_auto] sm:items-end">
      <SuggestInput label="Ticker 1" value={left} otherValue={right} onCommit={(symbol) => navigate(symbol, right || "_")} />
      <button
        type="button"
        onClick={() => navigate(right || "_", left || "_")}
        className="h-11 rounded-lg border border-white/10 px-3 text-sm font-semibold text-slate-200 hover:border-cyan-300/40 hover:text-white"
      >
        Swap
      </button>
      <SuggestInput label="Ticker 2" value={right} otherValue={left} onCommit={(symbol) => navigate(left || "_", symbol)} />
      {left ? (
        <a
          href={`/ticker/${encodeURIComponent(left)}`}
          className="inline-flex h-11 items-center justify-center rounded-lg border border-white/10 px-3 text-sm font-semibold text-slate-200 hover:border-cyan-300/40 hover:text-white"
        >
          Ticker page
        </a>
      ) : null}
    </div>
  );
}
