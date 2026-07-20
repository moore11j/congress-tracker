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
  const requestId = useRef(0);
  const normalized = cleanSymbol(query);

  useEffect(() => {
    setQuery(value);
  }, [value]);

  useEffect(() => {
    const trimmed = query.trim();
    requestId.current += 1;
    const currentRequest = requestId.current;
    if (trimmed.length < 1) {
      setItems([]);
      setOpen(false);
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    suggestSymbols(trimmed, "all", 8, { signal: controller.signal, source: "PeerCompareSelector" })
      .then((response) => {
        if (requestId.current !== currentRequest) return;
        setItems((response.items || []).filter((item) => item.type !== "government_agency"));
        setOpen(true);
      })
      .catch(() => {
        if (requestId.current === currentRequest) setItems([]);
      })
      .finally(() => {
        if (requestId.current === currentRequest) setLoading(false);
      });
    return () => controller.abort();
  }, [query]);

  function commit(symbol: string) {
    const next = cleanSymbol(symbol);
    if (!next || next === cleanSymbol(otherValue)) return;
    setOpen(false);
    onCommit(next);
  }

  return (
    <label className="relative block min-w-0 text-xs font-semibold uppercase tracking-[0.22em] text-slate-400">
      {label}
      <input
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        onFocus={() => setOpen(items.length > 0)}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            commit(items[0]?.symbol || normalized);
          }
        }}
        className="mt-2 h-11 w-full rounded-lg border border-white/10 bg-slate-950/80 px-3 text-sm font-semibold tracking-normal text-white outline-none transition focus:border-cyan-300/60"
        placeholder="Search ticker or company"
      />
      {open ? (
        <div className="absolute z-40 mt-2 max-h-72 w-full overflow-auto rounded-lg border border-white/10 bg-slate-950/95 p-1 shadow-2xl shadow-black/50">
          {loading && items.length === 0 ? <div className="px-3 py-2 text-sm normal-case tracking-normal text-slate-400">Searching...</div> : null}
          {!loading && items.length === 0 ? <div className="px-3 py-2 text-sm normal-case tracking-normal text-slate-400">No matching tickers.</div> : null}
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
  const left = useMemo(() => cleanSymbol(leftSymbol), [leftSymbol]);
  const right = useMemo(() => cleanSymbol(rightSymbol), [rightSymbol]);

  function navigate(nextLeft: string, nextRight: string) {
    const normalizedLeft = cleanSymbol(nextLeft);
    const normalizedRight = cleanSymbol(nextRight);
    if (!normalizedLeft || !normalizedRight || normalizedLeft === normalizedRight) return;
    router.push(`/compare/${encodeURIComponent(normalizedLeft)}/${encodeURIComponent(normalizedRight)}`);
  }

  return (
    <div className="grid gap-3 rounded-lg border border-white/10 bg-slate-950/55 p-3 sm:grid-cols-[1fr_auto_1fr_auto] sm:items-end">
      <SuggestInput label="Ticker 1" value={left} otherValue={right} onCommit={(symbol) => navigate(symbol, right)} />
      <button
        type="button"
        onClick={() => navigate(right, left)}
        className="h-11 rounded-lg border border-white/10 px-3 text-sm font-semibold text-slate-200 hover:border-cyan-300/40 hover:text-white"
      >
        Swap
      </button>
      <SuggestInput label="Ticker 2" value={right === "_" ? "" : right} otherValue={left} onCommit={(symbol) => navigate(left, symbol)} />
      <a
        href={`/ticker/${encodeURIComponent(left)}`}
        className="inline-flex h-11 items-center justify-center rounded-lg border border-white/10 px-3 text-sm font-semibold text-slate-200 hover:border-cyan-300/40 hover:text-white"
      >
        Ticker page
      </a>
    </div>
  );
}
