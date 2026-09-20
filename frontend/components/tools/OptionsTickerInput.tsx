"use client";

import { useEffect, useId, useState } from "react";
import { suggestSymbols, type SymbolSuggestion } from "@/lib/api";

export function OptionsTickerInput({ value, onSelect }: { value: string; onSelect: (symbol: string) => void }) {
  const id = useId();
  const [query, setQuery] = useState(value), [open, setOpen] = useState(false);
  const [items, setItems] = useState<SymbolSuggestion[]>([]), [active, setActive] = useState(-1);
  const [status, setStatus] = useState("");
  useEffect(() => setQuery(value), [value]);
  useEffect(() => {
    if (!open || !query.trim()) { setItems([]); setActive(-1); setStatus(""); return; }
    const controller = new AbortController();
    setItems([]); setActive(-1); setStatus("Searching…");
    const timer = setTimeout(async () => {
      try {
        const result = await suggestSymbols(query.trim(), "all", 10, { signal: controller.signal, source: "OptionsTickerInput", sameOrigin: true });
        if (controller.signal.aborted) return;
        const next = result.items.filter(item => item.type !== "government_agency" && /^[A-Z][A-Z0-9.\-]{0,9}$/.test(item.symbol));
        setItems(next); setStatus(next.length ? "" : "No matches. Enter a ticker directly.");
      } catch { if (!controller.signal.aborted) setStatus("Search unavailable. Enter a ticker directly."); }
    }, 180);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [query, open]);
  const choose = (symbol: string) => { setQuery(symbol); setOpen(false); onSelect(symbol); };
  return <div className="relative" onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) { setOpen(false); setQuery(value); } }}>
    <label htmlFor={id}>Underlying ticker</label>
    <input id={id} role="combobox" aria-autocomplete="list" aria-expanded={open} aria-controls={`${id}-list`} aria-activedescendant={open && active >= 0 ? `${id}-${active}` : undefined}
      autoComplete="off" placeholder="Ticker or company name" value={query} onFocus={() => setOpen(true)} onChange={e => { setQuery(e.target.value); setOpen(true); setActive(-1); }}
      onKeyDown={e => {
        if (e.key === "Escape") { setOpen(false); setQuery(value); }
        if (e.key === "ArrowDown" || e.key === "ArrowUp") { e.preventDefault(); setOpen(true); setActive(i => items.length ? (i + (e.key === "ArrowDown" ? 1 : -1) + items.length) % items.length : -1); }
        if (e.key === "Enter") { e.preventDefault(); const selected = open && active >= 0 ? items[active]?.symbol : query.trim().toUpperCase(); if (selected && /^[A-Z][A-Z0-9.\-]{0,9}$/.test(selected)) choose(selected); else setStatus("Select a company from the suggestions."); }
      }} />
    {open ? <div className="absolute z-30 mt-1 max-h-72 w-full min-w-[200px] overflow-auto rounded-xl border border-white/15 bg-slate-950 p-1 shadow-xl">
      <ul id={`${id}-list`} role="listbox" aria-label="Ticker suggestions">{items.map((item, i) => <li key={item.symbol} id={`${id}-${i}`} role="option" aria-selected={active === i}>
        <button type="button" tabIndex={-1} className={`w-full rounded-lg px-3 py-2 text-left text-sm ${active === i ? "bg-slate-800" : "hover:bg-slate-800"}`} onPointerDown={e => e.preventDefault()} onClick={() => choose(item.symbol)}><span className="font-semibold text-emerald-200">{item.symbol}</span><span className="block text-xs text-slate-400">{item.name}</span></button>
      </li>)}</ul><p role="status" className="px-3 py-2 text-xs text-slate-400">{status || "Choose a match, or enter an exact ticker."}</p>
    </div> : null}
  </div>;
}
