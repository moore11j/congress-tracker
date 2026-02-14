"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import type { ChangeEvent } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { cardClassName, ghostButtonClassName, inputClassName, pillClassName, selectClassName } from "@/lib/styles";
import type { EventItem } from "@/lib/api";

const debounceMs = 350;

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
  transactionType: string;
  role: string;
  ownership: string;
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
    a.transactionType === b.transactionType &&
    a.role === b.role &&
    a.ownership === b.ownership
  );
}

function normalizeValue(value: string | null): string {
  return (value ?? "").trim();
}

function clearHiddenFilters(mode: FeedMode, next: FilterState): FilterState {
  if (mode === "congress") {
    return { ...next, transactionType: "", role: "", ownership: "" };
  }
  if (mode === "insider") {
    return { ...next, member: "", chamber: "", party: "", tradeType: "" };
  }
  return {
    ...next,
    member: "",
    chamber: "",
    party: "",
    tradeType: "",
    transactionType: "",
    role: "",
    ownership: "",
  };
}

export function FeedFilters({ events, resultsCount }: FeedFiltersProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();

  const initialFilters = useMemo<FilterState>(() => {
    const tape = normalizeValue(searchParams.get("tape"));
    const mode: FeedMode = tape === "congress" || tape === "insider" || tape === "all" ? tape : "all";
    return {
      tape: mode,
      symbol: normalizeValue(searchParams.get("symbol")),
      minAmount: normalizeValue(searchParams.get("min_amount")),
      recentDays: normalizeValue(searchParams.get("recent_days")),
      member: normalizeValue(searchParams.get("member")),
      chamber: normalizeValue(searchParams.get("chamber")),
      party: normalizeValue(searchParams.get("party")),
      tradeType: normalizeValue(searchParams.get("trade_type")),
      transactionType: normalizeValue(searchParams.get("transaction_type")),
      role: normalizeValue(searchParams.get("role")),
      ownership: normalizeValue(searchParams.get("ownership")),
    };
  }, [searchParams]);

  const [filters, setFilters] = useState<FilterState>(initialFilters);

  useEffect(() => {
    setFilters(initialFilters);
  }, [initialFilters]);

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
      "ownership",
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
      if (nextFilters.transactionType) params.set("transaction_type", nextFilters.transactionType);
      if (nextFilters.role) params.set("role", nextFilters.role);
      if (nextFilters.ownership) params.set("ownership", nextFilters.ownership);
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
      transactionType: "",
      role: "",
      ownership: "",
    });
  };

  const symbols = useMemo(() => {
    const set = new Set<string>();
    events.forEach((event) => {
      const symbol = (event.payload?.symbol ?? event.ticker ?? "").toString().trim().toUpperCase();
      if (symbol) set.add(symbol);
    });
    return Array.from(set).slice(0, 10);
  }, [events]);

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
        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
          <input className={inputClassName} value={filters.symbol} onChange={update("symbol")} placeholder="NVDA" />
          {symbols.length > 0 ? <p className="mt-1 text-xs text-slate-500">Suggestions: {symbols.join(", ")}</p> : null}
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
          <input className={inputClassName} value={filters.minAmount} onChange={update("minAmount")} placeholder="250000" />
        </div>

        <div>
          <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
          <select className={selectClassName} value={filters.recentDays} onChange={update("recentDays")}>
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
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
            <input className={inputClassName} value={filters.member} onChange={update("member")} placeholder="Pelosi" />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
            <select className={selectClassName} value={filters.chamber} onChange={update("chamber")}>
              <option value="">All chambers</option>
              <option value="house">House</option>
              <option value="senate">Senate</option>
            </select>
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Party</label>
            <select className={selectClassName} value={filters.party} onChange={update("party")}>
              <option value="">All parties</option>
              <option value="democrat">Democrat</option>
              <option value="republican">Republican</option>
              <option value="independent">Independent</option>
              <option value="other">Other</option>
            </select>
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade type</label>
            <select className={selectClassName} value={filters.tradeType} onChange={update("tradeType")}>
              <option value="">All types</option>
              <option value="purchase">Purchase</option>
              <option value="sale">Sale</option>
              <option value="exchange">Exchange</option>
              <option value="received">Received</option>
            </select>
          </div>
        </div>
      ) : null}

      {filters.tape === "insider" ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4 border-t border-slate-800 pt-4">
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Transaction type</label>
            <input
              className={inputClassName}
              value={filters.transactionType}
              onChange={update("transactionType")}
              placeholder="P-Purchase"
            />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Role</label>
            <input className={inputClassName} value={filters.role} onChange={update("role")} placeholder="CEO" />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Ownership</label>
            <input className={inputClassName} value={filters.ownership} onChange={update("ownership")} placeholder="Direct" />
          </div>
        </div>
      ) : null}
    </section>
  );
}
