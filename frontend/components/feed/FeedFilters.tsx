"use client";

import { useMemo, useEffect, useState } from "react";
import type { ChangeEvent } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  cardClassName,
  ghostButtonClassName,
  inputClassName,
  pillClassName,
  primaryButtonClassName,
  selectClassName,
} from "@/lib/styles";

const recentDayOptions = ["1", "3", "7", "14", "30", "90"] as const;

type FilterState = {
  symbol: string;
  member: string;
  chamber: string;
  party: string;
  tradeType: string;
  minAmount: string;
  recentDays: string;
};

function normalizeValue(value: string | null): string {
  return value ? value.trim() : "";
}

export function FeedFilters() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const initialFilters = useMemo<FilterState>(() => {
    return {
      symbol: normalizeValue(searchParams.get("ticker") ?? searchParams.get("symbol")),
      member: normalizeValue(searchParams.get("member")),
      chamber: normalizeValue(searchParams.get("chamber")),
      party: normalizeValue(searchParams.get("party")),
      tradeType: normalizeValue(searchParams.get("trade_type")),
      minAmount: normalizeValue(searchParams.get("min_amount")),
      recentDays: normalizeValue(searchParams.get("recent_days")),
    };
  }, [searchParams]);

  const [filters, setFilters] = useState<FilterState>(initialFilters);

  useEffect(() => {
    setFilters(initialFilters);
  }, [initialFilters]);

  const updateFilter =
    (key: keyof FilterState) => (event: ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { value } = event.target;
    setFilters((current) => ({
      ...current,
      [key]: value,
    }));
  };

  const handleApply = () => {
    const params = new URLSearchParams();
    const limit = searchParams.get("limit");

    if (filters.symbol.trim()) params.set("ticker", filters.symbol.trim());
    if (filters.member.trim()) params.set("member", filters.member.trim());
    if (filters.chamber.trim()) params.set("chamber", filters.chamber.trim());
    if (filters.party.trim()) params.set("party", filters.party.trim());
    if (filters.tradeType.trim()) params.set("trade_type", filters.tradeType.trim());
    if (filters.minAmount.trim()) params.set("min_amount", filters.minAmount.trim());
    if (filters.recentDays.trim()) params.set("recent_days", filters.recentDays.trim());
    if (limit && limit.trim()) params.set("limit", limit.trim());

    const queryString = params.toString();
    router.push(queryString ? `/?${queryString}` : "/");
  };

  const handleReset = () => {
    setFilters({
      symbol: "",
      member: "",
      chamber: "",
      party: "",
      tradeType: "",
      minAmount: "",
      recentDays: "",
    });
    router.push("/");
  };

  const activeChips = useMemo(() => {
    const chips: { label: string; value: string }[] = [];

    if (filters.symbol.trim()) chips.push({ label: "Symbol", value: filters.symbol.trim().toUpperCase() });
    if (filters.member.trim()) chips.push({ label: "Member", value: filters.member.trim() });
    if (filters.chamber.trim())
      chips.push({
        label: "Chamber",
        value: filters.chamber.trim() === "house" ? "House" : "Senate",
      });
    if (filters.party.trim()) {
      const partyLabel =
        filters.party.trim() === "democrat"
          ? "Democrat"
          : filters.party.trim() === "republican"
            ? "Republican"
            : "Other";
      chips.push({ label: "Party", value: partyLabel });
    }
    if (filters.tradeType.trim()) {
      const tradeLabel =
        filters.tradeType.trim() === "purchase"
          ? "Purchase"
          : filters.tradeType.trim() === "sale"
            ? "Sale"
            : filters.tradeType.trim() === "exchange"
              ? "Exchange"
              : "Received";
      chips.push({ label: "Trade", value: tradeLabel });
    }
    if (filters.minAmount.trim()) chips.push({ label: "Min", value: `$${filters.minAmount.trim()}` });
    if (filters.recentDays.trim()) chips.push({ label: "Window", value: `${filters.recentDays.trim()} days` });

    return chips;
  }, [filters]);

  return (
    <div className="space-y-4">
      <div className={cardClassName}>
        <form
          className="grid gap-4 md:grid-cols-2 xl:grid-cols-3"
          onSubmit={(event) => {
            event.preventDefault();
            handleApply();
          }}
        >
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
            <input
              name="symbol"
              value={filters.symbol}
              onChange={updateFilter("symbol")}
              placeholder="NVDA"
              className={inputClassName}
            />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
            <input
              name="member"
              value={filters.member}
              onChange={updateFilter("member")}
              placeholder="Pelosi"
              className={inputClassName}
            />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
            <select name="chamber" value={filters.chamber} onChange={updateFilter("chamber")} className={selectClassName}>
              <option value="">All chambers</option>
              <option value="house">House</option>
              <option value="senate">Senate</option>
            </select>
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Party</label>
            <select name="party" value={filters.party} onChange={updateFilter("party")} className={selectClassName}>
              <option value="">All parties</option>
              <option value="democrat">Democrat</option>
              <option value="republican">Republican</option>
              <option value="other">Other</option>
            </select>
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade type</label>
            <select
              name="trade_type"
              value={filters.tradeType}
              onChange={updateFilter("tradeType")}
              className={selectClassName}
            >
              <option value="">All types</option>
              <option value="purchase">Purchase</option>
              <option value="sale">Sale</option>
              <option value="exchange">Exchange</option>
              <option value="received">Received</option>
            </select>
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Min amount</label>
            <input
              name="min_amount"
              value={filters.minAmount}
              onChange={updateFilter("minAmount")}
              placeholder="250000"
              className={inputClassName}
              inputMode="numeric"
            />
          </div>
          <div>
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
            <select
              name="recent_days"
              value={filters.recentDays}
              onChange={updateFilter("recentDays")}
              className={selectClassName}
            >
              <option value="">Anytime</option>
              {recentDayOptions.map((value) => (
                <option key={value} value={value}>
                  Last {value} days
                </option>
              ))}
            </select>
          </div>
          <div className="flex flex-wrap items-center gap-3 md:col-span-2 xl:col-span-3">
            <button type="submit" className={primaryButtonClassName}>
              Apply filters
            </button>
            <button type="button" onClick={handleReset} className={ghostButtonClassName}>
              Reset
            </button>
          </div>
        </form>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {activeChips.length > 0 ? (
          <>
            <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Active filters</span>
            {activeChips.map((chip) => (
              <span key={`${chip.label}-${chip.value}`} className={pillClassName}>
                <span className="text-slate-400">{chip.label}</span>
                <span className="text-slate-200">{chip.value}</span>
              </span>
            ))}
            <button type="button" onClick={handleReset} className={ghostButtonClassName}>
              Clear all
            </button>
          </>
        ) : (
          <span className="text-xs text-slate-500">No active filters.</span>
        )}
      </div>
    </div>
  );
}
