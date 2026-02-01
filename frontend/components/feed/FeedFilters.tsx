"use client";

import { useMemo, useEffect, useRef, useState, useTransition } from "react";
import type { ChangeEvent, KeyboardEvent } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  cardClassName,
  ghostButtonClassName,
  inputClassName,
  pillClassName,
  selectClassName,
} from "@/lib/styles";
import type { EventItem } from "@/lib/api";

const recentDayOptions = ["1", "7", "30", "90"] as const;
const minAmountPresets = [0, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000] as const;
const maxSuggestions = 8;
const debounceMs = 450;

type FilterState = {
  symbol: string;
  member: string;
  chamber: string;
  party: string;
  tradeType: string;
  minAmount: string;
  recentDays: string;
};

function normalizeValue(value: unknown): string {
  if (typeof value !== "string") return "";
  return value.trim();
}

function formatAmountShort(value: number): string {
  if (value >= 1_000_000) return `${value / 1_000_000}M`;
  if (value >= 1_000) return `${value / 1_000}k`;
  return String(value);
}

function clampSuggestionIndex(index: number, length: number) {
  if (length === 0) return -1;
  if (index < 0) return length - 1;
  if (index >= length) return 0;
  return index;
}

type FeedFiltersProps = {
  events: EventItem[];
  resultsCount: number;
};

export function FeedFilters({ events, resultsCount }: FeedFiltersProps) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [copied, setCopied] = useState(false);
  const isComposingRef = useRef(false);
  const isSyncingFromUrlRef = useRef(false);

  const initialFilters = useMemo<FilterState>(() => {
    return {
      symbol: normalizeValue(
        searchParams.get("tickers") ?? searchParams.get("ticker") ?? searchParams.get("symbol")
      ),
      member: normalizeValue(searchParams.get("member")),
      chamber: normalizeValue(searchParams.get("chamber")),
      party: normalizeValue(searchParams.get("party")),
      tradeType: normalizeValue(searchParams.get("trade_type")),
      minAmount: normalizeValue(searchParams.get("min_amount")),
      recentDays: normalizeValue(searchParams.get("recent_days")),
    };
  }, [searchParams]);

  const [filters, setFilters] = useState<FilterState>(initialFilters);
  const [compositionTick, setCompositionTick] = useState(0);
  const [symbolDropdownOpen, setSymbolDropdownOpen] = useState(false);
  const [memberDropdownOpen, setMemberDropdownOpen] = useState(false);
  const [symbolActiveIndex, setSymbolActiveIndex] = useState(-1);
  const [memberActiveIndex, setMemberActiveIndex] = useState(-1);

  useEffect(() => {
    isSyncingFromUrlRef.current = true;
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

  const buildParams = (nextFilters: FilterState) => {
    const params = new URLSearchParams();
    const limit = searchParams.get("limit");

    if (nextFilters.symbol.trim()) params.set("symbol", nextFilters.symbol.trim());
    if (nextFilters.member.trim()) params.set("member", nextFilters.member.trim());
    if (nextFilters.chamber.trim()) params.set("chamber", nextFilters.chamber.trim());
    if (nextFilters.party.trim()) params.set("party", nextFilters.party.trim());
    if (nextFilters.tradeType.trim()) params.set("trade_type", nextFilters.tradeType.trim());
    if (nextFilters.minAmount.trim()) params.set("min_amount", nextFilters.minAmount.trim());
    if (nextFilters.recentDays.trim()) params.set("recent_days", nextFilters.recentDays.trim());
    if (limit && limit.trim()) params.set("limit", limit.trim());
    return params;
  };

  const commitFilters = (nextFilters: FilterState) => {
    const params = buildParams(nextFilters);
    const queryString = params.toString();
    startTransition(() => {
      router.replace(queryString ? `/?${queryString}` : "/");
    });
  };

  const handleReset = () => {
    const cleared = {
      symbol: "",
      member: "",
      chamber: "",
      party: "",
      tradeType: "",
      minAmount: "",
      recentDays: "",
    };
    setFilters(cleared);
    commitFilters(cleared);
  };

  useEffect(() => {
    if (isSyncingFromUrlRef.current) {
      isSyncingFromUrlRef.current = false;
      return;
    }
    if (isComposingRef.current) return;
    const params = buildParams(filters);
    const nextQuery = params.toString();
    const currentParams = new URLSearchParams(searchParams.toString());
    const hasCursor = currentParams.has("cursor");
    currentParams.delete("cursor");
    const currentQuery = currentParams.toString();
    if (nextQuery === currentQuery && !hasCursor) return;
    const handle = window.setTimeout(() => {
      commitFilters(filters);
    }, debounceMs);
    return () => window.clearTimeout(handle);
  }, [filters, searchParams, compositionTick]);

  const symbolSuggestions = useMemo(() => {
    const seen = new Set<string>();
    const suggestions: string[] = [];

    events.forEach((event) => {
      const payload = event.payload ?? {};
      const symbol = normalizeValue(payload.symbol ?? event.ticker);
      if (!symbol) return;
      const normalized = symbol.toUpperCase();
      if (seen.has(normalized)) return;
      seen.add(normalized);
      suggestions.push(normalized);
    });

    return suggestions;
  }, [events]);

  const memberSuggestions = useMemo(() => {
    const seen = new Set<string>();
    const suggestions: { name: string; bioguideId?: string }[] = [];

    events.forEach((event) => {
      const payload = event.payload ?? {};
      const memberPayload = payload.member ?? {};
      const name = normalizeValue(memberPayload.name ?? payload.member_name ?? event.source);
      const bioguideId = normalizeValue(
        memberPayload.bioguide_id ?? payload.member_bioguide_id ?? memberPayload.member_bioguide_id,
      );
      if (!name && !bioguideId) return;
      const key = bioguideId ? `id:${bioguideId}` : `name:${name}`;
      if (seen.has(key)) return;
      seen.add(key);
      suggestions.push({
        name: name || bioguideId,
        bioguideId: bioguideId || undefined,
      });
    });

    return suggestions;
  }, [events]);

  const filteredSymbolSuggestions = useMemo(() => {
    const query = filters.symbol.trim().toLowerCase();
    const options = query
      ? symbolSuggestions.filter((symbol) => symbol.toLowerCase().includes(query))
      : symbolSuggestions;
    return options.slice(0, maxSuggestions);
  }, [filters.symbol, symbolSuggestions]);

  const filteredMemberSuggestions = useMemo(() => {
    const query = filters.member.trim().toLowerCase();
    const options = query
      ? memberSuggestions.filter((member) => {
          const name = member.name.toLowerCase();
          const id = member.bioguideId?.toLowerCase() ?? "";
          return name.includes(query) || id.includes(query);
        })
      : memberSuggestions;
    return options.slice(0, maxSuggestions);
  }, [filters.member, memberSuggestions]);

  useEffect(() => {
    setSymbolActiveIndex(filteredSymbolSuggestions.length ? 0 : -1);
  }, [filteredSymbolSuggestions]);

  useEffect(() => {
    setMemberActiveIndex(filteredMemberSuggestions.length ? 0 : -1);
  }, [filteredMemberSuggestions]);

  const minAmountIndex = useMemo(() => {
    const numeric = Number(filters.minAmount);
    if (!Number.isFinite(numeric) || numeric <= 0) return 0;
    const exactIndex = minAmountPresets.findIndex((preset) => preset === numeric);
    if (exactIndex >= 0) return exactIndex;
    let closestIndex = 0;
    let smallestDiff = Infinity;
    minAmountPresets.forEach((preset, index) => {
      const diff = Math.abs(preset - numeric);
      if (diff < smallestDiff) {
        smallestDiff = diff;
        closestIndex = index;
      }
    });
    return closestIndex;
  }, [filters.minAmount]);

  const setMinAmountFromPreset = (preset: number) => {
    setFilters((current) => ({
      ...current,
      minAmount: preset ? String(preset) : "",
    }));
  };

  const handleSymbolKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (!filteredSymbolSuggestions.length) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setSymbolDropdownOpen(true);
      setSymbolActiveIndex((index) => clampSuggestionIndex(index + 1, filteredSymbolSuggestions.length));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setSymbolDropdownOpen(true);
      setSymbolActiveIndex((index) => clampSuggestionIndex(index - 1, filteredSymbolSuggestions.length));
    } else if (event.key === "Enter") {
      if (symbolDropdownOpen && symbolActiveIndex >= 0) {
        event.preventDefault();
        const selection = filteredSymbolSuggestions[symbolActiveIndex];
        setFilters((current) => ({ ...current, symbol: selection }));
        setSymbolDropdownOpen(false);
      }
    } else if (event.key === "Escape") {
      setSymbolDropdownOpen(false);
    }
  };

  const handleMemberKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (!filteredMemberSuggestions.length) return;
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setMemberDropdownOpen(true);
      setMemberActiveIndex((index) => clampSuggestionIndex(index + 1, filteredMemberSuggestions.length));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setMemberDropdownOpen(true);
      setMemberActiveIndex((index) => clampSuggestionIndex(index - 1, filteredMemberSuggestions.length));
    } else if (event.key === "Enter") {
      if (memberDropdownOpen && memberActiveIndex >= 0) {
        event.preventDefault();
        const selection = filteredMemberSuggestions[memberActiveIndex];
        setFilters((current) => ({ ...current, member: selection.bioguideId ?? selection.name }));
        setMemberDropdownOpen(false);
      }
    } else if (event.key === "Escape") {
      setMemberDropdownOpen(false);
    }
  };

  const handleCopyLink = async () => {
    if (typeof window === "undefined") return;
    try {
      await navigator.clipboard.writeText(window.location.href);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };

  const activeChips = useMemo(() => {
    const chips: { label: string; value: string; key: keyof FilterState }[] = [];

    if (filters.symbol.trim()) chips.push({ label: "Symbol", value: filters.symbol.trim().toUpperCase(), key: "symbol" });
    if (filters.member.trim()) chips.push({ label: "Member", value: filters.member.trim(), key: "member" });
    if (filters.chamber.trim())
      chips.push({
        label: "Chamber",
        value: filters.chamber.trim() === "house" ? "House" : "Senate",
        key: "chamber",
      });
    if (filters.party.trim()) {
      const partyLabel =
        filters.party.trim() === "democrat"
          ? "Democrat"
          : filters.party.trim() === "republican"
            ? "Republican"
            : "Other";
      chips.push({ label: "Party", value: partyLabel, key: "party" });
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
      chips.push({ label: "Trade", value: tradeLabel, key: "tradeType" });
    }
    if (filters.minAmount.trim())
      chips.push({
        label: "Min",
        value: `$${filters.minAmount.trim()}`,
        key: "minAmount",
      });
    if (filters.recentDays.trim())
      chips.push({ label: "Window", value: `${filters.recentDays.trim()} days`, key: "recentDays" });

    return chips;
  }, [filters]);

  return (
    <div className="space-y-4">
      <div className={cardClassName}>
        <form
          className="grid gap-4 md:grid-cols-2 xl:grid-cols-3"
          onSubmit={(event) => {
            event.preventDefault();
          }}
        >
          <div className="relative">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
            <input
              name="symbol"
              value={filters.symbol}
              onChange={(event) => {
                updateFilter("symbol")(event);
                setSymbolDropdownOpen(true);
              }}
              onKeyDown={handleSymbolKeyDown}
              onFocus={() => {
                if (filteredSymbolSuggestions.length) {
                  setSymbolDropdownOpen(true);
                }
              }}
              onBlur={() => {
                window.setTimeout(() => setSymbolDropdownOpen(false), 120);
              }}
              onCompositionStart={() => {
                isComposingRef.current = true;
              }}
              onCompositionEnd={() => {
                isComposingRef.current = false;
                setCompositionTick((value) => value + 1);
              }}
              placeholder="NVDA"
              className={inputClassName}
            />
            {symbolDropdownOpen && filteredSymbolSuggestions.length > 0 ? (
              <div className="absolute z-20 mt-2 w-full overflow-hidden rounded-xl border border-slate-800 bg-slate-950 shadow-xl">
                {filteredSymbolSuggestions.map((symbol, index) => (
                  <button
                    type="button"
                    key={symbol}
                    onMouseDown={() => {
                      setFilters((current) => ({ ...current, symbol }));
                      setSymbolDropdownOpen(false);
                    }}
                    className={`flex w-full items-center justify-between px-3 py-2 text-left text-sm transition ${
                      symbolActiveIndex === index ? "bg-slate-900 text-white" : "text-slate-300 hover:bg-slate-900"
                    }`}
                  >
                    <span className="font-semibold">{symbol}</span>
                    <span className="text-xs text-slate-500">Ticker</span>
                  </button>
                ))}
              </div>
            ) : null}
          </div>
          <div className="relative">
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Member</label>
            <input
              name="member"
              value={filters.member}
              onChange={(event) => {
                updateFilter("member")(event);
                setMemberDropdownOpen(true);
              }}
              onKeyDown={handleMemberKeyDown}
              onFocus={() => {
                if (filteredMemberSuggestions.length) {
                  setMemberDropdownOpen(true);
                }
              }}
              onBlur={() => {
                window.setTimeout(() => setMemberDropdownOpen(false), 120);
              }}
              onCompositionStart={() => {
                isComposingRef.current = true;
              }}
              onCompositionEnd={() => {
                isComposingRef.current = false;
                setCompositionTick((value) => value + 1);
              }}
              placeholder="Pelosi"
              className={inputClassName}
            />
            {memberDropdownOpen && filteredMemberSuggestions.length > 0 ? (
              <div className="absolute z-20 mt-2 w-full overflow-hidden rounded-xl border border-slate-800 bg-slate-950 shadow-xl">
                {filteredMemberSuggestions.map((member, index) => (
                  <button
                    type="button"
                    key={`${member.name}-${member.bioguideId ?? "name"}`}
                    onMouseDown={() => {
                      setFilters((current) => ({
                        ...current,
                        member: member.bioguideId ?? member.name,
                      }));
                      setMemberDropdownOpen(false);
                    }}
                    className={`flex w-full flex-col items-start px-3 py-2 text-left text-sm transition ${
                      memberActiveIndex === index ? "bg-slate-900 text-white" : "text-slate-300 hover:bg-slate-900"
                    }`}
                  >
                    <span className="font-semibold">{member.name}</span>
                    {member.bioguideId ? (
                      <span className="text-xs text-slate-500">{member.bioguideId}</span>
                    ) : null}
                  </button>
                ))}
              </div>
            ) : null}
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
            <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent days</label>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => setFilters((current) => ({ ...current, recentDays: "" }))}
                className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wide transition ${
                  filters.recentDays.trim() === ""
                    ? "border-emerald-500/50 bg-emerald-500/10 text-emerald-200"
                    : "border-slate-800 text-slate-400 hover:border-slate-600 hover:text-slate-200"
                }`}
              >
                Anytime
              </button>
              {recentDayOptions.map((value) => (
                <button
                  key={value}
                  type="button"
                  onClick={() => setFilters((current) => ({ ...current, recentDays: value }))}
                  className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wide transition ${
                    filters.recentDays === value
                      ? "border-emerald-500/50 bg-emerald-500/10 text-emerald-200"
                      : "border-slate-800 text-slate-400 hover:border-slate-600 hover:text-slate-200"
                  }`}
                >
                  {value}d
                </button>
              ))}
            </div>
          </div>
          <div className="rounded-2xl border border-slate-800/70 bg-slate-950/40 p-4 md:col-span-2 xl:col-span-3">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Whale threshold</label>
                <div className="text-sm font-semibold text-white">
                  {minAmountPresets[minAmountIndex] === 0
                    ? "Off"
                    : `$${formatAmountShort(minAmountPresets[minAmountIndex])}+`}
                </div>
              </div>
              <div className="text-xs text-slate-400">
                Showing {resultsCount} events {isPending ? "• Updating…" : ""}
              </div>
            </div>
            <input
              type="range"
              min={0}
              max={minAmountPresets.length - 1}
              value={minAmountIndex}
              onChange={(event) => {
                const index = Number(event.target.value);
                const preset = minAmountPresets[index] ?? 0;
                setMinAmountFromPreset(preset);
              }}
              className="mt-3 w-full accent-emerald-400"
            />
            <div className="mt-3 flex flex-wrap gap-2">
              {minAmountPresets.map((preset) => {
                const isActive = preset === minAmountPresets[minAmountIndex];
                return (
                  <button
                    key={preset}
                    type="button"
                    onClick={() => setMinAmountFromPreset(preset)}
                    className={`rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wide transition ${
                      isActive
                        ? "border-emerald-500/60 bg-emerald-500/10 text-emerald-200"
                        : "border-slate-800 text-slate-400 hover:border-slate-600 hover:text-slate-200"
                    }`}
                  >
                    {preset === 0 ? "Off" : `${formatAmountShort(preset)}`}
                  </button>
                );
              })}
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-3 md:col-span-2 xl:col-span-3">
            <button type="button" onClick={handleReset} className={ghostButtonClassName}>
              Reset
            </button>
            <button type="button" onClick={handleCopyLink} className={ghostButtonClassName}>
              {copied ? "Copied!" : "Copy link"}
            </button>
          </div>
        </form>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        {activeChips.length > 0 ? (
          <>
            <span className="text-xs font-semibold uppercase tracking-wide text-slate-400">Active filters</span>
            {activeChips.map((chip) => (
              <button
                type="button"
                key={`${chip.label}-${chip.value}`}
                onClick={() => setFilters((current) => ({ ...current, [chip.key]: "" }))}
                className={`${pillClassName} transition hover:border-slate-500 hover:text-slate-100`}
              >
                <span className="text-slate-400">{chip.label}</span>
                <span className="text-slate-200">{chip.value}</span>
                <span className="text-xs text-slate-500">×</span>
              </button>
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
