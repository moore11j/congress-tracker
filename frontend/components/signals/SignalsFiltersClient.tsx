"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import type { SignalMode, SignalSort } from "@/lib/api";

type SignalFilters = {
  mode: SignalMode;
  side: string;
  limit: number;
  debug: boolean;
  sort: SignalSort;
};

const signalFilterParamKeys = [
  "mode",
  "side",
  "limit",
  "sort",
  "debug",
  "symbol",
] as const;

function filtersSignature(filters: SignalFilters): string {
  return [
    filters.mode,
    filters.side,
    filters.limit,
    filters.debug ? "1" : "0",
    filters.sort,
  ].join("|");
}

function normalizeFilters(filters: SignalFilters): SignalFilters {
  return {
    ...filters,
    mode: filters.mode === "insider" || filters.mode === "institutional" ? filters.mode : "congress",
    side: filters.side === "buy" || filters.side === "sell" ? filters.side : "all",
    sort: filters.sort === "amount" || filters.sort === "multiple" || filters.sort === "smart" ? filters.sort : "recent",
    limit: filters.limit === 50 || filters.limit === 100 ? filters.limit : 25,
  };
}

function buildSignalsHref(pathname: string, searchParamsString: string, filters: SignalFilters): string {
  const params = new URLSearchParams(searchParamsString);
  signalFilterParamKeys.forEach((key) => params.delete(key));
  params.delete("cursor");
  params.delete("cursor_stack");
  params.delete("offset");
  params.delete("page");
  params.delete("preset");

  params.set("mode", filters.mode);
  params.set("side", filters.side);
  params.set("limit", String(filters.limit));
  params.set("sort", filters.sort);
  if (filters.debug) params.set("debug", "1");

  const nextSearch = params.toString();
  return `${pathname}${nextSearch ? `?${nextSearch}` : ""}`;
}

const defaultSignalFilters: SignalFilters = {
  mode: "congress",
  side: "all",
  limit: 25,
  debug: false,
  sort: "recent",
};

const modeOptions = [
  ["congress", "Congress"],
  ["insider", "Insider"],
  ["institutional", "Institutional"],
] as const;

const sideOptions = [
  ["all", "All"],
  ["buy", "Buy"],
  ["sell", "Sell"],
] as const;

const sortOptions = [
  ["recent", "Recent"],
  ["amount", "Amount"],
  ["multiple", "Multiple"],
  ["smart", "Score"],
] as const;

function optionLabel<T extends string>(options: readonly (readonly [T, string])[], value: T): string {
  return options.find(([optionValue]) => optionValue === value)?.[1] ?? value;
}

function filterPillClassName(active: boolean): string {
  return `rounded-full border px-3 py-1 text-xs font-semibold transition ${
    active
      ? "border-emerald-300/60 bg-emerald-500/20 text-emerald-100"
      : "border-white/15 bg-white/[0.03] text-slate-300 hover:bg-white/[0.06]"
  }`;
}

export function SignalsFiltersClient({
  mode,
  side,
  limit,
  debug,
  sort,
  pill,
  defaultParams,
}: SignalFilters & {
  card: string;
  pill: string;
  defaultParams: Record<string, string>;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const initialFilters = useMemo(
    () =>
      normalizeFilters({
        mode,
        side,
        limit,
        debug,
        sort,
      }),
    [debug, limit, mode, side, sort],
  );
  const initialFiltersKey = useMemo(() => filtersSignature(initialFilters), [initialFilters]);
  const [draftFilters, setDraftFilters] = useState<SignalFilters>(() => initialFilters);
  const [appliedFilters, setAppliedFilters] = useState<SignalFilters>(() => initialFilters);
  const draftFiltersKey = filtersSignature(draftFilters);
  const appliedFiltersKey = filtersSignature(appliedFilters);
  const defaultFiltersKey = filtersSignature(defaultSignalFilters);
  const hasPendingChanges = draftFiltersKey !== appliedFiltersKey;
  const canReset = draftFiltersKey !== defaultFiltersKey || appliedFiltersKey !== defaultFiltersKey;

  useEffect(() => {
    setDraftFilters(initialFilters);
    setAppliedFilters(initialFilters);
  }, [initialFilters, initialFiltersKey]);

  const updateDraftFilters = (patch: Partial<SignalFilters>) => {
    setDraftFilters((current) => ({ ...current, ...patch }));
  };

  const applyFilters = () => {
    const nextFilters = normalizeFilters(draftFilters);
    const nextKey = filtersSignature(nextFilters);
    if (nextKey === appliedFiltersKey) return;
    setDraftFilters(nextFilters);
    setAppliedFilters(nextFilters);
    router.push(buildSignalsHref(pathname, searchParamsString, nextFilters), { scroll: false });
  };

  const resetFilters = () => {
    if (!canReset) return;
    setDraftFilters(defaultSignalFilters);
    setAppliedFilters(defaultSignalFilters);
    router.push(buildSignalsHref(pathname, searchParamsString, defaultSignalFilters), { scroll: false });
  };

  const filterLabel = "text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500";
  const filterGroup = "space-y-1.5";
  const pillGroup = "flex flex-wrap items-center gap-1";
  const applyButtonClassName = hasPendingChanges
    ? "inline-flex h-10 w-full items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20 sm:w-auto"
    : "inline-flex h-10 w-full cursor-not-allowed items-center justify-center rounded-2xl border border-slate-800 bg-slate-950/30 px-4 text-sm font-semibold text-slate-500 sm:w-auto";
  const resetButtonClassName = canReset
    ? "inline-flex h-10 w-full items-center justify-center rounded-2xl border border-white/15 bg-slate-950/40 px-4 text-sm font-semibold text-slate-200 transition hover:bg-white/[0.06] sm:w-auto"
    : "inline-flex h-10 w-full cursor-not-allowed items-center justify-center rounded-2xl border border-slate-800 bg-slate-950/30 px-4 text-sm font-semibold text-slate-600 sm:w-auto";

  return (
    <div className="mt-6 rounded-3xl border border-white/10 bg-slate-900/70 p-6 shadow-card backdrop-blur">
      <div className="space-y-3">
        <div className="flex flex-wrap items-start gap-4">
          <div className={filterGroup}>
            <div className={filterLabel}>Mode</div>
            <div className={pillGroup}>
              {modeOptions.map(([value, label]) => (
                <button
                  key={value}
                  type="button"
                  aria-pressed={draftFilters.mode === value}
                  onClick={() => updateDraftFilters({ mode: value })}
                  className={filterPillClassName(draftFilters.mode === value)}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          <div className={filterGroup}>
            <div className={filterLabel}>Side</div>
            <div className={pillGroup}>
              {sideOptions.map(([value, label]) => (
                <button
                  key={value}
                  type="button"
                  aria-pressed={draftFilters.side === value}
                  onClick={() => updateDraftFilters({ side: value })}
                  className={filterPillClassName(draftFilters.side === value)}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          <div className={filterGroup}>
            <div className={filterLabel}>Sort</div>
            <div className={pillGroup}>
              {sortOptions.map(([value, label]) => (
                <button
                  key={value}
                  type="button"
                  aria-pressed={draftFilters.sort === value}
                  onClick={() => updateDraftFilters({ sort: value })}
                  className={filterPillClassName(draftFilters.sort === value)}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          <div className={filterGroup}>
            <div className={filterLabel}>Limit</div>
            <div className={pillGroup}>
              {[25, 50, 100].map((value) => (
                <button
                  key={value}
                  type="button"
                  aria-pressed={draftFilters.limit === value}
                  onClick={() => updateDraftFilters({ limit: value })}
                  className={filterPillClassName(draftFilters.limit === value)}
                >
                  {value}
                </button>
              ))}
            </div>
          </div>

          <div className="min-w-[280px] max-w-full rounded-2xl bg-slate-950/20 p-2 lg:ml-auto">
            <SavedViewsBar
              surface="signals"
              restoreOnLoad={true}
              defaultParams={defaultParams}
              paramKeys={[...signalFilterParamKeys]}
              inline={true}
            />
          </div>
        </div>

        <div className="flex min-w-0 flex-wrap items-center justify-between gap-3 border-t border-slate-800 pt-3">
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <button type="button" disabled={!hasPendingChanges} onClick={applyFilters} className={applyButtonClassName}>
              Apply filters
            </button>
            <button type="button" disabled={!canReset} onClick={resetFilters} className={resetButtonClassName}>
              Reset
            </button>
          </div>
          <div className="flex min-w-0 flex-wrap items-center justify-start gap-2 sm:justify-end">
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              mode <span className="text-white">{optionLabel(modeOptions, appliedFilters.mode)}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              side <span className="text-white">{optionLabel(sideOptions, appliedFilters.side as (typeof sideOptions)[number][0])}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              sort <span className="text-white">{optionLabel(sortOptions, appliedFilters.sort)}</span>
            </span>
            {hasPendingChanges ? (
              <span className={`${pill} border-amber-300/25 text-amber-100 bg-amber-300/10`}>
                pending
              </span>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}
