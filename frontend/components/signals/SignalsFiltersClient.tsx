"use client";

import { useEffect, useMemo, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import type { SignalMode, SignalSort } from "@/lib/api";

type ConfirmationBandFilter = "all" | "active" | "weak" | "moderate" | "strong" | "exceptional" | "strong_plus";
type ConfirmationDirection = "bullish" | "bearish" | "neutral" | "mixed";
type ConfirmationDirectionFilter = "all" | ConfirmationDirection;

type SignalFilters = {
  mode: SignalMode;
  side: string;
  limit: number;
  debug: boolean;
  sort: SignalSort;
  confirmationBand: ConfirmationBandFilter;
  confirmationDirection: ConfirmationDirectionFilter;
  minConfirmationSources: number;
  multiSourceOnly: boolean;
};

const signalFilterParamKeys = [
  "mode",
  "side",
  "limit",
  "sort",
  "debug",
  "symbol",
  "confirmation_band",
  "confirmation_direction",
  "min_confirmation_sources",
  "multi_source_only",
] as const;

function filtersSignature(filters: SignalFilters): string {
  return [
    filters.mode,
    filters.side,
    filters.limit,
    filters.debug ? "1" : "0",
    filters.sort,
    filters.confirmationBand,
    filters.confirmationDirection,
    filters.minConfirmationSources,
    filters.multiSourceOnly ? "1" : "0",
  ].join("|");
}

function activeMinConfirmationSources(filters: SignalFilters): number {
  return filters.multiSourceOnly && filters.minConfirmationSources < 2 ? 2 : filters.minConfirmationSources;
}

function normalizeFilters(filters: SignalFilters): SignalFilters {
  const minConfirmationSources = activeMinConfirmationSources(filters);
  return {
    ...filters,
    minConfirmationSources,
    multiSourceOnly: minConfirmationSources >= 2,
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
  if (filters.confirmationBand !== "all") params.set("confirmation_band", filters.confirmationBand);
  if (filters.confirmationDirection !== "all") params.set("confirmation_direction", filters.confirmationDirection);
  if (filters.minConfirmationSources > 0) params.set("min_confirmation_sources", String(filters.minConfirmationSources));
  if (filters.multiSourceOnly) params.set("multi_source_only", "1");
  if (filters.debug) params.set("debug", "1");

  const nextSearch = params.toString();
  return `${pathname}${nextSearch ? `?${nextSearch}` : ""}`;
}

export function SignalsFiltersClient({
  mode,
  side,
  limit,
  debug,
  sort,
  confirmationBand,
  confirmationDirection,
  minConfirmationSources,
  multiSourceOnly,
  card,
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
        confirmationBand,
        confirmationDirection,
        minConfirmationSources,
        multiSourceOnly,
      }),
    [confirmationBand, confirmationDirection, debug, limit, minConfirmationSources, mode, multiSourceOnly, side, sort],
  );
  const initialFiltersKey = useMemo(() => filtersSignature(initialFilters), [initialFilters]);
  const [draftFilters, setDraftFilters] = useState<SignalFilters>(() => initialFilters);
  const [appliedFilters, setAppliedFilters] = useState<SignalFilters>(() => initialFilters);
  const draftFiltersKey = filtersSignature(draftFilters);
  const appliedFiltersKey = filtersSignature(appliedFilters);
  const hasPendingChanges = draftFiltersKey !== appliedFiltersKey;
  const activeAppliedMinSources = activeMinConfirmationSources(appliedFilters);

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

  const btn = "inline-flex items-center justify-center rounded-full border px-3 py-1 text-xs font-medium transition hover:bg-slate-900/60";
  const btnActive = "border-emerald-500/40 text-emerald-200 bg-emerald-500/10";
  const btnIdle = "border-slate-800 text-slate-200 bg-slate-950/30";
  const filterRow = "flex flex-col items-stretch gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:gap-x-3 sm:gap-y-2";
  const filterGroup = "flex max-w-full flex-wrap items-center gap-2 rounded-2xl border border-slate-800 bg-slate-950/30 p-1 sm:inline-flex sm:rounded-full";
  const applyButtonClassName = hasPendingChanges
    ? "inline-flex h-10 w-full items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20 sm:w-auto"
    : "inline-flex h-10 w-full cursor-not-allowed items-center justify-center rounded-2xl border border-slate-800 bg-slate-950/30 px-4 text-sm font-semibold text-slate-500 sm:w-auto";

  return (
    <div className={`mt-6 p-4 ${card}`}>
      <div className="space-y-3">
        <div className={filterRow}>
          <div className="text-xs text-slate-400">Mode</div>
          <div className={filterGroup}>
            {([
              ["all", "ALL"],
              ["congress", "CONGRESS"],
              ["insider", "INSIDER"],
            ] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ mode: value })}
                className={`${btn} ${draftFilters.mode === value ? btnActive : btnIdle}`}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="text-xs text-slate-400">Side</div>
          <div className={filterGroup}>
            {([
              ["all", "All"],
              ["buy", "Buy"],
              ["sell", "Sell"],
              ["buy_or_sell", "Buy/Sell"],
            ] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ side: value })}
                className={`${btn} ${draftFilters.side === value ? btnActive : btnIdle}`}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="text-xs text-slate-400 sm:ml-2">Sort</div>
          <div className={filterGroup}>
            {([
              ["multiple", "MULTIPLE"],
              ["smart", "CONVICTION"],
              ["confirmation", "CONFIRM"],
              ["freshness", "FRESH"],
              ["recent", "RECENT"],
              ["amount", "AMOUNT"],
            ] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ sort: value })}
                className={`${btn} ${draftFilters.sort === value ? btnActive : btnIdle}`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>

        <div className={filterRow}>
          <div className="text-xs text-slate-400">Confirm</div>
          <div className={filterGroup}>
            {([
              ["all", "All"],
              ["strong_plus", "Strong+"],
              ["exceptional", "Exceptional"],
              ["moderate", "Moderate"],
            ] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ confirmationBand: value })}
                className={`${btn} ${draftFilters.confirmationBand === value ? btnActive : btnIdle}`}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="text-xs text-slate-400">Direction</div>
          <div className={filterGroup}>
            {([
              ["all", "All"],
              ["bullish", "Bull"],
              ["bearish", "Bear"],
              ["mixed", "Mixed"],
            ] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ confirmationDirection: value })}
                className={`${btn} ${draftFilters.confirmationDirection === value ? btnActive : btnIdle}`}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="text-xs text-slate-400">Sources</div>
          <div className={filterGroup}>
            {([
              [0, "Any"],
              [2, "2+"],
              [3, "3+"],
            ] as const).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ minConfirmationSources: value, multiSourceOnly: value >= 2 })}
                className={`${btn} ${activeMinConfirmationSources(draftFilters) === value ? btnActive : btnIdle}`}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="text-xs text-slate-400 sm:ml-2">Limit</div>
          <div className="flex max-w-full flex-wrap items-center gap-2">
            {[25, 50, 100].map((value) => (
              <button
                key={value}
                type="button"
                onClick={() => updateDraftFilters({ limit: value })}
                className={`${btn} ${draftFilters.limit === value ? btnActive : btnIdle}`}
              >
                {value}
              </button>
            ))}
          </div>
        </div>

        <div className="pt-1">
          <button type="button" disabled={!hasPendingChanges} onClick={applyFilters} className={applyButtonClassName}>
            Apply filters
          </button>
        </div>
      </div>
      <SavedViewsBar
        surface="signals"
        restoreOnLoad={true}
        defaultParams={defaultParams}
        paramKeys={[...signalFilterParamKeys]}
        rightSlot={
          <>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              mode <span className="text-white">{appliedFilters.mode}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              side <span className="text-white">{appliedFilters.side}</span>
            </span>
            <span className={`${pill} border-slate-800 text-slate-300 bg-slate-950/30`}>
              sort <span className="text-white">{appliedFilters.sort}</span>
            </span>
            {appliedFilters.confirmationBand !== "all" || appliedFilters.confirmationDirection !== "all" || activeAppliedMinSources > 0 ? (
              <span className={`${pill} border-cyan-400/25 text-cyan-100 bg-cyan-400/10`}>
                confirm{" "}
                <span className="text-white">
                  {appliedFilters.confirmationBand !== "all"
                    ? appliedFilters.confirmationBand
                    : appliedFilters.confirmationDirection !== "all"
                      ? appliedFilters.confirmationDirection
                      : `${activeAppliedMinSources}+ src`}
                </span>
              </span>
            ) : null}
            {hasPendingChanges ? (
              <span className={`${pill} border-amber-300/25 text-amber-100 bg-amber-300/10`}>
                pending
              </span>
            ) : null}
          </>
        }
      />
    </div>
  );
}
