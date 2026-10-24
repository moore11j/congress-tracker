"use client";

import { FeedFilterAutoSubmit } from "@/components/feed/FeedFilterAutoSubmit";
import { FeedMemberAutosuggestEnhancer } from "@/components/feed/FeedMemberAutosuggestEnhancer";
import { FeedRoleAutosuggestEnhancer } from "@/components/feed/FeedRoleAutosuggestEnhancer";
import { FeedSymbolAutosuggestEnhancer } from "@/components/feed/FeedSymbolAutosuggestEnhancer";
import { SavedViewsBar } from "@/components/saved-views/SavedViewsBar";
import { feedModeOptions, type FeedMode } from "@/lib/feedModes";
import { activeFilterControlClassName, cardClassName, ghostButtonClassName, inputClassName, selectClassName } from "@/lib/styles";
import { useEffect, useState, type ReactNode } from "react";
import { getEntitlements } from "@/lib/api";
import { feedAccess, feedPageSize, feedSortOptions, type FeedPageSize, type FeedSortKey } from "@/lib/feedControls";

type FeedSortBy = FeedSortKey;
type FeedSortDir = "asc" | "desc";

type FeedFiltersServerProps = {
  mode: FeedMode;
  pageSize?: FeedPageSize;
  params: {
    symbol?: string;
    recent_days?: string;
    member?: string;
    chamber?: string;
    party?: string;
    trade_type?: string;
    role?: string;
    department?: string;
    sort_by?: string;
    sort_dir?: string;
  };
};

const sortOptions = feedSortOptions;

const directionOptions = [
  ["desc", "Desc"],
  ["asc", "Asc"],
] as const satisfies readonly (readonly [FeedSortDir, string])[];

function modeHref(nextMode: FeedMode, params: FeedFiltersServerProps["params"], pageSize: number) {
  const url = new URLSearchParams();
  url.set("mode", nextMode);
  url.set("page_size", String(pageSize));
  const keys =
    nextMode === "institutional"
      ? (["symbol", "member", "trade_type", "sort_by", "sort_dir"] as const)
      : nextMode === "government_contracts"
      ? (["symbol", "department", "trade_type", "sort_by", "sort_dir"] as const)
      : nextMode === "congress"
      ? (["symbol", "member", "trade_type", "party", "chamber", "sort_by", "sort_dir", "recent_days"] as const)
      : nextMode === "insider"
      ? (["symbol", "member", "trade_type", "role", "sort_by", "sort_dir", "recent_days"] as const)
      : (["symbol", "member", "trade_type", "sort_by", "sort_dir", "recent_days"] as const);
  for (const key of keys) {
    const value = params[key]?.trim();
    if (value) url.set(key, value);
  }
  return `/?${url.toString()}`;
}

function hasFilterValue(value?: string): boolean {
  return (value ?? "").trim().length > 0;
}

function feedInputClassName(value?: string): string {
  return hasFilterValue(value) ? `${inputClassName} ${activeFilterControlClassName}` : inputClassName;
}

function feedSelectClassName(value?: string): string {
  return hasFilterValue(value) ? `${selectClassName} ${activeFilterControlClassName}` : selectClassName;
}

function normalizeSortBy(value?: string): FeedSortBy {
  return sortOptions.some(([key]) => key === value) ? value as FeedSortBy : "filed_after";
}

function normalizeSortDir(value?: string): FeedSortDir {
  return value === "asc" ? "asc" : "desc";
}

const fieldBaseClassName = "min-w-0";
const symbolFieldClassName = `${fieldBaseClassName} w-full sm:w-[128px]`;
const nameFieldClassName = `${fieldBaseClassName} relative w-full sm:w-[230px]`;
const selectFieldClassName = `${fieldBaseClassName} w-full sm:w-[150px]`;
const narrowSelectFieldClassName = `${fieldBaseClassName} w-full sm:w-[124px]`;
const daysFieldClassName = `${fieldBaseClassName} w-full sm:w-[140px]`;

function SymbolField({ mode, value }: { mode: FeedMode; value?: string }) {
  return (
    <div className={`${symbolFieldClassName} relative`}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Symbol</label>
      <input id="feed-filter-symbol" name="symbol" defaultValue={value ?? ""} className={feedInputClassName(value)} placeholder="NVDA" autoComplete="off" />
      <FeedSymbolAutosuggestEnhancer formId="feed-filters-form" inputName="symbol" mode={mode} />
    </div>
  );
}

function NameField({ label, value, mode, placeholder = "Pelosi" }: { label: string; value?: string; mode: FeedMode; placeholder?: string }) {
  return (
    <div className={nameFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">{label}</label>
      <input id="feed-filter-member" name="member" defaultValue={value ?? ""} className={feedInputClassName(value)} placeholder={placeholder} autoComplete="off" />
      <FeedMemberAutosuggestEnhancer formId="feed-filters-form" inputName="member" mode={mode} />
    </div>
  );
}

function DepartmentField({ value }: { value?: string }) {
  return (
    <div className={`${nameFieldClassName} sm:w-[280px]`}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Department</label>
      <input id="feed-filter-department" name="department" defaultValue={value ?? ""} className={feedInputClassName(value)} placeholder="Department of Defense" autoComplete="off" />
      <FeedSymbolAutosuggestEnhancer formId="feed-filters-form" inputName="department" mode="government_contracts" selectValue="label" />
    </div>
  );
}

function TradeTypeField({ value }: { value?: string }) {
  return (
    <div className={selectFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Trade Type</label>
      <select name="trade_type" defaultValue={value ?? ""} className={feedSelectClassName(value)}>
        <option value="">All types</option>
        <option value="purchase">Purchase</option>
        <option value="sale">Sale</option>
      </select>
    </div>
  );
}

function SortField({ value, enabled }: { value?: string; enabled: boolean }) {
  const normalized = normalizeSortBy(value);
  return (
    <div className={selectFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Sort by {enabled ? "" : "(Pro)"}</label>
      <select name="sort_by" disabled={!enabled} defaultValue={normalized} className={`${feedSelectClassName(value)} disabled:opacity-50`} aria-label="Sort displayed rows (Pro)">
        {sortOptions.map(([optionValue, label]) => (
          <option key={optionValue} value={optionValue}>
            {label}
          </option>
        ))}
      </select>
    </div>
  );
}

function DirectionField({ value, enabled }: { value?: string; enabled: boolean }) {
  const normalized = normalizeSortDir(value);
  return (
    <div className={narrowSelectFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Direction</label>
      <select name="sort_dir" disabled={!enabled} defaultValue={normalized} className={`${feedSelectClassName(value)} disabled:opacity-50`} aria-label="Sort direction">
        {directionOptions.map(([optionValue, label]) => (
          <option key={optionValue} value={optionValue}>
            {label}
          </option>
        ))}
      </select>
    </div>
  );
}

function RecentDaysField({ value }: { value?: string }) {
  return (
    <div className={daysFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Recent Days</label>
      <select name="recent_days" defaultValue={value ?? ""} className={feedSelectClassName(value)}>
        <option value="">Anytime</option>
        <option value="1">1 day</option>
        <option value="7">7 days</option>
        <option value="30">30 days</option>
        <option value="90">90 days</option>
      </select>
    </div>
  );
}

function PartyField({ value }: { value?: string }) {
  return (
    <div className={selectFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Party</label>
      <select name="party" defaultValue={value ?? ""} className={feedSelectClassName(value)}>
        <option value="">All parties</option>
        <option value="democrat">Democrat</option>
        <option value="republican">Republican</option>
        <option value="independent">Independent</option>
      </select>
    </div>
  );
}

function ChamberField({ value }: { value?: string }) {
  return (
    <div className={selectFieldClassName}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Chamber</label>
      <select name="chamber" defaultValue={value ?? ""} className={feedSelectClassName(value)}>
        <option value="">All chambers</option>
        <option value="house">House</option>
        <option value="senate">Senate</option>
      </select>
    </div>
  );
}

function RoleField({ value }: { value?: string }) {
  return (
    <div className={`${fieldBaseClassName} relative w-full sm:w-[150px]`}>
      <label className="text-xs font-semibold uppercase tracking-wide text-slate-400">Role</label>
      <input id="feed-filter-role" name="role" defaultValue={value ?? ""} className={feedInputClassName(value)} placeholder="CEO" autoComplete="off" />
      <FeedRoleAutosuggestEnhancer formId="feed-filters-form" inputName="role" />
    </div>
  );
}

function FilterRow({ children }: { children: ReactNode }) {
  return <div className="flex flex-wrap items-end justify-start gap-3">{children}</div>;
}

function SavedFeedViews({ mode }: { mode: FeedMode }) {
  return (
    <div className="min-w-[220px] max-w-full sm:ml-auto">
      <SavedViewsBar
        surface="feed"
        restoreOnLoad={true}
        defaultParams={{ mode }}
        paramKeys={[
          "mode",
          "symbol",
          "recent_days",
          "member",
          "chamber",
          "party",
          "trade_type",
          "role",
          "department",
          "sort_by",
          "sort_dir",
          "ownership",
          "whale",
          "limit",
          "page_size",
        ]}
        inline={true}
      />
    </div>
  );
}

export function FeedFiltersServer({ mode, params, pageSize = 25 }: FeedFiltersServerProps) {
  const [access, setAccess] = useState({ canExpand: false, canSortExport: false });
  useEffect(() => {
    let cancelled = false;
    getEntitlements(undefined, { source: "FeedFilters" }).then((value) => {
      if (!cancelled) setAccess(feedAccess(value));
    }).catch(() => undefined);
    return () => { cancelled = true; };
  }, []);
  const selectedPageSize = feedPageSize(pageSize, access.canExpand);
  const normalizedParams = {
    ...params,
    sort_by: access.canSortExport ? normalizeSortBy(params.sort_by) : "",
    sort_dir: access.canSortExport ? normalizeSortDir(params.sort_dir) : "",
  };
  const formKey = JSON.stringify({ mode, ...normalizedParams });
  const modeLabel = feedModeOptions.find(([value]) => value === mode)?.[1] ?? "All";

  return (
    <details className={`${cardClassName} group relative z-30 overflow-visible`}>
      <summary className="flex cursor-pointer list-none flex-wrap items-center justify-between gap-3 outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/60 [&::-webkit-details-marker]:hidden">
        <div>
          <h2 className="text-lg font-semibold text-white">Feed mode & filters</h2>
          <p className="text-sm text-slate-400">Collapsed. Current mode: {modeLabel}.</p>
        </div>
        <span className="inline-flex h-10 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.03] px-4 text-sm font-semibold text-slate-200 transition group-open:hidden">
          Expand
        </span>
        <span className="hidden h-10 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.03] px-4 text-sm font-semibold text-slate-200 transition group-open:inline-flex">
          Collapse
        </span>
      </summary>

      <div className="mt-4 flex flex-wrap gap-1 border-t border-slate-800 pt-3">
        {feedModeOptions.map(([value, label]) => {
          const active = mode === value;
          return (
            <a
              key={value}
              href={modeHref(value, normalizedParams, selectedPageSize)}
              className={`rounded-full border px-3 py-1 text-xs font-semibold transition ${
                active
                  ? "border-emerald-300/60 bg-emerald-500/20 text-emerald-100"
                  : "border-white/15 bg-white/[0.03] text-slate-300 hover:bg-white/[0.06]"
              }`}
            >
              {label}
            </a>
          );
        })}
      </div>

      <form key={formKey} id="feed-filters-form" method="GET" action="/" className="mt-5 flex flex-col gap-4">
        <input type="hidden" name="mode" value={mode} />
        <input type="hidden" name="page_size" value={selectedPageSize} />

        {mode === "all" ? (
          <FilterRow>
            <SymbolField mode={mode} value={params.symbol} />
            <NameField label="Name" value={params.member} mode={mode} placeholder="Member, insider, department, institution" />
            <TradeTypeField value={params.trade_type} />
            <SortField value={normalizedParams.sort_by} enabled={access.canSortExport} />
            <DirectionField value={normalizedParams.sort_dir} enabled={access.canSortExport} />
            <RecentDaysField value={params.recent_days} />
            <SavedFeedViews mode={mode} />
          </FilterRow>
        ) : null}

        {mode === "congress" ? (
          <FilterRow>
            <SymbolField mode={mode} value={params.symbol} />
            <NameField label="Name" value={params.member} mode={mode} placeholder="Pelosi" />
            <TradeTypeField value={params.trade_type} />
            <PartyField value={params.party} />
            <ChamberField value={params.chamber} />
            <SortField value={normalizedParams.sort_by} enabled={access.canSortExport} />
            <DirectionField value={normalizedParams.sort_dir} enabled={access.canSortExport} />
            <RecentDaysField value={params.recent_days} />
            <SavedFeedViews mode={mode} />
          </FilterRow>
        ) : null}

        {mode === "insider" ? (
          <FilterRow>
            <SymbolField mode={mode} value={params.symbol} />
            <NameField label="Name" value={params.member} mode={mode} placeholder="Insider name" />
            <TradeTypeField value={params.trade_type} />
            <RoleField value={params.role} />
            <SortField value={normalizedParams.sort_by} enabled={access.canSortExport} />
            <DirectionField value={normalizedParams.sort_dir} enabled={access.canSortExport} />
            <RecentDaysField value={params.recent_days} />
            <SavedFeedViews mode={mode} />
          </FilterRow>
        ) : null}

        {mode === "government_contracts" ? (
          <FilterRow>
            <SymbolField mode={mode} value={params.symbol} />
            <DepartmentField value={params.department} />
            <TradeTypeField value={params.trade_type} />
            <SortField value={normalizedParams.sort_by} enabled={access.canSortExport} />
            <DirectionField value={normalizedParams.sort_dir} enabled={access.canSortExport} />
            <SavedFeedViews mode={mode} />
          </FilterRow>
        ) : null}

        {mode === "institutional" ? (
          <FilterRow>
            <SymbolField mode={mode} value={params.symbol} />
            <NameField label="Institution" value={params.member} mode={mode} placeholder="Institution name" />
            <TradeTypeField value={params.trade_type} />
            <SortField value={normalizedParams.sort_by} enabled={access.canSortExport} />
            <DirectionField value={normalizedParams.sort_dir} enabled={access.canSortExport} />
            <SavedFeedViews mode={mode} />
          </FilterRow>
        ) : null}

        <div className="flex flex-wrap items-center gap-3">
          <button
            type="submit"
            className="inline-flex h-10 items-center justify-center rounded-2xl border border-emerald-400/40 bg-emerald-500/10 px-4 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20"
          >
            Apply filters
          </button>
          <a href={`/?mode=${mode}&page_size=${selectedPageSize}`} className={ghostButtonClassName}>
            Reset
          </a>
        </div>
      </form>
      <FeedFilterAutoSubmit key={`autosubmit:${formKey}`} formId="feed-filters-form" />
    </details>
  );
}
