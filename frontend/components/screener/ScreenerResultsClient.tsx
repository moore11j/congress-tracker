"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { ApiError, getScreener, type ScreenerApiActivityOverlay, type ScreenerApiResponse, type ScreenerApiRow } from "@/lib/api";
import { formatCompanyName } from "@/lib/companyName";
import { ghostButtonClassName, tickerMonoLinkClassName } from "@/lib/styles";
import { tickerHref } from "@/lib/ticker";
import { ClickableScreenerRow } from "./ClickableScreenerRow";

const cardClassName = "rounded-2xl border border-slate-800 bg-slate-950/80 p-4 shadow-2xl shadow-black/20";
const tableCellClassName = "px-3 py-2.5 align-top";
const tableMetricClassName = `${tableCellClassName} whitespace-nowrap font-mono text-xs text-slate-300`;
const compactBadgeClassName = "inline-flex items-center rounded-full border px-2 py-1 text-xs font-semibold";
const tinyStateBadgeClassName = "inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em]";

function pageHref(params: Record<string, string | number>, overrides: Record<string, string | number | null>): string {
  const url = new URL("https://local/screener");
  Object.entries({ ...params, ...overrides }).forEach(([key, value]) => {
    if (value == null || value === "") return;
    url.searchParams.set(key, String(value));
  });
  return `${url.pathname}${url.search}`;
}

function formatCompact(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  if (Math.abs(value) >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(value) >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (Math.abs(value) >= 1_000) return `${(value / 1_000).toFixed(1)}K`;
  return value.toFixed(0);
}

function formatCurrency(value?: number | null, digits = 2): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `$${value.toFixed(digits)}`;
}

function formatCurrencyCompact(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `$${formatCompact(value)}`;
}

function formatBeta(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return value.toFixed(2);
}

function titleCase(value?: string | null): string {
  const normalized = (value ?? "").replace(/_/g, " ").trim();
  if (!normalized) return "--";
  return normalized.replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatShortDate(value?: string | null): string {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function lockedMetricLine(label: string) {
  return (
    <div className="space-y-1">
      <span className="inline-flex items-center rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-amber-100">
        Premium
      </span>
      <div className="text-[11px] leading-4 text-slate-500">{label}</div>
    </div>
  );
}

function activityTextClass(activity: ScreenerApiActivityOverlay): string {
  if (!activity.present) return "text-slate-500";
  if (activity.direction === "bearish") return "text-rose-300";
  if (activity.direction === "mixed") return "text-amber-300";
  return "text-emerald-300";
}

function activityMeta(activity: ScreenerApiActivityOverlay): string {
  if (!activity.present) return activity.label || "No recent activity";
  const freshness = typeof activity.freshness_days === "number" ? `${activity.freshness_days}d` : "recent";
  return `${titleCase(activity.direction ?? "active")} / ${freshness}`;
}

function confirmationBandClass(band: string): string {
  if (band === "exceptional") return "text-emerald-200";
  if (band === "strong") return "text-cyan-200";
  if (band === "moderate") return "text-amber-200";
  if (band === "weak") return "text-slate-300";
  return "text-slate-500";
}

function directionTextClass(direction: string): string {
  if (direction === "bullish") return "text-emerald-300";
  if (direction === "bearish") return "text-rose-300";
  if (direction === "mixed") return "text-amber-300";
  return "text-slate-400";
}

function confirmationDirectionLabel(direction: string): string {
  if (direction === "bullish") return "Bullish";
  if (direction === "bearish") return "Bearish";
  if (direction === "mixed") return "Mixed";
  return "Neutral";
}

function confirmationMeta(status: string, direction: string): string {
  if (status) return status;
  return `${confirmationDirectionLabel(direction)} confirmation`;
}

function whyNowClass(state: string, direction: string): string {
  if (state === "strong" || state === "strengthening") return "border-emerald-300/35 bg-emerald-300/10 text-emerald-100";
  if (state === "early") return "border-cyan-300/35 bg-cyan-300/10 text-cyan-100";
  if (state === "mixed" || direction === "mixed") return "border-amber-300/35 bg-amber-300/10 text-amber-100";
  if (state === "fading") return "border-rose-300/35 bg-rose-300/10 text-rose-100";
  return "border-white/10 bg-white/5 text-slate-300";
}

function freshnessStateLabel(state: string): string {
  if (state === "fresh" || state === "early") return "Fresh";
  if (state === "active") return "Active";
  if (state === "maturing") return "Maturing";
  if (state === "stale") return "Stale";
  return "Inactive";
}

function cleanScreenerError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402 || error.status === 403) return "Premium access required.";
    return "Unable to load screener.";
  }
  return error instanceof Error ? error.message : "Unable to load screener.";
}

function SortHeader({
  params,
  sort,
  label,
  locked = false,
}: {
  params: Record<string, string | number>;
  sort: string;
  label: string;
  locked?: boolean;
}) {
  const active = params.sort === sort;
  const nextDir = active && params.sort_dir === "desc" ? "asc" : "desc";
  if (locked) {
    return (
      <th className="px-3 py-2.5 text-left">
        <span className="inline-flex items-center gap-2 text-slate-400">
          {label}
          <span className="rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold tracking-[0.16em] text-amber-100">
            Premium
          </span>
        </span>
      </th>
    );
  }
  return (
    <th className={`px-3 py-2.5 text-left ${active ? "bg-emerald-400/[0.05] text-emerald-100" : ""}`}>
      <Link href={pageHref(params, { sort, sort_dir: nextDir, page: 1 })} className="inline-flex items-center gap-1 hover:text-white" prefetch={false}>
        {label}
        <span className="text-[10px] font-semibold normal-case tracking-normal text-slate-600">
          {active ? (params.sort_dir === "asc" ? "asc" : "desc") : ""}
        </span>
      </Link>
    </th>
  );
}

function InstitutionalActivityCell({ row, intelligenceLocked }: { row: ScreenerApiRow; intelligenceLocked?: boolean }) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
  if (!row.institutional_activity_active || row.institutional_activity_status !== "ok") return <span className="text-sm text-slate-500">--</span>;
  return (
    <div className="min-w-[10rem]">
      <div className="text-sm font-semibold text-slate-100">
        {formatCurrencyCompact(row.institutional_activity_net_activity)} net / {titleCase(row.institutional_activity_direction ?? "neutral")}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        {row.institutional_activity_institution_count ?? 0} institution{row.institutional_activity_institution_count === 1 ? "" : "s"}
      </div>
    </div>
  );
}

function OptionsFlowCell({ row, intelligenceLocked }: { row: ScreenerApiRow; intelligenceLocked?: boolean }) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
  if (!row.options_flow_active || row.options_flow_status === "unavailable") return <span className="text-sm text-slate-500">--</span>;
  return (
    <div className="min-w-[10rem]">
      <div className="text-sm font-semibold text-slate-100">
        {row.options_flow_score ?? "--"} / {titleCase(row.options_flow_direction ?? "neutral")}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        {formatCurrencyCompact(row.options_flow_total_premium)} premium / {titleCase(row.options_flow_intensity ?? "low")}
      </div>
    </div>
  );
}

function GovernmentContractsMetricCell({
  row,
  intelligenceLocked,
  availabilityStatus,
}: {
  row: ScreenerApiRow;
  intelligenceLocked?: boolean;
  availabilityStatus?: string;
}) {
  if (intelligenceLocked) return lockedMetricLine("Locked intelligence");
  if (availabilityStatus === "unavailable" && row.government_contracts_status !== "ok") {
    return <span className="text-sm text-slate-500">Unavailable</span>;
  }
  if (!row.government_contracts_active) return <span className="text-sm text-slate-500">--</span>;
  const count = row.government_contracts_count ?? 0;
  return (
    <div className="min-w-[11rem]">
      <div className="text-sm font-semibold text-slate-100">
        {formatCurrencyCompact(row.government_contracts_total_amount)} / {count} contract{count === 1 ? "" : "s"}
      </div>
      <div className="mt-0.5 truncate text-[11px] leading-4 text-slate-500">
        Latest: {formatShortDate(row.government_contracts_latest_date)} / {row.government_contracts_top_agency ?? "Agency"}
      </div>
    </div>
  );
}

function WhyNowHover({ row, locked = false }: { row: ScreenerApiRow; locked?: boolean }) {
  if (locked) {
    return (
      <div className="space-y-1">
        <span className="inline-flex items-center rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-amber-100">
          Locked
        </span>
        <div className="text-[11px] leading-4 text-slate-500">Premium Why Now + freshness</div>
      </div>
    );
  }
  const tooltipId = `why-now-client-${row.symbol}`;
  return (
    <div className="group/why relative inline-flex max-w-full items-center">
      <button
        type="button"
        aria-describedby={tooltipId}
        className={`${tinyStateBadgeClassName} ${whyNowClass(row.why_now.state, row.confirmation.direction)} cursor-help transition hover:border-white/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30`}
      >
        {titleCase(row.why_now.state)}
      </button>
      <div
        id={tooltipId}
        role="tooltip"
        className="pointer-events-none invisible absolute right-0 top-full z-30 mt-2 w-72 rounded-xl border border-white/10 bg-slate-950/95 p-3 text-left opacity-0 shadow-2xl shadow-black/40 backdrop-blur transition group-hover/why:visible group-hover/why:opacity-100 group-focus-within/why:visible group-focus-within/why:opacity-100"
      >
        <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Why now</p>
        <p className="mt-1 text-sm leading-5 text-slate-100">{row.why_now.headline}</p>
        <p className="mt-2 text-xs leading-4 text-slate-500">{row.confirmation.status}</p>
        <p className="mt-1 text-xs leading-4 text-slate-500">Freshness: {row.signal_freshness.freshness_label}</p>
      </div>
    </div>
  );
}

function ScreenerTableRow({
  row,
  intelligenceLocked = false,
  governmentContractsAvailabilityStatus = "ok",
}: {
  row: ScreenerApiRow;
  intelligenceLocked?: boolean;
  governmentContractsAvailabilityStatus?: string;
}) {
  const href = tickerHref(row.symbol) ?? row.ticker_url ?? `/ticker/${encodeURIComponent(row.symbol)}`;
  const confirmationDirection = confirmationDirectionLabel(row.confirmation.direction);
  const confirmationSourceMeta = confirmationMeta(row.confirmation.status, row.confirmation.direction);
  return (
    <ClickableScreenerRow href={href} label={`Open ${row.symbol} ticker page`}>
      <td className={`${tableCellClassName} whitespace-nowrap`}>
        <div className="flex items-center gap-2">
          <AddTickerToWatchlist symbol={row.symbol} variant="compact" align="left" />
          <Link href={href} prefetch={false} className={`${tickerMonoLinkClassName} transition group-hover:text-emerald-100`}>
            {row.symbol}
          </Link>
        </div>
      </td>
      <td className={`${tableCellClassName} min-w-[14rem]`}>
        <Link
          href={href}
          prefetch={false}
          className="block max-w-[18rem] truncate font-medium text-slate-100 transition hover:text-white hover:underline group-hover:text-white"
          title={formatCompanyName(row.company_name)}
        >
          {formatCompanyName(row.company_name)}
        </Link>
        <div className="mt-0.5 text-xs leading-4 text-slate-500">
          {[row.exchange, row.country].filter(Boolean).join(" / ") || "--"}
        </div>
      </td>
      <td className={`${tableCellClassName} min-w-[12rem] text-slate-300`}>
        <div className="max-w-[12rem] truncate">{row.sector ?? "--"}</div>
        {row.industry ? <div className="mt-0.5 max-w-[12rem] truncate text-xs leading-4 text-slate-500">{row.industry}</div> : null}
      </td>
      <td className={tableMetricClassName}>{formatCompact(row.market_cap)}</td>
      <td className={tableMetricClassName}>{formatCurrency(row.price)}</td>
      <td className={tableMetricClassName}>{formatCompact(row.volume)}</td>
      <td className={tableMetricClassName}>{formatBeta(row.beta)}</td>
      <td className={`${tableCellClassName} whitespace-nowrap`} title={row.congress_activity.label}>
        {intelligenceLocked ? (
          lockedMetricLine("Locked intelligence")
        ) : (
          <>
            <div className={`text-xs font-semibold ${activityTextClass(row.congress_activity)}`}>{row.congress_activity.present ? "Active" : "None"}</div>
            <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.congress_activity)}</div>
          </>
        )}
      </td>
      <td className={`${tableCellClassName} whitespace-nowrap`} title={row.insider_activity.label}>
        {intelligenceLocked ? (
          lockedMetricLine("Locked intelligence")
        ) : (
          <>
            <div className={`text-xs font-semibold ${activityTextClass(row.insider_activity)}`}>{row.insider_activity.present ? "Active" : "None"}</div>
            <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{activityMeta(row.insider_activity)}</div>
          </>
        )}
      </td>
      <td className={`${tableCellClassName} min-w-[10rem]`}><InstitutionalActivityCell row={row} intelligenceLocked={intelligenceLocked} /></td>
      <td className={`${tableCellClassName} min-w-[10rem]`}><OptionsFlowCell row={row} intelligenceLocked={intelligenceLocked} /></td>
      <td className={`${tableCellClassName} min-w-[11rem]`}>
        <GovernmentContractsMetricCell row={row} intelligenceLocked={intelligenceLocked} availabilityStatus={governmentContractsAvailabilityStatus} />
      </td>
      <td className={`${tableCellClassName} min-w-[8.5rem] whitespace-nowrap`} title={row.confirmation.status}>
        {intelligenceLocked ? (
          lockedMetricLine("Confirmation score, band, and direction are locked.")
        ) : (
          <>
            <div className="flex items-baseline gap-1.5">
              <span className="text-sm font-semibold tabular-nums text-slate-100">{row.confirmation.score}</span>
              <span className={`text-xs font-medium ${confirmationBandClass(row.confirmation.band)}`}>{titleCase(row.confirmation.band)}</span>
            </div>
            <div className={`mt-0.5 text-[11px] font-semibold uppercase tracking-[0.14em] ${directionTextClass(row.confirmation.direction)}`}>{confirmationDirection}</div>
            <div className="mt-0.5 text-[11px] leading-4 text-slate-500">{confirmationSourceMeta}</div>
          </>
        )}
      </td>
      <td className={`${tableCellClassName} min-w-[8rem] max-w-[10rem]`}>
        <WhyNowHover row={row} locked={intelligenceLocked} />
        <div className="mt-1 text-[11px] leading-4 text-slate-500">
          {intelligenceLocked ? "Premium freshness" : freshnessStateLabel(row.signal_freshness.freshness_state)}
        </div>
      </td>
    </ClickableScreenerRow>
  );
}

export function ScreenerResultsClient({
  params,
  page,
  pageSize,
  intelligenceLocked,
  resultCap,
}: {
  params: Record<string, string | number>;
  page: number;
  pageSize: number;
  intelligenceLocked: boolean;
  resultCap: number;
}) {
  const [data, setData] = useState<ScreenerApiResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErrorMessage(null);
    getScreener(params)
      .then((response) => {
        if (!alive) return;
        setData(response);
        setErrorMessage(null);
      })
      .catch((error) => {
        console.error("[screener] client fetch failed", error);
        if (alive) setErrorMessage(cleanScreenerError(error));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [params]);

  const rows = data?.items ?? [];
  const totalAvailable = data?.total_available ?? 0;
  const hasNext = data?.has_next ?? false;
  const governmentContractsAvailabilityStatus = data?.overlay_availability?.government_contracts?.status ?? "ok";

  return (
    <div className={`${cardClassName} min-h-[34rem] overflow-hidden p-0`}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 bg-slate-950/50 px-4 py-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Results</h2>
          <p className="mt-1 text-sm text-slate-400">
            {loading ? "Loading screener..." : errorMessage ? "Screener data temporarily unavailable" : `${rows.length} shown from ${totalAvailable} available results`}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <Link href={pageHref(params, { page: Math.max(page - 1, 1) })} className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${page <= 1 ? "pointer-events-none opacity-40" : ""}`} prefetch={false}>
            Prev
          </Link>
          <span className={`${compactBadgeClassName} rounded-lg border-slate-800 bg-slate-950/40 px-3 py-2 text-slate-300`}>Page {page}</span>
          <span className={`${compactBadgeClassName} rounded-lg border-slate-800 bg-slate-950/40 px-3 py-2 text-slate-300`}>cap {data?.result_cap ?? resultCap}</span>
          <Link href={pageHref(params, { page: page + 1 })} className={`${ghostButtonClassName} rounded-lg px-3 py-2 text-xs ${!hasNext ? "pointer-events-none opacity-40" : ""}`} prefetch={false}>
            Next
          </Link>
        </div>
      </div>

      <div className="overflow-x-auto overflow-y-hidden">
        <table className="min-w-full border-collapse text-sm">
          <thead className="bg-slate-950/50 text-xs uppercase tracking-wider text-slate-400">
            <tr>
              <SortHeader params={params} sort="symbol" label="Symbol" />
              <th className="px-3 py-2.5 text-left">Company</th>
              <th className="px-3 py-2.5 text-left">Sector</th>
              <SortHeader params={params} sort="market_cap" label="Market cap" />
              <SortHeader params={params} sort="price" label="Price" />
              <SortHeader params={params} sort="volume" label="Volume" />
              <SortHeader params={params} sort="beta" label="Beta" />
              <SortHeader params={params} sort="congress_activity" label="Congress" locked={intelligenceLocked} />
              <SortHeader params={params} sort="insider_activity" label="Insiders" locked={intelligenceLocked} />
              <th className="px-3 py-2.5 text-left">Institutional</th>
              <th className="px-3 py-2.5 text-left">Options Flow</th>
              <th className="px-3 py-2.5 text-left">Gov Contracts</th>
              <SortHeader params={params} sort="confirmation_score" label="Confirm" locked={intelligenceLocked} />
              <th className="px-3 py-2.5 text-left">
                <span className="inline-flex items-center gap-2">
                  Why Now
                  {intelligenceLocked ? (
                    <span className="rounded-full border border-amber-300/30 bg-amber-300/10 px-2 py-0.5 text-[10px] font-semibold tracking-[0.16em] text-amber-100">
                      Premium
                    </span>
                  ) : null}
                </span>
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {loading || errorMessage || rows.length === 0 ? (
              <tr>
                <td className="px-4 py-12 text-center text-slate-400" colSpan={14}>
                  {loading ? "Loading screener..." : errorMessage || "No names matched this screen. Widen the market cap, liquidity, or sector filters."}
                </td>
              </tr>
            ) : (
              rows.map((row) => (
                <ScreenerTableRow
                  key={row.symbol}
                  row={row}
                  intelligenceLocked={intelligenceLocked}
                  governmentContractsAvailabilityStatus={governmentContractsAvailabilityStatus}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800 bg-slate-950/40 px-4 py-3 text-xs text-slate-500">
        <span>
          Page size {pageSize}. Result cap {data?.result_cap ?? resultCap}. Confirmation overlays use a {data?.lookback_days ?? params.lookback_days ?? 30}d lookback.
        </span>
        <span>
          Market data with Walnut overlays
          {data?.overlay_availability?.options_flow?.filterable === false ? " / options flow unavailable" : ""}
          {data?.overlay_availability?.institutional_activity?.filterable === false ? " / institutional unavailable" : ""}
        </span>
      </div>
    </div>
  );
}
