"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { Badge, type BadgeTone } from "@/components/Badge";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import {
  ApiError,
  getSignalsAll,
  type SignalItem,
  type SignalMode,
  type SignalSort,
} from "@/lib/api";
import { getInsiderDisplayName, insiderHref } from "@/lib/insider";
import { insiderRoleBadgeTone, normalizeInsiderRoleBadge, resolveInsiderDisplayName } from "@/lib/insiderRole";
import { memberHref } from "@/lib/memberSlug";
import { tickerHref } from "@/lib/ticker";
import { tickerMonoLinkClassName } from "@/lib/styles";

type ConfirmationBandFilter = "all" | "active" | "weak" | "moderate" | "strong" | "exceptional" | "strong_plus";
type ConfirmationDirection = "bullish" | "bearish" | "neutral" | "mixed";
type ConfirmationDirectionFilter = "all" | ConfirmationDirection;

function formatUSD(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}M`;
  if (value >= 1_000) return `$${(value / 1_000).toFixed(0)}K`;
  return `$${value.toFixed(0)}`;
}

function formatMultiple(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(value >= 10 ? 1 : 2)}x`;
}

function formatSignalDate(value?: string | null): string {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function titleCase(value?: string | null): string {
  const normalized = (value ?? "").replace(/_/g, " ").trim();
  if (!normalized) return "--";
  return normalized.replace(/\b\w/g, (char) => char.toUpperCase());
}

function isInsiderSignalKind(kind?: string): boolean {
  const normalized = (kind ?? "").trim().toLowerCase();
  return normalized === "insider" || normalized === "insider_trade";
}

function resolveSignalReportingCik(item: SignalItem): string | null {
  const raw = item as SignalItem & { reportingCik?: string | null };
  return raw.reporting_cik ?? raw.reportingCik ?? null;
}

function sideLabel(kind: string | undefined, tradeType?: string | null) {
  const value = (tradeType ?? kind ?? "").toLowerCase();
  if (value.includes("sell")) return { label: "Sell", klass: "border-rose-400/30 bg-rose-400/10 text-rose-200" };
  if (value.includes("buy") || value.includes("purchase")) return { label: "Buy", klass: "border-emerald-400/30 bg-emerald-400/10 text-emerald-200" };
  if (value.includes("award")) return { label: "Award", klass: "border-sky-400/30 bg-sky-400/10 text-sky-200" };
  if (value.includes("exempt")) return { label: "Exempt", klass: "border-slate-500/40 bg-slate-500/10 text-slate-200" };
  return { label: titleCase(tradeType ?? kind), klass: "border-white/10 bg-white/5 text-slate-200" };
}

function smartLabel(band?: string | null, score?: number | null) {
  const normalized = (band ?? "").toLowerCase();
  if (normalized === "exceptional" || (typeof score === "number" && score >= 85)) {
    return { label: "Exceptional", klass: "border-emerald-300/35 bg-emerald-300/10 text-emerald-100", dotClass: "bg-emerald-300" };
  }
  if (normalized === "strong" || (typeof score === "number" && score >= 70)) {
    return { label: "Strong", klass: "border-cyan-300/35 bg-cyan-300/10 text-cyan-100", dotClass: "bg-cyan-300" };
  }
  if (normalized === "moderate" || (typeof score === "number" && score >= 50)) {
    return { label: "Moderate", klass: "border-amber-300/35 bg-amber-300/10 text-amber-100", dotClass: "bg-amber-300" };
  }
  return { label: titleCase(band ?? "weak"), klass: "border-white/10 bg-white/5 text-slate-200", dotClass: "bg-slate-500" };
}

function sourceBadge(item: SignalItem): { label: string; tone: BadgeTone } {
  if (isInsiderSignalKind(item.kind)) return { label: "INSIDER", tone: "insider_default" };
  const chamber = (item.chamber ?? "").toLowerCase();
  if (chamber.includes("senate")) return { label: "SENATE", tone: "senate" };
  if (chamber.includes("house")) return { label: "HOUSE", tone: "house" };
  return { label: "CONGRESS", tone: "neutral" };
}

function confirmationClass(direction?: string | null): string {
  if (direction === "bullish") return "text-emerald-300";
  if (direction === "bearish") return "text-rose-300";
  if (direction === "mixed") return "text-amber-300";
  return "text-slate-300";
}

function freshnessTextClass(state?: string | null): string {
  if (state === "fresh" || state === "early") return "text-emerald-300";
  if (state === "active") return "text-cyan-300";
  if (state === "maturing") return "text-amber-300";
  if (state === "stale") return "text-rose-300";
  return "text-slate-400";
}

function backtestingHrefFromItems(items: SignalItem[]): string | null {
  const tickers = Array.from(new Set(items.map((item) => item.symbol).filter(Boolean))).slice(0, 25);
  if (tickers.length === 0) return null;
  const url = new URL("https://local/backtesting");
  url.searchParams.set("strategy", "signals");
  url.searchParams.set("symbols", tickers.join(","));
  return `${url.pathname}${url.search}`;
}

function cleanSignalsError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 402 || error.status === 403) return "Premium access required.";
    if (error.status === 503) return "Signals temporarily unavailable. Retry.";
    return "Unable to load signals.";
  }
  return error instanceof Error ? error.message : "Unable to load signals.";
}

export function SignalsResultsClient({
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
  activeSort,
  confirmationSortHref,
  freshnessSortHref,
  canBacktest,
  upgradeUrl,
}: {
  mode: SignalMode;
  side: string;
  limit: number;
  debug: boolean;
  sort: SignalSort;
  confirmationBand: ConfirmationBandFilter;
  confirmationDirection: ConfirmationDirectionFilter;
  minConfirmationSources: number;
  multiSourceOnly: boolean;
  card: string;
  pill: string;
  activeSort: string;
  confirmationSortHref: string;
  freshnessSortHref: string;
  canBacktest: boolean;
  upgradeUrl: string;
}) {
  const [items, setItems] = useState<SignalItem[]>([]);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const backtestingHref = useMemo(() => backtestingHrefFromItems(items), [items]);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErrorMessage(null);
    getSignalsAll({
      mode,
      side,
      sort,
      limit,
      debug,
      confirmation_band: confirmationBand,
      confirmation_direction: confirmationDirection,
      min_confirmation_sources: minConfirmationSources,
      multi_source_only: multiSourceOnly,
    })
      .then((response) => {
        if (!alive) return;
        setItems(response.items);
        setErrorMessage(null);
      })
      .catch((error) => {
        console.error("[signals] client fetch failed", error);
        if (alive) setErrorMessage(cleanSignalsError(error));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [mode, side, sort, limit, debug, confirmationBand, confirmationDirection, minConfirmationSources, multiSourceOnly]);

  return (
    <div className={`${card} min-h-[32rem] overflow-hidden`}>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-800 px-4 py-3 text-sm">
        <p className="text-slate-400">
          {loading ? "Loading signals..." : items.length > 0 ? `${items.length} visible signals` : errorMessage ? "Signals unavailable" : "No visible signals"}
        </p>
        {canBacktest ? (
          backtestingHref ? (
            <Link
              href={backtestingHref}
              prefetch={false}
              className="rounded-full border border-emerald-300/30 bg-emerald-300/10 px-3 py-1 text-xs font-semibold text-emerald-100 transition hover:border-emerald-200/40 hover:text-white"
            >
              Backtest these signals
            </Link>
          ) : (
            <span className="rounded-full border border-white/10 px-3 py-1 text-xs font-semibold text-slate-500">No tickers to backtest</span>
          )
        ) : (
          <Link
            href={upgradeUrl}
            prefetch={false}
            className="rounded-full border border-white/10 px-3 py-1 text-xs font-semibold text-slate-300 transition hover:border-white/20 hover:text-white"
          >
            Backtest Signals with Premium.
          </Link>
        )}
      </div>
      <div className="w-full">
        <table className="w-full table-fixed border-collapse text-sm">
          <colgroup>
            <col className="w-[7.5rem]" />
            <col className="w-[6.5rem]" />
            <col />
            <col className="w-[5.5rem]" />
            <col className="w-[7rem]" />
            <col className="w-[7rem]" />
            <col className="w-[5.5rem]" />
            <col className="w-[8rem]" />
            <col className="w-[6rem]" />
            <col className="w-[8rem]" />
            <col className="w-[5.5rem]" />
          </colgroup>
          <thead className="bg-slate-950/50 text-xs uppercase tracking-wider text-slate-400">
            <tr>
              <th className="px-3 py-3 text-left">Time</th>
              <th className="px-3 py-3 text-left">Ticker</th>
              <th className="px-3 py-3 text-left">Member</th>
              <th className="px-3 py-3 text-left">Side</th>
              <th className="px-3 py-3 text-left">Amount</th>
              <th className="px-3 py-3 text-left">Baseline</th>
              <th className="px-3 py-3 text-left">Multiple</th>
              <th className="px-3 py-3 text-left">Smart</th>
              <th className="px-3 py-3 text-left">Source</th>
              <th className={`px-3 py-3 text-left ${activeSort === "confirmation" ? "text-emerald-100" : ""}`}>
                <Link href={confirmationSortHref} prefetch={false} className="hover:text-white">Confirm</Link>
              </th>
              <th className={`px-3 py-3 text-left ${activeSort === "freshness" ? "text-emerald-100" : ""}`}>
                <Link href={freshnessSortHref} prefetch={false} className="hover:text-white">Fresh</Link>
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800">
            {loading || items.length === 0 ? (
              <tr>
                <td className="px-4 py-10 text-center text-slate-400" colSpan={11}>
                  {loading ? "Loading signals..." : errorMessage || "No unusual signals returned."}
                </td>
              </tr>
            ) : (
              items.map((item) => {
                const sideLabelValue = sideLabel(item.kind, item.trade_type);
                const smart = smartLabel(item.smart_band, item.smart_score);
                const source = sourceBadge(item);
                const isInsider = isInsiderSignalKind(item.kind);
                const rawPosition = item.position ?? null;
                const roleCode = normalizeInsiderRoleBadge(rawPosition);
                const roleTone = insiderRoleBadgeTone(roleCode);
                const insiderName = getInsiderDisplayName(resolveInsiderDisplayName(item.who, rawPosition));
                const insiderProfileHref = insiderHref(insiderName, resolveSignalReportingCik(item));
                const freshness = item.signal_freshness;
                return (
                  <tr key={item.event_id} className="hover:bg-slate-900/20">
                    <td className="px-3 py-3 text-slate-300"><span className="font-mono text-[12px]" title={item.ts}>{formatSignalDate(item.ts)}</span></td>
                    <td className="px-3 py-3">
                      <div className="flex items-center gap-2">
                        {item.symbol ? <AddTickerToWatchlist symbol={item.symbol} variant="compact" align="left" /> : null}
                        {tickerHref(item.symbol) ? (
                          <Link href={tickerHref(item.symbol)!} prefetch={false} className={`truncate ${tickerMonoLinkClassName}`}>{item.symbol}</Link>
                        ) : (
                          <span className="truncate font-mono text-slate-300">{item.symbol}</span>
                        )}
                      </div>
                    </td>
                    <td className="px-3 py-3 text-slate-200">
                      {isInsider ? (
                        <div className="flex min-w-0 items-center gap-2">
                          <span title={rawPosition ?? undefined}><Badge tone={roleTone}>{roleCode}</Badge></span>
                          {insiderProfileHref ? (
                            <Link href={insiderProfileHref} prefetch={false} className="min-w-0 truncate text-slate-100 hover:underline">{insiderName ?? "--"}</Link>
                          ) : (
                            <span className="min-w-0 truncate text-slate-100">{insiderName ?? "--"}</span>
                          )}
                        </div>
                      ) : (
                        <div className="flex min-w-0 items-center gap-2 overflow-hidden">
                          <Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge>
                          {item.member_bioguide_id ? (
                            <Link href={memberHref({ name: item.who, memberId: item.member_bioguide_id })} prefetch={false} className="truncate hover:underline">{item.who ?? "--"}</Link>
                          ) : (
                            item.who ?? "--"
                          )}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-3"><span className={`${pill} ${sideLabelValue.klass}`}>{sideLabelValue.label}</span></td>
                    <td className="px-3 py-3 text-slate-200" title={`${formatUSD(item.amount_min)} - ${formatUSD(item.amount_max)}`}>{formatUSD(item.amount_max)}</td>
                    <td className="px-3 py-3 text-slate-200">{formatUSD(item.baseline_median_amount_max)}</td>
                    <td className="px-3 py-3 text-slate-200">{formatMultiple(item.unusual_multiple)}</td>
                    <td className="px-3 py-3">
                      <span className={`${pill} max-w-full ${smart.klass}`}>
                        <span className={`h-2 w-2 rounded-full ${smart.dotClass}`} />
                        <span className="font-mono">{typeof item.smart_score === "number" && Number.isFinite(item.smart_score) ? item.smart_score : "--"}</span>
                        <span className="opacity-80">{smart.label}</span>
                      </span>
                    </td>
                    <td className="px-3 py-3"><Badge tone={source.tone} className="px-2 py-0.5 text-[10px]">{source.label}</Badge></td>
                    <td className="px-3 py-3">
                      <div className="min-w-0" title={item.confirmation_explanation ?? item.confirmation_status ?? undefined}>
                        <div className={`text-xs font-semibold ${confirmationClass(item.confirmation_direction)}`}>
                          {titleCase(item.confirmation_band ?? "inactive")}
                        </div>
                        <div className="mt-0.5 text-[11px] text-slate-500">
                          {item.confirmation_source_count ?? 0} src
                        </div>
                      </div>
                    </td>
                    <td className="px-3 py-3">
                      <div title={freshness ? `${freshness.freshness_label} - ${freshness.explanation}` : "Freshness unavailable"}>
                        <span className={`whitespace-nowrap text-xs font-medium ${freshnessTextClass(freshness?.freshness_state)}`}>
                          {titleCase(freshness?.freshness_state ?? "inactive")}
                        </span>
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
