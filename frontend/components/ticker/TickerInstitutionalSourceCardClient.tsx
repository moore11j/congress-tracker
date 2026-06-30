"use client";

import { useEffect, useMemo, useState } from "react";
import { ApiError, getTickerSignalsSummary } from "@/lib/api";
import type { ConfirmationScoreSource } from "@/lib/types";

type InstitutionalSource = ConfirmationScoreSource & {
  locked?: boolean;
  lock_state?: string | null;
  required_plan?: string | null;
  reason?: string | null;
};

const inactiveSource: InstitutionalSource = {
  present: false,
  direction: "neutral",
  strength: 0,
  quality: 0,
  freshness_days: null,
  label: "No recent institutional activity",
  score_contribution: 0,
};

function sourceLocked(source: InstitutionalSource, canViewInstitutional = false): boolean {
  if (canViewInstitutional) return false;
  return source.locked === true || source.lock_state === "pro_locked";
}

function sourceUnavailable(source: InstitutionalSource): boolean {
  const status = (source.status ?? "").toLowerCase();
  return !source.present && ["unavailable", "not_configured", "disabled", "provider_error", "error"].includes(status);
}

function canSkipInstitutionalFetch(source: InstitutionalSource): boolean {
  return sourceUnavailable(source) && !["pro_locked", "stale_locked_metadata"].includes(source.reason ?? "");
}

function toneClass(source: InstitutionalSource, loading: boolean): string {
  if (!source.present || loading) return "text-slate-500";
  if (source.direction === "bullish") return "text-emerald-300";
  if (source.direction === "bearish") return "text-rose-300";
  if (source.direction === "mixed") return "text-amber-300";
  return "text-slate-400";
}

function borderClass(source: InstitutionalSource, loading: boolean): string {
  if (!source.present || loading) return "border-white/10 bg-white/[0.025]";
  if (source.direction === "bullish") return "border-emerald-400/20 bg-emerald-400/[0.045]";
  if (source.direction === "bearish") return "border-rose-400/20 bg-rose-400/[0.045]";
  return "border-amber-400/20 bg-amber-400/[0.04]";
}

function stateLabel(source: InstitutionalSource, loading: boolean, canViewInstitutional = false): string {
  if (sourceLocked(source, canViewInstitutional)) return "Locked";
  if (sourceUnavailable(source)) return "Unavailable";
  if (loading && !source.present) return "Quiet";
  return source.present ? "Active" : "Quiet";
}

function icon(className = "h-3.5 w-3.5") {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" className={className} fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M16 19v-1.5a3.5 3.5 0 0 0-3.5-3.5h-5A3.5 3.5 0 0 0 4 17.5V19" />
      <path d="M10 10a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z" />
      <path d="M20 19v-1a3 3 0 0 0-2.2-2.9" opacity="0.55" />
      <path d="M16 4.4a3 3 0 0 1 0 5.8" opacity="0.55" />
    </svg>
  );
}

function contextWindowNoun(lookbackDays: number): string {
  return `${lookbackDays} Day context window`;
}

function contextWindowLabel(lookbackDays: number): string {
  return `${lookbackDays}D confirmation`;
}

function bodyForSource(source: InstitutionalSource, canViewInstitutional = false): string {
  if (sourceLocked(source, canViewInstitutional)) return "Institutional Activity requires Pro.";
  if (sourceUnavailable(source)) return "Institutional Activity unavailable.";
  if (!source.present) return "No material newly filed institutional activity.";
  if (source.direction === "bearish") return "Net reported reduction";
  if (source.direction === "bullish") return "Net reported accumulation";
  return "Mixed reported institutional activity";
}

function supportForSource(source: InstitutionalSource, lookbackDays: number, canViewInstitutional = false): string {
  if (sourceLocked(source, canViewInstitutional)) return "13F filings disclose quarter-end holdings and may not reflect real-time trading.";
  if (sourceUnavailable(source)) return source.detail ?? source.summary ?? "No institutional activity data is available.";
  if (!source.present) return `Latest quarter available; no material filing-date activity in the ${contextWindowNoun(lookbackDays)}.`;
  return source.detail ?? source.summary ?? `13F filing-date freshness · ${contextWindowLabel(lookbackDays)}`;
}

function unavailableSource(reason = "unavailable"): InstitutionalSource {
  return {
    ...inactiveSource,
    label: "Institutional Activity unavailable",
    detail: "No institutional activity data is available.",
    summary: "Institutional Activity is unavailable.",
    status: "unavailable",
    reason,
  };
}

function lockedSource(): InstitutionalSource {
  return {
    ...inactiveSource,
    label: "Institutional Activity locked",
    locked: true,
    lock_state: "pro_locked",
    required_plan: "pro",
    status: "pro_locked",
  };
}

function normalizeSourceForAccess(source: InstitutionalSource, canViewInstitutional: boolean): InstitutionalSource {
  if (canViewInstitutional && sourceLocked(source)) {
    return unavailableSource(source.lock_state ?? source.status ?? "stale_locked_metadata");
  }
  return source;
}

export function TickerInstitutionalSourceCardClient({
  symbol,
  side,
  lookbackDays,
  initialSource,
  canViewInstitutional = false,
  initialResolved = false,
}: {
  symbol: string;
  side: string;
  lookbackDays: number;
  initialSource: InstitutionalSource;
  canViewInstitutional?: boolean;
  initialResolved?: boolean;
}) {
  const fallbackSource = useMemo<InstitutionalSource>(
    () => normalizeSourceForAccess({ ...inactiveSource, ...initialSource }, canViewInstitutional),
    [canViewInstitutional, initialSource],
  );
  const [source, setSource] = useState<InstitutionalSource>(fallbackSource);
  const [loading, setLoading] = useState(!initialResolved && !fallbackSource.present && !sourceUnavailable(fallbackSource) && !sourceLocked(fallbackSource, canViewInstitutional));

  useEffect(() => {
    if (initialResolved || sourceLocked(fallbackSource, canViewInstitutional) || canSkipInstitutionalFetch(fallbackSource)) {
      setSource(fallbackSource);
      setLoading(false);
      return;
    }

    let alive = true;
    const controller = new AbortController();
    setLoading(true);
    getTickerSignalsSummary(symbol, {
      side,
      limit: 3,
      lookback_days: lookbackDays,
      signal: controller.signal,
      source: "TickerInstitutionalSourceCard",
    })
      .then((response) => {
        if (!alive) return;
        const nextSource = response.confirmation_score_bundle?.sources?.institutional_activity as InstitutionalSource | undefined;
        const nextEntitlement = response.source_entitlements?.institutional_activity;
        if (nextEntitlement?.locked && !canViewInstitutional) {
          setSource(lockedSource());
        } else if (nextSource) {
          setSource(normalizeSourceForAccess({ ...inactiveSource, ...nextSource }, canViewInstitutional));
        } else {
          setSource(unavailableSource("missing_source"));
        }
      })
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        console.error("[ticker-institutional-source-card] client fetch failed", error);
        if (alive) {
          setSource(!canViewInstitutional && error instanceof ApiError && [401, 402, 403].includes(error.status) ? lockedSource() : unavailableSource("fetch_failed"));
        }
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    return () => {
      alive = false;
      controller.abort();
    };
  }, [canViewInstitutional, fallbackSource, initialResolved, lookbackDays, side, symbol]);

  return (
    <div className={`rounded-xl border px-3 py-2.5 ${borderClass(source, loading)}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className={`shrink-0 ${toneClass(source, loading)}`}>{icon()}</span>
          <p className="truncate text-[10px] font-semibold uppercase tracking-[0.1em] text-slate-400">Institutional Activity</p>
        </div>
        <p className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.08em] ${toneClass(source, loading)}`}>
          {stateLabel(source, loading, canViewInstitutional)}
        </p>
      </div>
      <p className="mt-2.5 text-sm font-semibold leading-snug text-slate-100">{loading ? "Checking Institutional Activity" : bodyForSource(source, canViewInstitutional)}</p>
      <p className="mt-1 text-xs leading-snug text-slate-500">{loading ? contextWindowLabel(lookbackDays) : supportForSource(source, lookbackDays, canViewInstitutional)}</p>
    </div>
  );
}
