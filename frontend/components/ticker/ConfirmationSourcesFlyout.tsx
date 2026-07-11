"use client";

import { useEffect, useMemo, useState } from "react";
import { getTickerMacroPositioning, type MacroPositioningResponse, type TickerSourceEntitlements } from "@/lib/api";
import type { ConfirmationScoreSource } from "@/lib/types";

type ConfirmationSourceKey =
  | "congress"
  | "insiders"
  | "signals"
  | "price_volume"
  | "fundamentals"
  | "options_flow"
  | "government_contracts"
  | "institutional_activity"
  | "macro_positioning";

const labels: Record<ConfirmationSourceKey, string> = {
  congress: "Congress",
  insiders: "Insiders",
  signals: "Signals",
  price_volume: "Price / Volume",
  fundamentals: "Fundamentals",
  options_flow: "Options Flow",
  government_contracts: "Government Contracts",
  institutional_activity: "Institutional Activity",
  macro_positioning: "Macro Positioning",
};

type Props = {
  symbol: string;
  alignedSources: ConfirmationSourceKey[];
  sources: Record<ConfirmationSourceKey, ConfirmationScoreSource & { locked?: boolean; lock_state?: string | null; required_plan?: string | null }>;
  sourceEntitlements?: TickerSourceEntitlements | null;
};

function sourceLocked(sourceKey: ConfirmationSourceKey, entitlements?: TickerSourceEntitlements | null) {
  return Boolean(entitlements?.[sourceKey]?.locked);
}

function biasClass(value?: string | null) {
  if (value === "bullish") return "text-emerald-300";
  if (value === "bearish") return "text-rose-300";
  return "text-slate-300";
}

function formatBias(value?: string | null) {
  if (value === "bullish") return "Bullish";
  if (value === "bearish") return "Bearish";
  if (value === "neutral") return "Neutral";
  return "Unavailable";
}

function formatUpdated(value?: string | null) {
  if (!value) return "Updated weekly";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Updated weekly";
  return `Updated ${new Intl.DateTimeFormat("en-US", { weekday: "long" }).format(date)}`;
}

function ratingStars(value?: number | null) {
  if (typeof value !== "number" || Number.isNaN(value)) return "";
  const rating = Math.max(1, Math.min(5, Math.round(value)));
  return `${"*".repeat(rating)}${"*".repeat(5 - rating)}`;
}

function macroUnavailable(data?: MacroPositioningResponse | null) {
  if (!data || data.locked) return false;
  return data.active !== true || !["bullish", "bearish", "neutral"].includes(String(data.overall ?? ""));
}

function macroUnavailableCopy(data?: MacroPositioningResponse | null) {
  if (data?.status === "disabled") return "Macro Positioning is currently disabled.";
  if (data?.status === "invalid_symbol") return "Macro Positioning is not available for this symbol.";
  return "Macro Positioning is not available for this ticker yet.";
}

function ratingText(value?: number | null) {
  if (typeof value !== "number" || Number.isNaN(value)) return "No rating";
  const rating = Math.max(1, Math.min(5, Math.round(value)));
  return `${rating}/5`;
}

export function ConfirmationSourcesFlyout({ symbol, alignedSources, sources, sourceEntitlements }: Props) {
  const [open, setOpen] = useState(false);
  const [macro, setMacro] = useState<MacroPositioningResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const macroLocked = sourceLocked("macro_positioning", sourceEntitlements);
  const macroSource = sources.macro_positioning;
  const chipSources = useMemo(() => {
    const active = alignedSources.filter((source) => sources[source]?.present);
    if ((macroSource?.present || macroSource?.locked) && !active.includes("macro_positioning")) {
      active.push("macro_positioning");
    }
    return active;
  }, [alignedSources, macroSource?.locked, macroSource?.present, sources]);

  useEffect(() => {
    if (!open || macroLocked || macro || loading) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    getTickerMacroPositioning(symbol, { signal: controller.signal, source: "TickerMacroPositioningFlyout" })
      .then(setMacro)
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        setError("Macro Positioning is temporarily unavailable.");
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [macro, macroLocked, loading, open, symbol]);

  if (chipSources.length === 0) {
    return <p className="mt-3 text-sm text-slate-300">No active sources aligned.</p>;
  }

  return (
    <>
      <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/45 px-3 py-3">
        <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Confirmation Sources</p>
        <div className="mt-3 flex flex-wrap gap-2">
          {chipSources.map((sourceKey) => {
            const locked = sourceLocked(sourceKey, sourceEntitlements);
            const isMacro = sourceKey === "macro_positioning";
            return (
              <button
                key={sourceKey}
                type="button"
                onClick={() => isMacro && setOpen(true)}
                className={`inline-flex items-center gap-1.5 rounded-md border px-2.5 py-1.5 text-xs font-semibold transition ${
                  isMacro
                    ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100 hover:bg-emerald-300/15"
                    : "cursor-default border-white/10 bg-white/[0.035] text-slate-200"
                }`}
                title={isMacro ? "Open Macro Positioning" : labels[sourceKey]}
              >
                <span className={locked ? "text-amber-200" : "text-emerald-300"}>{locked ? "Lock" : "OK"}</span>
                {labels[sourceKey]}
              </button>
            );
          })}
        </div>
      </div>

      {open ? (
        <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/70 backdrop-blur-sm" role="dialog" aria-modal="true" aria-label="Macro Positioning">
          <button type="button" aria-label="Close Macro Positioning" className="absolute inset-0 cursor-default" onClick={() => setOpen(false)} />
          <aside className="relative z-10 flex h-full w-full max-w-md flex-col border-l border-white/10 bg-slate-950 p-5 shadow-2xl shadow-black/50">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
                <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning supporting the current investment thesis.</p>
              </div>
              <button type="button" onClick={() => setOpen(false)} className="rounded-md border border-white/10 px-2.5 py-1.5 text-xs font-semibold text-slate-300 transition hover:border-white/25 hover:text-white">
                Close
              </button>
            </div>

            {macroLocked ? (
              <div className="mt-8 rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
                <p className="text-base font-semibold text-white">Macro Positioning</p>
                <p className="mt-3 text-sm leading-6 text-slate-300">Understand whether institutional macro positioning supports or conflicts with your investment thesis.</p>
                <p className="mt-4 text-sm font-semibold text-emerald-100">Included with Walnut Pro.</p>
              </div>
            ) : loading ? (
              <p className="mt-8 text-sm text-slate-400">Loading Macro Positioning...</p>
            ) : error ? (
              <p className="mt-8 text-sm text-rose-200">{error}</p>
            ) : macroUnavailable(macro) ? (
              <div className="mt-8 rounded-lg border border-white/10 bg-slate-950/45 p-4 text-sm leading-6 text-slate-300">
                {macroUnavailableCopy(macro)}
              </div>
            ) : (
              <div className="mt-8 space-y-7">
                <section>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Overall Bias</p>
                  <p className={`mt-3 text-lg font-semibold ${biasClass(macro?.overall)}`}>
                    <span className="mr-3 font-mono tracking-[0.2em]">{ratingStars(macro?.rating)}</span>
                    {formatBias(macro?.overall)}
                  </p>
                  <p className="mt-2 text-xs font-semibold text-slate-500">{ratingText(macro?.rating)} conviction</p>
                </section>
                <div className="h-px bg-white/10" />
                <section>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Primary Drivers</p>
                  <div className="mt-3 space-y-3">
                    {(macro?.drivers ?? []).map((driver) => (
                      <div key={`${driver.name}-${driver.bias}`} className="flex items-center justify-between gap-3">
                        <p className="text-sm font-semibold text-slate-100">{driver.name}</p>
                        <p className={`text-sm font-semibold ${biasClass(driver.bias)}`}>{formatBias(driver.bias)}</p>
                      </div>
                    ))}
                  </div>
                </section>
                <div className="h-px bg-white/10" />
                <section>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Summary</p>
                  <p className="mt-3 text-sm leading-6 text-slate-200">{macro?.summary ?? macroSource.summary ?? "Macro Positioning is available for this ticker."}</p>
                  <p className="mt-5 text-xs font-semibold text-slate-500">{formatUpdated(macro?.updated)}</p>
                </section>
              </div>
            )}
          </aside>
        </div>
      ) : null}
    </>
  );
}
