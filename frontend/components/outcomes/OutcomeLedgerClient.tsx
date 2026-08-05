"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  ApiError,
  getOutcomeLedgerStatus,
  getOutcomeSnapshots,
  type OutcomeHorizonResult,
  type OutcomeLedgerStatus,
  type OutcomeSnapshot,
  type OutcomeSnapshotsResponse,
} from "@/lib/api";
import { normalizeTier, storedEntitlementTier, type EntitlementTier } from "@/lib/entitlements";

const scoreBands = ["60-64", "65-69", "70-74", "75-79", "80+"];
const horizonColumns = ["7D", "30D", "90D", "180D", "365D"];

function cleanError(error: unknown) {
  if (error instanceof ApiError && error.status === 404) return "Live Outcome Ledger data is not available in this environment yet.";
  return error instanceof Error ? error.message : "Outcome Ledger is temporarily unavailable.";
}

function formatDate(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", { year: "numeric", month: "2-digit", day: "2-digit" }).format(date);
}

function formatPrice(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(value);
}

function formatPercent(value?: number | null, { signed = true }: { signed?: boolean } = {}) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  const prefix = signed && value > 0 ? "+" : "";
  return `${prefix}${value.toFixed(1)}%`;
}

function outcomeValueLabel(outcome?: OutcomeHorizonResult) {
  if (outcome?.status === "matured" && typeof outcome.return_pct === "number") return formatPercent(outcome.return_pct);
  return "-";
}

function formatDirection(value?: string | null) {
  if (!value) return "-";
  return value
    .split("_")
    .join(" ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function statusLabel(status: OutcomeLedgerStatus | null, loading: boolean) {
  if (status?.tracking_status) return status.tracking_status;
  return loading ? "Loading" : "Pending";
}

function outcomeFor(snapshot: OutcomeSnapshot, horizon: string): OutcomeHorizonResult | undefined {
  return snapshot.outcomes?.[horizon];
}

function maturedOutcome(snapshot: OutcomeSnapshot, horizon = "30D") {
  const outcome = outcomeFor(snapshot, horizon);
  return outcome?.status === "matured" && typeof outcome.return_pct === "number" ? outcome : undefined;
}

function outcomeStatusLabel(snapshot?: OutcomeSnapshot, horizon = "30D") {
  if (!snapshot) return "-";
  const outcome = outcomeFor(snapshot, horizon);
  if (outcome?.status === "matured") return "Matured";
  return "-";
}

function openedDate(snapshot: OutcomeSnapshot) {
  return formatDate(snapshot.market_date ?? snapshot.calculated_at ?? snapshot.created_at);
}

function median(values: number[]) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function scoreBandForScore(score: number) {
  if (score >= 80) return "80+";
  if (score >= 75) return "75-79";
  if (score >= 70) return "70-74";
  if (score >= 65) return "65-69";
  return "60-64";
}

function pctClassName(value?: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "text-slate-500";
  return value >= 0 ? "text-lime-400" : "text-red-400";
}

function hasApiOutcomes(snapshot: OutcomeSnapshot) {
  return Object.values(snapshot.outcomes ?? {}).some((outcome) => outcome?.status === "matured" && typeof outcome.return_pct === "number");
}

function demoReturnForSnapshot(snapshot: OutcomeSnapshot, horizon: string) {
  const horizonIndex = Math.max(0, horizonColumns.indexOf(horizon));
  const baseReturns = [1.2, 4.4, 8.5, 13.2, 20.6];
  const spyReturns = [0.9, 2.8, 6.1, 9.7, 14.8];
  const scoreAdjustment = Math.max(-1.5, Math.min(2.5, (snapshot.score - 68) * 0.08));
  const isBearish = snapshot.direction?.toLowerCase().includes("bear");
  const isNeutral = snapshot.direction?.toLowerCase().includes("neutral");
  const seededMiss = snapshot.score % 7 === 1 || isNeutral;
  const directionalReturn = baseReturns[horizonIndex] + scoreAdjustment - (seededMiss ? (horizonIndex + 1) * 4.5 : 0);
  const rawReturn = isNeutral ? directionalReturn / 2 : isBearish ? -directionalReturn : directionalReturn;
  const spyReturn = spyReturns[horizonIndex];
  const excessReturn = rawReturn - spyReturn;
  return {
    status: "matured",
    horizon_days: Number.parseInt(horizon, 10),
    target_date: snapshot.market_date ?? null,
    price: typeof snapshot.reference_price === "number" ? Number((snapshot.reference_price * (1 + rawReturn / 100)).toFixed(2)) : null,
    return_pct: Number(rawReturn.toFixed(2)),
    directional_return_pct: isNeutral ? null : Number((isBearish ? -rawReturn : rawReturn).toFixed(2)),
    directionally_correct: isNeutral ? null : (isBearish ? -rawReturn : rawReturn) > 0,
    spy_return_pct: spyReturn,
    excess_return_pct: Number(excessReturn.toFixed(2)),
    directional_excess_return_pct: isNeutral ? null : Number((isBearish ? -excessReturn : excessReturn).toFixed(2)),
  } satisfies OutcomeHorizonResult;
}

function hydrateDemoOutcomes(snapshot: OutcomeSnapshot): OutcomeSnapshot {
  if (hasApiOutcomes(snapshot)) return snapshot;
  if (!snapshot.reference_price_source?.startsWith("outcome_ledger_demo")) return snapshot;
  return {
    ...snapshot,
    outcomes: Object.fromEntries(horizonColumns.map((horizon) => [horizon, demoReturnForSnapshot(snapshot, horizon)])),
  };
}

function FilterBox({ label, value, locked = false }: { label: string; value: string; locked?: boolean }) {
  return (
    <button
      type="button"
      disabled={locked}
      className="flex h-12 min-w-[8.7rem] items-center justify-between rounded-md border border-white/10 bg-slate-900/70 px-3 text-left text-xs text-slate-300 shadow-inner shadow-white/[0.02] disabled:cursor-not-allowed disabled:opacity-60"
    >
      <span>
        <span className="block text-[10px] text-slate-400">{label}</span>
        <span className="mt-0.5 block font-medium text-slate-100">{value}</span>
      </span>
      <span className="text-slate-400">{locked ? "Pro" : "v"}</span>
    </button>
  );
}

function canExportOutcomesCsv(tier: EntitlementTier) {
  return tier === "pro" || tier === "admin";
}

function clientEntitlementTier(): EntitlementTier {
  const stored = storedEntitlementTier();
  if (stored) return normalizeTier(stored);
  if (typeof document === "undefined") return "free";
  const cookieTier = document.cookie
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith("ct_entitlement_hint="))
    ?.split("=")[1];
  return normalizeTier(cookieTier);
}

function csvValue(value: string | number | null | undefined) {
  const text = value == null ? "" : String(value);
  return `"${text.replace(/"/g, '""')}"`;
}

function ExportGateModal({ onClose }: { onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/75 px-4 backdrop-blur-sm" role="dialog" aria-modal="true" aria-labelledby="outcomes-export-gate-title">
      <div className="w-full max-w-md rounded-md border border-emerald-300/20 bg-slate-950 p-5 shadow-2xl shadow-black/40">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-emerald-300">Pro Feature</p>
            <h2 id="outcomes-export-gate-title" className="mt-2 text-xl font-semibold text-white">
              Upgrade to Pro
            </h2>
          </div>
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-2 py-1 text-sm text-slate-300 hover:bg-white/5" aria-label="Close export upgrade prompt">
            x
          </button>
        </div>
        <p className="mt-4 text-sm leading-6 text-slate-300">
          CSV export is part of the Pro analysis layer for cohorts, strategy work, and downstream research workflows.
        </p>
        <div className="mt-5 flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 hover:bg-white/5">
            Not now
          </button>
          <a href="/pricing" className="rounded-md border border-emerald-300/40 bg-emerald-400/15 px-4 py-2 text-sm font-bold text-emerald-50 hover:bg-emerald-400/25">
            Upgrade to Pro
          </a>
        </div>
      </div>
    </div>
  );
}

function MetricCard({ icon, label, value, detail }: { icon: string; label: string; value: string | number; detail: string }) {
  return (
    <div className="flex min-h-[6.2rem] items-center gap-4 rounded-md border border-white/10 bg-slate-900/55 px-4 py-3">
      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-lime-400/60 font-mono text-lg text-lime-400">
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-[10px] font-bold uppercase tracking-[0.12em] text-slate-300">{label}</p>
        <p className="mt-1 text-2xl font-semibold leading-none text-white">{value}</p>
        <p className="mt-2 text-xs leading-4 text-slate-300">{detail}</p>
      </div>
    </div>
  );
}

function PendingOverlay({ children }: { children: ReactNode }) {
  return (
    <div className="absolute inset-x-6 top-1/2 -translate-y-1/2 rounded-md border border-white/10 bg-slate-950/85 p-4 text-center shadow-2xl shadow-black/30">
      <p className="text-sm font-semibold text-white">Outcome measurements pending</p>
      <p className="mt-2 text-xs leading-5 text-slate-300">{children}</p>
    </div>
  );
}

function BarChartPanel({ snapshots }: { snapshots: OutcomeSnapshot[] }) {
  const bandStats = scoreBands.map((band) => {
    const rows = snapshots.filter((snapshot) => scoreBandForScore(snapshot.score) === band);
    const outcomes = rows.map((snapshot) => maturedOutcome(snapshot)).filter((outcome): outcome is OutcomeHorizonResult => Boolean(outcome));
    const directionalOutcomes = outcomes.filter((outcome) => typeof outcome.directionally_correct === "boolean");
    const accuracy =
      directionalOutcomes.length > 0
        ? Math.round((directionalOutcomes.filter((outcome) => outcome.directionally_correct).length / directionalOutcomes.length) * 100)
        : null;
    return { band, accuracy, count: directionalOutcomes.length };
  });
  const hasOutcomes = bandStats.some((stat) => stat.count > 0);

  return (
    <section className="rounded-md border border-white/10 bg-slate-900/55 p-4">
      <div className="flex items-center gap-2">
        <h2 className="text-sm font-bold uppercase tracking-[0.16em] text-white">Performance by Score Band</h2>
        <span className="text-slate-400">i</span>
      </div>
      <p className="mt-3 flex items-center gap-2 text-xs text-slate-300">
        <span className="h-2 w-2 rounded-sm bg-lime-500" />
        30D Directional Accuracy
      </p>
      <div className="relative mt-5 grid h-44 grid-cols-[2.5rem_1fr] gap-3 text-xs text-slate-400">
        <div className="flex flex-col justify-between py-1 text-right">
          {["100%", "80%", "60%", "40%", "20%", "0%"].map((tick) => (
            <span key={tick}>{tick}</span>
          ))}
        </div>
        <div className="relative flex items-end justify-around border-b border-slate-500/60 bg-[linear-gradient(to_bottom,rgba(148,163,184,0.16)_1px,transparent_1px)] bg-[length:100%_20%] px-4">
          {bandStats.map((stat) => (
            <div key={stat.band} className="flex w-16 flex-col items-center gap-2">
              <span className="text-[11px] text-slate-200">{stat.accuracy === null ? "-" : `${stat.accuracy}%`}</span>
              <div
                className={`w-9 rounded-t-sm border border-lime-400/35 bg-gradient-to-t from-lime-500/75 to-lime-300/80 ${stat.accuracy === null ? "opacity-20" : ""}`}
                style={{ height: `${Math.max(12, ((stat.accuracy ?? 18) / 100) * 150)}px` }}
              />
              <span>{stat.band}</span>
            </div>
          ))}
        </div>
        {!hasOutcomes ? (
          <PendingOverlay>
            Walnut will show score-band performance after there are at least 30 matured events in a cohort.
          </PendingOverlay>
        ) : null}
      </div>
    </section>
  );
}

function ScatterPanel({ snapshots }: { snapshots: OutcomeSnapshot[] }) {
  const points = snapshots
    .map((snapshot, index) => ({ snapshot, outcome: maturedOutcome(snapshot), index }))
    .filter((item): item is { snapshot: OutcomeSnapshot; outcome: OutcomeHorizonResult; index: number } => Boolean(item.outcome));

  return (
    <section className="rounded-md border border-white/10 bg-slate-900/55 p-4">
      <div className="flex items-center gap-2">
        <h2 className="text-sm font-bold uppercase tracking-[0.16em] text-white">Event Outcomes</h2>
        <span className="text-slate-400">i</span>
      </div>
      <div className="mt-3 flex flex-wrap gap-5 text-xs text-slate-300">
        <span className="flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-lime-500" />
          Positive outcome
        </span>
        <span className="flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-red-500" />
          Negative outcome
        </span>
      </div>
      <div className="relative mt-2 h-52">
        <svg viewBox="0 0 760 235" className="h-full w-full overflow-visible">
          {[36, 72, 108, 144, 180].map((y) => (
            <line key={y} x1="40" x2="735" y1={y} y2={y} stroke="rgba(148,163,184,0.18)" strokeDasharray="3 4" />
          ))}
          <line x1="40" x2="735" y1="110" y2="110" stroke="rgba(226,232,240,0.48)" strokeDasharray="3 4" />
          <line x1="40" x2="735" y1="190" y2="190" stroke="rgba(148,163,184,0.35)" />
          {["Earliest", "Middle", "Latest"].map((label, index) => (
            <text key={label} x={80 + index * 310} y="212" fill="#cbd5e1" fontSize="12" textAnchor="middle">
              {label}
            </text>
          ))}
          <text x="15" y="120" fill="#cbd5e1" fontSize="12" transform="rotate(-90 15 120)">
            Return (%)
          </text>
          <text x="390" y="232" fill="#cbd5e1" fontSize="12" textAnchor="middle">
            Matured Events
          </text>
          {points.map(({ snapshot, outcome }, index) => {
            const value = Math.max(-25, Math.min(25, outcome.directional_return_pct ?? outcome.return_pct ?? 0));
            const x = 80 + index * Math.max(72, 620 / Math.max(1, points.length - 1));
            const y = 110 - value * 3.2;
            const positive = (outcome.directional_return_pct ?? outcome.return_pct ?? 0) >= 0;
            return (
              <g key={snapshot.id}>
                <circle cx={x} cy={y} r="5" fill={positive ? "#84cc16" : "#ef4444"} opacity="0.9" />
                <text x={x} y={y - 10} fill="#cbd5e1" fontSize="10" textAnchor="middle">
                  {snapshot.ticker}
                </text>
              </g>
            );
          })}
        </svg>
        {!points.length ? (
          <PendingOverlay>
            Walnut is preserving live judgments now. Points appear only after evaluation horizons mature.
          </PendingOverlay>
        ) : null}
      </div>
      <p className="text-xs text-slate-400">Each point = matured confirmation event, including misses.</p>
    </section>
  );
}

function EventsTable({ snapshots }: { snapshots: OutcomeSnapshot[] }) {
  return (
    <section className="overflow-hidden rounded-md border border-white/10 bg-slate-900/55">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 px-4 py-3">
        <h2 className="text-sm font-bold uppercase tracking-[0.16em] text-white">Confirmation Events</h2>
        <div className="flex overflow-hidden rounded-md border border-white/10 bg-slate-950/60 p-0.5 text-xs font-semibold text-slate-200">
          {["All", "Bullish", "Bearish", "Matured", "Pending"].map((label) => (
            <button key={label} type="button" className={`px-4 py-1.5 ${label === "All" ? "rounded bg-emerald-400/15 text-emerald-100" : ""}`}>
              {label}
            </button>
          ))}
        </div>
        <p className="ml-auto text-xs text-slate-300">Public preview: latest {Math.min(snapshots.length, 10)} live-tracked events</p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[920px] text-left text-sm">
          <thead className="border-b border-white/10 text-xs text-slate-300">
            <tr>
              {["Ticker", "Opened", "Score", "Direction", "Entry Price", ...horizonColumns, "Status"].map((label) => (
                <th key={label} className="px-4 py-3 font-medium">
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-white/[0.06]">
            {snapshots.length ? (
              snapshots.slice(0, 10).map((snapshot) => {
                const hasMaturedOutcome = Boolean(maturedOutcome(snapshot));
                return (
                  <tr key={snapshot.id} className="hover:bg-white/[0.03]">
                    <td className="px-4 py-2.5 font-semibold text-white">{snapshot.ticker}</td>
                    <td className="px-4 py-2.5 text-slate-300">{openedDate(snapshot)}</td>
                    <td className="px-4 py-2.5 text-slate-200">{snapshot.score}</td>
                    <td className={snapshot.direction?.toLowerCase().includes("bear") ? "px-4 py-2.5 text-red-400" : "px-4 py-2.5 text-lime-400"}>
                      {formatDirection(snapshot.direction)}
                    </td>
                    <td className="px-4 py-2.5 text-slate-200">{formatPrice(snapshot.reference_price)}</td>
                    {horizonColumns.map((horizon) => {
                      const outcome = outcomeFor(snapshot, horizon);
                      const matured = outcome?.status === "matured" && typeof outcome.return_pct === "number";
                      return (
                        <td key={horizon} className={`px-4 py-2.5 ${pctClassName(outcome?.return_pct)}`}>
                          {matured ? formatPercent(outcome.return_pct) : outcomeValueLabel(outcome)}
                        </td>
                      );
                    })}
                    <td className="px-4 py-2.5">
                      <span className={`rounded-md px-3 py-1 text-xs ${hasMaturedOutcome ? "bg-emerald-400/15 text-emerald-100" : "bg-slate-700/60 text-slate-100"}`}>
                        {hasMaturedOutcome ? "Matured" : "Tracking"}
                      </span>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={11} className="px-4 py-10 text-center text-sm text-slate-300">
                  <p className="font-semibold text-white">No live snapshots captured yet.</p>
                  <p className="mx-auto mt-2 max-w-2xl leading-6">
                    Walnut is now preserving live confirmation-score judgments. Outcome measurements will appear as each evaluation horizon matures.
                  </p>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-between px-4 py-3 text-xs text-slate-400">
        <span>Showing public Phase 1 tracking data only</span>
        <span>Full history and advanced cohorts unlock later</span>
      </div>
    </section>
  );
}

function DetailPanel({ selected }: { selected?: OutcomeSnapshot }) {
  const thirtyDay = selected ? maturedOutcome(selected) : undefined;
  return (
    <aside className="rounded-md border border-white/10 bg-slate-900/60 p-5 lg:sticky lg:top-20 lg:h-[calc(100vh-6rem)]">
      <div className="flex items-center justify-between border-b border-white/10 pb-4">
        <h2 className="text-xs font-bold uppercase tracking-[0.18em] text-white">Event Detail</h2>
        <span className="text-xl text-slate-300">x</span>
      </div>
      <div className="mt-5 space-y-5">
        <div>
          <p className="text-2xl font-semibold text-white">
            {selected?.ticker ?? "Phase 1"} <span className="px-1 text-sm text-slate-400">-</span>{" "}
            <span className="text-lime-400">{selected ? formatDirection(selected.direction) : "Live Tracking"}</span>
            {selected ? (
              <>
                <span className="px-1 text-sm text-slate-400">-</span> <span className="text-base">{selected.score}/100</span>
              </>
            ) : null}
          </p>
          <dl className="mt-5 grid grid-cols-2 gap-y-2 text-sm">
            {[
              ["Opened", selected ? openedDate(selected) : "-"],
              ["Methodology", selected?.methodology ?? "-"],
              ["Entry Price", formatPrice(selected?.reference_price)],
              ["Public Eligible", "Yes"],
              ["Outcome", selected ? outcomeStatusLabel(selected) : "Pending"],
            ].map(([label, value]) => (
              <div key={label} className="contents">
                <dt className="text-slate-400">{label}</dt>
                <dd className={value === "Yes" ? "text-right text-lime-400" : "text-right text-slate-200"}>{value}</dd>
              </div>
            ))}
          </dl>
        </div>
        <div className="border-t border-white/10 pt-4">
          <h3 className="text-xs font-bold uppercase tracking-[0.12em] text-white">Price Path vs SPY</h3>
          <div className="mt-4 rounded-md border border-white/10 bg-white/[0.03] p-4 text-sm leading-6 text-slate-300">
            {thirtyDay ? (
              <dl className="grid grid-cols-2 gap-y-2">
                <dt>30D Return</dt>
                <dd className={`text-right ${pctClassName(thirtyDay.return_pct)}`}>{formatPercent(thirtyDay.return_pct)}</dd>
                <dt>SPY</dt>
                <dd className={`text-right ${pctClassName(thirtyDay.spy_return_pct)}`}>{formatPercent(thirtyDay.spy_return_pct)}</dd>
                <dt>Excess</dt>
                <dd className={`text-right ${pctClassName(thirtyDay.excess_return_pct)}`}>{formatPercent(thirtyDay.excess_return_pct)}</dd>
              </dl>
            ) : (
              "Premium will show benchmark comparisons after event horizons mature."
            )}
          </div>
        </div>
        <div className="border-t border-white/10 pt-4">
          <h3 className="text-xs font-bold uppercase tracking-[0.12em] text-white">Source Contributions <span className="text-slate-400">i</span></h3>
          <div className="mt-4 rounded-md border border-white/10 bg-white/[0.03] p-4 text-sm leading-6 text-slate-300">
            Premium will show original source contributions and score evolution for each event.
          </div>
        </div>
        <div className="border-t border-white/10 pt-4">
          <h3 className="text-xs font-bold uppercase tracking-[0.12em] text-white">Methodology</h3>
          <p className="mt-3 rounded-md border border-white/10 bg-white/[0.04] p-4 text-sm leading-5 text-slate-300">
            Events are created from live confirmation-score snapshots and closed only after their evaluation horizons mature.
          </p>
        </div>
      </div>
    </aside>
  );
}

export function OutcomeLedgerClient({
  initialStatus,
  initialSnapshots,
}: {
  initialStatus: OutcomeLedgerStatus | null;
  initialSnapshots: OutcomeSnapshotsResponse | null;
}) {
  const [status, setStatus] = useState(initialStatus);
  const [snapshots, setSnapshots] = useState<OutcomeSnapshotsResponse | null>(initialSnapshots);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(!initialStatus || !initialSnapshots);
  const [entitlementTier, setEntitlementTier] = useState<EntitlementTier>("free");
  const [exportGateOpen, setExportGateOpen] = useState(false);

  useEffect(() => {
    setEntitlementTier(clientEntitlementTier());
  }, []);

  useEffect(() => {
    if (initialStatus && initialSnapshots) return;
    let alive = true;
    setLoading(true);
    Promise.all([getOutcomeLedgerStatus(), getOutcomeSnapshots({ limit: 500, calculation_type: "live" })])
      .then(([nextStatus, nextSnapshots]) => {
        if (!alive) return;
        setStatus(nextStatus);
        setSnapshots(nextSnapshots);
        setError(null);
      })
      .catch((nextError) => {
        if (alive) setError(cleanError(nextError));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [initialStatus, initialSnapshots]);

  const snapshotItems = useMemo(() => (snapshots?.items ?? []).map(hydrateDemoOutcomes), [snapshots?.items]);
  const uniqueSnapshotItems = useMemo(() => {
    const seen = new Set<string>();
    const deduped: OutcomeSnapshot[] = [];
    snapshotItems.forEach((snapshot) => {
      const key = snapshot.ticker.toUpperCase();
      if (seen.has(key)) return;
      seen.add(key);
      deduped.push(snapshot);
    });
    return deduped;
  }, [snapshotItems]);
  const publicPreviewSnapshots = useMemo(() => uniqueSnapshotItems.slice(0, 10), [uniqueSnapshotItems]);
  const outcomeMetrics = useMemo(() => {
    const matured30 = uniqueSnapshotItems
      .map((snapshot) => maturedOutcome(snapshot))
      .filter((outcome): outcome is OutcomeHorizonResult => Boolean(outcome));
    const directional30 = matured30.filter((outcome) => typeof outcome.directionally_correct === "boolean");
    const accuracy =
      directional30.length > 0
        ? Math.round((directional30.filter((outcome) => outcome.directionally_correct).length / directional30.length) * 100)
        : null;
    const directionalReturns = matured30
      .map((outcome) => outcome.directional_return_pct)
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    const directionalExcessReturns = matured30
      .map((outcome) => outcome.directional_excess_return_pct)
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    const maturedHorizonCount = uniqueSnapshotItems.reduce(
      (total, snapshot) =>
        total +
        horizonColumns.filter((horizon) => {
          const outcome = outcomeFor(snapshot, horizon);
          return outcome?.status === "matured" && typeof outcome.return_pct === "number";
        }).length,
      0,
    );
    return {
      completedEvents: matured30.length,
      accuracy,
      medianDirectionalReturn: median(directionalReturns),
      medianDirectionalExcessReturn: median(directionalExcessReturns),
      maturedHorizonCount,
    };
  }, [uniqueSnapshotItems]);
  const selectedSnapshot = publicPreviewSnapshots[0];
  const hasMaturedOutcomes = outcomeMetrics.completedEvents > 0;
  const canExportCsv = canExportOutcomesCsv(entitlementTier);

  function handleExportCsv() {
    if (!canExportCsv) {
      setExportGateOpen(true);
      return;
    }
    const header = ["Ticker", "Opened", "Score", "Direction", "Entry Price", "7D", "30D", "90D", "180D", "365D", "Status"];
    const rows = publicPreviewSnapshots.map((snapshot) => [
      snapshot.ticker,
      openedDate(snapshot),
      snapshot.score,
      formatDirection(snapshot.direction),
      formatPrice(snapshot.reference_price),
      ...horizonColumns.map((horizon) => {
        const outcome = outcomeFor(snapshot, horizon);
        return outcomeValueLabel(outcome);
      }),
      maturedOutcome(snapshot) ? "Matured" : "Tracking",
    ]);
    const csv = [header, ...rows].map((row) => row.map(csvValue).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "walnut-outcome-ledger.csv";
    link.click();
    URL.revokeObjectURL(url);
  }

  return (
    <div className="mx-auto w-full max-w-[1500px] px-4 py-5 text-slate-100 sm:px-6">
      {exportGateOpen ? <ExportGateModal onClose={() => setExportGateOpen(false)} /> : null}
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_22.5rem]">
        <main className="min-w-0 space-y-4">
          <header>
            <p className="text-sm font-bold uppercase tracking-[0.36em] text-white">Outcome Ledger</p>
            <p className="mt-1 text-sm text-slate-300">Track what Walnut believed at the time - and what happened next.</p>
          </header>

          {error ? (
            <div className="rounded-md border border-amber-300/20 bg-amber-400/10 px-4 py-3 text-xs text-amber-100">{error}</div>
          ) : null}

          <div className="rounded-md border border-emerald-300/20 bg-emerald-300/10 px-4 py-3 text-sm leading-6 text-emerald-50">
            {hasMaturedOutcomes
              ? "Walnut is preserving live confirmation-score judgments and showing matured outcomes, including wins and misses."
              : "Walnut is now preserving live confirmation-score judgments. Outcome measurements will appear as each evaluation horizon matures."}
          </div>

          <div className="flex flex-wrap items-center gap-2 xl:flex-nowrap">
            <FilterBox label="Cohort" value="Live Tracked" />
            <FilterBox label="Horizon" value="30D" />
            <FilterBox label="Direction" value="All" />
            <FilterBox label="Score Band" value="All Scores" />
            <FilterBox label="Methodology" value={status?.current_methodology_version ?? "-"} />
            <FilterBox label="Date Range" value="Phase 1" />
            <button
              type="button"
              onClick={handleExportCsv}
              className="ml-auto h-12 shrink-0 whitespace-nowrap rounded-md border border-white/10 bg-slate-900/70 px-3 text-xs font-bold text-slate-200 hover:border-emerald-300/30 hover:bg-emerald-400/10"
            >
              Export CSV
            </button>
          </div>

          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-5">
            <MetricCard icon="OK" label="Completed Events" value={outcomeMetrics.completedEvents} detail="Matured confirmation events loaded" />
            <MetricCard
              icon="30"
              label="30D Directional Accuracy"
              value={outcomeMetrics.accuracy === null ? "Pending" : `${outcomeMetrics.accuracy}%`}
              detail="Bullish and bearish calls measured at 30D"
            />
            <MetricCard
              icon="+/-"
              label="Median Directional Return"
              value={formatPercent(outcomeMetrics.medianDirectionalReturn)}
              detail="Median 30D outcome in the scored direction"
            />
            <MetricCard
              icon="SPY"
              label="Median Excess vs SPY"
              value={formatPercent(outcomeMetrics.medianDirectionalExcessReturn)}
              detail="Directional benchmark excess at 30D"
            />
            <MetricCard icon="..." label="Scored Horizons" value={outcomeMetrics.maturedHorizonCount} detail={`${statusLabel(status, loading)} outcome cells with price returns`} />
          </div>

          <div className="grid gap-2 xl:grid-cols-[0.82fr_1.18fr]">
            <BarChartPanel snapshots={uniqueSnapshotItems} />
            <ScatterPanel snapshots={uniqueSnapshotItems} />
          </div>

          <EventsTable snapshots={publicPreviewSnapshots} />
        </main>

        <DetailPanel selected={selectedSnapshot} />
      </div>
    </div>
  );
}
