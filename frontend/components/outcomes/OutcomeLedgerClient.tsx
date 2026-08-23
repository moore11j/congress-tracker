"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import {
  ApiError,
  getEntitlements,
  getTickerChartBundle,
  getOutcomeLedgerOverview,
  getOutcomeLedgerSummary,
  type OutcomeHorizonResult,
  type OutcomeLedgerStatus,
  type OutcomeLedgerSummary,
  type OutcomeSnapshot,
  type OutcomeSnapshotsResponse,
  type TickerChartBundle,
} from "@/lib/api";
import { normalizeTier, storedEntitlementTier, type EntitlementTier } from "@/lib/entitlements";

const scoreBands = ["0-39", "40-59", "60-64", "65-69", "70-74", "75-79", "80+"];
const horizonColumns = ["7D", "30D", "90D", "180D", "365D"];
const featuredOutcomeTickers = ["NVDA", "BMNR", "AAPL", "PLTR", "AMZN", "META", "GOOGL", "MSFT"];
const outcomeTablePageSizes = [10, 25, 50] as const;
const outcomeTableFilterOptions = ["All", "Bullish", "Bearish", "Matured", "Open", "Closed"] as const;
const minimumHeadlineDirectionalSamples = 30;
const minimumScoreBandDirectionalSamples = 5;
const publicOutcomeCalculationTypes = new Set(["live", "historical_reconstruction"]);
const cohortFilterOptions = [
  { value: "all", label: "All" },
  { value: "live", label: "Live Tracked" },
  { value: "matured", label: "Matured" },
] as const;
const directionFilterOptions = ["All", "Bullish", "Bearish"];
const scoreBandFilterOptions = ["All Scores", ...scoreBands];
const dateRangeFilterOptions = [
  { value: "all", label: "All Available" },
  { value: "30d", label: "Last 30D" },
  { value: "90d", label: "Last 90D" },
  { value: "12m", label: "Last 12M" },
] as const;

type OutcomeSortKey = "ticker" | "opened" | "score" | "direction" | "entry";
type OutcomeSortDirection = "asc" | "desc";
type OutcomeSort = { key: OutcomeSortKey; direction: OutcomeSortDirection } | null;
type OutcomeTableFilterValue = (typeof outcomeTableFilterOptions)[number];
type CohortFilterValue = (typeof cohortFilterOptions)[number]["value"];
type DateRangeFilterValue = (typeof dateRangeFilterOptions)[number]["value"];
type EventOutcomePoint = {
  snapshot: OutcomeSnapshot;
  outcome: OutcomeHorizonResult;
  opened: number;
  openedLabel: string;
  targetLabel: string;
  returnValue: number;
};
type PricePathPoint = {
  date: string;
  label: string;
  stockReturn: number;
  spyReturn: number;
  excessReturn: number;
};

const outcomeSortableColumns: Record<OutcomeSortKey, string> = {
  ticker: "Ticker",
  opened: "Opened",
  score: "Opened Score",
  direction: "Opened Direction",
  entry: "Entry Price",
};

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

function outcomeLifecycleStatusLabel(snapshot?: OutcomeSnapshot, isClosed = false) {
  if (!snapshot) return "-";
  return snapshot.lifecycle_status === "closed" || isClosed ? "Closed" : "Open";
}

function isClosedOutcomeEvent(snapshot: OutcomeSnapshot, horizon?: string, replacedSnapshotIds?: Set<number>) {
  const selectedOutcome = horizon ? outcomeFor(snapshot, horizon) : undefined;
  return (
    snapshot.lifecycle_status === "closed" ||
    selectedOutcome?.status === "closed" ||
    selectedOutcome?.status === "replaced" ||
    replacedSnapshotIds?.has(snapshot.id) === true
  );
}

function openedDate(snapshot: OutcomeSnapshot) {
  return formatDate(snapshot.market_date ?? snapshot.calculated_at ?? snapshot.created_at);
}

function openedTime(snapshot: OutcomeSnapshot) {
  const raw = snapshot.market_date ?? snapshot.calculated_at ?? snapshot.created_at;
  if (!raw) return 0;
  const time = new Date(raw).getTime();
  return Number.isFinite(time) ? time : 0;
}

function calculatedTime(snapshot: OutcomeSnapshot) {
  const raw = snapshot.calculated_at ?? snapshot.created_at ?? snapshot.market_date;
  if (!raw) return 0;
  const time = new Date(raw).getTime();
  return Number.isFinite(time) ? time : 0;
}

function visibleOutcomeEventKey(snapshot: OutcomeSnapshot) {
  return [
    snapshot.calculation_type,
    snapshot.ticker.toUpperCase(),
    snapshot.market_date ?? snapshot.calculated_at?.slice(0, 10) ?? snapshot.created_at?.slice(0, 10) ?? "unknown",
  ].join(":");
}

function replacedOutcomeSnapshotIds(snapshots: OutcomeSnapshot[]) {
  const ids = new Set<number>();
  const byTicker = new Map<string, OutcomeSnapshot[]>();
  snapshots.forEach((snapshot) => {
    const key = snapshot.ticker.toUpperCase();
    const rows = byTicker.get(key);
    if (rows) rows.push(snapshot);
    else byTicker.set(key, [snapshot]);
  });
  byTicker.forEach((rows) => {
    const sorted = [...rows].sort((a, b) => openedTime(a) - openedTime(b) || calculatedTime(a) - calculatedTime(b) || a.id - b.id);
    const laterDirectionalSides = new Set<string>();
    for (let index = sorted.length - 1; index >= 0; index -= 1) {
      const snapshot = sorted[index];
      if (!snapshot) continue;
      const direction = snapshot.direction?.toLowerCase() ?? "";
      const side = direction.includes("bull") ? "bullish" : direction.includes("bear") ? "bearish" : null;
      if (!side) continue;
      if ([...laterDirectionalSides].some((laterSide) => laterSide !== side)) ids.add(snapshot.id);
      laterDirectionalSides.add(side);
    }
  });
  return ids;
}

function compactDate(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 10);
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" }).format(date);
}

function compactDateTime(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value.replace(" ", "T"));
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 16);
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

function numericReturn(value?: number | null) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function returnFromBase(price: number, base: number) {
  return ((price / base) - 1) * 100;
}

function average(values: number[]) {
  if (!values.length) return null;
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function scoreBandForScore(score: number) {
  if (score >= 80) return "80+";
  if (score >= 75) return "75-79";
  if (score >= 70) return "70-74";
  if (score >= 65) return "65-69";
  if (score >= 60) return "60-64";
  if (score >= 40) return "40-59";
  return "0-39";
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
  const isNonDirectional = snapshot.direction?.toLowerCase().includes("neutral") || snapshot.direction?.toLowerCase().includes("mixed");
  const seededMiss = snapshot.score % 7 === 1 || isNonDirectional;
  const directionalReturn = baseReturns[horizonIndex] + scoreAdjustment - (seededMiss ? (horizonIndex + 1) * 4.5 : 0);
  const rawReturn = isNonDirectional ? directionalReturn / 2 : isBearish ? -directionalReturn : directionalReturn;
  const spyReturn = spyReturns[horizonIndex];
  const excessReturn = rawReturn - spyReturn;
  return {
    status: "matured",
    horizon_days: Number.parseInt(horizon, 10),
    target_date: snapshot.market_date ?? null,
    price: typeof snapshot.reference_price === "number" ? Number((snapshot.reference_price * (1 + rawReturn / 100)).toFixed(2)) : null,
    return_pct: Number(rawReturn.toFixed(2)),
    directional_return_pct: isNonDirectional ? null : Number((isBearish ? -rawReturn : rawReturn).toFixed(2)),
    directionally_correct: isNonDirectional ? null : (isBearish ? -rawReturn : rawReturn) > 0,
    spy_return_pct: spyReturn,
    excess_return_pct: Number(excessReturn.toFixed(2)),
    directional_excess_return_pct: isNonDirectional ? null : Number((isBearish ? -excessReturn : excessReturn).toFixed(2)),
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

function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: { value: string; label: string }[] | readonly { value: string; label: string }[];
  onChange: (value: string) => void;
}) {
  const selectedLabel = options.find((option) => option.value === value)?.label ?? value;
  return (
    <label
      className="relative flex h-12 min-w-[8.7rem] cursor-pointer items-center justify-between rounded-md border border-white/10 bg-slate-900/70 px-3 text-left text-xs text-slate-300 shadow-inner shadow-white/[0.02] focus-within:border-emerald-300/40"
    >
      <span className="min-w-0 pr-6">
        <span className="block text-[10px] text-slate-400">{label}</span>
        <span className="mt-0.5 block truncate font-medium text-slate-100">{selectedLabel}</span>
      </span>
      <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-slate-400">v</span>
      <select
        aria-label={label}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="absolute inset-0 z-10 h-full w-full cursor-pointer rounded-md opacity-0 outline-none"
      >
        {options.map((option) => (
          <option key={option.value} value={option.value} className="bg-slate-950 text-slate-100">
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

function canExportOutcomesCsv(tier: EntitlementTier) {
  return tier === "pro" || tier === "admin";
}

function canViewPremiumOutcomes(tier: EntitlementTier) {
  return tier === "premium" || tier === "pro" || tier === "admin";
}

function canUsePremiumOutcomeTable(tier: EntitlementTier) {
  return canViewPremiumOutcomes(tier);
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

function sourceLabel(value: string) {
  return value
    .split("_")
    .join(" ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function contributionRows(snapshot?: OutcomeSnapshot) {
  const contributions = snapshot?.source_contributions;
  if (contributions && typeof contributions === "object") {
    const rows = Object.entries(contributions).flatMap(([key, raw]) => {
      if (!raw || typeof raw !== "object") return [];
      const row = raw as { label?: unknown; direction?: unknown; strength?: unknown; score_contribution?: unknown };
      return [
        {
          key,
          label: typeof row.label === "string" && row.label ? row.label : sourceLabel(key),
          direction: typeof row.direction === "string" ? formatDirection(row.direction) : "-",
          strength: typeof row.strength === "number" ? String(row.strength) : typeof row.strength === "string" ? row.strength : "-",
          score: typeof row.score_contribution === "number" ? row.score_contribution : null,
        },
      ];
    });
    if (rows.length) return rows;
  }
  return (snapshot?.active_sources ?? []).map((source) => ({
    key: source,
    label: sourceLabel(source),
    direction: "-",
    strength: "-",
    score: null,
  }));
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

function OutcomeTableGateModal({ onClose }: { onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/75 px-4 backdrop-blur-sm" role="dialog" aria-modal="true" aria-labelledby="outcomes-table-gate-title">
      <div className="w-full max-w-md rounded-md border border-emerald-300/20 bg-slate-950 p-5 shadow-2xl shadow-black/40">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-[10px] font-bold uppercase tracking-[0.18em] text-emerald-300">Premium Feature</p>
            <h2 id="outcomes-table-gate-title" className="mt-2 text-xl font-semibold text-white">
              Upgrade to Premium
            </h2>
          </div>
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-2 py-1 text-sm text-slate-300 hover:bg-white/5" aria-label="Close table upgrade prompt">
            x
          </button>
        </div>
        <p className="mt-4 text-sm leading-6 text-slate-300">
          Premium unlocks the full Outcome Ledger table, column sorting, pagination, and 25 or 50 row views.
        </p>
        <div className="mt-5 flex justify-end gap-2">
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 hover:bg-white/5">
            Not now
          </button>
          <a href="/pricing" className="rounded-md border border-emerald-300/40 bg-emerald-400/15 px-4 py-2 text-sm font-bold text-emerald-50 hover:bg-emerald-400/25">
            Upgrade to Premium
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

function ScoredHorizonsPill({ value, detail, className = "" }: { value: string | number; detail: string; className?: string }) {
  return (
    <div className={`flex h-12 min-w-[14.25rem] items-center rounded-md border border-white/10 bg-slate-900/70 px-3 shadow-inner shadow-white/[0.02] ${className}`}>
      <div className="min-w-0">
        <p className="text-[10px] font-bold uppercase tracking-[0.12em] text-slate-300">Scored Horizons</p>
        <p className="mt-0.5 truncate text-xs text-slate-300">
          <span className="mr-2 align-middle text-xl font-semibold leading-none text-white">{value}</span>
          {detail}
        </p>
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

function BarChartPanel({ snapshots, horizon, summary }: { snapshots: OutcomeSnapshot[]; horizon: string; summary: OutcomeLedgerSummary | null }) {
  const [activeBand, setActiveBand] = useState<string | null>(null);
  const bandStats = summary?.horizon === horizon && summary.score_bands.length
    ? summary.score_bands.map((stat) => ({ ...stat, reliable: stat.count >= minimumScoreBandDirectionalSamples }))
    : scoreBands.map((band) => {
        const rows = snapshots.filter((snapshot) => scoreBandForScore(snapshot.score) === band);
        const outcomes = rows.map((snapshot) => maturedOutcome(snapshot, horizon)).filter((outcome): outcome is OutcomeHorizonResult => Boolean(outcome));
        const directionalOutcomes = outcomes.filter((outcome) => typeof outcome.directionally_correct === "boolean");
        const accuracy =
          directionalOutcomes.length > 0
            ? Math.round((directionalOutcomes.filter((outcome) => outcome.directionally_correct).length / directionalOutcomes.length) * 100)
            : null;
        const reliable = directionalOutcomes.length >= minimumScoreBandDirectionalSamples;
        return { band, accuracy, count: directionalOutcomes.length, reliable };
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
        {horizon} Directional Accuracy
      </p>
      <div className="relative mt-5 grid h-52 grid-cols-[2.5rem_1fr] gap-3 text-xs text-slate-400">
        <div className="flex flex-col justify-between py-1 text-right">
          {["100%", "80%", "60%", "40%", "20%", "0%"].map((tick) => (
            <span key={tick}>{tick}</span>
          ))}
        </div>
        <div className="relative flex items-end justify-around border-b border-slate-500/60 bg-[linear-gradient(to_bottom,rgba(148,163,184,0.16)_1px,transparent_1px)] bg-[length:100%_20%] px-4">
          {bandStats.map((stat) => (
            <button key={stat.band} type="button" className={`flex w-16 flex-col items-center gap-2 rounded-sm outline-none transition focus-visible:ring-2 focus-visible:ring-lime-300/70 ${activeBand === null || activeBand === stat.band ? "opacity-100" : "opacity-40"}`} onPointerEnter={() => setActiveBand(stat.band)} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveBand(null); }} onFocus={() => setActiveBand(stat.band)} onBlur={() => setActiveBand(null)} onClick={() => setActiveBand((current) => current === stat.band ? null : stat.band)} aria-pressed={activeBand === stat.band} aria-label={`${stat.band} score band: ${stat.accuracy === null ? "no directional accuracy yet" : `${stat.accuracy}% directional accuracy`}, ${stat.count} calls`}>
              <span className="text-[11px] text-slate-200">{stat.accuracy === null ? "-" : stat.reliable ? `${stat.accuracy}%` : `n=${stat.count}`}</span>
              <div
                className={`w-9 rounded-t-sm border border-lime-400/35 bg-gradient-to-t from-lime-500/75 to-lime-300/80 ${stat.accuracy === null ? "opacity-20" : stat.reliable ? "" : "opacity-35"}`}
                style={{ height: `${Math.max(12, ((stat.accuracy ?? 18) / 100) * 130)}px` }}
              />
              <span className="text-[10px] text-slate-500">{stat.count ? `${stat.count} calls` : "no calls"}</span>
              <span>{stat.band}</span>
            </button>
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

function ScatterPanel({ snapshots, horizon }: { snapshots: OutcomeSnapshot[]; horizon: string }) {
  const [hoverPoint, setHoverPoint] = useState<(EventOutcomePoint & { x: number; y: number }) | null>(null);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [revealed, setRevealed] = useState(false);
  useEffect(() => {
    const frame = requestAnimationFrame(() => setRevealed(true));
    return () => cancelAnimationFrame(frame);
  }, []);
  const points = snapshots
    .map((snapshot) => {
      const outcome = maturedOutcome(snapshot, horizon);
      const opened = openedTime(snapshot);
      const returnValue = numericReturn(outcome?.directional_return_pct ?? outcome?.return_pct);
      if (!outcome || !opened || returnValue === null) return null;
      return {
        snapshot,
        outcome,
        opened,
        openedLabel: openedDate(snapshot),
        targetLabel: outcome.target_date ? compactDate(outcome.target_date) : "-",
        returnValue,
      };
    })
    .filter((item): item is EventOutcomePoint => item !== null)
    .sort((a, b) => a.opened - b.opened);
  const minOpened = Math.min(...points.map((point) => point.opened), Date.now());
  const maxOpened = Math.max(...points.map((point) => point.opened), minOpened);
  const xRange = Math.max(1, maxOpened - minOpened);
  const xTicks = points.length
    ? [
        { label: compactDate(points[0]?.snapshot.market_date), x: 80 },
        { label: compactDate(points[Math.floor((points.length - 1) / 2)]?.snapshot.market_date), x: 390 },
        { label: compactDate(points[points.length - 1]?.snapshot.market_date), x: 700 },
      ]
    : [];

  function pointCoordinates(point: EventOutcomePoint) {
    const x = points.length <= 1 ? 390 : 80 + ((point.opened - minOpened) / xRange) * 620;
    const y = 110 - Math.max(-25, Math.min(25, point.returnValue)) * 3.2;
    return { x, y };
  }

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
        <svg viewBox="0 0 760 235" className="h-full w-full overflow-visible outline-none" role="img" aria-label="Event outcomes by date and return" tabIndex={0} style={{ touchAction: "pan-y" }} onPointerLeave={(event) => { if (event.pointerType === "mouse") { setHoverPoint(null); setActiveIndex(null); } }} onKeyDown={(event) => { if (event.key === "Escape") { setHoverPoint(null); setActiveIndex(null); return; } if (event.key !== "ArrowLeft" && event.key !== "ArrowRight" || !points.length) return; event.preventDefault(); const next = Math.max(0, Math.min(points.length - 1, (activeIndex ?? 0) + (event.key === "ArrowLeft" ? -1 : 1))); const point = points[next]; const { x, y } = pointCoordinates(point); setActiveIndex(next); setHoverPoint({ ...point, x, y }); }}>
          {[36, 72, 108, 144, 180].map((y) => (
            <line key={y} x1="40" x2="735" y1={y} y2={y} stroke="rgba(148,163,184,0.18)" strokeDasharray="3 4" />
          ))}
          <line x1="40" x2="735" y1="110" y2="110" stroke="rgba(226,232,240,0.48)" strokeDasharray="3 4" />
          <line x1="40" x2="735" y1="190" y2="190" stroke="rgba(148,163,184,0.35)" />
          {xTicks.map(({ label, x }) => (
            <text key={`${label}-${x}`} x={x} y="212" fill="#cbd5e1" fontSize="12" textAnchor="middle">
              {label}
            </text>
          ))}
          <text x="15" y="120" fill="#cbd5e1" fontSize="12" transform="rotate(-90 15 120)">
            Return (%)
          </text>
          <text x="390" y="232" fill="#cbd5e1" fontSize="12" textAnchor="middle">
            Opened Date
          </text>
          {points.map((point, index) => {
            const { x, y } = pointCoordinates(point);
            const positive = point.returnValue >= 0;
            return (
              <g
                key={point.snapshot.id}
                onPointerEnter={() => { setActiveIndex(index); setHoverPoint({ ...point, x, y }); }}
                onPointerDown={() => { setActiveIndex(index); setHoverPoint({ ...point, x, y }); }}
                onFocus={() => { setActiveIndex(index); setHoverPoint({ ...point, x, y }); }}
                onBlur={() => { setActiveIndex(null); setHoverPoint(null); }}
                tabIndex={0}
                role="button"
                aria-label={`${point.snapshot.ticker} opened ${point.openedLabel}, return ${formatPercent(point.returnValue)}`}
              >
                <circle cx={x} cy={y} r="6" fill={positive ? "#84cc16" : "#ef4444"} opacity={revealed ? "0.92" : "0"} className="cursor-pointer transition-opacity duration-300" />
                <text x={x} y={y - 10} fill="#cbd5e1" fontSize="10" textAnchor="middle">
                  {point.snapshot.ticker}
                </text>
              </g>
            );
          })}
        </svg>
        {hoverPoint ? (
          <div
            className="pointer-events-none absolute z-10 w-52 rounded-md border border-white/10 bg-slate-950/95 p-3 text-xs leading-5 text-slate-300 shadow-2xl shadow-black/30"
            style={{
              left: `min(calc(100% - 13rem), max(0.5rem, ${(hoverPoint.x / 760) * 100}%))`,
              top: `min(calc(100% - 7rem), max(0.5rem, ${(hoverPoint.y / 235) * 100}%))`,
            }}
          >
            <p className="font-semibold text-white">{hoverPoint.snapshot.ticker}</p>
            <p>Opened {hoverPoint.openedLabel}</p>
            <p>{horizon} target {hoverPoint.targetLabel}</p>
            <p className={pctClassName(hoverPoint.returnValue)}>Return {formatPercent(hoverPoint.returnValue)}</p>
            <p>SPY {formatPercent(hoverPoint.outcome.spy_return_pct)}</p>
            <p className={pctClassName(hoverPoint.outcome.directional_excess_return_pct ?? hoverPoint.outcome.excess_return_pct)}>
              +/- {formatPercent(hoverPoint.outcome.directional_excess_return_pct ?? hoverPoint.outcome.excess_return_pct)}
            </p>
          </div>
        ) : null}
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

function sortedOutcomeSnapshots(snapshots: OutcomeSnapshot[], sort: OutcomeSort) {
  if (!sort) return snapshots;
  return snapshots
    .map((snapshot, index) => ({ snapshot, index }))
    .sort((left, right) => {
      const a = left.snapshot;
      const b = right.snapshot;
      let value = 0;
      if (sort.key === "ticker") value = a.ticker.localeCompare(b.ticker);
      if (sort.key === "opened") value = openedTime(a) - openedTime(b);
      if (sort.key === "score") value = a.score - b.score;
      if (sort.key === "direction") value = formatDirection(a.direction).localeCompare(formatDirection(b.direction));
      if (sort.key === "entry") value = (a.reference_price ?? 0) - (b.reference_price ?? 0);
      if (value === 0) value = left.index - right.index;
      return sort.direction === "asc" ? value : -value;
    })
    .map((item) => item.snapshot);
}

function dateRangeCutoff(value: DateRangeFilterValue) {
  const now = Date.now();
  if (value === "30d") return now - 30 * 24 * 60 * 60 * 1000;
  if (value === "90d") return now - 90 * 24 * 60 * 60 * 1000;
  if (value === "12m") return now - 365 * 24 * 60 * 60 * 1000;
  return null;
}

function matchesOutcomeFilters(
  snapshot: OutcomeSnapshot,
  {
    cohort,
    horizon,
    direction,
    scoreBand,
    methodology,
    dateRange,
  }: {
    cohort: CohortFilterValue;
    horizon: string;
    direction: string;
    scoreBand: string;
    methodology: string;
    dateRange: DateRangeFilterValue;
  },
) {
  const outcome = outcomeFor(snapshot, horizon);
  if (!publicOutcomeCalculationTypes.has(snapshot.calculation_type)) return false;
  if (cohort === "live" && snapshot.calculation_type !== "live") return false;
  if (cohort === "matured" && !(outcome?.status === "matured" && typeof outcome.return_pct === "number")) return false;
  if (direction !== "All" && formatDirection(snapshot.direction) !== direction) return false;
  if (scoreBand !== "All Scores" && scoreBandForScore(snapshot.score) !== scoreBand) return false;
  if (methodology !== "All Methodologies" && (snapshot.methodology ?? "-") !== methodology) return false;
  const cutoff = dateRangeCutoff(dateRange);
  if (cutoff !== null && openedTime(snapshot) < cutoff) return false;
  return true;
}

function pricePathPointsFromBundle(bundle: TickerChartBundle | null): PricePathPoint[] {
  const prices = [...(bundle?.prices ?? [])]
    .filter((point) => Number.isFinite(point.close))
    .sort((a, b) => a.date.localeCompare(b.date));
  const benchmark = [...(bundle?.benchmark.points ?? [])]
    .filter((point) => Number.isFinite(point.close))
    .sort((a, b) => a.date.localeCompare(b.date));
  const stockBase = prices[0]?.close;
  const spyBase = benchmark[0]?.close;
  if (!stockBase || !spyBase) return [];
  const benchmarkByDate = new Map(benchmark.map((point) => [point.date, point.close]));
  return prices.flatMap((point, index) => {
    const spyClose = benchmarkByDate.get(point.date) ?? benchmark[index]?.close;
    if (!spyClose) return [];
    const stockReturn = returnFromBase(point.close, stockBase);
    const spyReturn = returnFromBase(spyClose, spyBase);
    return [
      {
        date: point.date,
        label: point.date.includes(":") ? compactDateTime(point.date) : compactDate(point.date),
        stockReturn,
        spyReturn,
        excessReturn: stockReturn - spyReturn,
      },
    ];
  });
}

function linePath(points: PricePathPoint[], valueKey: "stockReturn" | "spyReturn", minValue: number, valueRange: number) {
  if (points.length < 2) return "";
  return points
    .map((point, index) => {
      const x = 34 + (index / Math.max(1, points.length - 1)) * 266;
      const y = 122 - ((point[valueKey] - minValue) / valueRange) * 92;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");
}

function PricePathVsSpyChart({ bundle, loading }: { bundle: TickerChartBundle | null; loading: boolean }) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const points = useMemo(() => pricePathPointsFromBundle(bundle), [bundle]);
  if (loading) return <p className="text-sm leading-6 text-slate-300">Loading historical 1D price path...</p>;
  if (points.length < 2) return <p className="text-sm leading-6 text-slate-300">No SPY benchmark chart is available yet for this event window.</p>;

  const values = points.flatMap((point) => [point.stockReturn, point.spyReturn]);
  const minValue = Math.min(...values, 0);
  const maxValue = Math.max(...values, 0);
  const valueRange = Math.max(1, maxValue - minValue);
  const hover = hoverIndex === null ? null : points[hoverIndex];
  const hoverX = hoverIndex === null ? null : 34 + (hoverIndex / Math.max(1, points.length - 1)) * 266;
  const hoverY = hover && hoverX !== null ? 122 - ((hover.stockReturn - minValue) / valueRange) * 92 : null;
  const ticks = [0, Math.floor((points.length - 1) / 2), points.length - 1].map((index) => ({ index, point: points[index] }));

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center gap-4 text-xs text-slate-300">
        <span className="flex items-center gap-2">
          <span className="h-2 w-5 rounded-full bg-lime-400" />
          {bundle?.symbol ?? "Ticker"}
        </span>
        <span className="flex items-center gap-2">
          <span className="h-2 w-5 rounded-full bg-slate-300" />
          SPY
        </span>
        <span className="ml-auto text-slate-400">Historical 1D</span>
      </div>
      <div className="relative h-44">
        <svg viewBox="0 0 320 160" className="h-full w-full overflow-visible" onMouseLeave={() => setHoverIndex(null)}>
          {[30, 76, 122].map((y) => (
            <line key={y} x1="34" x2="300" y1={y} y2={y} stroke="rgba(148,163,184,0.18)" strokeDasharray="3 4" />
          ))}
          <line x1="34" x2="300" y1={122 - ((0 - minValue) / valueRange) * 92} y2={122 - ((0 - minValue) / valueRange) * 92} stroke="rgba(226,232,240,0.4)" strokeDasharray="3 4" />
          <path d={linePath(points, "spyReturn", minValue, valueRange)} fill="none" stroke="#cbd5e1" strokeWidth="2" opacity="0.85" />
          <path d={linePath(points, "stockReturn", minValue, valueRange)} fill="none" stroke="#84cc16" strokeWidth="2.4" />
          {ticks.map(({ index, point }) => (
            <text key={`${point.date}-${index}`} x={34 + (index / Math.max(1, points.length - 1)) * 266} y="150" fill="#94a3b8" fontSize="9" textAnchor="middle">
              {point.label}
            </text>
          ))}
          {points.map((point, index) => {
            const x = 34 + (index / Math.max(1, points.length - 1)) * 266;
            return <rect key={point.date} x={x - 4} y="20" width="8" height="112" fill="transparent" onMouseEnter={() => setHoverIndex(index)} />;
          })}
          {hover && hoverX !== null && hoverY !== null ? (
            <>
              <line x1={hoverX} x2={hoverX} y1="22" y2="126" stroke="rgba(226,232,240,0.38)" />
              <circle cx={hoverX} cy={hoverY} r="4" fill="#84cc16" />
            </>
          ) : null}
        </svg>
      </div>
      <div className="mt-2 min-h-[4.75rem] rounded-md border border-white/10 bg-slate-950/55 p-3 text-xs leading-5 text-slate-300">
        {hover ? (
          <>
            <p className="font-semibold text-white">{hover.label}</p>
            <div className="mt-1 grid grid-cols-3 gap-2">
              <p className={pctClassName(hover.stockReturn)}>Ticker {formatPercent(hover.stockReturn)}</p>
              <p className={pctClassName(hover.spyReturn)}>SPY {formatPercent(hover.spyReturn)}</p>
              <p className={pctClassName(hover.excessReturn)}>+/- {formatPercent(hover.excessReturn)}</p>
            </div>
          </>
        ) : (
          <p className="text-slate-400">Hover the chart to inspect ticker, SPY, and +/- returns.</p>
        )}
      </div>
    </div>
  );
}

function EventsTable({
  snapshots,
  replacedSnapshotIds,
  entitlementTier,
  horizon,
  selectedSnapshotId,
  onSelectSnapshot,
}: {
  snapshots: OutcomeSnapshot[];
  replacedSnapshotIds: Set<number>;
  entitlementTier: EntitlementTier;
  horizon: string;
  selectedSnapshotId: number | null;
  onSelectSnapshot: (snapshot: OutcomeSnapshot) => void;
}) {
  const hasPremiumTable = canUsePremiumOutcomeTable(entitlementTier);
  const [sort, setSort] = useState<OutcomeSort>(null);
  const [tableFilter, setTableFilter] = useState<OutcomeTableFilterValue>("All");
  const [pageSize, setPageSize] = useState<(typeof outcomeTablePageSizes)[number]>(10);
  const [page, setPage] = useState(0);
  const [tableGateOpen, setTableGateOpen] = useState(false);
  const tableSnapshots = useMemo(
    () =>
      snapshots.filter((snapshot) => {
        const hasMaturedOutcome = Boolean(maturedOutcome(snapshot, horizon));
        const isClosed = isClosedOutcomeEvent(snapshot, horizon, replacedSnapshotIds);
        if (tableFilter === "Bullish" || tableFilter === "Bearish") return formatDirection(snapshot.direction) === tableFilter;
        if (tableFilter === "Matured") return hasMaturedOutcome;
        if (tableFilter === "Open") return !isClosed;
        if (tableFilter === "Closed") return isClosed;
        return true;
      }),
    [horizon, replacedSnapshotIds, snapshots, tableFilter],
  );
  const sortedSnapshots = useMemo(() => sortedOutcomeSnapshots(tableSnapshots, hasPremiumTable ? sort : null), [tableSnapshots, hasPremiumTable, sort]);
  const totalRows = hasPremiumTable ? sortedSnapshots.length : Math.min(10, sortedSnapshots.length);
  const effectivePageSize = hasPremiumTable ? pageSize : 10;
  const pageCount = Math.max(1, Math.ceil(totalRows / effectivePageSize));
  const safePage = Math.min(page, pageCount - 1);
  const visibleSnapshots = sortedSnapshots.slice(safePage * effectivePageSize, safePage * effectivePageSize + effectivePageSize);
  const pageStart = totalRows ? safePage * effectivePageSize + 1 : 0;
  const pageEnd = totalRows ? Math.min(totalRows, safePage * effectivePageSize + visibleSnapshots.length) : 0;

  useEffect(() => {
    setPage(0);
  }, [snapshots.length, pageSize, sort?.key, sort?.direction, hasPremiumTable, tableFilter]);

  function gatePremiumTable() {
    if (!hasPremiumTable) setTableGateOpen(true);
    return hasPremiumTable;
  }

  function handleSort(key: OutcomeSortKey) {
    if (!gatePremiumTable()) return;
    setSort((current) => {
      if (current?.key !== key) return { key, direction: "asc" };
      return { key, direction: current.direction === "asc" ? "desc" : "asc" };
    });
  }

  function handlePageSize(nextPageSize: (typeof outcomeTablePageSizes)[number]) {
    if (nextPageSize > 10 && !gatePremiumTable()) return;
    setPageSize(nextPageSize);
  }

  function handlePage(nextPage: number) {
    if (!gatePremiumTable()) return;
    setPage(Math.max(0, Math.min(pageCount - 1, nextPage)));
  }

  return (
    <section className="overflow-hidden rounded-md border border-white/10 bg-slate-900/55">
      {tableGateOpen ? <OutcomeTableGateModal onClose={() => setTableGateOpen(false)} /> : null}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 px-4 py-3">
        <h2 className="text-sm font-bold uppercase tracking-[0.16em] text-white">Confirmation Events</h2>
        <div className="flex overflow-hidden rounded-md border border-white/10 bg-slate-950/60 p-0.5 text-xs font-semibold text-slate-200">
          {outcomeTableFilterOptions.map((label) => (
            <button
              key={label}
              type="button"
              onClick={() => setTableFilter(label)}
              className={`px-4 py-1.5 ${label === tableFilter ? "rounded bg-emerald-400/15 text-emerald-100" : "hover:text-white"}`}
            >
              {label}
            </button>
          ))}
        </div>
        <p className="ml-auto text-xs text-slate-300">{hasPremiumTable ? "Full table: featured tickers plus live-tracked history" : "Free preview: first 10 featured and recent events"}</p>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-white/10 px-4 py-2 text-xs text-slate-300">
        <span>
          Showing {pageStart}-{pageEnd} of {totalRows}
        </span>
        <div className="flex items-center gap-2">
          <span>Rows</span>
          <div className="flex overflow-hidden rounded-md border border-white/10 bg-slate-950/60 p-0.5">
            {outcomeTablePageSizes.map((size) => (
              <button
                key={size}
                type="button"
                onClick={() => handlePageSize(size)}
                className={`min-w-9 px-2 py-1 font-semibold ${effectivePageSize === size ? "rounded bg-emerald-400/15 text-emerald-100" : "text-slate-300 hover:text-white"}`}
              >
                {size}
                {size > 10 && !hasPremiumTable ? <span className="ml-1 text-[10px] text-emerald-300">Premium</span> : null}
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[920px] text-left text-sm">
          <thead className="border-b border-white/10 text-xs text-slate-300">
            <tr>
              {(Object.entries(outcomeSortableColumns) as [OutcomeSortKey, string][]).map(([key, label]) => (
                <th key={key} className="px-4 py-3 font-medium">
                  <button type="button" onClick={() => handleSort(key)} className="inline-flex items-center gap-1 rounded-sm text-left hover:text-white">
                    {label}
                    <span className="text-[10px] text-slate-500">{sort?.key === key ? (sort.direction === "asc" ? "^" : "v") : hasPremiumTable ? "Sort" : "Premium"}</span>
                  </button>
                </th>
              ))}
              {[...horizonColumns, "Status"].map((label) => (
                <th key={label} className="px-4 py-3 font-medium">
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-white/[0.06]">
            {visibleSnapshots.length ? (
              visibleSnapshots.map((snapshot) => {
                const isClosed = isClosedOutcomeEvent(snapshot, horizon, replacedSnapshotIds);
                const isSelected = selectedSnapshotId === snapshot.id;
                return (
                  <tr
                    key={snapshot.id}
                    tabIndex={0}
                    aria-selected={isSelected}
                    onClick={() => onSelectSnapshot(snapshot)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        onSelectSnapshot(snapshot);
                      }
                    }}
                    className={`cursor-pointer outline-none hover:bg-white/[0.04] focus-visible:bg-white/[0.05] ${
                      isSelected ? "bg-emerald-400/[0.08] ring-1 ring-inset ring-emerald-400/30" : ""
                    }`}
                  >
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
                      <span
                        className={`rounded-md px-3 py-1 text-xs ${
                          isClosed ? "bg-amber-400/15 text-amber-100" : "bg-emerald-400/15 text-emerald-100"
                        }`}
                      >
                        {outcomeLifecycleStatusLabel(snapshot, isClosed)}
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
        <span>{hasPremiumTable ? `Page ${safePage + 1} of ${pageCount}` : "Free users are locked to one 10-row page"}</span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => handlePage(safePage - 1)}
            disabled={hasPremiumTable && safePage === 0}
            className="rounded-md border border-white/10 px-3 py-1.5 font-semibold text-slate-200 hover:bg-white/5 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Previous
          </button>
          <button
            type="button"
            onClick={() => handlePage(safePage + 1)}
            disabled={hasPremiumTable && safePage >= pageCount - 1}
            className="rounded-md border border-white/10 px-3 py-1.5 font-semibold text-slate-200 hover:bg-white/5 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Next
          </button>
        </div>
      </div>
    </section>
  );
}

function DetailPanel({
  selected,
  isSelectedReplaced,
  entitlementTier,
  horizon,
  onClose,
}: {
  selected?: OutcomeSnapshot;
  isSelectedReplaced: boolean;
  entitlementTier: EntitlementTier;
  horizon: string;
  onClose: () => void;
}) {
  const selectedHorizonOutcome = selected ? maturedOutcome(selected, horizon) : undefined;
  const canViewPremium = canViewPremiumOutcomes(entitlementTier);
  const [chartBundle, setChartBundle] = useState<TickerChartBundle | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  const maturedHorizons = selected
    ? horizonColumns
        .map((horizon) => ({ horizon, outcome: outcomeFor(selected, horizon) }))
        .filter((item): item is { horizon: string; outcome: OutcomeHorizonResult } => item.outcome?.status === "matured" && typeof item.outcome.return_pct === "number")
    : [];
  const sourceRows = contributionRows(selected);

  useEffect(() => {
    if (!selected?.ticker || !canViewPremium) {
      setChartBundle(null);
      setChartLoading(false);
      return;
    }
    const controller = new AbortController();
    setChartLoading(true);
    getTickerChartBundle(selected.ticker, 30, { signal: controller.signal, source: "OutcomeLedgerPricePath" })
      .then((bundle) => setChartBundle(bundle))
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        setChartBundle(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) setChartLoading(false);
      });
    return () => controller.abort();
  }, [canViewPremium, selected?.ticker]);

  return (
    <aside className="rounded-md border border-white/10 bg-slate-900/60 p-5 lg:sticky lg:top-20 lg:h-[calc(100vh-6rem)]">
      <div className="flex items-center justify-between border-b border-white/10 pb-4">
        <h2 className="text-xs font-bold uppercase tracking-[0.18em] text-white">Event Detail</h2>
        <button type="button" onClick={onClose} className="rounded-md border border-transparent px-2 py-1 text-xl leading-none text-slate-300 hover:border-white/10 hover:bg-white/5" aria-label="Close event detail">
          x
        </button>
      </div>
      <div className="mt-5 space-y-5">
        <div>
          <p className="text-2xl font-semibold text-white">
            {selected?.ticker ?? "Outcome"} <span className="px-1 text-sm text-slate-400">-</span>{" "}
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
              ["Status", selected ? outcomeLifecycleStatusLabel(selected, isSelectedReplaced) : "-"],
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
            {canViewPremium ? (
              <>
                <PricePathVsSpyChart bundle={chartBundle} loading={chartLoading} />
                <p className="mt-3 text-xs leading-5 text-slate-400">
                  SPY is the benchmark. Horizon excess compares the ticker return from opened date to target date against SPY over that same window.
                </p>
                {selectedHorizonOutcome ? (
                  <dl className="mt-3 grid grid-cols-2 gap-y-2 border-t border-white/[0.08] pt-3">
                    <dt>{horizon} Return</dt>
                    <dd className={`text-right ${pctClassName(selectedHorizonOutcome.return_pct)}`}>{formatPercent(selectedHorizonOutcome.return_pct)}</dd>
                    <dt>SPY {horizon}</dt>
                    <dd className={`text-right ${pctClassName(selectedHorizonOutcome.spy_return_pct)}`}>{formatPercent(selectedHorizonOutcome.spy_return_pct)}</dd>
                    <dt>{horizon} Excess</dt>
                    <dd className={`text-right ${pctClassName(selectedHorizonOutcome.excess_return_pct)}`}>{formatPercent(selectedHorizonOutcome.excess_return_pct)}</dd>
                  </dl>
                ) : maturedHorizons.length ? (
                  <dl className="mt-3 grid grid-cols-2 gap-y-2 border-t border-white/[0.08] pt-3">
                    {maturedHorizons.map(({ horizon, outcome }) => (
                      <div key={horizon} className="contents">
                        <dt>{horizon} Return</dt>
                        <dd className={`text-right ${pctClassName(outcome.return_pct)}`}>{formatPercent(outcome.return_pct)}</dd>
                        <dt>{horizon} vs SPY</dt>
                        <dd className={`text-right ${pctClassName(outcome.excess_return_pct)}`}>{formatPercent(outcome.excess_return_pct)}</dd>
                      </div>
                    ))}
                  </dl>
                ) : null}
              </>
            ) : (
              "Premium will show benchmark comparisons after event horizons mature."
            )}
          </div>
        </div>
        <div className="border-t border-white/10 pt-4">
          <h3 className="text-xs font-bold uppercase tracking-[0.12em] text-white">Source Contributions <span className="text-slate-400">i</span></h3>
          <div className="mt-4 rounded-md border border-white/10 bg-white/[0.03] p-4 text-sm leading-6 text-slate-300">
            {canViewPremium ? (
              sourceRows.length ? (
                <div className="space-y-2">
                  {sourceRows.map((row) => (
                    <div key={row.key} className="grid grid-cols-[1fr_auto] gap-x-3 border-b border-white/[0.06] pb-2 last:border-0 last:pb-0">
                      <span className="text-slate-100">{row.label}</span>
                      <span className="text-right text-lime-300">{row.score === null ? "active" : `${row.score > 0 ? "+" : ""}${row.score}`}</span>
                      <span className="text-xs text-slate-400">{row.direction}</span>
                      <span className="text-right text-xs text-slate-400">{row.strength}</span>
                    </div>
                  ))}
                </div>
              ) : (
                "No active source contribution payload was captured for this event."
              )
            ) : (
              "Premium will show original source contributions and score evolution for each event."
            )}
          </div>
        </div>
        <div className="border-t border-white/10 pt-4">
          <h3 className="text-xs font-bold uppercase tracking-[0.12em] text-white">Methodology</h3>
          <p className="mt-3 rounded-md border border-white/10 bg-white/[0.04] p-4 text-sm leading-5 text-slate-300">
            {selected
              ? `${selected.methodology ?? "confirmation-v1"} preserved ${selected.ticker} at ${openedDate(selected)} with an opened ${selected.score}/100 ${formatDirection(selected.direction)} score. The live ticker page can move after this snapshot.`
              : "Events are created from live confirmation-score snapshots. Outcome windows mature independently while the event remains open until the thesis closes."}
          </p>
        </div>
      </div>
    </aside>
  );
}

export function OutcomeLedgerClient({
  initialStatus,
  initialSummary,
  initialSnapshots,
}: {
  initialStatus: OutcomeLedgerStatus | null;
  initialSummary: OutcomeLedgerSummary | null;
  initialSnapshots: OutcomeSnapshotsResponse | null;
}) {
  const [status, setStatus] = useState(initialStatus);
  const [summary, setSummary] = useState(initialSummary);
  const [snapshots, setSnapshots] = useState<OutcomeSnapshotsResponse | null>(initialSnapshots);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(!initialStatus || !initialSummary || !initialSnapshots);
  const [entitlementTier, setEntitlementTier] = useState<EntitlementTier>("free");
  const [exportGateOpen, setExportGateOpen] = useState(false);
  const [cohortFilter, setCohortFilter] = useState<CohortFilterValue>("all");
  const [horizonFilter, setHorizonFilter] = useState("7D");
  const [directionFilter, setDirectionFilter] = useState("All");
  const [scoreBandFilter, setScoreBandFilter] = useState("All Scores");
  const [methodologyFilter, setMethodologyFilter] = useState("All Methodologies");
  const [dateRangeFilter, setDateRangeFilter] = useState<DateRangeFilterValue>("all");
  const [eventDetailOpen, setEventDetailOpen] = useState(true);
  const [selectedSnapshotId, setSelectedSnapshotId] = useState<number | null>(null);

  useEffect(() => {
    setEntitlementTier(clientEntitlementTier());
    let alive = true;
    getEntitlements(undefined, { source: "OutcomeLedgerEntitlements" })
      .then((entitlements) => {
        if (!alive) return;
        setEntitlementTier(normalizeTier(entitlements.effective_tier ?? entitlements.tier));
      })
      .catch(() => {
        if (alive) setEntitlementTier(clientEntitlementTier());
      });
    return () => {
      alive = false;
    };
  }, []);

  useEffect(() => {
    if (initialStatus && initialSummary && initialSnapshots) return;
    let alive = true;
    setLoading(true);
    getOutcomeLedgerOverview({ limit: 100, horizons: "7D,30D" })
      .then((overview) => {
        if (!alive) return;
        setStatus(overview.status);
        setSummary(overview.summaries[horizonFilter] ?? overview.summaries[overview.default_horizon] ?? null);
        setSnapshots(overview.snapshots);
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
  }, [horizonFilter, initialStatus, initialSummary, initialSnapshots]);

  useEffect(() => {
    if (summary?.horizon === horizonFilter) return;
    let alive = true;
    getOutcomeLedgerSummary({ horizon: horizonFilter })
      .then((nextSummary) => {
        if (alive) setSummary(nextSummary);
      })
      .catch(() => undefined);
    return () => {
      alive = false;
    };
  }, [horizonFilter, summary?.horizon]);

  const snapshotItems = useMemo(() => (snapshots?.items ?? []).map(hydrateDemoOutcomes), [snapshots?.items]);
  const uniqueSnapshotItems = useMemo(() => {
    const byVisibleEvent = new Map<string, OutcomeSnapshot>();
    snapshotItems.forEach((snapshot) => {
      const key = visibleOutcomeEventKey(snapshot);
      const current = byVisibleEvent.get(key);
      if (!current || calculatedTime(snapshot) > calculatedTime(current) || (calculatedTime(snapshot) === calculatedTime(current) && snapshot.id > current.id)) {
        byVisibleEvent.set(key, snapshot);
      }
    });
    return [...byVisibleEvent.values()];
  }, [snapshotItems]);
  const replacedSnapshotIds = useMemo(() => replacedOutcomeSnapshotIds(uniqueSnapshotItems), [uniqueSnapshotItems]);
  const methodologyOptions = useMemo(() => {
    const values = [...new Set(uniqueSnapshotItems.map((snapshot) => snapshot.methodology ?? "-").filter(Boolean))].sort();
    return ["All Methodologies", ...values].map((value) => ({ value, label: value }));
  }, [uniqueSnapshotItems]);
  const filteredSnapshotItems = useMemo(
    () =>
      uniqueSnapshotItems.filter((snapshot) =>
        matchesOutcomeFilters(snapshot, {
          cohort: cohortFilter,
          horizon: horizonFilter,
          direction: directionFilter,
          scoreBand: scoreBandFilter,
          methodology: methodologyFilter,
          dateRange: dateRangeFilter,
        }),
      ),
    [cohortFilter, dateRangeFilter, directionFilter, horizonFilter, methodologyFilter, scoreBandFilter, uniqueSnapshotItems],
  );
  const canUseServerSummary =
    summary?.horizon === horizonFilter &&
    cohortFilter === "all" &&
    directionFilter === "All" &&
    scoreBandFilter === "All Scores" &&
    methodologyFilter === "All Methodologies" &&
    dateRangeFilter === "all";
  const publicPreviewSnapshots = useMemo(() => {
    const byTicker = new Map<string, OutcomeSnapshot[]>();
    filteredSnapshotItems.forEach((snapshot) => {
      const key = snapshot.ticker.toUpperCase();
      const rows = byTicker.get(key);
      if (rows) rows.push(snapshot);
      else byTicker.set(key, [snapshot]);
    });
    const featured = featuredOutcomeTickers.flatMap((ticker) => {
      const snapshot = [...(byTicker.get(ticker) ?? [])].sort(
        (a, b) =>
          Number(replacedSnapshotIds.has(a.id)) - Number(replacedSnapshotIds.has(b.id)) ||
          openedTime(b) - openedTime(a) ||
          calculatedTime(b) - calculatedTime(a) ||
          b.id - a.id,
      )[0];
      return snapshot ? [snapshot] : [];
    });
    const featuredIds = new Set(featured.map((snapshot) => snapshot.id));
    const recentFill = filteredSnapshotItems.filter((snapshot) => !featuredIds.has(snapshot.id));
    return [...featured, ...recentFill];
  }, [filteredSnapshotItems, replacedSnapshotIds]);
  const selectedSnapshot = useMemo(
    () => publicPreviewSnapshots.find((snapshot) => snapshot.id === selectedSnapshotId) ?? publicPreviewSnapshots[0],
    [publicPreviewSnapshots, selectedSnapshotId],
  );
  useEffect(() => {
    if (!publicPreviewSnapshots.length) {
      if (selectedSnapshotId !== null) setSelectedSnapshotId(null);
      return;
    }
    if (selectedSnapshotId === null || !publicPreviewSnapshots.some((snapshot) => snapshot.id === selectedSnapshotId)) {
      setSelectedSnapshotId(publicPreviewSnapshots[0].id);
    }
  }, [publicPreviewSnapshots, selectedSnapshotId]);
  const outcomeMetrics = useMemo(() => {
    if (canUseServerSummary && summary) {
      return {
        completedEvents: summary.completed_events,
        accuracy: summary.accuracy,
        accuracyReliable: summary.directional_sample_count >= minimumHeadlineDirectionalSamples,
        directionalSampleCount: summary.directional_sample_count,
        averageDirectionalReturn: summary.average_directional_return,
        averageSpyReturn: summary.average_spy_return,
        averageDirectionalExcessReturn: summary.average_directional_excess_return,
        benchmarkedEvents: summary.benchmarked_events,
        maturedHorizonCount: summary.matured_horizon_count,
      };
    }
    const maturedForHorizon = filteredSnapshotItems
      .map((snapshot) => maturedOutcome(snapshot, horizonFilter))
      .filter((outcome): outcome is OutcomeHorizonResult => Boolean(outcome));
    const directionalForHorizon = maturedForHorizon.filter((outcome) => typeof outcome.directionally_correct === "boolean");
    const accuracy =
      directionalForHorizon.length > 0
        ? Math.round((directionalForHorizon.filter((outcome) => outcome.directionally_correct).length / directionalForHorizon.length) * 100)
        : null;
    const accuracyReliable = directionalForHorizon.length >= minimumHeadlineDirectionalSamples;
    const directionalReturns = maturedForHorizon
      .map((outcome) => outcome.directional_return_pct)
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    const benchmarkedDirectionalOutcomes = maturedForHorizon.filter(
      (outcome) =>
        typeof outcome.directional_return_pct === "number" &&
        Number.isFinite(outcome.directional_return_pct) &&
        typeof outcome.spy_return_pct === "number" &&
        Number.isFinite(outcome.spy_return_pct),
    );
    const averageDirectionalReturn = average(directionalReturns);
    const averageBenchmarkedDirectionalReturn = average(benchmarkedDirectionalOutcomes.map((outcome) => outcome.directional_return_pct as number));
    const averageSpyReturn = average(benchmarkedDirectionalOutcomes.map((outcome) => outcome.spy_return_pct as number));
    const averageDirectionalExcessReturn =
      averageBenchmarkedDirectionalReturn !== null && averageSpyReturn !== null ? Number((averageBenchmarkedDirectionalReturn - averageSpyReturn).toFixed(2)) : null;
    const maturedHorizonCount = filteredSnapshotItems.reduce(
      (total, snapshot) =>
        total +
        horizonColumns.filter((horizon) => {
          const outcome = outcomeFor(snapshot, horizon);
          return outcome?.status === "matured" && typeof outcome.return_pct === "number";
        }).length,
      0,
    );
    return {
      completedEvents: maturedForHorizon.length,
      accuracy,
      accuracyReliable,
      directionalSampleCount: directionalForHorizon.length,
      averageDirectionalReturn,
      averageSpyReturn,
      averageDirectionalExcessReturn,
      benchmarkedEvents: benchmarkedDirectionalOutcomes.length,
      maturedHorizonCount,
    };
  }, [canUseServerSummary, filteredSnapshotItems, horizonFilter, summary]);
  const canExportCsv = canExportOutcomesCsv(entitlementTier);

  function handleSelectSnapshot(snapshot: OutcomeSnapshot) {
    setSelectedSnapshotId(snapshot.id);
    setEventDetailOpen(true);
  }

  function handleExportCsv() {
    if (!canExportCsv) {
      setExportGateOpen(true);
      return;
    }
    const header = ["Ticker", "Opened", "Opened Score", "Opened Direction", "Entry Price", "7D", "30D", "90D", "180D", "365D", "Status"];
    const rows = publicPreviewSnapshots.map((snapshot) => {
      const isClosed = isClosedOutcomeEvent(snapshot, horizonFilter, replacedSnapshotIds);
      return [
        snapshot.ticker,
        openedDate(snapshot),
        snapshot.score,
        formatDirection(snapshot.direction),
        formatPrice(snapshot.reference_price),
        ...horizonColumns.map((horizon) => {
          const outcome = outcomeFor(snapshot, horizon);
          return outcomeValueLabel(outcome);
        }),
        outcomeLifecycleStatusLabel(snapshot, isClosed),
      ];
    });
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
      <div className={`grid gap-4 ${eventDetailOpen ? "xl:grid-cols-[minmax(0,1fr)_22.5rem]" : "xl:grid-cols-1"}`}>
        <main className="min-w-0 space-y-4">
          <header>
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">OUTCOMES</p>
            <p className="text-sm font-bold uppercase tracking-[0.36em] text-white">Outcome Ledger</p>
            <p className="mt-1 text-sm text-slate-300">Track what Walnut believed at the time - and what happened next.</p>
          </header>

          {error ? (
            <div className="rounded-md border border-amber-300/20 bg-amber-400/10 px-4 py-3 text-xs text-amber-100">{error}</div>
          ) : null}

          <div className="grid gap-2 xl:grid-cols-[minmax(0,1fr)_14.25rem]">
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-6">
              <FilterSelect label="Outcome Set" value={cohortFilter} options={cohortFilterOptions} onChange={(value) => setCohortFilter(value as CohortFilterValue)} />
              <FilterSelect label="Horizon" value={horizonFilter} options={horizonColumns.map((value) => ({ value, label: value }))} onChange={setHorizonFilter} />
              <FilterSelect label="Direction" value={directionFilter} options={directionFilterOptions.map((value) => ({ value, label: value }))} onChange={setDirectionFilter} />
              <FilterSelect label="Score Band" value={scoreBandFilter} options={scoreBandFilterOptions.map((value) => ({ value, label: value }))} onChange={setScoreBandFilter} />
              <FilterSelect label="Methodology" value={methodologyFilter} options={methodologyOptions} onChange={setMethodologyFilter} />
              <FilterSelect label="Date Range" value={dateRangeFilter} options={dateRangeFilterOptions} onChange={(value) => setDateRangeFilter(value as DateRangeFilterValue)} />
            </div>
            <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
              <button
                type="button"
                onClick={handleExportCsv}
                className="h-12 w-full whitespace-nowrap rounded-md border border-white/10 bg-slate-900/70 px-3 text-xs font-bold text-slate-200 hover:border-emerald-300/30 hover:bg-emerald-400/10"
              >
                Export CSV
              </button>
              <ScoredHorizonsPill className="w-full" value={outcomeMetrics.maturedHorizonCount} detail={`${statusLabel(status, loading)} outcome cells`} />
              {!eventDetailOpen ? (
                <button
                  type="button"
                  onClick={() => {
                    if (!selectedSnapshot && publicPreviewSnapshots[0]) setSelectedSnapshotId(publicPreviewSnapshots[0].id);
                    setEventDetailOpen(true);
                  }}
                  className="h-12 w-full whitespace-nowrap rounded-md border border-white/10 bg-slate-900/70 px-3 text-xs font-bold text-slate-200 hover:border-emerald-300/30 hover:bg-emerald-400/10 sm:col-span-2 xl:col-span-1"
                >
                  Event Detail
                </button>
              ) : null}
            </div>
          </div>

          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              icon="OK"
              label="Completed Events"
              value={outcomeMetrics.completedEvents}
              detail={`${horizonFilter} matured rows loaded; mixed/neutral excluded from accuracy`}
            />
            <MetricCard
              icon={horizonFilter.replace("D", "")}
              label={`${horizonFilter} Directional Accuracy`}
              value={
                outcomeMetrics.accuracy === null
                  ? "Pending"
                  : outcomeMetrics.accuracyReliable
                    ? `${outcomeMetrics.accuracy}%`
                    : "Building"
              }
              detail={
                outcomeMetrics.accuracy === null
                  ? `No bullish or bearish calls matured at ${horizonFilter}`
                  : outcomeMetrics.accuracyReliable
                    ? `${outcomeMetrics.directionalSampleCount} bullish/bearish calls measured at ${horizonFilter}`
                    : `${outcomeMetrics.directionalSampleCount}/${minimumHeadlineDirectionalSamples} directional samples; show percent at 30`
              }
            />
            <MetricCard
              icon="+/-"
              label="Average Directional Return"
              value={formatPercent(outcomeMetrics.averageDirectionalReturn)}
              detail={`Average ${horizonFilter} outcome across ${outcomeMetrics.directionalSampleCount} directional samples`}
            />
            <MetricCard
              icon="SPY"
              label="Average Excess vs SPY"
              value={formatPercent(outcomeMetrics.averageDirectionalExcessReturn)}
              detail={
                outcomeMetrics.benchmarkedEvents
                  ? `Average scored return minus average SPY ${formatPercent(outcomeMetrics.averageSpyReturn)} (${outcomeMetrics.benchmarkedEvents} events)`
                  : `Pending SPY benchmark samples at ${horizonFilter}`
              }
            />
          </div>

          <div className="grid gap-2 xl:grid-cols-[0.82fr_1.18fr]">
            <BarChartPanel snapshots={filteredSnapshotItems} horizon={horizonFilter} summary={canUseServerSummary ? summary : null} />
            <ScatterPanel snapshots={filteredSnapshotItems} horizon={horizonFilter} />
          </div>

          <EventsTable
            snapshots={publicPreviewSnapshots}
            replacedSnapshotIds={replacedSnapshotIds}
            entitlementTier={entitlementTier}
            horizon={horizonFilter}
            selectedSnapshotId={selectedSnapshot?.id ?? null}
            onSelectSnapshot={handleSelectSnapshot}
          />
        </main>

        {eventDetailOpen ? (
          <DetailPanel
            selected={selectedSnapshot}
            isSelectedReplaced={selectedSnapshot ? isClosedOutcomeEvent(selectedSnapshot, horizonFilter, replacedSnapshotIds) : false}
            entitlementTier={entitlementTier}
            horizon={horizonFilter}
            onClose={() => setEventDetailOpen(false)}
          />
        ) : null}
      </div>
    </div>
  );
}
