import type { TickerValuationResponse } from "@/lib/api";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import type { ReactNode } from "react";
import Link from "next/link";

type Props = {
  data: TickerValuationResponse;
  symbol: string;
  canViewDetails?: boolean;
};

const cardSurface = "rounded-lg border border-white/10 bg-slate-950/55";
const muted = "text-slate-400";
const ratioAssumptionKeys = new Set([
  "revenueGrowthPct",
  "ebitdaPct",
  "depreciationAndAmortizationPct",
  "cashAndShortTermInvestmentsPct",
  "receivablesPct",
  "inventoriesPct",
  "payablePct",
  "ebitPct",
  "capitalExpenditurePct",
  "operatingCashFlowPct",
  "sellingGeneralAndAdministrativeExpensesPct",
  "taxRate",
]);

function asNumber(value: number | null | undefined): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatMoney(value: number | null | undefined, options?: { compact?: boolean; maximumFractionDigits?: number }): string {
  const numeric = asNumber(value);
  if (numeric === null) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: options?.compact ? "compact" : "standard",
    maximumFractionDigits: options?.maximumFractionDigits ?? (Math.abs(numeric) >= 1000 ? 0 : 2),
  }).format(numeric);
}

function formatPercent(value: number | null | undefined, options?: { signed?: boolean; ratio?: boolean }): string {
  const numeric = asNumber(value);
  if (numeric === null) return "-";
  const scaled = options?.ratio && Math.abs(numeric) <= 1 ? numeric * 100 : numeric;
  const prefix = options?.signed && scaled > 0 ? "+" : "";
  return `${prefix}${new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(scaled)}%`;
}

function formatAssumption(value: number, key?: string): string {
  if ((key ?? "").toLowerCase() === "beta") {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value);
  }
  if (key && ratioAssumptionKeys.has(key)) {
    return formatPercent(value, { ratio: Math.abs(value) <= 5 });
  }
  if (Math.abs(value) <= 100) return formatPercent(value);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value);
}

function toneForJudgment(value: string | null | undefined): string {
  const normalized = (value ?? "").toLowerCase();
  if (normalized.includes("under") || normalized.includes("bull")) return "text-emerald-300";
  if (normalized.includes("over") || normalized.includes("bear")) return "text-rose-300";
  if (normalized.includes("fair") || normalized.includes("neutral")) return "text-slate-200";
  return "text-slate-400";
}

function signalTone(value: string | null | undefined): string {
  const normalized = (value ?? "").toLowerCase();
  if (normalized.includes("bull")) return "text-emerald-300";
  if (normalized.includes("bear") || normalized.includes("not cheap")) return "text-rose-300";
  if (normalized.includes("mixed")) return "text-amber-300";
  return "text-slate-300";
}

function markerPosition(value: number, min: number, max: number): number {
  if (max <= min) return 50;
  return Math.min(100, Math.max(0, ((value - min) / (max - min)) * 100));
}

function SummaryCard({
  label,
  value,
  sub,
  tone = "text-white",
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: string;
}) {
  return (
    <div className={`${cardSurface} p-4`}>
      <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className={`mt-3 text-2xl font-semibold tabular-nums ${tone}`}>{value}</p>
      {sub ? <p className="mt-1 text-xs leading-5 text-slate-400">{sub}</p> : null}
    </div>
  );
}

function ProBlur({ children, className = "" }: { children: ReactNode; className?: string }) {
  return (
    <div className={`relative overflow-hidden rounded-lg ${className}`}>
      <div className="pointer-events-none select-none blur-[7px] saturate-50 opacity-45" aria-hidden="true">
        {children}
      </div>
      <div className="absolute inset-0 grid place-items-center bg-slate-950/70 backdrop-blur-[4px]">
        <Link href="/pricing" className="rounded-lg border border-indigo-300/30 bg-indigo-500/15 px-3 py-2 text-center shadow-[0_0_22px_rgba(99,102,241,0.16)] transition hover:border-indigo-200/50 hover:bg-indigo-500/20">
          <p className="text-[10px] font-semibold uppercase tracking-[0.16em] text-indigo-200">Upgrade to Pro</p>
          <p className="mt-1 text-xs font-semibold text-slate-100">Full valuation details</p>
        </Link>
      </div>
    </div>
  );
}

function ValuationRange({ data }: { data: TickerValuationResponse }) {
  const dcf = data.dcf ?? {};
  const consensus = data.consensus ?? null;
  const markers = [
    { key: "bear", label: "Bear", value: asNumber(dcf.bearValue), className: "bg-rose-300", shape: "circle" },
    { key: "base", label: "Fair Value", value: asNumber(dcf.fairValue), className: "bg-teal-300", shape: "circle" },
    { key: "bull", label: "Bull", value: asNumber(dcf.bullValue), className: "bg-emerald-300", shape: "circle" },
    { key: "current", label: "Current Price", value: asNumber(dcf.currentPrice), className: "bg-white", shape: "triangle" },
    { key: "consensus", label: "Analyst Consensus", value: asNumber(consensus?.targetConsensus), className: "bg-sky-300", shape: "square" },
  ] as const;
  const values = markers.map((marker) => marker.value).filter((value): value is number => value !== null);
  if (!values.length) {
    return (
      <section className={`${cardSurface} p-5`}>
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Valuation Range</p>
        <p className="mt-4 text-sm text-slate-400">Valuation range inputs are not available for this ticker yet.</p>
      </section>
    );
  }

  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const padding = Math.max((rawMax - rawMin) * 0.12, rawMax * 0.04, 1);
  const min = rawMin - padding;
  const max = rawMax + padding;

  return (
    <section className={`${cardSurface} p-5`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Valuation Range</p>
          <p className="mt-2 text-sm text-slate-400">Anchored fair value, scenario sensitivity, current price, and street consensus.</p>
        </div>
        {dcf.rangeSource === "fair_value_anchor" ? (
          <span className="rounded-md border border-teal-300/20 bg-teal-300/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-teal-100">
            Anchored FMV
          </span>
        ) : dcf.rangeSource === "dcf_sensitivity" ? (
          <span className="rounded-md border border-teal-300/20 bg-teal-300/10 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-teal-100">
            DCF sensitivity
          </span>
        ) : null}
      </div>
      <div className="mt-8 px-3 pb-12 pt-8">
        <div className="relative h-1 rounded-full bg-gradient-to-r from-rose-300 via-teal-300 to-emerald-300">
          {markers.map((marker) => {
            if (marker.value === null) return null;
            const left = markerPosition(marker.value, min, max);
            return (
              <div key={marker.key} className="absolute top-1/2" style={{ left: `${left}%` }}>
                {marker.shape === "triangle" ? (
                  <div className="-translate-x-1/2 translate-y-3">
                    <div className="mx-auto h-0 w-0 border-x-[6px] border-b-[10px] border-x-transparent border-b-white" />
                  </div>
                ) : (
                  <div
                    className={`-translate-x-1/2 -translate-y-1/2 border border-slate-950 shadow-[0_0_18px_rgba(45,212,191,0.22)] ${marker.className} ${
                      marker.shape === "square" ? "h-3 w-3 rounded-[3px]" : "h-3.5 w-3.5 rounded-full"
                    }`}
                  />
                )}
                <div className="mt-4 -translate-x-1/2 whitespace-nowrap text-center">
                  <p className={`text-[11px] font-semibold ${marker.key === "consensus" ? "text-sky-200" : marker.key === "current" ? "text-white" : "text-slate-200"}`}>
                    {marker.label}
                  </p>
                  <p className="mt-1 text-[11px] tabular-nums text-slate-500">{formatMoney(marker.value, { maximumFractionDigits: 0 })}</p>
                </div>
              </div>
            );
          })}
        </div>
      </div>
      <div className="mt-2 grid gap-2 text-xs text-slate-500 sm:grid-cols-3">
        <span>{formatMoney(min, { maximumFractionDigits: 0 })}</span>
        <span className="text-center">Base {formatMoney(dcf.fairValue, { maximumFractionDigits: 0 })}</span>
        <span className="text-right">{formatMoney(max, { maximumFractionDigits: 0 })}</span>
      </div>
    </section>
  );
}

function CashFlowChart({ data }: { data: TickerValuationResponse }) {
  const points = (data.dcf.cashFlows ?? []).filter((point) => asNumber(point.actualCashFlow) !== null || asNumber(point.discountedCashFlow) !== null);
  if (!points.length) {
    return (
      <section className={`${cardSurface} p-5`}>
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Cash Flow Bridge</p>
        <p className="mt-4 text-sm text-slate-400">Cash-flow projection inputs are not available for this ticker yet.</p>
      </section>
    );
  }
  const maxAbs = Math.max(
    ...points.flatMap((point) => [Math.abs(asNumber(point.actualCashFlow) ?? 0), Math.abs(asNumber(point.discountedCashFlow) ?? 0)]),
    1,
  );

  return (
    <section className={`${cardSurface} p-5`}>
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Cash Flow Bridge</p>
          <p className="mt-2 text-sm text-slate-400">Projected free cash flows compared with discounted cash flows by projection year.</p>
        </div>
        <div className="flex gap-3 text-[11px] text-slate-400">
          <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-sm bg-teal-300" />Projected FCF</span>
          <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-sm bg-sky-300" />Discounted CF</span>
        </div>
      </div>
      <div className="mt-6 grid min-h-[210px] grid-cols-[auto_minmax(0,1fr)] gap-4">
        <div className="flex flex-col justify-between py-2 text-[10px] tabular-nums text-slate-500">
          <span>{formatMoney(maxAbs, { compact: true, maximumFractionDigits: 1 })}</span>
          <span>$0</span>
        </div>
        <div className="relative overflow-hidden rounded-lg border border-white/10 bg-slate-950/70 px-4 pb-8 pt-4">
          <div className="absolute inset-x-4 bottom-8 border-t border-white/10" />
          <div className="relative z-10 grid h-40 items-end gap-3" style={{ gridTemplateColumns: `repeat(${points.length}, minmax(0, 1fr))` }}>
            {points.map((point) => {
              const actual = asNumber(point.actualCashFlow);
              const discounted = asNumber(point.discountedCashFlow);
              const actualHeight = `${Math.max(4, ((Math.abs(actual ?? 0) / maxAbs) * 100))}%`;
              const discountedHeight = `${Math.max(4, ((Math.abs(discounted ?? 0) / maxAbs) * 100))}%`;
              return (
                <div key={point.year} className="flex h-full min-w-0 items-end justify-center gap-1.5">
                  <div title={`Projected FCF ${formatMoney(actual, { compact: true })}`} className="w-full max-w-5 rounded-t bg-teal-300/85 shadow-[0_0_14px_rgba(45,212,191,0.22)]" style={{ height: actual === null ? 0 : actualHeight }} />
                  <div title={`Discounted CF ${formatMoney(discounted, { compact: true })}`} className="w-full max-w-5 rounded-t bg-sky-300/85 shadow-[0_0_14px_rgba(125,211,252,0.18)]" style={{ height: discounted === null ? 0 : discountedHeight }} />
                </div>
              );
            })}
          </div>
          <div className="absolute inset-x-4 bottom-2 grid gap-3 text-center text-[10px] text-slate-500" style={{ gridTemplateColumns: `repeat(${points.length}, minmax(0, 1fr))` }}>
            {points.map((point) => <span key={point.year} className="truncate">{point.year}</span>)}
          </div>
        </div>
      </div>
    </section>
  );
}

function MethodSignals({ data }: { data: TickerValuationResponse }) {
  const signals = data.dcf.methodSignals ?? [];
  if (!signals.length) return null;
  return (
    <section className={`${cardSurface} p-5`}>
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Method Signals</p>
      <div className="mt-4 grid gap-2">
        {signals.map((item) => (
          <div key={`${item.method}-${item.signal}`} className="grid grid-cols-[minmax(0,1fr)_auto] gap-3 rounded-lg border border-white/10 bg-slate-950/70 px-3 py-2.5 text-sm">
            <span className="font-semibold text-slate-100">{item.method}</span>
            <span className={`font-semibold ${signalTone(item.signal)}`}>{item.signal}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function Assumptions({ data }: { data: TickerValuationResponse }) {
  const assumptions = data.dcf.assumptions ?? [];
  if (!assumptions.length) {
    return (
      <section className={`${cardSurface} p-5`}>
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Key Inputs</p>
        <p className="mt-4 text-sm text-slate-400">DCF assumption inputs are not available for this ticker yet.</p>
      </section>
    );
  }
  return (
    <section className={`${cardSurface} p-5`}>
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">Key Inputs</p>
      <div className="mt-4 grid gap-2">
        {assumptions.map((item) => (
          <div key={item.key ?? item.label} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3 rounded-lg border border-white/10 bg-slate-950/70 px-3 py-2.5 text-sm">
            <span className="min-w-0 truncate text-slate-300">{item.label}</span>
            <span className="font-semibold tabular-nums text-teal-200">{formatAssumption(item.value, item.key)}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

export function TickerValuationSkeleton() {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        {Array.from({ length: 5 }).map((_, index) => (
          <div key={index} className={`${cardSurface} p-4`}>
            <SkeletonBlock className="h-3 w-24" />
            <SkeletonBlock className="mt-3 h-7 w-20" />
            <SkeletonBlock className="mt-2 h-3 w-28" />
          </div>
        ))}
      </div>
      <div className={`${cardSurface} p-5`}>
        <SkeletonBlock className="h-3 w-36" />
        <SkeletonBlock className="mt-8 h-24 w-full" />
      </div>
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.4fr)_minmax(280px,0.8fr)]">
        <div className={`${cardSurface} p-5`}>
          <SkeletonBlock className="h-3 w-36" />
          <SkeletonBlock className="mt-6 h-56 w-full" />
        </div>
        <div className={`${cardSurface} p-5`}>
          <SkeletonBlock className="h-3 w-24" />
          <SkeletonBlock className="mt-4 h-40 w-full" />
        </div>
      </div>
    </div>
  );
}

export function TickerValuationTab({ data, symbol, canViewDetails = false }: Props) {
  const dcf = data.dcf ?? {};
  const consensus = data.consensus ?? null;
  const hasAnyValuation = asNumber(dcf.fairValue) !== null || asNumber(dcf.currentPrice) !== null || asNumber(consensus?.targetConsensus) !== null;

  if (data.status === "unavailable" && !hasAnyValuation) {
    return (
      <section className={`${cardSurface} p-5`}>
        <p className="text-sm font-semibold text-white">Valuation unavailable</p>
        <p className="mt-2 text-sm leading-6 text-slate-400">{data.message ?? "Valuation inputs are not available for this ticker yet."}</p>
      </section>
    );
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <SummaryCard
          label={canViewDetails ? "Walnut Fair Value" : "Street Consensus"}
          value={formatMoney(canViewDetails ? dcf.fairValue : consensus?.targetConsensus, { maximumFractionDigits: 0 })}
          sub={canViewDetails ? "/ share" : consensus?.status === "ok" ? "analyst target" : "unavailable"}
          tone={canViewDetails ? "text-teal-200" : "text-sky-200"}
        />
        {canViewDetails ? (
          <>
            <SummaryCard
              label="Upside / Downside"
              value={formatPercent(dcf.upsideDownsidePct, { signed: true })}
              sub="vs current price"
              tone={(dcf.upsideDownsidePct ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300"}
            />
            <SummaryCard
              label="Street Consensus"
              value={formatMoney(consensus?.targetConsensus, { maximumFractionDigits: 0 })}
              sub={consensus?.status === "ok" ? "analyst target" : "unavailable"}
              tone="text-sky-200"
            />
            <SummaryCard label="Valuation Judgment" value={dcf.judgment ?? "Unavailable"} sub={symbol.toUpperCase()} tone={toneForJudgment(dcf.judgment)} />
            <SummaryCard label="Method" value={dcf.method ?? "Discounted Cash Flow"} sub="fair value model" tone="text-slate-100" />
          </>
        ) : (
          <>
            <ProBlur><SummaryCard label="Upside / Downside" value={formatPercent(dcf.upsideDownsidePct, { signed: true })} sub="vs current price" tone={(dcf.upsideDownsidePct ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300"} /></ProBlur>
            <ProBlur><SummaryCard label="Walnut Fair Value" value={formatMoney(dcf.fairValue, { maximumFractionDigits: 0 })} sub="/ share" tone="text-teal-200" /></ProBlur>
            <ProBlur><SummaryCard label="Valuation Judgment" value={dcf.judgment ?? "Unavailable"} sub={symbol.toUpperCase()} tone={toneForJudgment(dcf.judgment)} /></ProBlur>
            <ProBlur><SummaryCard label="Method" value={dcf.method ?? "Discounted Cash Flow"} sub="fair value model" tone="text-slate-100" /></ProBlur>
          </>
        )}
      </div>

      {canViewDetails ? <ValuationRange data={data} /> : <ProBlur><ValuationRange data={data} /></ProBlur>}

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(280px,0.75fr)]">
        {canViewDetails ? (
          <>
            <CashFlowChart data={data} />
            <div className="grid gap-4">
              <Assumptions data={data} />
              <MethodSignals data={data} />
            </div>
          </>
        ) : (
          <>
            <ProBlur><CashFlowChart data={data} /></ProBlur>
            <ProBlur>
              <div className="grid gap-4">
                <Assumptions data={data} />
                <MethodSignals data={data} />
              </div>
            </ProBlur>
          </>
        )}
      </div>

      <section className={`${cardSurface} p-4`}>
        <p className={`text-xs leading-5 ${muted}`}>
          Valuation is model-based and depends on assumptions. Street consensus is third-party analyst target data and is shown only as a comparison point. Not investment advice.
        </p>
      </section>
    </div>
  );
}
