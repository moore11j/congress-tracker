import type { TickerValuationResponse } from "@/lib/api";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import type { ReactNode } from "react";
import Link from "next/link";

type Props = {
  data: TickerValuationResponse;
  symbol: string;
  canViewDetails?: boolean;
};

type RangeMarker = {
  key: string;
  label: string;
  value: number | null;
  className: string;
  shape: "circle" | "square" | "triangle";
  priority?: boolean;
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

function formatAssumption(value: number | null | undefined, key?: string): string {
  const numeric = asNumber(value);
  if (numeric === null) return "-";
  if ((key ?? "").toLowerCase() === "beta") {
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(numeric);
  }
  if (key && ratioAssumptionKeys.has(key)) {
    return formatPercent(numeric, { ratio: Math.abs(numeric) <= 5 });
  }
  if (Math.abs(numeric) <= 100) return formatPercent(numeric);
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(numeric);
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
  if (normalized.includes("bear") || normalized.includes("not cheap") || normalized.includes("expensive")) return "text-rose-300";
  if (normalized.includes("elevated") || normalized.includes("negative")) return "text-rose-300";
  if (normalized.includes("bull") || normalized.includes("cheap") || normalized.includes("expanding") || normalized.includes("healthy") || normalized.includes("low")) return "text-emerald-300";
  if (normalized.includes("mixed") || normalized.includes("moderate") || normalized.includes("developing")) return "text-amber-300";
  return "text-slate-300";
}

function markerPosition(value: number, min: number, max: number): number {
  if (max <= min) return 50;
  return Math.min(100, Math.max(0, ((value - min) / (max - min)) * 100));
}

function assumptionValue(data: TickerValuationResponse, key: string): number | null {
  return asNumber((data.dcf.assumptions ?? []).find((item) => item.key === key)?.value);
}

function compactMethodLabel(value: string): string {
  const normalized = value.toLowerCase();
  if (normalized.includes("discounted") || normalized === "dcf") return "DCF";
  if (normalized.includes("multiple")) return "Multiples";
  if (normalized.includes("asset") || normalized.includes("nav")) return "Asset / NAV";
  return value;
}

function isStreetComparisonMethod(value: string | null | undefined): boolean {
  const normalized = (value ?? "").toLowerCase();
  return normalized.includes("street") || normalized.includes("consensus") || normalized.includes("analyst");
}

function methodPills(data: TickerValuationResponse): string[] {
  const methods = (data.dcf.methodSignals ?? [])
    .filter((item) => !isStreetComparisonMethod(item.method))
    .map((item) => compactMethodLabel(item.method))
    .filter(Boolean);
  const currentMethod = data.dcf.method ? compactMethodLabel(data.dcf.method) : null;
  const ordered = [currentMethod, ...methods, "DCF", "Multiples", "Asset / NAV"].filter((item): item is string => Boolean(item));
  return Array.from(new Set(ordered)).slice(0, 3);
}

function verdictCopy(upside: number | null, fallback: string | null | undefined): { title: string; subtitle: string; tone: string } {
  if (upside === null) {
    const title = fallback ?? "Unavailable";
    return { title, subtitle: "Valuation confidence unavailable", tone: toneForJudgment(title) };
  }
  const magnitude = Math.abs(upside);
  if (magnitude < 5) {
    return { title: "Fairly valued", subtitle: "Near fair value", tone: "text-slate-200" };
  }
  if (upside > 0) {
    const subtitle = magnitude >= 50 ? "Deeply undervalued" : magnitude >= 25 ? "Highly undervalued" : "Moderately undervalued";
    return { title: "Still cheap", subtitle, tone: "text-emerald-300" };
  }
  const subtitle = magnitude >= 50 ? "Highly overvalued" : magnitude >= 15 ? "Moderately overvalued" : "Slightly overvalued";
  return { title: "Still expensive", subtitle, tone: "text-rose-300" };
}

function marginPathLabel(value: number | null): string {
  if (value === null) return "Unavailable";
  if (value >= 0.45) return "Expanding";
  if (value >= 0.20) return "Healthy";
  if (value > 0) return "Developing";
  return "Negative";
}

function dilutionRiskLabel(data: TickerValuationResponse): string {
  const operatingCashFlow = assumptionValue(data, "operatingCashFlowPct");
  const capex = assumptionValue(data, "capitalExpenditurePct");
  const cash = assumptionValue(data, "cashAndShortTermInvestmentsPct");
  if ((operatingCashFlow ?? 0) <= 0) return "Elevated";
  if ((cash ?? 0) >= 0.25 && (operatingCashFlow ?? 0) > (capex ?? 0)) return "Low";
  return "Moderate";
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

function SectionLabel({ children }: { children: ReactNode }) {
  return <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-400">{children}</p>;
}

function InlineInfoIcon() {
  return (
    <span className="ml-1 inline-grid h-4 w-4 place-items-center rounded-full border border-slate-500/60 text-[10px] font-semibold text-slate-500">
      i
    </span>
  );
}

function ValuationRange({ data, compact = false }: { data: TickerValuationResponse; compact?: boolean }) {
  const dcf = data.dcf ?? {};
  const consensus = data.consensus ?? null;
  const markers: RangeMarker[] = [
    { key: "bear", label: "Bear", value: asNumber(dcf.bearValue), className: "bg-rose-300", shape: "circle" },
    { key: "base", label: "Base", value: asNumber(dcf.fairValue), className: "bg-teal-300", shape: "circle", priority: true },
    { key: "bull", label: "Bull", value: asNumber(dcf.bullValue), className: "bg-emerald-300", shape: "circle" },
    { key: "current", label: "Current price", value: asNumber(dcf.currentPrice), className: "bg-white", shape: "triangle", priority: true },
    { key: "consensus", label: "Analyst consensus", value: asNumber(consensus?.targetConsensus), className: "bg-sky-300", shape: "square" },
  ];
  const values = markers.map((marker) => marker.value).filter((value): value is number => value !== null);
  if (!values.length) {
    return <p className="text-sm text-slate-400">Valuation range inputs are not available for this ticker yet.</p>;
  }

  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const padding = Math.max((rawMax - rawMin) * 0.12, Math.abs(rawMax) * 0.04, 1);
  const min = rawMin - padding;
  const max = rawMax + padding;

  return (
    <div className={compact ? "" : `${cardSurface} p-5`}>
      {!compact ? (
        <div className="mb-8">
          <SectionLabel>Valuation Range</SectionLabel>
          <p className="mt-2 text-sm text-slate-400">Fair value, scenario sensitivity, current price, and street consensus.</p>
        </div>
      ) : null}
      <div className="flex items-center justify-between gap-4">
        <p className="text-sm text-slate-300">
          Valuation range (USD / share)
          <InlineInfoIcon />
        </p>
      </div>
      <div className="mt-8 px-8 pb-14 pt-8">
        <div className="relative h-1 rounded-full bg-slate-300/80">
          <div className="absolute inset-y-0 left-0 rounded-full bg-gradient-to-r from-rose-300 via-teal-300 to-emerald-300" style={{ width: "100%" }} />
          {markers.map((marker) => {
            if (marker.value === null) return null;
            const left = markerPosition(marker.value, min, max);
            const isScenario = marker.key === "bear" || marker.key === "base" || marker.key === "bull";
            const scenarioTone = marker.key === "bear" ? "text-rose-300" : marker.key === "bull" ? "text-emerald-300" : "text-teal-300";
            return (
              <div key={marker.key} className="absolute top-1/2" style={{ left: `${left}%` }}>
                {marker.shape === "triangle" ? (
                  <div className="absolute left-0 top-3 -translate-x-1/2">
                    <div className="mx-auto h-0 w-0 border-x-[6px] border-b-[10px] border-x-transparent border-b-white" />
                  </div>
                ) : (
                  <div
                    className={`absolute left-0 top-0 -translate-x-1/2 -translate-y-1/2 border border-slate-950 shadow-[0_0_18px_rgba(45,212,191,0.22)] ${marker.className} ${
                      marker.shape === "square" ? "h-3 w-3 rounded-[3px]" : "h-3.5 w-3.5 rounded-full"
                    }`}
                  />
                )}
                {isScenario ? (
                  <>
                    <div className={`absolute left-0 -top-8 -translate-x-1/2 whitespace-nowrap text-center text-sm font-semibold tabular-nums ${scenarioTone}`}>
                      {formatMoney(marker.value, { maximumFractionDigits: 0 }).replace("$", "")}
                    </div>
                    <div className="absolute left-0 top-4 -translate-x-1/2 whitespace-nowrap text-center">
                      <p className={`text-[11px] font-semibold ${marker.key === "bear" ? "text-rose-200" : "text-teal-200"}`}>{marker.label}</p>
                    </div>
                  </>
                ) : marker.key === "consensus" ? (
                  <div className="absolute left-0 top-3 -translate-x-1/2 whitespace-nowrap text-center">
                    <p className="text-[10px] font-semibold text-sky-200">Analyst consensus</p>
                    <p className="mt-0.5 text-[10px] tabular-nums text-slate-500">{formatMoney(marker.value, { maximumFractionDigits: 0 })}</p>
                  </div>
                ) : (
                  <div className="absolute left-0 top-8 -translate-x-1/2 whitespace-nowrap text-center">
                    <p className="text-[11px] font-semibold text-white">{marker.label}</p>
                    <p className="mt-1 text-[11px] tabular-nums text-slate-500">{formatMoney(marker.value, { maximumFractionDigits: 0 })}</p>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
      {!compact ? (
        <div className="grid grid-cols-3 text-xs text-slate-500">
          <span>{formatMoney(min, { maximumFractionDigits: 0 })}</span>
          <span className="text-center">Base {formatMoney(dcf.fairValue, { maximumFractionDigits: 0 })}</span>
          <span className="text-right">{formatMoney(max, { maximumFractionDigits: 0 })}</span>
        </div>
      ) : null}
    </div>
  );
}

function ValuationOverview({ data }: { data: TickerValuationResponse }) {
  const dcf = data.dcf ?? {};
  const currentPrice = asNumber(dcf.currentPrice);
  const fairValue = asNumber(dcf.fairValue);
  const upside = asNumber(dcf.upsideDownsidePct);
  const methods = methodPills(data);
  const verdict = verdictCopy(upside, dcf.judgment);

  return (
    <section className={`${cardSurface} p-5`}>
      <SectionLabel>Valuation Overview</SectionLabel>
      <div className="mt-5 grid gap-5 xl:grid-cols-[minmax(220px,0.95fr)_minmax(0,2.95fr)]">
        <div>
          <p className="text-lg font-semibold text-slate-100">
            Fair Value
            <InlineInfoIcon />
          </p>
          <div className="mt-3 flex items-end gap-2">
            <span className="text-5xl font-semibold leading-none tracking-normal text-teal-200 tabular-nums">{formatMoney(fairValue, { maximumFractionDigits: 0 })}</span>
            <span className="pb-1 text-lg font-semibold text-teal-200/75">/ share</span>
          </div>
          <div className="mt-7 flex flex-wrap items-center gap-2">
            <span className="mr-2 text-sm font-semibold text-slate-400">Methods</span>
            {methods.map((method) => (
              <span key={method} className="rounded-md border border-white/15 bg-slate-900/80 px-4 py-2 text-xs font-semibold text-slate-100 shadow-inner shadow-white/5">
                {method}
              </span>
            ))}
          </div>
        </div>

        <div className="grid gap-5 xl:grid-cols-[0.8fr_0.82fr_1.02fr_2.2fr]">
          <div className="border-white/10 xl:border-l xl:pl-6">
            <p className="text-sm text-slate-400">Current Price</p>
            <p className="mt-4 text-2xl font-semibold tabular-nums text-slate-100">{formatMoney(currentPrice, { maximumFractionDigits: 2 })}</p>
          </div>
          <div className="border-white/10 xl:border-l xl:pl-6">
            <p className="text-sm text-slate-400">vs Fair Value</p>
            <p className={`mt-4 text-2xl font-semibold tabular-nums ${(upside ?? 0) >= 0 ? "text-emerald-300" : "text-rose-300"}`}>{formatPercent(upside, { signed: true })}</p>
            <p className="mt-2 text-sm text-slate-400">vs fair value</p>
          </div>
          <div className="border-white/10 xl:border-l xl:pl-6">
            <p className="text-sm text-slate-400">Verdict</p>
            <p className={`mt-4 text-2xl font-semibold ${verdict.tone}`}>{verdict.title}</p>
            <p className="mt-2 text-sm text-slate-400">{verdict.subtitle}</p>
          </div>
          <div className="border-white/10 xl:border-l xl:pl-6">
            <ValuationRange data={data} compact />
          </div>
        </div>
      </div>
    </section>
  );
}

function CashFlowChart({ data }: { data: TickerValuationResponse }) {
  const points = (data.dcf.cashFlows ?? []).filter((point) => asNumber(point.actualCashFlow) !== null || asNumber(point.discountedCashFlow) !== null);
  if (!points.length) {
    return (
      <section className={`${cardSurface} p-5`}>
        <SectionLabel>Cash Flow Bridge</SectionLabel>
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
      <div className="flex items-start justify-between gap-3">
        <div>
          <SectionLabel>Cash Flow Bridge</SectionLabel>
          <p className="mt-2 text-sm text-slate-400">Projected free cash flows compared with discounted cash flows by projection year.</p>
        </div>
        <div className="flex shrink-0 gap-3 text-[11px] text-slate-400">
          <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-sm bg-teal-300" />Projected FCF</span>
          <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-sm bg-sky-300" />Discounted CF</span>
        </div>
      </div>
      <div className="mt-5 grid min-h-[150px] grid-cols-[auto_minmax(0,1fr)] gap-3">
        <div className="flex flex-col justify-between py-2 text-[10px] tabular-nums text-slate-500">
          <span>{formatMoney(maxAbs, { compact: true, maximumFractionDigits: 1 })}</span>
          <span>$0</span>
        </div>
        <div className="relative overflow-hidden rounded-lg border border-white/10 bg-slate-950/70 px-3 pb-7 pt-3">
          <div className="absolute inset-x-3 bottom-7 border-t border-white/10" />
          <div className="relative z-10 grid h-28 items-end gap-2" style={{ gridTemplateColumns: `repeat(${points.length}, minmax(0, 1fr))` }}>
            {points.map((point, index) => {
              const actual = asNumber(point.actualCashFlow);
              const discounted = asNumber(point.discountedCashFlow);
              const actualHeight = `${Math.max(4, ((Math.abs(actual ?? 0) / maxAbs) * 100))}%`;
              const discountedHeight = `${Math.max(4, ((Math.abs(discounted ?? 0) / maxAbs) * 100))}%`;
              const tooltipPosition = index === 0 ? "left-0" : index === points.length - 1 ? "right-0" : "left-1/2 -translate-x-1/2";
              return (
                <div key={point.year} className="group relative flex h-full min-w-0 items-end justify-center gap-1">
                  <div className={`pointer-events-none absolute top-1 z-20 hidden min-w-44 rounded-md border border-white/10 bg-slate-950/95 px-3 py-2 text-left text-[11px] shadow-2xl shadow-slate-950/50 group-hover:block ${tooltipPosition}`}>
                    <p className="font-semibold text-slate-100">{point.year}</p>
                    <div className="mt-1.5 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 tabular-nums">
                      <span className="text-slate-400">Projected FCF</span>
                      <span className="text-right font-semibold text-teal-200">{formatMoney(actual)}</span>
                      <span className="text-slate-400">Discounted CF</span>
                      <span className="text-right font-semibold text-sky-200">{formatMoney(discounted)}</span>
                    </div>
                  </div>
                  <div className="w-full max-w-4 rounded-t bg-teal-300/85 shadow-[0_0_14px_rgba(45,212,191,0.22)]" style={{ height: actual === null ? 0 : actualHeight }} />
                  <div className="w-full max-w-4 rounded-t bg-sky-300/85 shadow-[0_0_14px_rgba(125,211,252,0.18)]" style={{ height: discounted === null ? 0 : discountedHeight }} />
                </div>
              );
            })}
          </div>
          <div className="absolute inset-x-3 bottom-2 grid gap-2 text-center text-[10px] text-slate-500" style={{ gridTemplateColumns: `repeat(${points.length}, minmax(0, 1fr))` }}>
            {points.map((point) => <span key={point.year} className="truncate">{point.year}</span>)}
          </div>
        </div>
      </div>
    </section>
  );
}

function KeyInputs({ data }: { data: TickerValuationResponse }) {
  const revenueGrowth = assumptionValue(data, "revenueGrowthPct");
  const discountRate = assumptionValue(data, "costOfEquity");
  const ebitdaMargin = assumptionValue(data, "ebitdaPct");
  const dilutionRisk = dilutionRiskLabel(data);
  const items = [
    { label: "Revenue growth (5Y CAGR)", value: formatAssumption(revenueGrowth, "revenueGrowthPct"), tone: "text-teal-200", icon: "trend" },
    { label: "Discount rate (WACC)", value: formatAssumption(discountRate, "costOfEquity"), tone: "text-slate-100", icon: "shield" },
    { label: "Margin path (EBITDA)", value: marginPathLabel(ebitdaMargin), tone: signalTone(marginPathLabel(ebitdaMargin)), icon: "briefcase" },
    { label: "Dilution risk", value: dilutionRisk, tone: signalTone(dilutionRisk), icon: "drop" },
  ];

  return (
    <section className={`${cardSurface} p-5`}>
      <SectionLabel>
        Key Inputs
        <InlineInfoIcon />
      </SectionLabel>
      <div className="mt-4 divide-y divide-white/10 rounded-lg border border-white/10 bg-slate-950/45">
        {items.map((item) => (
          <div key={item.label} className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-3 px-3 py-3 text-sm">
            <MiniIcon name={item.icon} />
            <span className="min-w-0 truncate font-medium text-slate-200">{item.label}</span>
            <span className={`font-semibold tabular-nums ${item.tone}`}>{item.value}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function MethodSignals({ data }: { data: TickerValuationResponse }) {
  const visibleMethodSignals = (data.dcf.methodSignals ?? []).filter((item) => !isStreetComparisonMethod(item.method));
  const signals = visibleMethodSignals.length
    ? visibleMethodSignals
    : [
        { method: "DCF", signal: data.dcf.dcfValue ? "Active" : "Unavailable" },
        { method: "Multiples", signal: data.dcf.multiplesValue ? "Active" : "Unavailable" },
        { method: "Final valuation", signal: data.dcf.judgment ?? "Unavailable" },
      ];
  return (
    <section className={`${cardSurface} p-5`}>
      <SectionLabel>
        Method Signals
        <InlineInfoIcon />
      </SectionLabel>
      <div className="mt-4 grid gap-2">
        {signals.slice(0, 4).map((item) => (
          <div key={`${item.method}-${item.signal}`} className="grid grid-cols-[minmax(0,1fr)_auto] gap-3 rounded-lg border border-white/10 bg-slate-950/70 px-3 py-2.5 text-sm">
            <span className="font-semibold text-slate-100">{compactMethodLabel(item.method)}</span>
            <span className={`font-semibold ${signalTone(item.signal)}`}>{item.signal}</span>
          </div>
        ))}
      </div>
    </section>
  );
}

function MiniIcon({ name }: { name: string }) {
  const common = "h-4 w-4 text-slate-300";
  if (name === "trend") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M4 16l5-5 4 4 7-8" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M15 7h5v5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    );
  }
  if (name === "shield") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M12 3l7 3v5c0 4.4-2.8 7.9-7 10-4.2-2.1-7-5.6-7-10V6l7-3z" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" />
        <path d="M12 8v5l3 2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
      </svg>
    );
  }
  if (name === "briefcase") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M9 7V5h6v2" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        <path d="M5 8h14v10H5z" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" />
        <path d="M5 12h14" stroke="currentColor" strokeWidth="2" />
      </svg>
    );
  }
  if (name === "document") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M7 3h7l4 4v14H7z" stroke="currentColor" strokeWidth="2" strokeLinejoin="round" />
        <path d="M14 3v5h5M9 13h6M9 17h6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
      </svg>
    );
  }
  if (name === "bars") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M5 20V9M12 20V4M19 20v-7" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
        <path d="M3 20h18" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
      </svg>
    );
  }
  if (name === "coin") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <circle cx="12" cy="12" r="8" stroke="currentColor" strokeWidth="2" />
        <path d="M12 7v10M15 9.5c-.7-.8-1.7-1.2-3-1.2-1.5 0-2.5.7-2.5 1.8 0 1.2 1 1.7 2.7 2 1.7.3 2.8.8 2.8 2.1 0 1.1-1.1 1.9-2.8 1.9-1.4 0-2.5-.4-3.3-1.3" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" />
      </svg>
    );
  }
  if (name === "users") {
    return (
      <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M9 11a3 3 0 100-6 3 3 0 000 6zM15 10a2.5 2.5 0 100-5 2.5 2.5 0 000 5zM3.5 20c.8-3.3 2.6-5 5.5-5s4.7 1.7 5.5 5M13.5 15.5c2.8.2 4.5 1.7 5 4.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
      </svg>
    );
  }
  return (
    <svg className={common} viewBox="0 0 24 24" fill="none" aria-hidden="true">
      <path d="M12 4v16M8 8l4-4 4 4M8 16l4 4 4-4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function WhatToWatchNext() {
  const items = [
    { icon: "document", title: "Guidance", body: "Watch upcoming management outlook and margin commentary." },
    { icon: "bars", title: "Backlog / demand", body: "Monitor order backlog trends and customer cadence." },
    { icon: "coin", title: "Free cash flow", body: "Track FCF conversion and capex intensity." },
    { icon: "users", title: "Share count / dilution", body: "Watch equity issuance and employee stock activity." },
  ];

  return (
    <section className={`${cardSurface} p-5`}>
      <SectionLabel>What To Watch Next</SectionLabel>
      <div className="mt-5 grid gap-5 md:grid-cols-2 xl:grid-cols-4">
        {items.map((item, index) => (
          <div key={item.title} className={`grid grid-cols-[auto_minmax(0,1fr)] gap-4 ${index > 0 ? "xl:border-l xl:border-white/10 xl:pl-5" : ""}`}>
            <div className="mt-1 grid h-9 w-9 place-items-center rounded-md text-slate-300">
              <MiniIcon name={item.icon} />
            </div>
            <div>
              <p className="font-semibold text-slate-100">{item.title}</p>
              <p className="mt-1 text-sm leading-5 text-slate-400">{item.body}</p>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

export function TickerValuationSkeleton() {
  return (
    <div className="space-y-4">
      <div className={`${cardSurface} p-5`}>
        <SkeletonBlock className="h-3 w-36" />
        <div className="mt-6 grid gap-5 xl:grid-cols-[minmax(220px,0.95fr)_minmax(0,2.95fr)]">
          <SkeletonBlock className="h-28 w-full" />
          <SkeletonBlock className="h-28 w-full" />
        </div>
      </div>
      <div className="grid gap-4 xl:grid-cols-3">
        {Array.from({ length: 3 }).map((_, index) => (
          <div key={index} className={`${cardSurface} p-5`}>
            <SkeletonBlock className="h-3 w-32" />
            <SkeletonBlock className="mt-5 h-36 w-full" />
          </div>
        ))}
      </div>
      <div className={`${cardSurface} p-5`}>
        <SkeletonBlock className="h-3 w-40" />
        <SkeletonBlock className="mt-5 h-20 w-full" />
      </div>
    </div>
  );
}

export function TickerValuationTab({ data, canViewDetails = false }: Props) {
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

  const detailRows = (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(280px,0.86fr)_minmax(280px,0.86fr)]">
      <CashFlowChart data={data} />
      <KeyInputs data={data} />
      <MethodSignals data={data} />
    </div>
  );

  return (
    <div className="space-y-4">
      <ValuationOverview data={data} />
      {canViewDetails ? detailRows : <ProBlur>{detailRows}</ProBlur>}
      {canViewDetails ? <WhatToWatchNext /> : <ProBlur><WhatToWatchNext /></ProBlur>}
      <p className={`py-1 text-center text-xs italic leading-5 ${muted}`}>
        Illustrative valuation model. Research view, not investment advice.
      </p>
    </div>
  );
}
