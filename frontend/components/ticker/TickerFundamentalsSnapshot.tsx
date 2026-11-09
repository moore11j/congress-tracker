"use client";

import type { TickerFundamentalsSummary } from "@/lib/api";
import { formatDateShort } from "@/lib/format";

export function TickerFundamentalsSnapshot({ summary }: { summary: TickerFundamentalsSummary }) {
  const metrics = summary?.metrics ?? {};
  const sourceLabel = summary?.context?.source_label?.trim() || summary?.context?.provider?.trim().toUpperCase();
  const reportedCash = summary?.context?.free_cash_flow_display;
  const useReportedCash = typeof reportedCash?.value === "number" && Number.isFinite(reportedCash.value)
    && /^[A-Z]{3}$/.test(reportedCash.currency) && reportedCash.period_basis === "TTM"
    && reportedCash.period_date === summary?.as_of;
  const cash = useReportedCash ? reportedCash.value : summary?.context?.free_cash_flow;
  const hasCash = typeof cash === "number" && Number.isFinite(cash);
  const cashCurrency = useReportedCash ? reportedCash.currency : "USD";
  const cashDisplay = hasCash ? new Intl.NumberFormat("en-US", { style: "currency", currency: cashCurrency, currencyDisplay: useReportedCash ? "code" : "symbol", notation: "compact", maximumFractionDigits: 1 }).format(cash) : "—";
  const items = [
    { label: "Revenue growth", value: metrics.revenue_growth?.display ?? "—", note: "Reported growth", tone: metrics.revenue_growth?.state },
    { label: "Cash generation", value: cashDisplay, note: useReportedCash ? `Free cash flow · TTM · ${cashCurrency}` : "Free cash flow · latest available", tone: hasCash ? cash > 0 ? "bullish" : cash < 0 ? "bearish" : "neutral" : "unavailable" },
    { label: "Leverage", value: metrics.net_debt_to_ebitda?.display ?? "—", note: "Net debt / EBITDA", tone: metrics.net_debt_to_ebitda?.state },
    { label: "Valuation", value: metrics.ev_to_ebitda?.display ?? "—", note: "EV / EBITDA · no peer comparison", tone: "neutral" },
  ];
  return <section aria-label="Fundamentals at a glance" className="mt-4 rounded-xl border border-white/10 bg-slate-950/30 p-4">
    <div className="flex flex-wrap items-center justify-between gap-2">
      <h3 className="text-sm font-semibold text-white">Fundamentals at a glance</h3>
      <button type="button" onClick={() => { window.location.hash = "financials"; window.dispatchEvent(new HashChangeEvent("hashchange")); }} className="rounded text-xs font-semibold text-sky-300 hover:text-sky-200 focus-visible:outline focus-visible:outline-2">Explore financials →</button>
    </div>
    <dl className="mt-3 grid grid-cols-2 gap-3 2xl:grid-cols-4">
      {items.map((item) => <div key={item.label} className="min-w-0 rounded-lg border border-white/[0.06] bg-white/[0.025] p-3">
        <dt className="text-xs font-medium text-slate-400">{item.label}</dt>
        <dd className={`mt-1.5 text-xl font-semibold tabular-nums ${item.tone === "bullish" ? "text-emerald-300" : item.tone === "bearish" ? "text-rose-300" : "text-slate-100"}`}>{item.value}</dd>
        <p className="mt-1 text-[11px] leading-4 text-slate-500">{item.note}</p>
      </div>)}
    </dl>
    {useReportedCash ? <p className="mt-2 text-[11px] leading-5 text-slate-400">{reportedCash.basis}. Reported in {cashCurrency}; no currency conversion.</p> : null}
    <p className="mt-3 text-[11px] leading-5 text-slate-400">
      {sourceLabel ? `Source: ${sourceLabel} · ` : "Source unavailable · "}
      {summary?.as_of ? `Period ${formatDateShort(summary.as_of)}` : "Reporting period unavailable"}
      {summary?.updated_at ? ` · Retrieved ${formatDateShort(summary.updated_at)}` : ""}
      {summary?.data_state === "stale" ? " · Stale data" : ""} · Missing values shown as —.
    </p>
    {summary?.context?.source_note ? <p className="text-[11px] leading-5 text-slate-400">{summary.context.source_note}</p> : null}
  </section>;
}
