"use client";

import { useMemo } from "react";
import { WalnutLineChart } from "@/components/charts/WalnutLineChart";
import { WalnutChartContainer } from "@/components/charts/WalnutChartContainer";
import { formatChartCurrency } from "@/components/charts/chartFormatters";
import type { BacktestTimelinePoint } from "@/lib/api";

type Props = { timeline: BacktestTimelinePoint[] };

function dateLabel(value: string) {
  const parsed = new Date(value);
  return Number.isFinite(parsed.getTime()) ? parsed.toLocaleDateString("en-US", { month: "short", day: "numeric" }) : value;
}

export function BacktestChart({ timeline }: Props) {
  const chartData = useMemo(() => timeline.map((point) => ({ label: dateLabel(point.date) })), [timeline]);
  const series = useMemo(() => [
    { key: "strategy", label: "Strategy value", color: "rgba(110,231,183,0.96)", values: timeline.map((point) => point.strategy_value) },
    { key: "benchmark", label: "Benchmark value", color: "rgba(226,232,240,0.78)", dashed: true, values: timeline.map((point) => point.benchmark_value) },
  ], [timeline]);

  if (timeline.length < 2) {
    return <div className="rounded-2xl border border-white/10 bg-[#07111d] px-4 py-10 text-center text-sm text-slate-400">Not enough data points to draw a curve yet.</div>;
  }

  return (
    <div className="rounded-2xl border border-white/10 bg-[#07111d] p-3">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-3 text-xs text-slate-400">
          <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-emerald-300" />Strategy</span>
          <span className="inline-flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full bg-slate-300" />S&amp;P 500</span>
        </div>
        <span className="text-xs uppercase tracking-[0.18em] text-slate-500">Portfolio Value ($)</span>
      </div>
      <WalnutChartContainer label="Strategy versus S&P 500 performance chart" heightClassName="h-[320px]">
        <WalnutLineChart
          data={chartData}
          series={series}
          ariaLabel="Strategy and S&P 500 portfolio value over time"
          formatValue={(value) => formatChartCurrency(value, 0)}
          renderTooltip={(index) => {
            const point = timeline[index];
            return <><div className="text-xs uppercase tracking-[0.18em] text-slate-500">{dateLabel(point.date)}</div><div className="mt-3 space-y-2"><TooltipRow label="Strategy value" value={formatChartCurrency(point.strategy_value)} tone="text-emerald-200" /><TooltipRow label="Benchmark value" value={formatChartCurrency(point.benchmark_value)} /><TooltipRow label="Active tickers" value={String(point.active_positions)} /></div></>;
          }}
        />
      </WalnutChartContainer>
    </div>
  );
}

function TooltipRow({ label, value, tone = "text-slate-100" }: { label: string; value: string; tone?: string }) {
  return <div className="flex items-center justify-between gap-3"><span className="text-slate-400">{label}</span><span className={`font-semibold tabular-nums ${tone}`}>{value}</span></div>;
}
