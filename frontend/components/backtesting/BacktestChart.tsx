import type { BacktestTimelinePoint } from "@/lib/api";

type Props = {
  timeline: BacktestTimelinePoint[];
};

const WIDTH = 1000;
const HEIGHT = 300;
const MARGIN = { top: 16, right: 56, bottom: 30, left: 52 };

function pct(value: number) {
  return `${value.toFixed(1)}%`;
}

function dateLabel(value: string) {
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return value;
  return parsed.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function scaleBounds(values: number[]) {
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = Math.max(max - min, 1);
  const padding = spread * 0.18;
  return { min: min - padding, max: max + padding, range: Math.max(spread + padding * 2, 1) };
}

export function BacktestChart({ timeline }: Props) {
  if (timeline.length < 2) {
    return (
      <div className="rounded-2xl border border-white/10 bg-[#07111d] px-4 py-10 text-center text-sm text-slate-400">
        Not enough data points to draw a curve yet.
      </div>
    );
  }

  const strategyBase = timeline[0]?.strategy_value || 100;
  const benchmarkBase = timeline[0]?.benchmark_value || 100;
  const points = timeline.map((point, index) => ({
    index,
    date: point.date,
    strategyPct: ((point.strategy_value / strategyBase) - 1) * 100,
    benchmarkPct: ((point.benchmark_value / benchmarkBase) - 1) * 100,
  }));

  const innerWidth = WIDTH - MARGIN.left - MARGIN.right;
  const innerHeight = HEIGHT - MARGIN.top - MARGIN.bottom;
  const xStep = innerWidth / Math.max(points.length - 1, 1);
  const bounds = scaleBounds(points.flatMap((point) => [point.strategyPct, point.benchmarkPct]));
  const yFor = (value: number) => MARGIN.top + innerHeight - (((value - bounds.min) / bounds.range) * innerHeight);

  const strategyPath = points.map((point, index) => `${MARGIN.left + index * xStep},${yFor(point.strategyPct)}`).join(" ");
  const benchmarkPath = points.map((point, index) => `${MARGIN.left + index * xStep},${yFor(point.benchmarkPct)}`).join(" ");
  const yTicks = Array.from({ length: 5 }, (_, index) => {
    const ratio = index / 4;
    const value = bounds.max - ratio * bounds.range;
    return { value, y: MARGIN.top + ratio * innerHeight };
  });
  const tickIndexes = Array.from(new Set([0, Math.floor((points.length - 1) / 3), Math.floor(((points.length - 1) * 2) / 3), points.length - 1])).sort((a, b) => a - b);

  return (
    <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d] p-3">
      <div className="mb-3 flex flex-wrap items-center gap-3 text-xs text-slate-400">
        <span className="inline-flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-emerald-300" />
          Strategy
        </span>
        <span className="inline-flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-slate-300" />
          S&amp;P 500
        </span>
      </div>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="h-[300px] w-full">
        {yTicks.map((tick) => (
          <g key={`y-${tick.y}`}>
            <line x1={MARGIN.left} x2={WIDTH - MARGIN.right} y1={tick.y} y2={tick.y} stroke="rgba(148,163,184,0.12)" strokeWidth="1" />
            <text x={WIDTH - MARGIN.right + 8} y={tick.y + 4} textAnchor="start" className="fill-slate-300/55 text-[11px] tabular-nums">
              {pct(tick.value)}
            </text>
          </g>
        ))}

        {tickIndexes.map((index) => {
          const x = MARGIN.left + index * xStep;
          return (
            <g key={`x-${index}`}>
              <line x1={x} x2={x} y1={MARGIN.top} y2={HEIGHT - MARGIN.bottom} stroke="rgba(148,163,184,0.08)" strokeWidth="1" />
              <text x={x} y={HEIGHT - 10} textAnchor="middle" className="fill-slate-400 text-[11px]">
                {dateLabel(points[index]?.date ?? "")}
              </text>
            </g>
          );
        })}

        <polyline fill="none" stroke="rgba(226,232,240,0.78)" strokeDasharray="6 4" strokeWidth="2" points={benchmarkPath} />
        <polyline fill="none" stroke="rgba(110,231,183,0.96)" strokeWidth="2.8" strokeLinecap="round" strokeLinejoin="round" points={strategyPath} />

        {points.map((point, index) => (
          <circle
            key={`${point.date}-${index}`}
            cx={MARGIN.left + index * xStep}
            cy={yFor(point.strategyPct)}
            r={2.3}
            fill="rgba(167,243,208,0.9)"
            stroke="rgba(16,185,129,0.55)"
            strokeWidth="1"
          />
        ))}
      </svg>
    </div>
  );
}
