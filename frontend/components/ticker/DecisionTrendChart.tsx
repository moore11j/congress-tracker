"use client";

import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type PointerEvent } from "react";
import { formatDateShort } from "@/lib/format";

type DecisionTrendPoint = { date: string; score: number };
type RenderedPoint = DecisionTrendPoint & { x: number; y: number };

const width = 720;
const height = 238;
const padding = { top: 18, right: 24, bottom: 38, left: 46 };

function clamp(value: number, min: number, max: number): number { return Math.max(min, Math.min(max, value)); }
function scoreBand(score: number): string { if (score <= 19) return "Inactive"; if (score <= 39) return "Weak"; if (score <= 59) return "Moderate"; if (score <= 79) return "Strong"; return "Very strong"; }
function confirmationLabel(score: number, direction?: string | null): string { const normalized = (direction ?? "").toLowerCase(); if (score <= 19 && normalized === "neutral") return "Inactive"; if (normalized === "mixed") return "Conflicted confirmation"; if (normalized === "neutral" || !normalized) return "No clear direction"; return `${scoreBand(score)} ${normalized}`; }
function formatAxisDate(value: string): string { const formatted = formatDateShort(value); return formatted ? formatted.replace(/, \d{4}$/, "") : value; }
function chartTheme(direction?: string | null) { if (direction === "bearish") return { stroke: "#fb7185", areaTop: "rgba(251,113,133,.34)", areaBottom: "rgba(251,113,133,0)" }; if (direction === "mixed") return { stroke: "#fbbf24", areaTop: "rgba(251,191,36,.30)", areaBottom: "rgba(251,191,36,0)" }; return { stroke: "#6ee7b7", areaTop: "rgba(74,222,128,.35)", areaBottom: "rgba(74,222,128,0)" }; }

export function DecisionTrendChart({ history, direction }: { history?: DecisionTrendPoint[]; direction?: string | null }) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [revealed, setRevealed] = useState(false);
  const frame = useRef<number | null>(null);
  const gradientId = useId().replace(/:/g, "");
  const points = useMemo(() => Array.isArray(history) ? history.filter((point) => Number.isFinite(point.score)).slice(-30) : [], [history]);
  const chart = useMemo(() => {
    if (points.length < 2) return null;
    const scores = points.map((point) => clamp(point.score, 0, 100));
    const rawMin = Math.min(...scores);
    const rawMax = Math.max(...scores);
    const spread = Math.max(rawMax - rawMin, 1);
    const yMin = clamp(Math.floor((rawMin - Math.max(4, spread * 0.15)) / 5) * 5, 0, 100);
    const yMax = clamp(Math.ceil((rawMax + Math.max(4, spread * 0.15)) / 5) * 5, 0, 100);
    const finalMin = yMax === yMin ? clamp(yMin - 5, 0, 95) : yMin;
    const finalMax = yMax === yMin ? clamp(yMax + 5, 5, 100) : yMax;
    const range = Math.max(finalMax - finalMin, 1);
    const innerWidth = width - padding.left - padding.right;
    const innerHeight = height - padding.top - padding.bottom;
    const rendered = points.map((point, index): RenderedPoint => {
      const score = clamp(point.score, 0, 100);
      return { ...point, x: padding.left + index * (innerWidth / Math.max(points.length - 1, 1)), y: padding.top + ((finalMax - score) / range) * innerHeight };
    });
    const xTicks = [0, Math.floor((points.length - 1) / 3), Math.floor(((points.length - 1) * 2) / 3), points.length - 1].filter((value, index, values) => values.indexOf(value) === index);
    return { rendered, yTicks: [finalMax, Math.round((finalMax + finalMin) / 2), finalMin], xTicks, yFor: (score: number) => padding.top + ((finalMax - score) / range) * innerHeight };
  }, [points]);

  useEffect(() => { const animationFrame = requestAnimationFrame(() => setRevealed(true)); return () => cancelAnimationFrame(animationFrame); }, []);
  useEffect(() => () => { if (frame.current !== null) cancelAnimationFrame(frame.current); }, []);

  if (!chart) return <div className="flex h-56 items-center justify-center rounded-lg border border-white/10 bg-slate-950/35 px-3 text-sm font-medium text-slate-500">Score history unavailable</div>;

  const active = activeIndex === null ? null : chart.rendered[activeIndex] ?? null;
  const linePoints = chart.rendered.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const areaPoints = `${linePoints} ${chart.rendered.at(-1)?.x ?? width - padding.right},${height - padding.bottom} ${chart.rendered[0]?.x ?? padding.left},${height - padding.bottom}`;
  const theme = chartTheme(direction);
  const setNearestIndex = (event: PointerEvent<SVGSVGElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    const pointerX = ((event.clientX - rect.left) / Math.max(rect.width, 1)) * width;
    const nearestIndex = chart.rendered.reduce((nearest, point, index) => Math.abs(point.x - pointerX) < Math.abs(chart.rendered[nearest].x - pointerX) ? index : nearest, 0);
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => { setActiveIndex(nearestIndex); frame.current = null; });
  };
  const inspectWithKeyboard = (event: KeyboardEvent<SVGSVGElement>) => {
    if (event.key === "Escape") { setActiveIndex(null); return; }
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    setActiveIndex((current) => clamp((current ?? chart.rendered.length - 1) + (event.key === "ArrowLeft" ? -1 : 1), 0, chart.rendered.length - 1));
  };

  return <div className="relative z-20 h-56 w-full overflow-visible">
    <svg className="h-full w-full overflow-visible outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/70" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="30-day confirmation score history. Hover, touch, or use arrow keys to inspect scores." tabIndex={0} style={{ touchAction: "pan-y" }} onKeyDown={inspectWithKeyboard} onPointerMove={setNearestIndex} onPointerDown={(event) => { if (event.pointerType !== "mouse") event.currentTarget.setPointerCapture(event.pointerId); setNearestIndex(event); }} onPointerLeave={(event) => { if (event.pointerType === "mouse") setActiveIndex(null); }}>
      <defs><linearGradient id={gradientId} x1="0" x2="0" y1="0" y2="1"><stop stopColor={theme.areaTop} /><stop offset="1" stopColor={theme.areaBottom} /></linearGradient><clipPath id={`${gradientId}-clip`}><rect x={padding.left} y={padding.top} width={width - padding.left - padding.right} height={height - padding.top - padding.bottom} /></clipPath></defs>
      <line x1={padding.left} y1={height - padding.bottom} x2={width - padding.right} y2={height - padding.bottom} className="stroke-white/20" />
      {chart.yTicks.map((tick) => { const y = chart.yFor(tick); return <g key={tick}><line x1={padding.left} y1={y} x2={width - padding.right} y2={y} className="stroke-white/10" strokeDasharray="3 5" /><text x={padding.left - 9} y={y + 4} textAnchor="end" className="fill-slate-400 tabular-nums" fontSize="12">{tick}</text></g>; })}
      {chart.xTicks.map((index) => { const point = chart.rendered[index]; const anchor = index === 0 ? "start" : index === points.length - 1 ? "end" : "middle"; return <text key={`${point.date}-${index}`} x={point.x} y={height - 12} textAnchor={anchor} className="fill-slate-400" fontSize="12">{formatAxisDate(point.date)}</text>; })}
      <g clipPath={`url(#${gradientId}-clip)`} style={{ clipPath: revealed ? "inset(0 0 0 0)" : "inset(0 100% 0 0)", transition: "clip-path 560ms cubic-bezier(.22,1,.36,1)" }}><polygon points={areaPoints} fill={`url(#${gradientId})`} /><polyline points={linePoints} fill="none" stroke={theme.stroke} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" /></g>
      {active ? <><line x1={active.x} y1={padding.top} x2={active.x} y2={height - padding.bottom} className="stroke-white/35" strokeDasharray="3 4" /><circle cx={active.x} cy={active.y} r="5" fill={theme.stroke} stroke="#020617" strokeWidth="2" /></> : null}
    </svg>
    {active ? <div role="status" className="pointer-events-none absolute z-50 min-w-44 rounded-lg border border-emerald-300/25 bg-slate-950/95 px-3 py-2.5 text-xs shadow-2xl shadow-black/50 ring-1 ring-emerald-300/10 backdrop-blur" style={{ left: `${clamp((active.x / width) * 100, 14, 86)}%`, top: "0.4rem", transform: "translateX(-50%)" }}><p className="font-semibold text-slate-100">{formatDateShort(active.date) ?? active.date}</p><p className="mt-1 tabular-nums text-emerald-200">Score {Math.round(active.score)} / 100</p><p className="mt-1 font-medium text-slate-400">{confirmationLabel(active.score, direction)}</p></div> : null}
    <span className="sr-only">Use left and right arrow keys to inspect each day&apos;s confirmation score.</span>
  </div>;
}
