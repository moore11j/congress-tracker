"use client";

import { useMemo, useState, type MouseEvent } from "react";
import { getSvgLocalPoint } from "@/lib/chartPointer";
import { formatCurrencyRange, formatDateShort } from "@/lib/format";

export type PricePoint = {
  date: string;
  close: number;
};

// NOTE: "signals" here is a chart marker bucket, not the app-wide SignalMode type.
export type MarkerKind = "congress" | "insider" | "signals";

export type ActivityMarker = {
  id: string;
  kind: MarkerKind;
  date: string;
  label: string;
  actor: string;
  action: string;
  amountMin?: number | null;
  amountMax?: number | null;
};

const markerPalette: Record<MarkerKind, { label: string; color: string; border: string; glow: string }> = {
  congress: { label: "Congress", color: "#1d4ed8", border: "#93c5fd", glow: "rgba(59,130,246,0.5)" },
  insider: { label: "Insider", color: "#059669", border: "#6ee7b7", glow: "rgba(5,150,105,0.45)" },
  signals: { label: "Signals", color: "#7c3aed", border: "#c4b5fd", glow: "rgba(124,58,237,0.5)" },
};

const WIDTH = 920;
const HEIGHT = 256;
const PADDING = { top: 20, right: 18, bottom: 28, left: 16 };
const MARKER_HIT_RADIUS = 12;


function markerTone(action: string): "pos" | "neg" | "neutral" {
  const t = action.toLowerCase();
  if (t.includes("buy") || t.includes("strong") || t.includes("notable")) return "pos";
  if (t.includes("sell") || t.includes("weak")) return "neg";
  return "neutral";
}

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

export function TickerActivityChart({
  points,
  markers,
  symbol,
}: {
  points: PricePoint[];
  markers: ActivityMarker[];
  symbol?: string;
}) {
  const [visibleKinds, setVisibleKinds] = useState<Record<MarkerKind, boolean>>({
    congress: true,
    insider: true,
    signals: true,
  });
  const [hoveredMarkerId, setHoveredMarkerId] = useState<string | null>(null);

  const chart = useMemo(() => {
    if (points.length === 0) return null;

    const x0 = PADDING.left;
    const x1 = WIDTH - PADDING.right;
    const y0 = PADDING.top;
    const y1 = HEIGHT - PADDING.bottom;

    const minClose = Math.min(...points.map((p) => p.close));
    const maxClose = Math.max(...points.map((p) => p.close));
    const spread = Math.max(maxClose - minClose, maxClose * 0.01, 1);
    const minY = minClose - spread * 0.1;
    const maxY = maxClose + spread * 0.12;

    const toX = (idx: number) => {
      if (points.length === 1) return (x0 + x1) / 2;
      return x0 + (idx / (points.length - 1)) * (x1 - x0);
    };
    const toY = (price: number) => y1 - ((price - minY) / (maxY - minY)) * (y1 - y0);

    const pointByDate = new Map(points.map((p, idx) => [p.date, { idx, p }]));

    const path = points
      .map((p, idx) => `${idx === 0 ? "M" : "L"}${toX(idx).toFixed(2)},${toY(p.close).toFixed(2)}`)
      .join(" ");

    const grid = [0, 0.25, 0.5, 0.75, 1].map((t) => {
      const y = y0 + t * (y1 - y0);
      const price = maxY - t * (maxY - minY);
      return { y, label: `$${price.toFixed(price < 10 ? 2 : 0)}` };
    });

    const visibleMarkers = markers
      .filter((m) => visibleKinds[m.kind])
      .map((m) => {
        const match = pointByDate.get(m.date);
        if (!match) return null;
        return {
          ...m,
          x: toX(match.idx),
          y: toY(match.p.close),
          close: match.p.close,
        };
      })
      .filter((m): m is NonNullable<typeof m> => Boolean(m));

    const xTicks = [0, Math.floor((points.length - 1) / 3), Math.floor((2 * (points.length - 1)) / 3), points.length - 1]
      .filter((value, idx, arr) => arr.indexOf(value) === idx)
      .map((idx) => ({ x: toX(idx), label: formatDateShort(points[idx].date) }));

    return { path, grid, visibleMarkers, xTicks };
  }, [points, markers, visibleKinds]);

  const activeMarker = chart?.visibleMarkers.find((marker) => marker.id === hoveredMarkerId) ?? null;
  const activeMarkerXPercent = activeMarker ? (activeMarker.x / WIDTH) * 100 : null;
  const shouldFlipTooltip = activeMarkerXPercent !== null && activeMarkerXPercent > 74;

  const handleChartMove = (event: MouseEvent<SVGSVGElement>) => {
    if (!chart?.visibleMarkers.length) {
      setHoveredMarkerId(null);
      return;
    }
    const local = getSvgLocalPoint(event.currentTarget, event.clientX, event.clientY);
    if (!local) {
      setHoveredMarkerId(null);
      return;
    }

    const closest = chart.visibleMarkers.reduce<{ marker: (typeof chart.visibleMarkers)[number] | null; dist: number }>(
      (best, marker) => {
        const dist = Math.hypot(marker.x - local.x, marker.y - local.y);
        return dist < best.dist ? { marker, dist } : best;
      },
      { marker: null, dist: Number.POSITIVE_INFINITY },
    );

    setHoveredMarkerId(closest.marker && closest.dist <= MARKER_HIT_RADIUS ? closest.marker.id : null);
  };

  return (
    <section className="rounded-2xl border border-white/10 bg-gradient-to-b from-slate-900/90 to-slate-950/90 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs uppercase tracking-widest text-slate-400">Price + activity timeline</p>
        <div className="flex flex-wrap items-center gap-2">
          {(Object.keys(markerPalette) as MarkerKind[]).map((kind) => (
            <button
              key={kind}
              type="button"
              onClick={() => setVisibleKinds((prev) => ({ ...prev, [kind]: !prev[kind] }))}
              className={`inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs font-semibold transition ${
                visibleKinds[kind]
                  ? "border-white/20 bg-white/10 text-slate-100"
                  : "border-white/10 bg-slate-900/50 text-slate-500"
              }`}
            >
              <span className="h-2 w-2 rounded-full" style={{ backgroundColor: markerPalette[kind].color }} />
              {markerPalette[kind].label}
            </button>
          ))}
        </div>
      </div>

      {points.length === 0 || !chart ? (
        <p className="text-sm text-slate-400">No price history available for this window.</p>
      ) : (
        <div className="relative overflow-hidden">
          <div className="overflow-x-auto">
            <svg
              viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
              className="h-[256px] w-full min-w-[720px]"
              onMouseMove={handleChartMove}
              onMouseLeave={() => setHoveredMarkerId(null)}
            >
              <defs>
                <linearGradient id="price-line" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#67e8f9" />
                  <stop offset="100%" stopColor="#22d3ee" />
                </linearGradient>
              </defs>
              <rect x="0" y="0" width={WIDTH} height={HEIGHT} fill="transparent" />
              {chart.grid.map((row) => (
                <g key={row.y}>
                  <line x1={PADDING.left} x2={WIDTH - PADDING.right} y1={row.y} y2={row.y} stroke="rgba(148,163,184,0.18)" strokeDasharray="4 6" />
                  <text x={WIDTH - PADDING.right} y={row.y - 4} textAnchor="end" fontSize="10" fill="rgba(148,163,184,0.75)">{row.label}</text>
                </g>
              ))}
              <path d={chart.path} fill="none" stroke="url(#price-line)" strokeWidth="2.1" strokeOpacity="0.84" strokeLinejoin="round" strokeLinecap="round" />
              {chart.visibleMarkers.map((marker) => {
                const palette = markerPalette[marker.kind];
                const color = palette.color;
                const tone = markerTone(marker.action);
                const shape = marker.kind === "signals" ? "square" : tone === "neg" ? "down" : "up";
                const isActive = marker.id === activeMarker?.id;
                const triangleWidth = isActive ? 7.5 : 6;
                const triangleHeight = isActive ? 6.8 : 6;
                const triangleBaseOffset = isActive ? 5.1 : 4;
                const squareSize = isActive ? 12 : 10;
                const squareHalf = squareSize / 2;

                return (
                  <g key={marker.id} onMouseEnter={() => setHoveredMarkerId(marker.id)}>
                    {shape === "square" ? (
                      <rect
                        x={marker.x - squareHalf}
                        y={marker.y - squareHalf}
                        width={squareSize}
                        height={squareSize}
                        rx={2}
                        fill={color}
                        stroke={isActive ? "#e2e8f0" : palette.border}
                        strokeWidth={isActive ? 2.4 : 1.6}
                        style={{ filter: `drop-shadow(0 0 ${isActive ? 10 : 5}px ${isActive ? "rgba(226,232,240,0.42)" : palette.glow})` }}
                      />
                    ) : shape === "down" ? (
                      <path
                        d={`M ${marker.x} ${marker.y + triangleHeight} L ${marker.x - triangleWidth} ${marker.y - triangleBaseOffset} L ${marker.x + triangleWidth} ${marker.y - triangleBaseOffset} Z`}
                        fill={color}
                        stroke={isActive ? "#e2e8f0" : palette.border}
                        strokeWidth={isActive ? 2.4 : 1.7}
                        style={{ filter: `drop-shadow(0 0 ${isActive ? 10 : 6}px ${isActive ? "rgba(226,232,240,0.42)" : palette.glow})` }}
                      />
                    ) : (
                      <path
                        d={`M ${marker.x} ${marker.y - triangleHeight} L ${marker.x - triangleWidth} ${marker.y + triangleBaseOffset} L ${marker.x + triangleWidth} ${marker.y + triangleBaseOffset} Z`}
                        fill={color}
                        stroke={isActive ? "#e2e8f0" : palette.border}
                        strokeWidth={isActive ? 2.4 : 1.7}
                        style={{ filter: `drop-shadow(0 0 ${isActive ? 10 : 6}px ${isActive ? "rgba(226,232,240,0.42)" : palette.glow})` }}
                      />
                    )}
                  </g>
                );
              })}
              {chart.xTicks.map((tick) => (
                <text key={tick.x} x={tick.x} y={HEIGHT - 8} textAnchor="middle" fontSize="10" fill="rgba(148,163,184,0.75)">{tick.label}</text>
              ))}
            </svg>
          </div>

          {activeMarker ? (
            <div
              className="pointer-events-none absolute z-20 min-w-[220px] rounded-lg border border-white/15 bg-[#071626]/95 px-3 py-2.5 text-xs text-white/85 shadow-[0_12px_30px_rgba(2,6,23,0.5)] backdrop-blur"
              style={{
                left: `${clamp((activeMarker.x / WIDTH) * 100, shouldFlipTooltip ? 26 : 2, shouldFlipTooltip ? 98 : 69)}%`,
                top: `${clamp((activeMarker.y / HEIGHT) * 100, 10, 88)}%`,
                transform: shouldFlipTooltip ? "translate(calc(-100% - 12px), -50%)" : "translate(12px, -50%)",
              }}
            >
              <div className="flex items-center justify-between gap-2">
                <p className="font-semibold tracking-wide text-white">{symbol?.toUpperCase() ?? "Ticker"}</p>
                <p className="text-[11px] text-white/55">{formatDateShort(activeMarker.date)}</p>
              </div>
              <p className="mt-0.5 text-[11px] uppercase tracking-[0.08em] text-white/45">{markerPalette[activeMarker.kind].label}</p>
              <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px]">
                <span className="text-white/55">Actor</span><span className="text-right text-white/90">{activeMarker.actor || "—"}</span>
                <span className="text-white/55">Action</span><span className="text-right text-white/90">{activeMarker.action || "—"}</span>
                <span className="text-white/55">Amount</span><span className="text-right text-white/90">{formatCurrencyRange(activeMarker.amountMin ?? null, activeMarker.amountMax ?? null)}</span>
                <span className="text-white/55">Price</span><span className="text-right text-cyan-200">${activeMarker.close.toFixed(2)}</span>
              </div>
            </div>
          ) : null}
        </div>
      )}
    </section>
  );
}
