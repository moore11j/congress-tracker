"use client";

import { useMemo, useState } from "react";
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

const markerPalette: Record<MarkerKind, { label: string; color: string; ring: string }> = {
  congress: { label: "Congress", color: "#34d399", ring: "rgba(52,211,153,0.35)" },
  insider: { label: "Insider", color: "#38bdf8", ring: "rgba(56,189,248,0.35)" },
  signals: { label: "Signals", color: "#c084fc", ring: "rgba(192,132,252,0.35)" },
};

const WIDTH = 920;
const HEIGHT = 240;
const PADDING = { top: 20, right: 18, bottom: 28, left: 16 };


function markerTone(action: string): "pos" | "neg" | "neutral" {
  const t = action.toLowerCase();
  if (t.includes("buy") || t.includes("strong") || t.includes("notable")) return "pos";
  if (t.includes("sell") || t.includes("weak")) return "neg";
  return "neutral";
}

export function TickerActivityChart({
  points,
  markers,
}: {
  points: PricePoint[];
  markers: ActivityMarker[];
}) {
  const [visibleKinds, setVisibleKinds] = useState<Record<MarkerKind, boolean>>({
    congress: true,
    insider: true,
    signals: true,
  });

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
        <div className="overflow-x-auto">
          <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="h-[240px] w-full min-w-[720px]">
            <defs>
              <linearGradient id="price-line" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#5eead4" />
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
            <path d={chart.path} fill="none" stroke="url(#price-line)" strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
            {chart.visibleMarkers.map((marker) => {
              const color = markerPalette[marker.kind].color;
              const tone = markerTone(marker.action);
              const shape = marker.kind === "signals" ? "square" : tone === "neg" ? "down" : "up";

              return (
                <g key={marker.id}>
                  {shape === "square" ? (
                    <rect x={marker.x - 4} y={marker.y - 4} width={8} height={8} rx={2} fill={color} stroke={markerPalette[marker.kind].ring} strokeWidth={4} />
                  ) : shape === "down" ? (
                    <path d={`M ${marker.x} ${marker.y + 5} L ${marker.x - 5} ${marker.y - 4} L ${marker.x + 5} ${marker.y - 4} Z`} fill={color} stroke={markerPalette[marker.kind].ring} strokeWidth={2} />
                  ) : (
                    <path d={`M ${marker.x} ${marker.y - 5} L ${marker.x - 5} ${marker.y + 4} L ${marker.x + 5} ${marker.y + 4} Z`} fill={color} stroke={markerPalette[marker.kind].ring} strokeWidth={2} />
                  )}
                  <title>{`${formatDateShort(marker.date)} • ${marker.actor} • ${marker.action} • ${formatCurrencyRange(marker.amountMin ?? null, marker.amountMax ?? null)} • Close $${marker.close.toFixed(2)}`}</title>
                </g>
              );
            })}
            {chart.xTicks.map((tick) => (
              <text key={tick.x} x={tick.x} y={HEIGHT - 8} textAnchor="middle" fontSize="10" fill="rgba(148,163,184,0.75)">{tick.label}</text>
            ))}
          </svg>
        </div>
      )}
    </section>
  );
}
