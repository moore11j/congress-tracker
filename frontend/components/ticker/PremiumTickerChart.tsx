"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  AreaSeries,
  ColorType,
  CrosshairMode,
  LineSeries,
  LineStyle,
  createChart,
  createSeriesMarkers,
  type AreaData,
  type LineData,
  type MouseEventParams,
  type SeriesMarker,
  type Time,
} from "lightweight-charts";
import type { TickerChartBundle, TickerChartMarker } from "@/lib/api";

type MarkerGroup = {
  id: string;
  chartDate: string;
  close: number;
  events: TickerChartMarker[];
  kinds: Set<TickerChartMarker["kind"]>;
};

type HoverReadout = {
  date: string;
  x: number;
  y: number;
  close: number | null;
  benchmarkClose: number | null;
  tickerReturnPct: number | null;
  benchmarkReturnPct: number | null;
  relativePct: number | null;
  events: TickerChartMarker[];
  pinned?: boolean;
};

type MarkerKindConfig = {
  color: string;
  label: string;
  toggleLabel: string;
};

const markerConfig: Record<TickerChartMarker["kind"], MarkerKindConfig> = {
  congress: { color: "#38bdf8", label: "Congress", toggleLabel: "Congress" },
  insider: { color: "#34d399", label: "Insiders", toggleLabel: "Insiders" },
  signals: { color: "#f59e0b", label: "Signal", toggleLabel: "Signals" },
  government_contract: { color: "#60a5fa", label: "Government Contract", toggleLabel: "Gov Contracts" },
};

const markerKinds = Object.keys(markerConfig) as TickerChartMarker["kind"][];
const defaultMarkerVisibility: Record<TickerChartMarker["kind"], boolean> = {
  congress: true,
  insider: true,
  signals: true,
  government_contract: true,
};
const MAX_VISIBLE_GOVERNMENT_CONTRACT_MARKERS = 8;
const GOVERNMENT_CONTRACT_CLUSTER_THRESHOLD = 10;

function formatMoney(value: number | null | undefined, digits = 2): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: Math.abs(value) >= 1000 ? 0 : digits,
    maximumFractionDigits: Math.abs(value) >= 1000 ? 0 : digits,
  }).format(value);
}

function formatCompact(value: number | null | undefined, placeholder = "--"): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return placeholder;
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: Math.abs(value) >= 1_000_000 ? 2 : 1,
  }).format(value);
}

function formatCurrencyCompact(value: number | null | undefined, placeholder = "--"): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return placeholder;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: Math.abs(value) >= 1_000_000 ? 1 : 0,
  }).format(value);
}

function formatNumber(value: number | null | undefined, digits = 2, placeholder = "--"): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return placeholder;
  return value.toFixed(digits);
}

function formatPct(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

function formatDate(value: string): string {
  const [year, month, day] = value.split("-").map(Number);
  if (!year || !month || !day) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  }).format(new Date(Date.UTC(year, month - 1, day)));
}

function formatAmountRange(min?: number | null, max?: number | null): string {
  if (typeof min !== "number" && typeof max !== "number") return "--";
  if (typeof min === "number" && typeof max === "number" && min !== max) {
    return `${formatMoney(min, 0)} - ${formatMoney(max, 0)}`;
  }
  return formatMoney(typeof max === "number" ? max : min, 0);
}

function markerAmount(marker: TickerChartMarker): number {
  const value = marker.meta?.amount ?? marker.amount_max ?? marker.amount_min ?? 0;
  return Number.isFinite(value) ? Number(value) : 0;
}

function applyMarkerDensity(markers: TickerChartMarker[]): TickerChartMarker[] {
  const governmentContracts = markers.filter((marker) => marker.kind === "government_contract");
  if (governmentContracts.length <= GOVERNMENT_CONTRACT_CLUSTER_THRESHOLD) return markers;

  const keepIds = new Set(
    [...governmentContracts]
      .sort((a, b) => markerAmount(b) - markerAmount(a) || b.date.localeCompare(a.date))
      .slice(0, MAX_VISIBLE_GOVERNMENT_CONTRACT_MARKERS)
      .map((marker) => marker.id),
  );

  return markers.filter((marker) => marker.kind !== "government_contract" || keepIds.has(marker.id));
}

function markerTitle(event: TickerChartMarker): string {
  return event.label ?? markerConfig[event.kind].label;
}

function markerSecondaryLine(event: TickerChartMarker): string {
  if (event.kind === "government_contract") {
    const amount = formatCurrencyCompact(event.meta?.amount ?? event.amount_max ?? event.amount_min);
    const agency = event.meta?.agency?.trim() || "Agency unavailable";
    return `${formatDate(event.date)} / ${amount} — ${agency}`;
  }
  return `${formatDate(event.date)} / ${event.action} / ${formatAmountRange(event.amount_min, event.amount_max)}`;
}

function resolveMarkerChartDate(date: string, priceDates: string[]): string | null {
  if (priceDates.length === 0) return null;
  let lo = 0;
  let hi = priceDates.length - 1;
  let best: string | null = null;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    const current = priceDates[mid];
    if (current === date) return current;
    if (current < date) {
      best = current;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return best ?? priceDates[0];
}

function timeToDateKey(time: Time | undefined): string | null {
  if (typeof time === "string") return time;
  if (typeof time === "number") return new Date(time * 1000).toISOString().slice(0, 10);
  if (time && typeof time === "object") {
    const month = String(time.month).padStart(2, "0");
    const day = String(time.day).padStart(2, "0");
    return `${time.year}-${month}-${day}`;
  }
  return null;
}

function readoutForDate(
  date: string,
  x: number,
  y: number,
  priceByDate: Map<string, number>,
  benchmarkByDate: Map<string, number>,
  eventsByChartDate: Map<string, TickerChartMarker[]>,
  firstClose: number | null,
  firstBenchmarkClose: number | null,
): HoverReadout {
  const close = priceByDate.get(date) ?? null;
  const benchmarkClose = benchmarkByDate.get(date) ?? null;
  const tickerReturnPct =
    close !== null && firstClose && firstClose > 0 ? ((close / firstClose) - 1) * 100 : null;
  const benchmarkReturnPct =
    benchmarkClose !== null && firstBenchmarkClose && firstBenchmarkClose > 0
      ? ((benchmarkClose / firstBenchmarkClose) - 1) * 100
      : null;
  const relativePct =
    tickerReturnPct !== null && benchmarkReturnPct !== null ? tickerReturnPct - benchmarkReturnPct : null;

  return {
    date,
    x,
    y,
    close,
    benchmarkClose,
    tickerReturnPct,
    benchmarkReturnPct,
    relativePct,
    events: eventsByChartDate.get(date) ?? [],
  };
}

export function PremiumTickerChartSkeleton() {
  return (
    <section className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d]">
      <div className="border-b border-white/10 p-4">
        <div className="h-3 w-40 animate-pulse rounded bg-white/10" />
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-7">
          {Array.from({ length: 7 }).map((_, idx) => (
            <div key={idx} className="rounded-lg border border-white/10 bg-white/[0.03] px-3 py-2">
              <div className="h-2 w-16 animate-pulse rounded bg-white/10" />
              <div className="mt-2 h-4 w-20 animate-pulse rounded bg-white/10" />
            </div>
          ))}
        </div>
      </div>
      <div className="h-[420px] animate-pulse bg-[linear-gradient(180deg,rgba(15,23,42,0.72),rgba(2,6,23,0.94))]" />
    </section>
  );
}

export function PremiumTickerChart({ bundle }: { bundle: TickerChartBundle | null }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [readout, setReadout] = useState<HoverReadout | null>(null);
  const [markerVisibility, setMarkerVisibility] = useState<Record<TickerChartMarker["kind"], boolean>>(defaultMarkerVisibility);

  const normalized = useMemo(() => {
    const prices = [...(bundle?.prices ?? [])]
      .filter((point) => Number.isFinite(point.close))
      .sort((a, b) => a.date.localeCompare(b.date));
    const benchmark = [...(bundle?.benchmark.points ?? [])]
      .filter((point) => Number.isFinite(point.close))
      .sort((a, b) => a.date.localeCompare(b.date));
    const priceByDate = new Map(prices.map((point) => [point.date, point.close]));
    const benchmarkByDate = new Map(benchmark.map((point) => [point.date, point.close]));
    const priceDates = prices.map((point) => point.date);
    const firstClose = prices[0]?.close ?? null;
    const firstBenchmarkClose = benchmark[0]?.close ?? null;

    const benchmarkData: LineData[] = [];
    if (firstClose && firstBenchmarkClose) {
      for (const point of benchmark) {
        const matchedClose = priceByDate.get(point.date);
        if (matchedClose === undefined) continue;
        benchmarkData.push({
          time: point.date,
          value: (point.close / firstBenchmarkClose) * firstClose,
        });
      }
    }

    const filteredMarkers = applyMarkerDensity(
      (bundle?.markers ?? []).filter((marker) => markerVisibility[marker.kind] !== false),
    );

    const eventsByChartDate = new Map<string, TickerChartMarker[]>();
    for (const marker of filteredMarkers) {
      const chartDate = resolveMarkerChartDate(marker.date, priceDates);
      if (!chartDate) continue;
      const list = eventsByChartDate.get(chartDate) ?? [];
      list.push(marker);
      eventsByChartDate.set(chartDate, list);
    }

    const markerGroups: MarkerGroup[] = [...eventsByChartDate.entries()]
      .map(([chartDate, events]) => ({
        id: `marker-${chartDate}`,
        chartDate,
        close: priceByDate.get(chartDate) ?? prices[0]?.close ?? 0,
        events,
        kinds: new Set(events.map((event) => event.kind)),
      }))
      .sort((a, b) => a.chartDate.localeCompare(b.chartDate));

    return {
      prices,
      areaData: prices.map((point): AreaData => ({ time: point.date, value: point.close })),
      benchmarkData,
      priceByDate,
      benchmarkByDate,
      eventsByChartDate,
      markerGroups,
      firstClose,
      firstBenchmarkClose,
    };
  }, [bundle, markerVisibility]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container || normalized.areaData.length === 0) return;

    const chart = createChart(container, {
      width: container.clientWidth,
      height: 420,
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "#07111d" },
        textColor: "rgba(226,232,240,0.78)",
        fontFamily: "Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
      },
      grid: {
        vertLines: { color: "rgba(148,163,184,0.08)" },
        horzLines: { color: "rgba(148,163,184,0.12)" },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: {
          color: "rgba(226,232,240,0.55)",
          width: 1,
          style: LineStyle.Solid,
          labelBackgroundColor: "#0f172a",
        },
        horzLine: {
          color: "rgba(226,232,240,0.38)",
          width: 1,
          style: LineStyle.Dashed,
          labelBackgroundColor: "#0f172a",
        },
      },
      rightPriceScale: {
        borderColor: "rgba(148,163,184,0.18)",
        scaleMargins: { top: 0.12, bottom: 0.14 },
      },
      timeScale: {
        borderColor: "rgba(148,163,184,0.18)",
        timeVisible: false,
        secondsVisible: false,
      },
      handleScroll: {
        mouseWheel: true,
        pressedMouseMove: true,
        horzTouchDrag: true,
        vertTouchDrag: false,
      },
      handleScale: {
        axisPressedMouseMove: true,
        mouseWheel: true,
        pinch: true,
      },
    });

    const priceSeries = chart.addSeries(AreaSeries, {
      lineColor: "#22d3ee",
      topColor: "rgba(34,211,238,0.28)",
      bottomColor: "rgba(34,211,238,0.02)",
      lineWidth: 2,
      priceLineColor: "rgba(34,211,238,0.45)",
      priceLineWidth: 1,
      lastValueVisible: true,
      priceFormat: { type: "price", precision: 2, minMove: 0.01 },
    });
    priceSeries.setData(normalized.areaData);

    if (normalized.benchmarkData.length >= 2) {
      const benchmarkSeries = chart.addSeries(LineSeries, {
        color: "rgba(203,213,225,0.58)",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      benchmarkSeries.setData(normalized.benchmarkData);
    }

    const seriesMarkers: SeriesMarker<Time>[] = normalized.markerGroups.map((group) => {
      const isMixed = group.kinds.size > 1;
      const first = group.events[0];
      const hasSell = group.events.some((event) => event.side === "sell");
      const hasBuy = group.events.some((event) => event.side === "buy");
      const color = isMixed ? "#e5e7eb" : markerConfig[first.kind].color;
      const shape = isMixed
        ? "square"
        : first.kind === "signals" || first.kind === "government_contract"
          ? "circle"
          : hasSell && !hasBuy
            ? "arrowDown"
            : "arrowUp";
      return {
        id: group.id,
        time: group.chartDate,
        position: "atPriceTop",
        price: group.close,
        shape,
        color,
        size: group.events.length > 1 ? 1.45 : 1.18,
        text: group.events.length > 1 ? String(group.events.length) : "",
      };
    });
    createSeriesMarkers(priceSeries, seriesMarkers, { zOrder: "top" });

    const handleMove = (param: MouseEventParams) => {
      const date = timeToDateKey(param.time);
      if (!param.point || !date) {
        setReadout((current) => (current?.pinned ? current : null));
        return;
      }
      setReadout((current) => {
        if (current?.pinned) return current;
        return readoutForDate(
          date,
          param.point?.x ?? 0,
          param.point?.y ?? 0,
          normalized.priceByDate,
          normalized.benchmarkByDate,
          normalized.eventsByChartDate,
          normalized.firstClose,
          normalized.firstBenchmarkClose,
        );
      });
    };

    const handleClick = (param: MouseEventParams) => {
      const date = timeToDateKey(param.time);
      if (!param.point || !date) {
        setReadout(null);
        return;
      }
      const next = readoutForDate(
        date,
        param.point.x,
        param.point.y,
        normalized.priceByDate,
        normalized.benchmarkByDate,
        normalized.eventsByChartDate,
        normalized.firstClose,
        normalized.firstBenchmarkClose,
      );
      setReadout((current) =>
        current?.pinned && current.date === next.date ? null : { ...next, pinned: true },
      );
    };

    chart.subscribeCrosshairMove(handleMove);
    chart.subscribeClick(handleClick);
    chart.timeScale().fitContent();

    return () => {
      chart.unsubscribeCrosshairMove(handleMove);
      chart.unsubscribeClick(handleClick);
      chart.remove();
    };
  }, [normalized]);

  const symbol = bundle?.symbol ?? "Ticker";
  const quote = bundle?.quote;
  const dayTone =
    typeof quote?.day_change === "number" && quote.day_change > 0
      ? "text-emerald-300"
      : typeof quote?.day_change === "number" && quote.day_change < 0
        ? "text-rose-300"
        : "text-slate-200";

  const stats = [
    { label: "Current Price", value: formatMoney(quote?.current_price) },
    { label: "Day Change", value: formatMoney(quote?.day_change), tone: dayTone },
    { label: "Day %", value: formatPct(quote?.day_change_pct), tone: dayTone },
    { label: "Day Volume", value: formatCompact(quote?.day_volume, "—") },
    { label: "Avg. Volume (30D)", value: formatCompact(quote?.average_volume, "—") },
    { label: "Trailing P/E", value: formatNumber(quote?.trailing_pe, 2, "—") },
    { label: "Beta", value: formatNumber(quote?.beta, 2, "—") },
  ];
  const toggleMarkerKind = (kind: TickerChartMarker["kind"]) => {
    setMarkerVisibility((current) => ({ ...current, [kind]: !current[kind] }));
  };
  const markerEventsClassName = [
    "mt-2 max-h-36 space-y-2 overflow-y-auto overscroll-contain pr-1",
    "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
    "[&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-track]:bg-white/[0.03]",
    "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60",
    readout?.pinned ? "pointer-events-auto" : "",
  ].join(" ");

  if (!bundle || normalized.areaData.length === 0) {
    return (
      <section className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d] p-5">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Daily price terminal</p>
        <div className="mt-4 rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-6">
          <p className="text-sm font-semibold text-slate-100">No daily price history available.</p>
          <p className="mt-1 text-sm text-slate-400">
            The chart will render once daily closes are available for this ticker.
          </p>
        </div>
      </section>
    );
  }

  return (
    <section className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d] shadow-[0_24px_80px_rgba(2,6,23,0.34)]">
      <div className="border-b border-white/10 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Daily price terminal</p>
            <h2 className="mt-1 text-lg font-semibold text-white">{symbol} vs S&amp;P 500</h2>
          </div>
          <div className="flex flex-wrap items-center gap-3 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
            <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-5 rounded bg-cyan-300" />{symbol}</span>
            <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-5 border-t border-dashed border-slate-300/70" />S&amp;P 500</span>
            <span>Daily</span>
          </div>
        </div>
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-7">
          {stats.map((stat) => (
            <div key={stat.label} className="rounded-lg border border-white/10 bg-white/[0.035] px-3 py-2">
              <p className="text-[10px] font-semibold uppercase tracking-[0.13em] text-slate-500">{stat.label}</p>
              <p className={`mt-1 text-sm font-semibold tabular-nums ${stat.tone ?? "text-slate-100"}`}>{stat.value}</p>
            </div>
          ))}
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2">
          {markerKinds.map((kind) => (
            <button
              key={kind}
              type="button"
              onClick={() => toggleMarkerKind(kind)}
              className={`inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] transition ${
                markerVisibility[kind]
                  ? "border-white/15 bg-white/[0.06] text-slate-100"
                  : "border-white/10 bg-white/[0.025] text-slate-500"
              }`}
              aria-pressed={markerVisibility[kind]}
            >
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{
                  backgroundColor: markerConfig[kind].color,
                  opacity: markerVisibility[kind] ? 1 : 0.35,
                }}
              />
              {markerConfig[kind].toggleLabel}
            </button>
          ))}
        </div>
      </div>

      <div className="relative">
        <div ref={containerRef} className="h-[420px] w-full" />

        {readout ? (
          <div
            className="pointer-events-none absolute z-20 w-[min(330px,calc(100%-24px))] rounded-lg border border-white/15 bg-[#050b13]/95 p-3 text-xs text-slate-200 shadow-[0_18px_45px_rgba(0,0,0,0.48)] backdrop-blur"
            style={{
              left: readout.x > 340 ? Math.max(12, readout.x - 342) : Math.min(readout.x + 14, 40),
              top: Math.max(12, Math.min(readout.y + 14, 284)),
            }}
          >
            <div className="flex items-start justify-between gap-3">
              <div>
                <p className="font-semibold text-white">{formatDate(readout.date)}</p>
                <p className="mt-0.5 text-[10px] uppercase tracking-[0.14em] text-slate-500">
                  {readout.pinned ? "Pinned readout" : "Crosshair readout"}
                </p>
              </div>
              <p className="rounded border border-cyan-300/25 bg-cyan-300/10 px-2 py-1 font-semibold text-cyan-100">
                {formatMoney(readout.close)}
              </p>
            </div>
            <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1.5">
              <span className="text-slate-500">{symbol} close</span>
              <span className="text-right tabular-nums text-white">{formatMoney(readout.close)}</span>
              <span className="text-slate-500">S&amp;P 500</span>
              <span className="text-right tabular-nums text-slate-100">{formatNumber(readout.benchmarkClose, 2)}</span>
              <span className="text-slate-500">{symbol} return</span>
              <span className="text-right tabular-nums text-slate-100">{formatPct(readout.tickerReturnPct)}</span>
              <span className="text-slate-500">Relative vs benchmark</span>
              <span className={`text-right tabular-nums ${readout.relativePct !== null && readout.relativePct >= 0 ? "text-emerald-300" : "text-rose-300"}`}>
                {formatPct(readout.relativePct)}
              </span>
            </div>
            {readout.events.length > 0 ? (
              <div className="mt-3 border-t border-white/10 pt-3">
                <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">
                  Events on this marker
                </p>
                <div className={markerEventsClassName}>
                  {readout.events.map((event) => (
                    <div key={event.id} className="grid grid-cols-[auto_1fr] gap-2">
                      <span
                        className="mt-1 h-2 w-2 rounded-full"
                        style={{ backgroundColor: markerConfig[event.kind].color }}
                      />
                      <div className="min-w-0">
                        <p className="truncate font-semibold text-slate-100">
                          {event.kind === "government_contract"
                            ? markerTitle(event)
                            : `${markerTitle(event)}: ${event.actor}`}
                        </p>
                        <p className="truncate text-slate-500">{markerSecondaryLine(event)}</p>
                        {event.kind === "government_contract" && event.meta?.description ? (
                          <p className="truncate text-slate-400">{event.meta.description}</p>
                        ) : null}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </section>
  );
}
