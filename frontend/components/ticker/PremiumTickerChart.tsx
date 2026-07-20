"use client";

import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";
import {
  AreaSeries,
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  LineStyle,
  createChart,
  createSeriesMarkers,
  type AreaData,
  type CandlestickData,
  type HistogramData,
  type LineData,
  type MouseEventParams,
  type SeriesMarker,
  type Time,
} from "lightweight-charts";
import { getTickerChartBundle, type TickerChartBundle, type TickerChartMarker } from "@/lib/api";

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

type ChartMode = "line" | "candles";
type IndicatorKey = "sma20" | "sma50" | "bollinger" | "vwap";

const markerConfig: Record<TickerChartMarker["kind"], MarkerKindConfig> = {
  congress: { color: "#38bdf8", label: "Congress", toggleLabel: "Congress" },
  insider: { color: "#34d399", label: "Insiders", toggleLabel: "Insiders" },
  institutional: { color: "#818cf8", label: "Institutional Activity", toggleLabel: "Institutional" },
  signals: { color: "#f59e0b", label: "Signal", toggleLabel: "Signals" },
  government_contract: { color: "#60a5fa", label: "Government Contract", toggleLabel: "Gov Contracts" },
};

const markerKinds = Object.keys(markerConfig) as TickerChartMarker["kind"][];
const defaultMarkerVisibility: Record<TickerChartMarker["kind"], boolean> = {
  congress: true,
  insider: true,
  institutional: true,
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

function movingAverage(points: { date: string; close: number }[], period: number): LineData[] {
  const data: LineData[] = [];
  for (let index = 0; index < points.length; index += 1) {
    const window = points.slice(Math.max(0, index - period + 1), index + 1);
    if (window.length < period) continue;
    const value = window.reduce((sum, point) => sum + point.close, 0) / period;
    data.push({ time: points[index].date, value });
  }
  return data;
}

function bollingerBands(points: { date: string; close: number }[], period = 20, deviations = 2): { upper: LineData[]; lower: LineData[] } {
  const upper: LineData[] = [];
  const lower: LineData[] = [];
  for (let index = 0; index < points.length; index += 1) {
    const window = points.slice(Math.max(0, index - period + 1), index + 1);
    if (window.length < period) continue;
    const average = window.reduce((sum, point) => sum + point.close, 0) / period;
    const variance = window.reduce((sum, point) => sum + (point.close - average) ** 2, 0) / period;
    const bandWidth = Math.sqrt(variance) * deviations;
    upper.push({ time: points[index].date, value: average + bandWidth });
    lower.push({ time: points[index].date, value: average - bandWidth });
  }
  return { upper, lower };
}

function vwapLine(points: { date: string; close: number; volume?: number | null }[]): LineData[] {
  let cumulativeValue = 0;
  let cumulativeVolume = 0;
  return points.flatMap((point) => {
    const volume = typeof point.volume === "number" && Number.isFinite(point.volume) ? point.volume : 0;
    if (volume <= 0) return [];
    cumulativeValue += point.close * volume;
    cumulativeVolume += volume;
    return [{ time: point.date, value: cumulativeValue / cumulativeVolume }];
  });
}

function relativeLineData(points: { date: string; close: number }[], anchor: number | null | undefined, dates?: Set<string>): LineData[] {
  if (!anchor || anchor <= 0) return [];
  return points.flatMap((point) => {
    if (dates && !dates.has(point.date)) return [];
    return [{ time: point.date, value: ((point.close / anchor) - 1) * 100 }];
  });
}

function relativeCompareData(basePoints: { date: string; close: number }[], comparePoints: { date: string; close: number }[]): LineData[] {
  const compareByDate = new Map(comparePoints.map((point) => [point.date, point.close]));
  const firstMatch = basePoints.find((point) => compareByDate.has(point.date));
  if (!firstMatch) return [];
  const firstCompareClose = compareByDate.get(firstMatch.date);
  if (!firstCompareClose || firstCompareClose <= 0) return [];
  return relativeLineData(comparePoints, firstCompareClose, new Set(basePoints.map((point) => point.date)));
}

function relativeIndicatorData(data: LineData[], anchor: number | null | undefined): LineData[] {
  if (!anchor || anchor <= 0) return [];
  return data.map((point) => ({ ...point, value: ((point.value / anchor) - 1) * 100 }));
}

function relativeCandleData(data: { date: string; open: number; high: number; low: number; close: number }[], anchor: number | null | undefined): CandlestickData[] {
  if (!anchor || anchor <= 0) return [];
  return data.map((point) => ({
    time: point.date,
    open: ((point.open / anchor) - 1) * 100,
    high: ((point.high / anchor) - 1) * 100,
    low: ((point.low / anchor) - 1) * 100,
    close: ((point.close / anchor) - 1) * 100,
  }));
}

function volumeProfileBuckets(points: { close: number; volume?: number | null }[], bucketCount = 18): { topPct: number; heightPct: number; widthPct: number }[] {
  const usable = points.filter((point) => Number.isFinite(point.close) && typeof point.volume === "number" && Number.isFinite(point.volume) && point.volume > 0);
  if (usable.length === 0) return [];
  const min = Math.min(...usable.map((point) => point.close));
  const max = Math.max(...usable.map((point) => point.close));
  if (min === max) return [{ topPct: 46, heightPct: 8, widthPct: 100 }];
  const buckets = Array.from({ length: bucketCount }, () => 0);
  for (const point of usable) {
    const index = Math.min(bucketCount - 1, Math.max(0, Math.floor(((point.close - min) / (max - min)) * bucketCount)));
    buckets[index] += point.volume ?? 0;
  }
  const maxVolume = Math.max(...buckets, 1);
  return buckets.map((volume, index) => ({
    topPct: ((bucketCount - index - 1) / bucketCount) * 100,
    heightPct: 100 / bucketCount,
    widthPct: (volume / maxVolume) * 100,
  }));
}

export function assertDailyPriceTerminalConsistency(bundle: TickerChartBundle | null): void {
  const latestPoint = bundle?.prices?.[bundle.prices.length - 1];
  const latestClose = typeof latestPoint?.close === "number" && Number.isFinite(latestPoint.close) ? latestPoint.close : null;
  const currentPrice =
    typeof bundle?.quote?.current_price === "number" && Number.isFinite(bundle.quote.current_price)
      ? bundle.quote.current_price
      : null;
  if (latestClose === null || currentPrice === null || Math.abs(currentPrice - latestClose) <= 0.01) return;
  const message = `Daily Price Terminal current_price ${currentPrice} differs from chart latest close ${latestClose}`;
  if (process.env.NODE_ENV === "test") {
    throw new Error(message);
  }
  if (process.env.NODE_ENV === "development") {
    console.error(message);
  }
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
  if (event.kind === "insider" && event.meta) {
    const filing = event.meta.filing_date ? ` / Filed ${formatDate(event.meta.filing_date)}` : "";
    const shares = typeof event.meta.shares === "number" ? `${formatCompact(event.meta.shares)} sh` : null;
    const value = formatCurrencyCompact(event.meta.value ?? event.amount_max ?? event.amount_min);
    const price = typeof event.meta.price === "number" ? ` @ ${formatMoney(event.meta.price)}` : "";
    const signal = event.meta.signal_label || typeof event.meta.signal_score === "number"
      ? ` / ${event.meta.signal_label ?? "Signal"}${typeof event.meta.signal_score === "number" ? ` ${event.meta.signal_score.toFixed(0)}` : ""}`
      : "";
    return `${formatDate(event.meta.transaction_date ?? event.date)}${filing} / ${event.action} / ${shares ? `${shares}${price}` : value}${signal}`;
  }
  if (event.kind === "government_contract") {
    const amount = formatCurrencyCompact(event.meta?.amount ?? event.amount_max ?? event.amount_min);
    const agency = event.meta?.agency?.trim() || "Agency unavailable";
    const dateLabel = event.meta?.event_subtype === "funding_action" ? "Report Date" : "Start Date";
    return `${dateLabel}: ${formatDate(event.meta?.report_date ?? event.date)} / ${amount} - ${agency}`;
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
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-8">
          {Array.from({ length: 8 }).map((_, idx) => (
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

export function PremiumTickerChart({
  bundle,
  title,
  eyebrow = "Daily price terminal",
  subtitle,
  allowedMarkerKinds,
  showMarkerControls = true,
  emptyTitle = "No daily price history available.",
  emptyMessage = "The chart will render once daily closes are available for this ticker.",
}: {
  bundle: TickerChartBundle | null;
  title?: string;
  eyebrow?: string;
  subtitle?: string;
  allowedMarkerKinds?: TickerChartMarker["kind"][];
  showMarkerControls?: boolean;
  emptyTitle?: string;
  emptyMessage?: string;
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [readout, setReadout] = useState<HoverReadout | null>(null);
  const [markerVisibility, setMarkerVisibility] = useState<Record<TickerChartMarker["kind"], boolean>>(defaultMarkerVisibility);
  const [chartMode, setChartMode] = useState<ChartMode>("line");
  const [indicatorVisibility, setIndicatorVisibility] = useState<Record<IndicatorKey, boolean>>({
    sma20: false,
    sma50: false,
    bollinger: false,
    vwap: false,
  });
  const [compareInput, setCompareInput] = useState("");
  const [compareSymbol, setCompareSymbol] = useState<string | null>(null);
  const [compareBundle, setCompareBundle] = useState<TickerChartBundle | null>(null);
  const [compareLoading, setCompareLoading] = useState(false);
  const visibleMarkerKinds = useMemo(() => allowedMarkerKinds ?? markerKinds, [allowedMarkerKinds]);

  useEffect(() => {
    if (!compareSymbol || !bundle?.days) {
      setCompareBundle(null);
      setCompareLoading(false);
      return;
    }
    const controller = new AbortController();
    setCompareLoading(true);
    getTickerChartBundle(compareSymbol, bundle.days, { signal: controller.signal, source: "TickerChartCompare" })
      .then((response) => setCompareBundle(response))
      .catch((error) => {
        if (error instanceof Error && error.name === "AbortError") return;
        setCompareBundle(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) setCompareLoading(false);
      });
    return () => controller.abort();
  }, [bundle?.days, compareSymbol]);

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
    const volumeByDate = new Map((bundle?.volumes ?? []).map((point) => [point.date, point.volume]));
    const candleSource = bundle?.candles?.length
      ? [...bundle.candles].sort((a, b) => a.date.localeCompare(b.date))
      : prices.map((point, index) => {
          const previousClose = prices[index - 1]?.close ?? point.close;
          return {
            date: point.date,
            open: previousClose,
            high: Math.max(previousClose, point.close),
            low: Math.min(previousClose, point.close),
            close: point.close,
            volume: volumeByDate.get(point.date) ?? null,
          };
        });
    const closeVolumePoints = prices.map((point) => ({
      date: point.date,
      close: point.close,
      volume: volumeByDate.get(point.date) ?? candleSource.find((candle) => candle.date === point.date)?.volume ?? null,
    }));
    const compareData = compareBundle?.prices ? relativeCompareData(prices, compareBundle.prices) : [];
    const performanceMode = compareData.length >= 2;
    const priceDateSet = new Set(priceDates);
    const candleData: CandlestickData[] = performanceMode
      ? relativeCandleData(candleSource, firstClose)
      : candleSource.map((point) => ({
          time: point.date,
          open: point.open,
          high: point.high,
          low: point.low,
          close: point.close,
        }));
    const volumeData: HistogramData[] = closeVolumePoints
      .filter((point) => typeof point.volume === "number" && Number.isFinite(point.volume))
      .map((point, index) => {
        const previous = closeVolumePoints[index - 1]?.close ?? point.close;
        return {
          time: point.date,
          value: point.volume ?? 0,
          color: point.close >= previous ? "rgba(52,211,153,0.34)" : "rgba(251,113,133,0.34)",
        };
      });

    const benchmarkData: LineData[] = [];
    if (firstClose && firstBenchmarkClose) {
      for (const point of benchmark) {
        const matchedClose = priceByDate.get(point.date);
        if (matchedClose === undefined) continue;
        benchmarkData.push({
          time: point.date,
          value: performanceMode ? ((point.close / firstBenchmarkClose) - 1) * 100 : (point.close / firstBenchmarkClose) * firstClose,
        });
      }
    }
    const areaData: AreaData[] = performanceMode
      ? relativeLineData(prices, firstClose, priceDateSet).map((point): AreaData => ({ time: point.time, value: point.value }))
      : prices.map((point): AreaData => ({ time: point.date, value: point.close }));
    const chartValueByDate = new Map(areaData.map((point) => [String(point.time), point.value]));
    const sma20Data = movingAverage(prices, 20);
    const sma50Data = movingAverage(prices, 50);
    const bollingerData = bollingerBands(prices);
    const vwapData = vwapLine(closeVolumePoints);

    const filteredMarkers = applyMarkerDensity(
      (bundle?.markers ?? []).filter((marker) => visibleMarkerKinds.includes(marker.kind) && markerVisibility[marker.kind] !== false),
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
        close: chartValueByDate.get(chartDate) ?? priceByDate.get(chartDate) ?? prices[0]?.close ?? 0,
        events,
        kinds: new Set(events.map((event) => event.kind)),
      }))
      .sort((a, b) => a.chartDate.localeCompare(b.chartDate));

    return {
      prices,
      areaData,
      candleData,
      volumeData,
      sma20Data: performanceMode ? relativeIndicatorData(sma20Data, firstClose) : sma20Data,
      sma50Data: performanceMode ? relativeIndicatorData(sma50Data, firstClose) : sma50Data,
      bollingerData: performanceMode
        ? {
            upper: relativeIndicatorData(bollingerData.upper, firstClose),
            lower: relativeIndicatorData(bollingerData.lower, firstClose),
          }
        : bollingerData,
      vwapData: performanceMode ? relativeIndicatorData(vwapData, firstClose) : vwapData,
      compareData,
      performanceMode,
      volumeProfile: volumeProfileBuckets(closeVolumePoints),
      benchmarkData,
      priceByDate,
      benchmarkByDate,
      eventsByChartDate,
      markerGroups,
      firstClose,
      firstBenchmarkClose,
    };
  }, [bundle, compareBundle, markerVisibility, visibleMarkerKinds]);

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
        scaleMargins: { top: 0.12, bottom: normalized.volumeData.length > 0 ? 0.28 : 0.14 },
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

    const priceFormat = normalized.performanceMode
      ? {
          type: "custom" as const,
          minMove: 0.01,
          formatter: (value: number) => `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`,
        }
      : { type: "price" as const, precision: 2, minMove: 0.01 };

    const priceSeries = chartMode === "candles"
      ? chart.addSeries(CandlestickSeries, {
          upColor: "#34d399",
          downColor: "#fb7185",
          borderUpColor: "#34d399",
          borderDownColor: "#fb7185",
          wickUpColor: "rgba(52,211,153,0.9)",
          wickDownColor: "rgba(251,113,133,0.9)",
          priceLineColor: "rgba(34,211,238,0.45)",
          priceLineWidth: 1,
          lastValueVisible: true,
          priceFormat,
        })
      : chart.addSeries(AreaSeries, {
          lineColor: "#22d3ee",
          topColor: "rgba(34,211,238,0.28)",
          bottomColor: "rgba(34,211,238,0.02)",
          lineWidth: 2,
          priceLineColor: "rgba(34,211,238,0.45)",
          priceLineWidth: 1,
          lastValueVisible: true,
          priceFormat,
        });
    if (chartMode === "candles") {
      priceSeries.setData(normalized.candleData);
    } else {
      priceSeries.setData(normalized.areaData);
    }

    if (normalized.volumeData.length > 0) {
      const volumeSeries = chart.addSeries(HistogramSeries, {
        priceFormat: { type: "volume" },
        priceScaleId: "volume",
        priceLineVisible: false,
        lastValueVisible: false,
      });
      volumeSeries.setData(normalized.volumeData);
      chart.priceScale("volume").applyOptions({
        scaleMargins: { top: 0.78, bottom: 0 },
        borderVisible: false,
      });
    }

    if (indicatorVisibility.sma20 && normalized.sma20Data.length > 0) {
      chart.addSeries(LineSeries, {
        color: "rgba(96,165,250,0.95)",
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      }).setData(normalized.sma20Data);
    }
    if (indicatorVisibility.sma50 && normalized.sma50Data.length > 0) {
      chart.addSeries(LineSeries, {
        color: "rgba(168,85,247,0.9)",
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      }).setData(normalized.sma50Data);
    }
    if (indicatorVisibility.bollinger) {
      chart.addSeries(LineSeries, {
        color: "rgba(251,191,36,0.75)",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      }).setData(normalized.bollingerData.upper);
      chart.addSeries(LineSeries, {
        color: "rgba(251,191,36,0.75)",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      }).setData(normalized.bollingerData.lower);
    }
    if (indicatorVisibility.vwap && normalized.vwapData.length > 0) {
      chart.addSeries(LineSeries, {
        color: "rgba(45,212,191,0.95)",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      }).setData(normalized.vwapData);
    }

    if (normalized.performanceMode) {
      chart.addSeries(LineSeries, {
        color: "rgba(148,163,184,0.35)",
        lineWidth: 1,
        lineStyle: LineStyle.Dotted,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      }).setData(normalized.areaData.map((point) => ({ time: point.time, value: 0 })));
    }

    if (normalized.benchmarkData.length >= 2) {
      const benchmarkSeries = chart.addSeries(LineSeries, {
        color: "rgba(203,213,225,0.58)",
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      });
      benchmarkSeries.setData(normalized.benchmarkData);
    }

    if (normalized.compareData.length >= 2 && compareSymbol) {
      const compareSeries = chart.addSeries(LineSeries, {
        color: "rgba(244,114,182,0.95)",
        lineWidth: 2,
        priceLineVisible: false,
        lastValueVisible: false,
        priceFormat,
      });
      compareSeries.setData(normalized.compareData);
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
  }, [chartMode, compareSymbol, indicatorVisibility, normalized]);

  const symbol = bundle?.symbol ?? "Ticker";
  const quote = bundle?.quote;
  assertDailyPriceTerminalConsistency(bundle);
  const dayTone =
    typeof quote?.day_change === "number" && quote.day_change > 0
      ? "text-emerald-300"
      : typeof quote?.day_change === "number" && quote.day_change < 0
        ? "text-rose-300"
        : "text-slate-200";
  const marketCapUnavailable = typeof quote?.market_cap !== "number" || !Number.isFinite(quote.market_cap);

  const stats = [
    {
      label: "Market Cap",
      value: formatCurrencyCompact(quote?.market_cap, "Unavailable"),
      tone: marketCapUnavailable ? "text-slate-500" : undefined,
    },
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
  const toggleIndicator = (indicator: IndicatorKey) => {
    setIndicatorVisibility((current) => ({ ...current, [indicator]: !current[indicator] }));
  };
  const submitCompare = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextSymbol = compareInput.trim().toUpperCase();
    setCompareSymbol(nextSymbol || null);
    if (!nextSymbol) setCompareBundle(null);
  };
  const clearCompare = () => {
    setCompareInput("");
    setCompareSymbol(null);
    setCompareBundle(null);
  };
  const markerEventsClassName = [
    "mt-2 max-h-36 space-y-2 overflow-y-auto overscroll-contain pr-1",
    "[scrollbar-color:rgba(148,163,184,0.45)_rgba(15,23,42,0.28)] [scrollbar-width:thin]",
    "[&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-track]:rounded-full [&::-webkit-scrollbar-track]:bg-white/[0.03]",
    "[&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-slate-500/45 [&::-webkit-scrollbar-thumb:hover]:bg-slate-400/60",
    readout?.pinned ? "pointer-events-auto" : "",
  ].join(" ");
  const freshness = bundle?.freshness;
  const benchmarkLabel = bundle?.benchmark.label?.trim() || "S&P 500 (SPY)";
  const freshnessBlocksChart = Boolean(
    bundle && (freshness?.is_stale || bundle.status === "stale" || bundle.status === "unavailable"),
  );
  const latestDateLabel = freshness?.latest_date ? formatDate(freshness.latest_date) : null;
  const expectedDateLabel = freshness?.expected_latest_date ? formatDate(freshness.expected_latest_date) : null;

  if (bundle && freshnessBlocksChart) {
    return (
      <section className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d] p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{eyebrow}</p>
            <h2 className="mt-1 text-lg font-semibold text-white">Price chart updating</h2>
            <p className="mt-1 text-sm text-slate-400">
              {latestDateLabel
                ? `Updated through ${latestDateLabel}. Latest market data is temporarily unavailable.`
                : "Latest market data is temporarily unavailable."}
            </p>
          </div>
          {expectedDateLabel ? (
            <span className="rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-1.5 text-xs font-semibold text-amber-100">
              Expected through {expectedDateLabel}
            </span>
          ) : null}
        </div>
      </section>
    );
  }

  if (!bundle || normalized.areaData.length === 0) {
    return (
      <section className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d] p-5">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{eyebrow}</p>
        <div className="mt-4 rounded-lg border border-dashed border-white/15 bg-white/[0.03] p-6">
          <p className="text-sm font-semibold text-slate-100">{emptyTitle}</p>
          <p className="mt-1 text-sm text-slate-400">{emptyMessage}</p>
        </div>
      </section>
    );
  }

  return (
    <section className="overflow-hidden rounded-2xl border border-white/10 bg-[#07111d] shadow-[0_24px_80px_rgba(2,6,23,0.34)]">
      <div className="border-b border-white/10 p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">{eyebrow}</p>
            <h2 className="mt-1 text-lg font-semibold text-white">{title ?? `${symbol} vs ${benchmarkLabel}`}</h2>
            {subtitle ? <p className="mt-1 text-sm text-slate-400">{subtitle}</p> : null}
          </div>
          <div className="flex flex-wrap items-center gap-3 text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
            <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-5 rounded bg-cyan-300" />{symbol}</span>
            <span className="inline-flex items-center gap-1.5"><span className="h-[2px] w-5 border-t border-dashed border-slate-300/70" />{benchmarkLabel}</span>
            {normalized.performanceMode ? <span className="text-cyan-100">Relative %</span> : null}
            <span>Daily</span>
          </div>
        </div>
        <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-8">
          {stats.map((stat) => (
            <div key={stat.label} className="rounded-lg border border-white/10 bg-white/[0.035] px-3 py-2">
              <p className="text-[10px] font-semibold uppercase tracking-[0.13em] text-slate-500">{stat.label}</p>
              <p className={`mt-1 text-sm font-semibold tabular-nums ${stat.tone ?? "text-slate-100"}`}>{stat.value}</p>
            </div>
          ))}
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2">
          <div className="inline-flex rounded-lg border border-white/10 bg-slate-950/70 p-1">
            {(["line", "candles"] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => setChartMode(mode)}
                className={`rounded-md px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] transition ${
                  chartMode === mode ? "bg-cyan-300/15 text-cyan-100" : "text-slate-400 hover:bg-white/5 hover:text-slate-200"
                }`}
                aria-pressed={chartMode === mode}
              >
                {mode === "line" ? "Line" : "Candles"}
              </button>
            ))}
          </div>
          {([
            ["sma20", "SMA 20"],
            ["sma50", "SMA 50"],
            ["bollinger", "Bollinger"],
            ["vwap", "VWAP"],
          ] as const).map(([key, label]) => (
            <button
              key={key}
              type="button"
              onClick={() => toggleIndicator(key)}
              className={`rounded-lg border px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] transition ${
                indicatorVisibility[key]
                  ? "border-cyan-300/35 bg-cyan-300/10 text-cyan-100"
                  : "border-white/10 bg-white/[0.025] text-slate-400 hover:bg-white/[0.05] hover:text-slate-200"
              }`}
              aria-pressed={indicatorVisibility[key]}
            >
              {label}
            </button>
          ))}
          <form onSubmit={submitCompare} className="flex min-w-[16rem] items-center gap-2 rounded-lg border border-white/10 bg-slate-950/70 px-2 py-1">
            <input
              value={compareInput}
              onChange={(event) => setCompareInput(event.target.value.toUpperCase())}
              placeholder="Compare ticker"
              className="min-w-0 flex-1 bg-transparent px-2 py-1 text-sm font-semibold uppercase text-slate-100 outline-none placeholder:text-slate-600"
              maxLength={12}
            />
            {compareSymbol ? (
              <button type="button" onClick={clearCompare} className="rounded-md px-2 py-1 text-xs font-semibold text-slate-400 hover:bg-white/5 hover:text-slate-100">
                Clear
              </button>
            ) : null}
            <button type="submit" className="rounded-md bg-cyan-300/15 px-2.5 py-1 text-xs font-semibold text-cyan-100 hover:bg-cyan-300/20">
              Compare
            </button>
          </form>
          {compareSymbol ? (
            <span className="inline-flex items-center gap-1.5 rounded-lg border border-pink-300/25 bg-pink-300/10 px-3 py-1.5 text-[11px] font-semibold uppercase tracking-[0.12em] text-pink-100">
              <span className="h-[2px] w-5 rounded bg-pink-300" />
              {compareLoading ? "Loading" : compareSymbol}
            </span>
          ) : null}
        </div>
        {showMarkerControls ? (
        <div className="mt-4 flex flex-wrap items-center gap-2">
          {visibleMarkerKinds.map((kind) => (
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
        ) : null}
      </div>

      <div className="relative">
        <div ref={containerRef} className="h-[420px] w-full" />
        {normalized.volumeProfile.length > 0 ? (
          <div className="pointer-events-none absolute bottom-12 right-12 top-8 z-10 w-24 opacity-65">
            {normalized.volumeProfile.map((bucket, index) => (
              <div
                key={`${bucket.topPct}-${index}`}
                className="absolute right-0 rounded-l bg-cyan-300/20"
                style={{
                  top: `${bucket.topPct}%`,
                  height: `${Math.max(bucket.heightPct - 0.8, 1)}%`,
                  width: `${Math.max(bucket.widthPct, 5)}%`,
                }}
              />
            ))}
          </div>
        ) : null}

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
              <span className="text-slate-500">{benchmarkLabel}</span>
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
                        {event.kind === "government_contract" && event.meta?.modification_number ? (
                          <p className="truncate text-slate-500">Modification: {event.meta.modification_number}</p>
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
