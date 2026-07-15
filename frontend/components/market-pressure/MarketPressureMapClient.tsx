"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { recordProductEvent } from "@/lib/api";
import {
  marketPressureLayerLabels,
  marketPressureTimeRanges,
  marketPressureUniverses,
  marketPressureViewModes,
  type MarketPressureLayerAccess,
  type MarketPressureLayerKey,
  type MarketPressureMapResult,
  type MarketPressureTile,
  type MarketPressureTimeRange,
  type MarketPressureUniverse,
  type MarketPressureViewMode,
} from "@/lib/marketPressure";
import { ghostButtonClassName, subtlePrimaryButtonClassName } from "@/lib/styles";

type Props = {
  initialData: MarketPressureMapResult;
  canonicalUrl: string;
};

const pressureOrder: MarketPressureLayerKey[] = [
  "priceVolume",
  "fundamentals",
  "congress",
  "insiders",
  "governmentContracts",
  "institutions",
  "optionsFlow",
  "macroPositioning",
];

function segmentedButtonClass(active: boolean) {
  return [
    "min-h-9 rounded-lg border px-3 py-1.5 text-xs font-semibold transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950",
    active
      ? "border-emerald-300/50 bg-emerald-300/15 text-emerald-100"
      : "border-white/10 bg-slate-950/50 text-slate-300 hover:border-white/20 hover:text-white",
  ].join(" ");
}

function AnalyticsButton({
  active,
  children,
  onClick,
  ariaLabel,
}: {
  active: boolean;
  children: ReactNode;
  onClick: () => void;
  ariaLabel: string;
}) {
  return (
    <button type="button" className={segmentedButtonClass(active)} aria-pressed={active} aria-label={ariaLabel} onClick={onClick}>
      {children}
    </button>
  );
}

function layerStatusLabel(access: MarketPressureLayerAccess, latestSuccessfulDataAt: string | null) {
  if (access === "locked") return "Locked";
  if (access === "unavailable") return "Unavailable";
  return latestSuccessfulDataAt ? "Available" : "No data";
}

function layerStatusClass(access: MarketPressureLayerAccess, latestSuccessfulDataAt: string | null) {
  if (access === "locked") return "border-amber-300/25 bg-amber-300/10 text-amber-100";
  if (access === "unavailable") return "border-slate-500/25 bg-slate-700/20 text-slate-300";
  return latestSuccessfulDataAt
    ? "border-emerald-300/25 bg-emerald-300/10 text-emerald-100"
    : "border-cyan-300/25 bg-cyan-300/10 text-cyan-100";
}

function priceDirectionClass(value: number | null) {
  if (value == null) return "border-slate-600/40 bg-slate-800/60";
  if (value > 0) return "border-emerald-300/30 bg-emerald-500/15";
  if (value < 0) return "border-rose-300/30 bg-rose-500/15";
  return "border-slate-400/25 bg-slate-700/40";
}

function pressureDirectionClass(direction: MarketPressureTile["pressureDirection"]) {
  if (direction === "bullish") return "border-l-emerald-300";
  if (direction === "bearish") return "border-l-rose-300";
  if (direction === "conflicted") return "border-l-amber-300";
  return "border-l-slate-400";
}

function formatPct(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "Price unavailable";
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatStrength(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "Strength unavailable";
  return `${Math.round(value)} strength`;
}

function marketPressureTickerHref(symbol: string) {
  const trimmed = symbol.trim().toUpperCase();
  return trimmed ? `/ticker/${encodeURIComponent(trimmed)}` : null;
}

function MarketPressureTileCard({ tile }: { tile: MarketPressureTile }) {
  const href = marketPressureTickerHref(tile.symbol);
  const body = (
    <div
      className={`min-h-[6.5rem] rounded-xl border border-l-4 p-3 ${priceDirectionClass(tile.priceChangePct)} ${pressureDirectionClass(tile.pressureDirection)}`}
      aria-label={`${tile.symbol} price ${formatPct(tile.priceChangePct)}, pressure ${tile.pressureDirection}`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate font-mono text-sm font-semibold text-white">{tile.symbol}</div>
          <div className="mt-1 truncate text-[11px] text-slate-300">{tile.companyName ?? "Company unavailable"}</div>
        </div>
        <div className="shrink-0 text-right text-[11px] font-semibold text-slate-100">{formatPct(tile.priceChangePct)}</div>
      </div>
      <div className="mt-3 flex flex-wrap gap-1.5 text-[10px] font-semibold uppercase tracking-wide">
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-200">{tile.pressureDirection}</span>
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-300">{formatStrength(tile.pressureStrength)}</span>
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-400">{tile.pressureTrend ?? "trend unavailable"}</span>
      </div>
    </div>
  );

  if (!href) return body;

  return (
    <Link
      href={href}
      prefetch={false}
      className="block rounded-xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/60 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950"
      onClick={() => recordProductEvent({ event_name: "market_pressure_ticker_opened", properties: { symbol: tile.symbol } })}
    >
      {body}
    </Link>
  );
}

function MarketPressureStatusState({
  state,
  message,
}: {
  state: MarketPressureMapResult["status"];
  message: string | null;
}) {
  const copy = {
    loading: {
      title: "Loading market pressure",
      body: "Fetching the latest successful batch when the provider endpoint is available.",
    },
    ready: {
      title: "Market pressure ready",
      body: "Pressure tiles are grouped by sector.",
    },
    "no-data": {
      title: "No Market Pressure data yet",
      body: message ?? "The map is ready for the batch endpoint, but no tiles were returned.",
    },
    error: {
      title: "Provider/API error",
      body: message ?? "Walnut could not load Market Pressure data. Existing data is not hidden or replaced.",
    },
    entitlement: {
      title: "Layer access required",
      body: message ?? "Some pressure layers are locked by your current Walnut plan.",
    },
  }[state];

  return (
    <div className="flex min-h-[22rem] items-center justify-center rounded-2xl border border-dashed border-white/15 bg-slate-950/45 px-4 py-10 text-center" aria-live="polite" aria-busy={state === "loading"}>
      <div className="max-w-xl">
        <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-200/80">{state.replace("-", " ")}</p>
        <h2 className="mt-3 text-xl font-semibold text-white">{copy.title}</h2>
        <p className="mt-3 text-sm leading-6 text-slate-300">{copy.body}</p>
      </div>
    </div>
  );
}

function MarketPressureVisualization({ data }: { data: MarketPressureMapResult }) {
  const sectorGroups = useMemo(() => {
    const groups = new Map<string, MarketPressureTile[]>();
    data.tiles.forEach((tile) => {
      const sector = tile.sector.trim() || "Sector unavailable";
      groups.set(sector, [...(groups.get(sector) ?? []), tile]);
    });
    return Array.from(groups.entries());
  }, [data.tiles]);

  if (data.status !== "ready" || data.tiles.length === 0) {
    return <MarketPressureStatusState state={data.status === "ready" ? "no-data" : data.status} message={data.providerMessage} />;
  }

  return (
    <div className="space-y-6">
      {sectorGroups.map(([sector, tiles]) => (
        <section key={sector} aria-label={`${sector} sector pressure`}>
          <div className="mb-3 flex items-center justify-between gap-3">
            <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-300">{sector}</h2>
            <span className="text-xs text-slate-500">{tiles.length} tickers</span>
          </div>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(9.5rem,1fr))] gap-3">
            {tiles.map((tile) => (
              <MarketPressureTileCard key={`${tile.sector}:${tile.symbol}`} tile={tile} />
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}

function MarketPressureLegend({ data }: { data: MarketPressureMapResult }) {
  return (
    <div className="grid gap-3 text-xs text-slate-300 sm:grid-cols-3">
      <div className="rounded-xl border border-white/10 bg-slate-950/45 p-3">
        <div className="font-semibold text-white">Tile interior</div>
        <p className="mt-1 leading-5">Price direction: positive, negative, flat, or unavailable.</p>
      </div>
      <div className="rounded-xl border border-white/10 bg-slate-950/45 p-3">
        <div className="font-semibold text-white">Left rail</div>
        <p className="mt-1 leading-5">Pressure direction: bullish, bearish, neutral, or conflicted.</p>
      </div>
      <div className="rounded-xl border border-white/10 bg-slate-950/45 p-3">
        <div className="font-semibold text-white">Freshness</div>
        <p className="mt-1 leading-5">{data.latestSuccessfulDataAt ? `Latest successful batch: ${data.latestSuccessfulDataAt}` : "Latest successful batch: not available"}</p>
      </div>
    </div>
  );
}

function LayerAccessPanel({ data }: { data: MarketPressureMapResult }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-slate-950/45 p-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-white">Layer access</h2>
          <p className="mt-1 text-xs text-slate-400">Locked layers are not requested or exposed in the browser payload.</p>
        </div>
        <Link href="/pricing" prefetch={false} className="text-xs font-semibold text-emerald-200 hover:text-emerald-100 hover:underline">
          Compare plans
        </Link>
      </div>
      <div className="mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        {pressureOrder.map((key) => {
          const access = data.layerAccess[key];
          const label = layerStatusLabel(access, data.latestSuccessfulDataAt);
          return (
            <div key={key} className={`rounded-xl border px-3 py-2 ${layerStatusClass(access, data.latestSuccessfulDataAt)}`}>
              <div className="text-xs font-semibold text-white">{marketPressureLayerLabels[key]}</div>
              <div className="mt-1 text-[11px] font-semibold uppercase tracking-wide">{label}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ShareMapButton({ canonicalUrl }: { canonicalUrl: string }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    if (!open) return undefined;
    const onPointerDown = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  useEffect(() => {
    return () => {
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
    };
  }, []);

  async function copyLink() {
    try {
      await navigator.clipboard.writeText(canonicalUrl);
      setCopied(true);
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
      timerRef.current = window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  }

  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        className={`${subtlePrimaryButtonClassName} h-10 rounded-xl px-4`}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => {
          const nextOpen = !open;
          setOpen(nextOpen);
          if (nextOpen) recordProductEvent({ event_name: "market_pressure_share_opened" });
        }}
      >
        Share Map
      </button>
      {open ? (
        <div role="menu" aria-label="Share Market Pressure Map" className="absolute right-0 top-full z-50 mt-2 w-64 rounded-2xl border border-white/10 bg-slate-950/95 p-2 shadow-2xl shadow-black/50 ring-1 ring-white/[0.04]">
          <button type="button" role="menuitem" className={`${ghostButtonClassName} h-10 w-full justify-start rounded-xl px-3 text-xs`} onClick={copyLink}>
            {copied ? "Copied" : "Copy link"}
          </button>
          <button
            type="button"
            role="menuitem"
            disabled
            title="Download image will be enabled when the map renderer supports export."
            className={`${ghostButtonClassName} mt-2 h-10 w-full cursor-not-allowed justify-start rounded-xl px-3 text-xs opacity-50`}
          >
            Download image
          </button>
          <button
            type="button"
            role="menuitem"
            disabled
            title="Share to X will be enabled after public map snapshots are implemented."
            className={`${ghostButtonClassName} mt-2 h-10 w-full cursor-not-allowed justify-start rounded-xl px-3 text-xs opacity-50`}
          >
            Share to X
          </button>
        </div>
      ) : null}
    </div>
  );
}

export function MarketPressureMapClient({ initialData, canonicalUrl }: Props) {
  const [timeRange, setTimeRange] = useState<MarketPressureTimeRange>("1D");
  const [universe, setUniverse] = useState<MarketPressureUniverse>("sp500");
  const [viewMode, setViewMode] = useState<MarketPressureViewMode>("market-pressure");

  useEffect(() => {
    recordProductEvent({ event_name: "market_pressure_page_view" });
  }, []);

  const selectedUniverseLabel = marketPressureUniverses.find((option) => option.value === universe)?.label ?? "S&P 500";
  const selectedViewLabel = marketPressureViewModes.find((option) => option.value === viewMode)?.label ?? "Market Pressure";

  return (
    <div className="min-w-0 max-w-full space-y-6 overflow-x-hidden">
      <section className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
        <div className="min-w-0">
          <div className="text-xs font-semibold uppercase tracking-[0.25em] text-emerald-300/70">MARKET PRESSURE</div>
          <h1 className="mt-2 text-3xl font-semibold tracking-normal text-white sm:text-4xl">Market Pressure Map</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-300">
            See where bullish and bearish pressure is building before price fully reflects it.
          </p>
          <p className="mt-3 max-w-4xl text-sm leading-6 text-slate-400">
            Price performance tells you what moved. Market Pressure combines Walnut&apos;s confirmation layers to surface accumulation, deterioration, divergence, and changing market leadership.
          </p>
          <p className="mt-3 max-w-3xl text-xs font-semibold uppercase tracking-[0.18em] text-cyan-200/80">
            Most heatmaps show where the market has been. Walnut shows where pressure is building.
          </p>
        </div>
        <ShareMapButton canonicalUrl={canonicalUrl} />
      </section>

      <section className="rounded-2xl border border-white/10 bg-slate-900/55 p-4 shadow-card">
        <div className="grid gap-4 xl:grid-cols-[auto_auto_minmax(18rem,1fr)] xl:items-start">
          <div>
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Time range</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure time range">
              {marketPressureTimeRanges.map((option) => (
                <AnalyticsButton
                  key={option}
                  active={timeRange === option}
                  ariaLabel={`Set time range to ${option}`}
                  onClick={() => {
                    setTimeRange(option);
                    recordProductEvent({ event_name: "market_pressure_time_range_changed", properties: { time_range: option } });
                  }}
                >
                  {option}
                </AnalyticsButton>
              ))}
            </div>
          </div>
          <div>
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">Universe</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure universe">
              {marketPressureUniverses.map((option) => (
                <AnalyticsButton
                  key={option.value}
                  active={universe === option.value}
                  ariaLabel={`Set universe to ${option.label}`}
                  onClick={() => {
                    setUniverse(option.value);
                    recordProductEvent({ event_name: "market_pressure_universe_changed", properties: { universe: option.value } });
                  }}
                >
                  {option.label}
                </AnalyticsButton>
              ))}
            </div>
          </div>
          <div>
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-400">View mode</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure view mode">
              {marketPressureViewModes.map((option) => (
                <AnalyticsButton
                  key={option.value}
                  active={viewMode === option.value}
                  ariaLabel={`Set view mode to ${option.label}`}
                  onClick={() => {
                    setViewMode(option.value);
                    recordProductEvent({ event_name: "market_pressure_view_changed", properties: { view_mode: option.value } });
                  }}
                >
                  {option.label}
                </AnalyticsButton>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-2xl border border-white/10 bg-slate-900/60 p-4 shadow-card sm:p-5">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">{selectedViewLabel}</h2>
            <p className="mt-1 text-xs text-slate-400">
              {selectedUniverseLabel} - {timeRange} - Batch endpoint pending
            </p>
          </div>
          <div className="rounded-full border border-cyan-300/25 bg-cyan-300/10 px-3 py-1 text-xs font-semibold text-cyan-100">
            FAST, RELIABLE, COMPLETE data only
          </div>
        </div>
        <MarketPressureVisualization data={initialData} />
        <div className="mt-4">
          <MarketPressureLegend data={initialData} />
        </div>
      </section>

      <LayerAccessPanel data={initialData} />
    </div>
  );
}
