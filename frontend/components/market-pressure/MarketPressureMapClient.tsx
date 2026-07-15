"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import { recordProductEvent } from "@/lib/api";
import {
  marketPressureQueryString,
  marketPressureLayerLabels,
  marketPressureTimeRanges,
  marketPressureUniverses,
  marketPressureViewModes,
  periodToTimeRange,
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
  disabled,
  title,
}: {
  active: boolean;
  children: ReactNode;
  onClick: () => void;
  ariaLabel: string;
  disabled?: boolean;
  title?: string;
}) {
  return (
    <button type="button" className={segmentedButtonClass(active)} aria-pressed={active} aria-label={ariaLabel} disabled={disabled} title={title} onClick={onClick}>
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

function pressureDirectionClass(direction: MarketPressureTile["confirmationDirection"]) {
  if (direction === "bullish") return "border-l-emerald-300";
  if (direction === "bearish") return "border-l-rose-300";
  if (direction === "conflicted") return "border-l-amber-300";
  if (direction === "unavailable") return "border-l-slate-600";
  return "border-l-slate-400";
}

function formatPct(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "Price unavailable";
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatStrength(value: MarketPressureTile["confirmationStrength"]) {
  return value ? `${value} strength` : "Strength unavailable";
}

function formatScore(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "Score unavailable";
  return `${Math.round(value)} score`;
}

function marketPressureTickerHref(symbol: string) {
  const trimmed = symbol.trim().toUpperCase();
  return trimmed ? `/ticker/${encodeURIComponent(trimmed)}` : null;
}

function MarketPressureTileCard({ tile }: { tile: MarketPressureTile }) {
  const href = marketPressureTickerHref(tile.symbol);
  const body = (
    <div
      className={`min-h-[6.5rem] rounded-xl border border-l-4 p-3 ${priceDirectionClass(tile.priceChangePct)} ${pressureDirectionClass(tile.confirmationDirection)}`}
      aria-label={`${tile.symbol} price ${formatPct(tile.priceChangePct)}, confirmation ${tile.confirmationDirection}`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate font-mono text-sm font-semibold text-white">{tile.symbol}</div>
          <div className="mt-1 truncate text-[11px] text-slate-300">{tile.companyName ?? "Company unavailable"}</div>
        </div>
        <div className="shrink-0 text-right text-[11px] font-semibold text-slate-100">{formatPct(tile.priceChangePct)}</div>
      </div>
      <div className="mt-3 flex flex-wrap gap-1.5 text-[10px] font-semibold uppercase tracking-wide">
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-200">{tile.confirmationDirection}</span>
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-300">{formatScore(tile.confirmationScore)}</span>
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-300">{formatStrength(tile.confirmationStrength)}</span>
        <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-400">{tile.divergence}</span>
        {tile.dataState === "complete" ? null : (
          <span className="rounded-md border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-400">{tile.dataState}</span>
        )}
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
      title: "Market Pressure is available with Pro",
      body: message ?? "See where price movement and Walnut's complete confirmation stack are aligning, or diverging, across the market.",
    },
    unsupported: {
      title: "Unsupported Market Pressure request",
      body: message ?? "This universe or view needs canonical data that is not available yet.",
    },
    "auth-required": {
      title: "Sign in with Pro",
      body: message ?? "Market Pressure is a Pro feature and does not expose protected map data to logged-out users.",
    },
  }[state];

  return (
    <div className="flex min-h-[22rem] items-center justify-center rounded-2xl border border-dashed border-white/15 bg-slate-950/45 px-4 py-10 text-center" aria-live="polite" aria-busy={state === "loading"}>
      <div className="max-w-xl">
        <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-200/80">{state.replace("-", " ")}</p>
        <h2 className="mt-3 text-xl font-semibold text-white">{copy.title}</h2>
        <p className="mt-3 text-sm leading-6 text-slate-300">{copy.body}</p>
        {state === "entitlement" ? (
          <Link href="/pricing" prefetch={false} className={`${subtlePrimaryButtonClassName} mt-5 inline-flex h-10 rounded-xl px-4`}>
            Upgrade to Pro
          </Link>
        ) : null}
        {state === "auth-required" ? (
          <Link href="/login?return_to=%2Fmarket-pressure" prefetch={false} className={`${subtlePrimaryButtonClassName} mt-5 inline-flex h-10 rounded-xl px-4`}>
            Sign in
          </Link>
        ) : null}
      </div>
    </div>
  );
}

function MarketPressureVisualization({ data }: { data: MarketPressureMapResult }) {
  if (data.status !== "ready" || data.tiles.length === 0) {
    return <MarketPressureStatusState state={data.status === "ready" ? "no-data" : data.status} message={data.providerMessage} />;
  }

  return (
    <div className="space-y-6">
      {data.sectors.map((sectorGroup) => (
        <section key={sectorGroup.sector} aria-label={`${sectorGroup.sector} sector pressure`}>
          <div className="mb-3 flex items-center justify-between gap-3">
            <h2 className="text-sm font-semibold uppercase tracking-[0.18em] text-slate-300">{sectorGroup.sector}</h2>
            <span className="text-xs text-slate-500">{sectorGroup.summary.symbolCount} tickers</span>
          </div>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(9.5rem,1fr))] gap-3">
            {sectorGroup.tiles.map((tile) => (
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
        <p className="mt-1 leading-5">
          {data.latestSuccessfulDataAt ? `Latest batch: ${data.latestSuccessfulDataAt}. Confirmation window: ${data.confirmationFreshnessWindowDays} days.` : "Latest batch: not available"}
        </p>
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
          <p className="mt-1 text-xs text-slate-400">Pro and Admin responses use the complete canonical confirmation stack.</p>
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
  const router = useRouter();
  const [timeRange, setTimeRange] = useState<MarketPressureTimeRange>(periodToTimeRange(initialData.period));
  const [universe, setUniverse] = useState<MarketPressureUniverse>(initialData.universe);
  const [viewMode, setViewMode] = useState<MarketPressureViewMode>(initialData.view);

  useEffect(() => {
    recordProductEvent({ event_name: "market_pressure_page_view" });
  }, []);

  const selectedUniverseLabel = marketPressureUniverses.find((option) => option.value === universe)?.label ?? "S&P 500";
  const selectedViewLabel = marketPressureViewModes.find((option) => option.value === viewMode)?.label ?? "Market Pressure";
  const currentCanonicalUrl = `${canonicalUrl}?${marketPressureQueryString({ timeRange, universe, viewMode })}`;

  function updateQuery(next: Partial<{ timeRange: MarketPressureTimeRange; universe: MarketPressureUniverse; viewMode: MarketPressureViewMode }>) {
    const nextQuery = {
      timeRange: next.timeRange ?? timeRange,
      universe: next.universe ?? universe,
      viewMode: next.viewMode ?? viewMode,
    };
    setTimeRange(nextQuery.timeRange);
    setUniverse(nextQuery.universe);
    setViewMode(nextQuery.viewMode);
    router.push(`/market-pressure?${marketPressureQueryString(nextQuery)}`);
  }

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
        <ShareMapButton canonicalUrl={currentCanonicalUrl} />
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
                    updateQuery({ timeRange: option });
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
                  disabled={!initialData.capabilities.universes[option.value]}
                  title={!initialData.capabilities.universes[option.value] ? "This universe requires canonical membership data and is not available yet." : undefined}
                  onClick={() => {
                    updateQuery({ universe: option.value });
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
                  disabled={!initialData.capabilities.views[option.value]}
                  title={!initialData.capabilities.views[option.value] ? "Rotation and crowded-trades views require historical or positioning data that is not available yet." : undefined}
                  onClick={() => {
                    updateQuery({ viewMode: option.value });
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
              {selectedUniverseLabel} - {timeRange} - Canonical confirmation window {initialData.confirmationFreshnessWindowDays} days
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

      {initialData.status === "entitlement" || initialData.status === "auth-required" ? null : <LayerAccessPanel data={initialData} />}
    </div>
  );
}
