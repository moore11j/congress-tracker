"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties, KeyboardEvent, ReactNode } from "react";
import { AddTickerToWatchlist } from "@/components/watchlists/AddTickerToWatchlist";
import { WalnutModal } from "@/components/ui/WalnutModal";
import { recordProductEvent } from "@/lib/api";
import {
  marketPressureLayerLabels,
  marketPressureQueryString,
  marketPressureTimeRanges,
  marketPressureUniverses,
  marketPressureViewModes,
  periodToTimeRange,
  type MarketPressureLayer,
  type MarketPressureLayerKey,
  type MarketPressureMapResult,
  type MarketPressureSector,
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

type QueryState = {
  timeRange: MarketPressureTimeRange;
  universe: MarketPressureUniverse;
  viewMode: MarketPressureViewMode;
};

type TreemapRect = {
  x: number;
  y: number;
  width: number;
  height: number;
};

type TreemapWeightedItem<T> = {
  item: T;
  weight: number;
};

type TreemapLayoutItem<T> = TreemapWeightedItem<T> & {
  rect: TreemapRect;
};

const TREEMAP_ROOT_RECT: TreemapRect = { x: 0, y: 0, width: 100, height: 100 };

const divergenceLabel: Record<MarketPressureTile["divergence"], string> = {
  hidden_accumulation: "Accumulation",
  fragile_winner: "Fragile",
  aligned_bullish: "Aligned bullish",
  aligned_bearish: "Aligned bearish",
  conflicted: "Conflicted",
  none: "No divergence",
  unavailable: "Unavailable",
};

const directionGlyph: Record<MarketPressureTile["confirmationDirection"], string> = {
  bullish: "UP",
  bearish: "DN",
  neutral: "NEU",
  conflicted: "MIX",
  unavailable: "NA",
};

const layerOrder = Object.keys(marketPressureLayerLabels) as MarketPressureLayerKey[];

function segmentedButtonClass(active: boolean) {
  return [
    "min-h-9 rounded-md border px-3 py-1.5 text-xs font-semibold transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 disabled:cursor-not-allowed disabled:opacity-45",
    active
      ? "border-emerald-300/55 bg-emerald-300/15 text-emerald-100"
      : "border-white/10 bg-slate-950/45 text-slate-300 hover:border-white/20 hover:text-white",
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

function formatPct(value: number | null, compact = false) {
  if (value == null || !Number.isFinite(value)) return compact ? "NA" : "Price unavailable";
  return `${value > 0 ? "+" : ""}${value.toFixed(compact ? 1 : 2)}%`;
}

function formatScore(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "NA";
  return `${Math.round(value)}`;
}

function formatDate(value: string | null) {
  if (!value) return "not available";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function statusTitle(status: string) {
  return status.replaceAll("_", " ").replaceAll("-", " ");
}

function priceFillClass(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "bg-[repeating-linear-gradient(135deg,rgba(51,65,85,0.82)_0,rgba(51,65,85,0.82)_6px,rgba(15,23,42,0.92)_6px,rgba(15,23,42,0.92)_12px)] text-slate-100";
  if (value >= 6) return "bg-[#00a64a] text-white";
  if (value >= 4) return "bg-[#09b850] text-white";
  if (value >= 3) return "bg-[#18c861] text-white";
  if (value >= 2) return "bg-[#2fbd63] text-white";
  if (value >= 1) return "bg-[#2f8f59] text-white";
  if (value >= 0.5) return "bg-[#376f5d] text-white";
  if (value > 0.15) return "bg-[#3d5f5d] text-white";
  if (value <= -6) return "bg-[#ff202f] text-white";
  if (value <= -4) return "bg-[#f02c37] text-white";
  if (value <= -3) return "bg-[#df3542] text-white";
  if (value <= -2) return "bg-[#be3e4d] text-white";
  if (value <= -1) return "bg-[#8f4555] text-white";
  if (value <= -0.5) return "bg-[#6d4655] text-white";
  if (value < -0.15) return "bg-[#584856] text-white";
  return "bg-[#35495a] text-slate-100";
}

function confirmationFrameClass(tile: MarketPressureTile) {
  const width = tile.confirmationStrength === "strong" ? "border-[3px]" : tile.confirmationStrength === "moderate" ? "border-2" : "border";
  if (tile.confirmationDirection === "bullish") return `${width} border-emerald-200 shadow-[inset_0_0_0_1px_rgba(16,185,129,0.26)]`;
  if (tile.confirmationDirection === "bearish") return `${width} border-rose-200 shadow-[inset_0_0_0_1px_rgba(244,63,94,0.22)]`;
  if (tile.confirmationDirection === "conflicted") return `${width} border-dashed border-amber-200 shadow-[inset_0_0_0_1px_rgba(251,191,36,0.20)]`;
  if (tile.confirmationDirection === "unavailable") return `${width} border-dotted border-slate-500`;
  return `${width} border-slate-300/65`;
}

function divergenceRank(tile: MarketPressureTile) {
  if (tile.divergence === "hidden_accumulation") return 0;
  if (tile.divergence === "fragile_winner") return 1;
  if (tile.divergence === "conflicted") return 2;
  if (tile.divergence === "aligned_bullish" || tile.divergence === "aligned_bearish") return 3;
  if (tile.confirmationDirection === "neutral") return 4;
  if (tile.dataState !== "complete") return 6;
  return 5;
}

function tileSortKey(tile: MarketPressureTile) {
  const score = tile.confirmationScore ?? -1;
  const priceMagnitude = Math.abs(tile.priceChangePct ?? 0);
  return [divergenceRank(tile), -(score), -priceMagnitude, tile.symbol] as const;
}

function compareTiles(a: MarketPressureTile, b: MarketPressureTile) {
  const left = tileSortKey(a);
  const right = tileSortKey(b);
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] < right[index]) return -1;
    if (left[index] > right[index]) return 1;
  }
  return 0;
}

function tileWeight(tile: MarketPressureTile) {
  return typeof tile.marketCap === "number" && Number.isFinite(tile.marketCap) && tile.marketCap > 0 ? tile.marketCap : 1_000_000_000;
}

function sectorWeight(sector: MarketPressureSector) {
  return Math.max(1, sector.tiles.reduce((total, tile) => total + tileWeight(tile), 0));
}

function sortSectors(sectors: MarketPressureSector[]) {
  return [...sectors]
    .map((sector) => ({ ...sector, tiles: [...sector.tiles].sort(compareTiles) }))
    .sort((a, b) => {
      const divergenceDelta = b.summary.divergenceCount - a.summary.divergenceCount;
      if (divergenceDelta !== 0) return divergenceDelta;
      const balanceA = Math.abs(a.summary.bullishCount - a.summary.bearishCount);
      const balanceB = Math.abs(b.summary.bullishCount - b.summary.bearishCount);
      if (balanceB !== balanceA) return balanceB - balanceA;
      return a.sector.localeCompare(b.sector);
    });
}

function sumTreemapWeight<T>(items: TreemapWeightedItem<T>[]) {
  return items.reduce((total, item) => total + Math.max(0, item.weight), 0);
}

function layoutTreemap<T>(items: TreemapWeightedItem<T>[], rect: TreemapRect = TREEMAP_ROOT_RECT): TreemapLayoutItem<T>[] {
  const validItems = items.filter((item) => item.weight > 0);
  if (validItems.length === 0) return [];
  if (validItems.length === 1) return [{ ...validItems[0], rect }];

  const totalWeight = sumTreemapWeight(validItems);
  if (totalWeight <= 0) return [];

  let runningWeight = 0;
  let splitIndex = 1;
  let bestDelta = Number.POSITIVE_INFINITY;
  for (let index = 1; index < validItems.length; index += 1) {
    runningWeight += validItems[index - 1].weight;
    const delta = Math.abs(totalWeight / 2 - runningWeight);
    if (delta < bestDelta) {
      bestDelta = delta;
      splitIndex = index;
    }
  }

  const firstItems = validItems.slice(0, splitIndex);
  const secondItems = validItems.slice(splitIndex);
  const firstWeight = sumTreemapWeight(firstItems);
  const firstRatio = firstWeight / totalWeight;

  if (rect.width >= rect.height) {
    const firstWidth = rect.width * firstRatio;
    return [
      ...layoutTreemap(firstItems, { x: rect.x, y: rect.y, width: firstWidth, height: rect.height }),
      ...layoutTreemap(secondItems, { x: rect.x + firstWidth, y: rect.y, width: rect.width - firstWidth, height: rect.height }),
    ];
  }

  const firstHeight = rect.height * firstRatio;
  return [
    ...layoutTreemap(firstItems, { x: rect.x, y: rect.y, width: rect.width, height: firstHeight }),
    ...layoutTreemap(secondItems, { x: rect.x, y: rect.y + firstHeight, width: rect.width, height: rect.height - firstHeight }),
  ];
}

function rectStyle(rect: TreemapRect): CSSProperties {
  return {
    left: `${rect.x}%`,
    top: `${rect.y}%`,
    width: `${rect.width}%`,
    height: `${rect.height}%`,
  };
}

function tickerHref(symbol: string) {
  const trimmed = symbol.trim().toUpperCase();
  return trimmed ? `/ticker/${encodeURIComponent(trimmed)}` : "/screener";
}

function explainTile(tile: MarketPressureTile, period: MarketPressureTimeRange) {
  const priceText = tile.priceChangePct == null ? "has unavailable price performance" : `is ${tile.priceChangePct >= 0 ? "up" : "down"} ${Math.abs(tile.priceChangePct).toFixed(2)}% over ${period}`;
  const scoreText = tile.confirmationScore == null ? "with unavailable confirmation" : `with Walnut confirmation at ${Math.round(tile.confirmationScore)}/100`;
  if (tile.divergence === "hidden_accumulation") return `${tile.symbol} ${priceText} while Walnut's complete confirmation stack remains bullish ${tile.confirmationScore == null ? "" : `at ${Math.round(tile.confirmationScore)}/100`}.`;
  if (tile.divergence === "fragile_winner") return `${tile.symbol} ${priceText}, but confirmation is ${tile.confirmationDirection}, marking a fragile winner.`;
  if (tile.confirmationDirection === "conflicted") return `${tile.symbol} ${priceText} with mixed evidence across the confirmation stack.`;
  return `${tile.symbol} ${priceText} ${scoreText} and ${tile.confirmationDirection} direction.`;
}

function accessibleTileLabel(tile: MarketPressureTile, period: MarketPressureTimeRange) {
  const priceDirection = tile.priceChangePct == null ? "price unavailable" : `price ${tile.priceChangePct >= 0 ? "up" : "down"} ${Math.abs(tile.priceChangePct).toFixed(2)} percent over ${period}`;
  const score = tile.confirmationScore == null ? "Confirmation Score unavailable" : `Confirmation Score ${Math.round(tile.confirmationScore)}`;
  return `${tile.symbol}, ${tile.sector}, ${priceDirection}, ${tile.confirmationDirection} confirmation, ${score}, ${divergenceLabel[tile.divergence]}.`;
}

function capabilityReason(data: MarketPressureMapResult) {
  const details = data.capabilities.universeDetails?.[data.universe];
  if (!details) return data.providerMessage;
  if (details.status === "stale") return `${marketPressureUniverses.find((item) => item.value === data.universe)?.label ?? "Universe"} membership is stale. Source as of ${details.sourceAsOf ?? "unknown"}.`;
  if (details.status === "unavailable") return `${marketPressureUniverses.find((item) => item.value === data.universe)?.label ?? "Universe"} membership is unavailable.`;
  return data.providerMessage;
}

function statusCopy(data: MarketPressureMapResult) {
  if (data.status === "entitlement") return { title: "Market Pressure is available with Pro", body: data.providerMessage ?? "Upgrade to see the complete confirmation map." };
  if (data.status === "auth-required") return { title: "Sign in with Pro", body: data.providerMessage ?? "Market Pressure is protected and does not expose map data to logged-out users." };
  if (data.status === "unsupported") return { title: "Canonical universe unavailable", body: capabilityReason(data) ?? "This request needs data that is not available yet." };
  if (data.status === "error") return { title: "Market Pressure could not load", body: data.providerMessage ?? "The backend request failed. No synthetic tiles were substituted." };
  if (data.status === "loading") return { title: "Loading market pressure", body: "Fetching the latest authorized Market Pressure payload." };
  if (data.universe === "watchlist" && data.summary.symbolCount === 0) return { title: "No watchlist tickers yet", body: "Add tickers to a watchlist to render your personal Market Pressure map." };
  if (data.view === "hidden_accumulation") return { title: "No Hidden Accumulation names", body: "No backend-classified Hidden Accumulation names qualified for this universe and period." };
  if (data.view === "fragile_winners") return { title: "No Fragile Winners", body: "No backend-classified Fragile Winner names qualified for this universe and period." };
  return { title: "No Market Pressure data", body: capabilityReason(data) ?? "The map returned no tiles. Partial and unavailable symbols are not hidden when they exist." };
}

function unavailableUniverseNotice(data: MarketPressureMapResult) {
  const warning = data.warnings.find((item) => item.startsWith("requested_universe_unavailable:"));
  if (!warning) return null;
  const requested = warning.split(":")[1] as MarketPressureUniverse | undefined;
  const requestedLabel = marketPressureUniverses.find((item) => item.value === requested)?.label ?? "Requested universe";
  const selectedLabel = marketPressureUniverses.find((item) => item.value === data.universe)?.label ?? "Watchlist";
  return `${requestedLabel} is unavailable, so Walnut opened ${selectedLabel}. Index membership data is temporarily unavailable.`;
}

function disabledUniverseTitle(option: MarketPressureUniverse, data: MarketPressureMapResult) {
  if (option === "all_us") return "The complete US equity universe is not available yet.";
  if (option === "etf") return "ETF universe data is temporarily unavailable.";
  const details = data.capabilities.universeDetails?.[option];
  if (details?.status === "unavailable") return "Index membership data is temporarily unavailable.";
  return "This universe is unavailable.";
}

function MarketPressureStatusState({ data }: { data: MarketPressureMapResult }) {
  const copy = statusCopy(data);
  return (
    <div className="flex min-h-[21rem] items-center justify-center rounded-md border border-dashed border-white/15 bg-slate-950/45 px-4 py-10 text-center" aria-live="polite" aria-busy={data.status === "loading"}>
      <div className="max-w-xl">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-200/80">{data.status.replace("-", " ")}</p>
        <h2 className="mt-3 text-xl font-semibold text-white">{copy.title}</h2>
        <p className="mt-3 text-sm leading-6 text-slate-300">{copy.body}</p>
        {data.status === "entitlement" ? (
          <Link href="/pricing" prefetch={false} className={`${subtlePrimaryButtonClassName} mt-5 inline-flex h-10 rounded-md px-4`}>
            Upgrade to Pro
          </Link>
        ) : null}
        {data.universe === "watchlist" && data.summary.symbolCount === 0 && data.status === "no-data" ? (
          <Link href="/watchlists" prefetch={false} className={`${subtlePrimaryButtonClassName} mt-5 inline-flex h-10 rounded-md px-4`}>
            Manage Watchlists
          </Link>
        ) : null}
        {data.status === "auth-required" ? (
          <Link href="/login?return_to=%2Fmarket-pressure" prefetch={false} className={`${subtlePrimaryButtonClassName} mt-5 inline-flex h-10 rounded-md px-4`}>
            Sign in
          </Link>
        ) : null}
      </div>
    </div>
  );
}

function MarketSummaryStrip({ data }: { data: MarketPressureMapResult }) {
  const partialUnavailable = data.summary.partialCount + data.summary.unavailableCount;
  const universeDetails = data.capabilities.universeDetails?.[data.universe];
  const items = [
    ["Bullish", data.summary.bullishCount],
    ["Bearish", data.summary.bearishCount],
    ["Conflicted", data.summary.conflictedCount],
    ["Hidden Accumulation", data.summary.hiddenAccumulationCount],
    ["Fragile Winners", data.summary.fragileWinnerCount],
    ["Partial / Unavailable", partialUnavailable],
  ] as const;
  return (
    <section className="grid gap-2 border-y border-white/10 bg-slate-950/35 px-1 py-3 md:grid-cols-[1fr_auto] md:items-center">
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-6">
        {items.map(([label, value]) => (
          <div key={label} className="min-w-0 rounded-md border border-white/10 bg-slate-900/55 px-3 py-2">
            <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">{label}</div>
            <div className="mt-1 text-lg font-semibold text-white">{value}</div>
          </div>
        ))}
      </div>
      <div className="text-xs leading-5 text-slate-400 md:max-w-xs md:text-right">
        <div>Price as of {formatDate(data.priceAsOf)}</div>
        <div>Confirmation as of {formatDate(data.confirmationAsOf)}</div>
        {universeDetails?.sourceLabel ? <div>Source: {universeDetails.sourceLabel}</div> : null}
        {universeDetails?.sourceAsOf ? <div>Membership source as of {formatDate(universeDetails.sourceAsOf)}</div> : null}
        {universeDetails?.status === "stale" ? <div className="font-semibold text-amber-200">Membership data is stale</div> : null}
      </div>
    </section>
  );
}

function TileMarkers({ tile }: { tile: MarketPressureTile }) {
  if (tile.divergence === "hidden_accumulation") {
    return <span className="rounded-sm border border-emerald-100/70 bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-emerald-100">Accumulation</span>;
  }
  if (tile.divergence === "fragile_winner") {
    return <span className="rounded-sm border border-amber-100/70 bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-amber-100">Fragile</span>;
  }
  if (tile.divergence === "conflicted") {
    return <span className="rounded-sm border border-amber-100/50 bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-amber-100">Mixed</span>;
  }
  return null;
}

function MarketTile({
  tile,
  period,
  onOpen,
  rect,
}: {
  tile: MarketPressureTile;
  period: MarketPressureTimeRange;
  onOpen: (tile: MarketPressureTile) => void;
  rect?: TreemapRect;
}) {
  const label = accessibleTileLabel(tile, period);
  const compact = rect ? rect.width < 8 || rect.height < 7 : false;
  const micro = rect ? rect.width < 4 || rect.height < 5 : false;
  const feature = rect ? rect.width >= 16 && rect.height >= 14 : false;
  const hero = rect ? rect.width >= 24 && rect.height >= 20 : false;
  const tileClassName = rect
    ? `group absolute flex flex-col overflow-hidden rounded-none px-1.5 py-1 text-left shadow-none transition hover:z-20 hover:brightness-110 focus-visible:z-30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/80 focus-visible:ring-offset-0 ${feature ? "justify-center" : "justify-start"} ${priceFillClass(tile.priceChangePct)} ${confirmationFrameClass(tile)}`
    : `group relative min-h-[5.7rem] overflow-hidden rounded-md p-2 text-left shadow-sm transition hover:-translate-y-0.5 hover:shadow-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/70 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${priceFillClass(tile.priceChangePct)} ${confirmationFrameClass(tile)}`;
  const handleKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onOpen(tile);
    }
  };
  return (
    <button
      type="button"
      className={tileClassName}
      style={rect ? rectStyle(rect) : undefined}
      data-treemap-tile={rect ? "true" : undefined}
      aria-label={label}
      title={`${label} ${explainTile(tile, period)}`}
      onClick={() => onOpen(tile)}
      onKeyDown={handleKeyDown}
    >
      <span className={`pointer-events-none absolute right-1 top-1 rounded-sm bg-slate-950/55 px-1 py-0.5 text-[8px] font-bold uppercase tracking-wide text-white/85 ${compact ? "hidden" : ""}`}>
        {directionGlyph[tile.confirmationDirection]}
      </span>
      <span className={`${micro ? "text-[9px]" : compact ? "text-xs" : hero ? "text-4xl" : feature ? "text-3xl" : "text-base"} block max-w-full truncate text-center font-mono font-black leading-tight tracking-normal drop-shadow-[0_1px_1px_rgba(0,0,0,0.7)]`}>{tile.symbol}</span>
      <span className={`${micro ? "hidden" : compact ? "mt-0.5 text-[10px]" : hero ? "mt-1 text-xl" : feature ? "mt-1 text-lg" : "mt-0.5 text-sm"} block text-center font-black leading-tight drop-shadow-[0_1px_1px_rgba(0,0,0,0.7)]`}>{formatPct(tile.priceChangePct, true)}</span>
      <span className={`${compact || feature ? "hidden" : "mt-1 block text-[10px]"} text-center font-semibold text-white/80`}>CS {formatScore(tile.confirmationScore)}</span>
      <span className={`${compact || feature ? "hidden" : "mt-1 flex"} min-h-5 flex-wrap gap-1`}>
        <TileMarkers tile={tile} />
        {tile.dataState === "complete" ? null : <span className="rounded-sm bg-slate-950/65 px-1.5 py-0.5 text-[9px] font-bold uppercase text-slate-200">{tile.dataState}</span>}
      </span>
    </button>
  );
}

function SectorMap({
  sectors,
  period,
  onOpen,
}: {
  sectors: MarketPressureSector[];
  period: MarketPressureTimeRange;
  onOpen: (tile: MarketPressureTile) => void;
}) {
  const sectorLayouts = useMemo(
    () =>
      layoutTreemap(sectors.map((sectorGroup) => ({ item: sectorGroup, weight: sectorWeight(sectorGroup) }))).map((sectorLayout) => ({
        ...sectorLayout,
        tileLayouts: layoutTreemap(sectorLayout.item.tiles.map((tile) => ({ item: tile, weight: tileWeight(tile) }))),
      })),
    [sectors],
  );

  return (
    <div className="relative min-h-[34rem] overflow-hidden rounded-md border border-slate-950 bg-slate-950 shadow-inner sm:min-h-[42rem] xl:min-h-[48rem]" data-market-pressure-map data-sector-treemap>
      {sectorLayouts.map(({ item: sectorGroup, rect, tileLayouts }) => {
        const showHeader = rect.width >= 8 && rect.height >= 7;
        return (
          <section key={sectorGroup.sector} className="absolute overflow-hidden border border-slate-950 bg-slate-900/50" style={rectStyle(rect)} aria-label={`${sectorGroup.sector} sector pressure`}>
            {showHeader ? (
              <div className="absolute inset-x-0 top-0 z-10 flex h-5 min-w-0 items-center justify-between gap-2 border-b border-slate-950 bg-slate-800/85 px-1.5 text-[9px] font-bold uppercase tracking-normal text-slate-100">
                <span className="truncate">{sectorGroup.sector}</span>
                <span className="shrink-0 text-slate-300">{formatPct(sectorGroup.summary.averagePriceChangePct, true)}</span>
              </div>
            ) : null}
            <div className={showHeader ? "absolute inset-x-0 bottom-0 top-5" : "absolute inset-0"}>
              {tileLayouts.map(({ item: tile, rect: tileRect }) => (
                <MarketTile key={`${tile.sector}:${tile.symbol}`} tile={tile} period={period} onOpen={onOpen} rect={tileRect} />
              ))}
            </div>
            {showHeader ? (
              <div className="pointer-events-none absolute bottom-1 left-1 z-10 hidden rounded-sm bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-semibold text-slate-200 sm:block">
                {sectorGroup.summary.symbolCount} names
              </div>
            ) : null}
          </section>
        );
      })}
    </div>
  );
}

function MarketPressureLegend() {
  const legendItems = [
    ["Tile colour", "Selected-period price performance"],
    ["Tile border", "Confirmation direction"],
    ["Border weight", "Weak, moderate, or strong confirmation"],
    ["Dashed border", "Conflicted evidence"],
    ["Accumulation", "Price weak, confirmation bullish"],
    ["Fragile", "Price strong, confirmation bearish or conflicted"],
  ];
  return (
    <div className="grid gap-2 text-xs text-slate-300 md:grid-cols-3 xl:grid-cols-6">
      {legendItems.map(([label, body]) => (
        <div key={label} className="rounded-md border border-white/10 bg-slate-950/40 px-3 py-2">
          <div className="font-semibold text-white">{label}</div>
          <p className="mt-1 leading-5">{body}</p>
        </div>
      ))}
    </div>
  );
}

function EvidenceRow({ label, layer }: { label: string; layer: MarketPressureLayer }) {
  const tone = layer.status === "available" ? "text-emerald-200" : layer.status === "stale" ? "text-amber-200" : "text-slate-400";
  return (
    <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-3 border-b border-white/10 py-2 last:border-b-0">
      <div className="min-w-0">
        <div className="truncate text-sm font-semibold text-white">{label}</div>
        <div className={`mt-0.5 text-xs ${tone}`}>{statusTitle(layer.status)}{layer.direction ? ` - ${layer.direction}` : ""}</div>
      </div>
      <div className="text-right text-xs text-slate-400">
        {layer.contribution == null ? "" : `${layer.contribution}`}
        <div>{formatDate(layer.asOf ?? null)}</div>
      </div>
    </div>
  );
}

function TickerFlyout({
  tile,
  period,
  onClose,
}: {
  tile: MarketPressureTile | null;
  period: MarketPressureTimeRange;
  onClose: () => void;
}) {
  useEffect(() => {
    if (!tile) return;
    recordProductEvent({ event_name: "market_pressure_ticker_opened", properties: { symbol: tile.symbol } });
  }, [tile]);

  if (!tile) return null;
  return (
    <WalnutModal
      open={Boolean(tile)}
      onClose={() => {
        recordProductEvent({ event_name: "market_pressure_ticker_closed", properties: { symbol: tile.symbol } });
        onClose();
      }}
      title={tile.symbol}
      eyebrow={tile.sector}
      description={tile.companyName ?? "Company name unavailable"}
      panelClassName="max-w-2xl"
    >
      <div className="space-y-5">
        <div className="grid gap-3 sm:grid-cols-4">
          <Metric label={`${period} price`} value={formatPct(tile.priceChangePct)} />
          <Metric label="Confirmation" value={tile.confirmationScore == null ? "NA" : `${Math.round(tile.confirmationScore)}/100`} />
          <Metric label="Direction" value={statusTitle(tile.confirmationDirection)} />
          <Metric label="Divergence" value={divergenceLabel[tile.divergence]} />
        </div>
        <div className="rounded-md border border-white/10 bg-slate-950/45 p-4">
          <h3 className="text-sm font-semibold text-white">Why it stands out</h3>
          <p className="mt-2 text-sm leading-6 text-slate-300">{explainTile(tile, period)}</p>
          <p className="mt-2 text-xs text-slate-500">
            Price as of {formatDate(tile.priceEndAt)}. Confirmation as of {formatDate(tile.confirmationAsOf)}. Data state: {statusTitle(tile.dataState)}.
          </p>
        </div>
        <div className="rounded-md border border-white/10 bg-slate-950/45 p-4">
          <h3 className="text-sm font-semibold text-white">Evidence summary</h3>
          <div className="mt-2">
            {layerOrder.map((key) => (
              <EvidenceRow key={key} label={marketPressureLayerLabels[key]} layer={tile.layers[key]} />
            ))}
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <Link href={tickerHref(tile.symbol)} prefetch={false} className={`${subtlePrimaryButtonClassName} h-10 rounded-md px-4`}>
            Open full ticker page
          </Link>
          <AddTickerToWatchlist symbol={tile.symbol} variant="compact" align="left" />
          <button
            type="button"
            className={`${ghostButtonClassName} h-10 rounded-md px-4`}
            onClick={() => navigator.clipboard?.writeText(explainTile(tile, period)).catch(() => undefined)}
          >
            Copy ticker insight
          </button>
        </div>
      </div>
    </WalnutModal>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-white/10 bg-slate-900/55 px-3 py-2">
      <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-500">{label}</div>
      <div className="mt-1 truncate text-sm font-semibold text-white">{value}</div>
    </div>
  );
}

function svgEscape(value: string | number | null | undefined) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
}

function priceFillHex(value: number | null) {
  if (value == null) return "#334155";
  if (value >= 6) return "#00a64a";
  if (value >= 4) return "#09b850";
  if (value >= 3) return "#18c861";
  if (value >= 2) return "#2fbd63";
  if (value >= 1) return "#2f8f59";
  if (value >= 0.5) return "#376f5d";
  if (value > 0.15) return "#3d5f5d";
  if (value <= -6) return "#ff202f";
  if (value <= -4) return "#f02c37";
  if (value <= -3) return "#df3542";
  if (value <= -2) return "#be3e4d";
  if (value <= -1) return "#8f4555";
  if (value <= -0.5) return "#6d4655";
  if (value < -0.15) return "#584856";
  return "#35495a";
}

function tileSvg(tile: MarketPressureTile, x: number, y: number, width: number, height: number) {
  const fill = priceFillHex(tile.priceChangePct);
  const stroke = tile.confirmationDirection === "bullish" ? "#6ee7b7" : tile.confirmationDirection === "bearish" ? "#fda4af" : tile.confirmationDirection === "conflicted" ? "#fde68a" : "#94a3b8";
  const dash = tile.confirmationDirection === "conflicted" || tile.confirmationDirection === "unavailable" ? ` stroke-dasharray="5 3"` : "";
  const marker = tile.divergence === "hidden_accumulation" ? "ACC" : tile.divergence === "fragile_winner" ? "FRG" : "";
  const showPrice = width >= 42 && height >= 32;
  const showScore = width >= 70 && height >= 54;
  const fontSize = width >= 120 && height >= 72 ? 30 : width >= 90 && height >= 54 ? 22 : width >= 48 && height >= 28 ? 13 : 9;
  const textAnchor = width >= 90 && height >= 54 ? "middle" : "start";
  const labelX = textAnchor === "middle" ? x + width / 2 : x + 6;
  const labelY = textAnchor === "middle" ? y + height / 2 - 2 : y + Math.max(12, fontSize + 4);
  const priceY = textAnchor === "middle" ? labelY + Math.max(14, fontSize * 0.68) : y + fontSize + 22;
  return `<g><rect x="${x}" y="${y}" width="${width}" height="${height}" rx="0" fill="${fill}" stroke="${stroke}" stroke-width="${tile.confirmationStrength === "strong" ? 3 : 1.5}"${dash}/><text x="${labelX}" y="${labelY}" text-anchor="${textAnchor}" fill="#fff" font-size="${fontSize}" font-weight="800" font-family="Arial">${svgEscape(tile.symbol)}</text>${showPrice ? `<text x="${labelX}" y="${priceY}" text-anchor="${textAnchor}" fill="#f8fafc" font-size="${Math.max(9, Math.round(fontSize * 0.56))}" font-weight="700" font-family="Arial">${svgEscape(formatPct(tile.priceChangePct, true))}</text>` : ""}${showScore && textAnchor === "start" ? `<text x="${x + 6}" y="${y + fontSize + 40}" fill="#cbd5e1" font-size="11" font-family="Arial">CS ${svgEscape(formatScore(tile.confirmationScore))}</text>` : ""}${marker && width >= 58 && height >= 28 ? `<text x="${x + width - 30}" y="${y + 15}" fill="#fff7ed" font-size="9" font-weight="700" font-family="Arial">${marker}</text>` : ""}</g>`;
}

function renderShareSvg(data: MarketPressureMapResult, sectors: MarketPressureSector[], query: QueryState) {
  const width = 1200;
  const height = 675;
  const mapX = 44;
  const mapY = 150;
  const mapWidth = 1112;
  const mapHeight = 432;
  const universe = marketPressureUniverses.find((item) => item.value === query.universe)?.label ?? "S&P 500";
  const view = marketPressureViewModes.find((item) => item.value === query.viewMode)?.label ?? "Market Pressure";
  const sectorMarkup = layoutTreemap(sectors.map((sector) => ({ item: sector, weight: sectorWeight(sector) })))
    .map(({ item: sector, rect }) => {
      const x = mapX + (rect.x / 100) * mapWidth;
      const y = mapY + (rect.y / 100) * mapHeight;
      const sectorWidth = (rect.width / 100) * mapWidth;
      const sectorHeight = (rect.height / 100) * mapHeight;
      const showHeader = sectorWidth >= 70 && sectorHeight >= 40;
      const headerHeight = showHeader ? 18 : 0;
      const tileHeight = Math.max(0, sectorHeight - headerHeight);
      const tiles = layoutTreemap(sector.tiles.map((tile) => ({ item: tile, weight: tileWeight(tile) })));
      const tileMarkup = tiles
        .map(({ item: tile, rect: tileRect }) => tileSvg(tile, x + (tileRect.x / 100) * sectorWidth, y + headerHeight + (tileRect.y / 100) * tileHeight, (tileRect.width / 100) * sectorWidth, (tileRect.height / 100) * tileHeight))
        .join("");
      const headerMarkup = showHeader
        ? `<rect x="${x}" y="${y}" width="${sectorWidth}" height="${headerHeight}" fill="#1e293b"/><text x="${x + 6}" y="${y + 13}" fill="#e2e8f0" font-size="10" font-weight="700" font-family="Arial">${svgEscape(sector.sector)} ${svgEscape(formatPct(sector.summary.averagePriceChangePct, true))}</text>`
        : "";
      return `<g><rect x="${x}" y="${y}" width="${sectorWidth}" height="${sectorHeight}" fill="#020617" stroke="#020617" stroke-width="2"/>${headerMarkup}${tileMarkup}</g>`;
    })
    .join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}"><rect width="${width}" height="${height}" fill="#020617"/><text x="44" y="54" fill="#ecfdf5" font-size="28" font-weight="800" font-family="Arial">Walnut Market Pressure Map</text><text x="44" y="86" fill="#94a3b8" font-size="15" font-family="Arial">${svgEscape(universe)} - ${svgEscape(query.timeRange)} - ${svgEscape(view)} - Generated ${svgEscape(formatDate(data.generatedAt))}</text><text x="44" y="120" fill="#67e8f9" font-size="16" font-weight="700" font-family="Arial">Most heatmaps show where the market has been. Walnut shows where pressure is building.</text><g>${sectorMarkup}</g><rect x="44" y="604" width="1112" height="1" fill="#1e293b"/><text x="44" y="636" fill="#cbd5e1" font-size="14" font-family="Arial">Tile colour = price performance. Border = Walnut confirmation direction and strength. ACC = hidden accumulation. FRG = fragile winner.</text><text x="1018" y="636" fill="#34d399" font-size="16" font-weight="800" font-family="Arial">walnutmarkets.com</text></svg>`;
}

function xShareText(data: MarketPressureMapResult, query: QueryState) {
  const universe = marketPressureUniverses.find((item) => item.value === query.universe)?.label ?? "the market";
  if (query.viewMode === "hidden_accumulation") return `Hidden accumulation across ${universe}: ${data.summary.hiddenAccumulationCount} names where price is weak but Walnut confirmation remains bullish.`;
  if (query.viewMode === "fragile_winners") return `Fragile winners across ${universe}: ${data.summary.fragileWinnerCount} names where price strength is diverging from Walnut confirmation.`;
  return `Market Pressure across ${universe}: ${data.summary.hiddenAccumulationCount} hidden accumulation names and ${data.summary.fragileWinnerCount} fragile winners over ${query.timeRange}.`;
}

function ShareMapButton({
  data,
  sectors,
  shareUrl,
  query,
}: {
  data: MarketPressureMapResult;
  sectors: MarketPressureSector[];
  shareUrl: string;
  query: QueryState;
}) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    if (!open) return undefined;
    const onPointerDown = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const onKeyDown = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  useEffect(() => () => {
    if (timerRef.current !== null) window.clearTimeout(timerRef.current);
  }, []);

  const copyLink = useCallback(async () => {
    setError(null);
    try {
      await navigator.clipboard.writeText(shareUrl);
      setCopied(true);
      recordProductEvent({ event_name: "market_pressure_link_copied", properties: { universe: query.universe, view: query.viewMode, period: query.timeRange } });
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
      timerRef.current = window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setError("Could not copy link.");
    }
  }, [query.timeRange, query.universe, query.viewMode, shareUrl]);

  const downloadImage = useCallback(() => {
    setError(null);
    try {
      const svg = renderShareSvg(data, sectors, query);
      const blob = new Blob([svg], { type: "image/svg+xml;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `walnut-market-pressure-${query.universe}-${query.timeRange.toLowerCase()}-${query.viewMode}.svg`;
      document.body.append(anchor);
      anchor.click();
      anchor.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      recordProductEvent({ event_name: "market_pressure_image_downloaded", properties: { universe: query.universe, view: query.viewMode, period: query.timeRange } });
    } catch {
      setError("Share export failed.");
    }
  }, [data, query, sectors]);

  const shareToX = useCallback(() => {
    const intent = new URL("https://twitter.com/intent/tweet");
    intent.searchParams.set("url", shareUrl);
    intent.searchParams.set("text", xShareText(data, query));
    recordProductEvent({ event_name: "market_pressure_x_share_opened", properties: { universe: query.universe, view: query.viewMode, period: query.timeRange } });
    window.open(intent.toString(), "_blank", "noopener,noreferrer");
  }, [data, query, shareUrl]);

  const canExport = data.status === "ready" && data.tiles.length > 0;
  return (
    <div ref={containerRef} className="relative">
      <button
        type="button"
        className={`${subtlePrimaryButtonClassName} h-10 rounded-md px-4`}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => {
          const nextOpen = !open;
          setOpen(nextOpen);
          if (nextOpen) recordProductEvent({ event_name: "market_pressure_share_opened", properties: { universe: query.universe, view: query.viewMode, period: query.timeRange } });
        }}
      >
        Share Map
      </button>
      {open ? (
        <div role="menu" aria-label="Share Market Pressure Map" className="absolute right-0 top-full z-50 mt-2 w-72 rounded-md border border-white/10 bg-slate-950/95 p-2 shadow-2xl shadow-black/50 ring-1 ring-white/[0.04]">
          <button type="button" role="menuitem" className={`${ghostButtonClassName} h-10 w-full justify-start rounded-md px-3 text-xs`} onClick={copyLink}>
            {copied ? "Copied" : "Copy link"}
          </button>
          <button type="button" role="menuitem" disabled={!canExport} className={`${ghostButtonClassName} mt-2 h-10 w-full justify-start rounded-md px-3 text-xs disabled:cursor-not-allowed disabled:opacity-50`} onClick={downloadImage}>
            Download image
          </button>
          <button type="button" role="menuitem" disabled={!canExport} className={`${ghostButtonClassName} mt-2 h-10 w-full justify-start rounded-md px-3 text-xs disabled:cursor-not-allowed disabled:opacity-50`} onClick={shareToX}>
            Share to X
          </button>
          <p className="px-3 py-2 text-[11px] leading-4 text-slate-500">Exports include visible map data only. No account details or session data are included.</p>
          {error ? <p className="px-3 pb-2 text-[11px] text-rose-200">{error}</p> : null}
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
  const [selectedTile, setSelectedTile] = useState<MarketPressureTile | null>(null);
  const renderStartRef = useRef<number>(typeof performance !== "undefined" ? performance.now() : 0);

  const query = useMemo(() => ({ timeRange, universe, viewMode }), [timeRange, universe, viewMode]);
  const sectors = useMemo(() => sortSectors(initialData.sectors), [initialData.sectors]);
  const selectedUniverseLabel = marketPressureUniverses.find((option) => option.value === universe)?.label ?? "S&P 500";
  const selectedViewLabel = marketPressureViewModes.find((option) => option.value === viewMode)?.label ?? "Market Pressure";
  const fallbackNotice = unavailableUniverseNotice(initialData);
  const shareBaseUrl = typeof window !== "undefined" ? `${window.location.origin}/market-pressure` : canonicalUrl;
  const currentShareUrl = `${shareBaseUrl}?${marketPressureQueryString(query)}`;

  useEffect(() => {
    recordProductEvent({ event_name: "market_pressure_page_view", properties: { universe: initialData.universe, view: initialData.view, period: initialData.period } });
  }, [initialData.period, initialData.universe, initialData.view]);

  useEffect(() => {
    const start = renderStartRef.current;
    if (!start || typeof performance === "undefined") return;
    const frame = window.requestAnimationFrame(() => {
      recordProductEvent({
        event_name: "market_pressure_visualization_rendered",
        properties: {
          universe,
          view: viewMode,
          period: timeRange,
          tile_count: initialData.tiles.length,
          render_ms: Math.round(performance.now() - start),
        },
      });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [initialData.tiles.length, timeRange, universe, viewMode]);

  function updateQuery(next: Partial<QueryState>) {
    const nextQuery = {
      timeRange: next.timeRange ?? timeRange,
      universe: next.universe ?? universe,
      viewMode: next.viewMode ?? viewMode,
    };
    const nextUrl = `/market-pressure?${marketPressureQueryString(nextQuery)}`;
    renderStartRef.current = typeof performance !== "undefined" ? performance.now() : 0;
    if (typeof window !== "undefined") {
      window.location.assign(nextUrl);
      return;
    }
    router.push(nextUrl);
  }

  function openTile(tile: MarketPressureTile) {
    setSelectedTile(tile);
  }

  return (
    <div className="min-w-0 max-w-full space-y-4 overflow-x-hidden">
      <section className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-end">
        <div className="min-w-0">
          <h1 className="text-2xl font-semibold tracking-normal text-white sm:text-3xl">Market Pressure Map</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-300">See where price movement and Walnut&apos;s complete confirmation stack are aligning-or diverging-across the market.</p>
          <p className="mt-2 text-xs font-semibold uppercase tracking-[0.16em] text-cyan-200/80">Most heatmaps show where the market has been. Walnut shows where pressure is building.</p>
        </div>
        <ShareMapButton data={initialData} sectors={sectors} shareUrl={currentShareUrl} query={query} />
      </section>

      {fallbackNotice ? (
        <div className="rounded-md border border-amber-300/25 bg-amber-300/10 px-3 py-2 text-xs font-medium text-amber-100">
          {fallbackNotice}
        </div>
      ) : null}

      <section className="rounded-md border border-white/10 bg-slate-900/55 p-3 shadow-card">
        <div className="grid gap-3 xl:grid-cols-[auto_auto_minmax(18rem,1fr)] xl:items-start">
          <div>
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Universe</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure universe">
              {marketPressureUniverses.map((option) => (
                <AnalyticsButton
                  key={option.value}
                  active={universe === option.value}
                  ariaLabel={`Set universe to ${option.label}`}
                  disabled={!initialData.capabilities.universes[option.value]}
                  title={!initialData.capabilities.universes[option.value] ? disabledUniverseTitle(option.value, initialData) : undefined}
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
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">Period</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure time range">
              {marketPressureTimeRanges.map((option) => (
                <AnalyticsButton
                  key={option}
                  active={timeRange === option}
                  ariaLabel={`Set period to ${option}`}
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
            <div className="mb-2 text-xs font-semibold uppercase tracking-[0.16em] text-slate-400">View</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure view mode">
              {marketPressureViewModes.map((option) => (
                <AnalyticsButton
                  key={option.value}
                  active={viewMode === option.value}
                  ariaLabel={`Set view to ${option.label}`}
                  disabled={!initialData.capabilities.views[option.value]}
                  title={!initialData.capabilities.views[option.value] ? "This view requires additional canonical positioning or historical snapshots." : undefined}
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

      <MarketSummaryStrip data={initialData} />

      <section className="rounded-md border border-white/10 bg-slate-900/45 p-3 shadow-card sm:p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-white">{selectedViewLabel}</h2>
            <p className="mt-1 text-xs text-slate-400">{selectedUniverseLabel} - {timeRange} - {initialData.summary.symbolCount} symbols</p>
          </div>
        </div>
        {initialData.status === "ready" && initialData.tiles.length > 0 ? (
          <SectorMap sectors={sectors} period={timeRange} onOpen={openTile} />
        ) : (
          <MarketPressureStatusState data={initialData.status === "ready" ? { ...initialData, status: "no-data" } : initialData} />
        )}
      </section>

      <MarketPressureLegend />
      <TickerFlyout tile={selectedTile} period={timeRange} onClose={() => setSelectedTile(null)} />
    </div>
  );
}
