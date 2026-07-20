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

type MarketPressureColorMode = "price" | "confirmation";

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

type SectorHoverState = {
  sector: MarketPressureSector;
  x: number;
  y: number;
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
  conflicted: "CNF",
  unavailable: "NA",
};

const priceLegendItems = [
  ["-3%", "#df3542"],
  ["-2%", "#be3e4d"],
  ["-1%", "#8f4555"],
  ["0%", "#35495a"],
  ["+1%", "#2f8f59"],
  ["+2%", "#2fbd63"],
  ["+3%", "#18c861"],
] as const;

const confirmationLegendItems = [
  ["Strong Bearish", "#c2410c"],
  ["Bearish", "#ea580c"],
  ["Weak Bearish", "#f97316"],
  ["Neutral", "#3f4b5d"],
  ["Weak Bullish", "#3b9af3"],
  ["Bullish", "#1d7fe5"],
  ["Strong Bullish", "#0b63ce"],
  ["Conflicted", "#6d28d9"],
] as const;

function legendItemsForColorMode(colorMode: MarketPressureColorMode) {
  return colorMode === "confirmation" ? confirmationLegendItems : priceLegendItems;
}

const layerOrder = Object.keys(marketPressureLayerLabels) as MarketPressureLayerKey[];

function segmentedButtonClass(active: boolean) {
  return [
    "min-h-9 rounded-md border px-3 py-1.5 text-xs font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/50 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 disabled:cursor-not-allowed disabled:opacity-45",
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

function confirmationFillClass(tile: MarketPressureTile) {
  const score = tile.confirmationScore;
  if (score == null || !Number.isFinite(score) || tile.confirmationDirection === "unavailable") {
    return "bg-[repeating-linear-gradient(135deg,rgba(51,65,85,0.82)_0,rgba(51,65,85,0.82)_6px,rgba(15,23,42,0.92)_6px,rgba(15,23,42,0.92)_12px)] text-slate-100";
  }
  if (tile.confirmationDirection === "bullish") {
    if (score >= 80) return "bg-[#0b63ce] text-white";
    if (score >= 65) return "bg-[#1d7fe5] text-white";
    if (score >= 50) return "bg-[#3b9af3] text-white";
    return "bg-[#5eb2ff] text-slate-950";
  }
  if (tile.confirmationDirection === "bearish") {
    if (score >= 80) return "bg-[#c2410c] text-white";
    if (score >= 65) return "bg-[#ea580c] text-white";
    if (score >= 50) return "bg-[#f97316] text-white";
    return "bg-[#fb923c] text-slate-950";
  }
  if (tile.confirmationDirection === "conflicted") {
    return "bg-[#6d28d9] text-white";
  }
  return "bg-[#3f4b5d] text-slate-100";
}

function tileFillClass(tile: MarketPressureTile, colorMode: MarketPressureColorMode) {
  return colorMode === "confirmation" ? confirmationFillClass(tile) : priceFillClass(tile.priceChangePct);
}

function confirmationFrameClass(tile: MarketPressureTile) {
  void tile;
  return "border border-slate-950";
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

function compareSectorHoverRows(a: MarketPressureTile, b: MarketPressureTile) {
  const leftCap = a.marketCap ?? 0;
  const rightCap = b.marketCap ?? 0;
  if (rightCap !== leftCap) return rightCap - leftCap;
  return a.symbol.localeCompare(b.symbol);
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
  const validItems = items.filter((item) => item.weight > 0).sort((a, b) => b.weight - a.weight);
  if (validItems.length === 0) return [];
  if (validItems.length === 1) return [{ ...validItems[0], rect }];

  const totalWeight = sumTreemapWeight(validItems);
  if (totalWeight <= 0) return [];

  const areaScale = (rect.width * rect.height) / totalWeight;
  const pending = validItems.map((item) => ({ ...item, area: Math.max(0, item.weight * areaScale) }));
  const layouts: TreemapLayoutItem<T>[] = [];

  function worstRatio(row: typeof pending, side: number) {
    if (row.length === 0 || side <= 0) return Number.POSITIVE_INFINITY;
    const areas = row.map((item) => item.area).filter((area) => area > 0);
    if (areas.length === 0) return Number.POSITIVE_INFINITY;
    const sum = areas.reduce((total, area) => total + area, 0);
    const max = Math.max(...areas);
    const min = Math.min(...areas);
    const sideSquared = side * side;
    const sumSquared = sum * sum;
    return Math.max((sideSquared * max) / sumSquared, sumSquared / (sideSquared * min));
  }

  function layoutRow(row: typeof pending, remaining: TreemapRect): TreemapRect {
    const rowArea = row.reduce((total, item) => total + item.area, 0);
    if (remaining.width >= remaining.height) {
      const rowWidth = Math.min(remaining.width, rowArea / remaining.height);
      let y = remaining.y;
      row.forEach((item, index) => {
        const isLast = index === row.length - 1;
        const height = isLast ? remaining.y + remaining.height - y : item.area / rowWidth;
        layouts.push({ item: item.item, weight: item.weight, rect: { x: remaining.x, y, width: rowWidth, height } });
        y += height;
      });
      return { x: remaining.x + rowWidth, y: remaining.y, width: remaining.width - rowWidth, height: remaining.height };
    }

    const rowHeight = Math.min(remaining.height, rowArea / remaining.width);
    let x = remaining.x;
    row.forEach((item, index) => {
      const isLast = index === row.length - 1;
      const width = isLast ? remaining.x + remaining.width - x : item.area / rowHeight;
      layouts.push({ item: item.item, weight: item.weight, rect: { x, y: remaining.y, width, height: rowHeight } });
      x += width;
    });
    return { x: remaining.x, y: remaining.y + rowHeight, width: remaining.width, height: remaining.height - rowHeight };
  }

  let remaining = { ...rect };
  let row: typeof pending = [];
  while (pending.length > 0) {
    const next = pending[0];
    const side = Math.min(remaining.width, remaining.height);
    if (row.length === 0 || worstRatio([...row, next], side) <= worstRatio(row, side)) {
      row.push(next);
      pending.shift();
    } else {
      remaining = layoutRow(row, remaining);
      row = [];
    }
  }
  if (row.length > 0) layoutRow(row, remaining);
  return layouts.filter((layout) => layout.rect.width > 0 && layout.rect.height > 0);
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
  if (tile.confirmationDirection === "conflicted") return `${tile.symbol} ${priceText} with conflicted evidence across the confirmation stack.`;
  return `${tile.symbol} ${priceText} ${scoreText} and ${tile.confirmationDirection} direction.`;
}

function accessibleTileLabel(tile: MarketPressureTile, period: MarketPressureTimeRange) {
  const priceDirection = tile.priceChangePct == null ? "price unavailable" : `price ${tile.priceChangePct >= 0 ? "up" : "down"} ${Math.abs(tile.priceChangePct).toFixed(2)} percent over ${period}`;
  const score = tile.confirmationScore == null ? "Confirmation Score unavailable" : `Confirmation Score ${Math.round(tile.confirmationScore)}`;
  return `${tile.symbol}, ${tile.sector}, ${priceDirection}, ${tile.confirmationDirection} confirmation, ${score}, ${divergenceLabel[tile.divergence]}.`;
}

function tileMetricLabel(tile: MarketPressureTile, colorMode: MarketPressureColorMode) {
  return colorMode === "confirmation" ? `CS ${formatScore(tile.confirmationScore)}` : formatPct(tile.priceChangePct, true);
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

function marketPressureAuditNotice(data: MarketPressureMapResult) {
  const audit = data.audit;
  if (!audit || audit.status === "ok") return null;
  const issues: string[] = [];
  if (audit.importantMissingSymbols.length > 0) {
    issues.push(`missing key names: ${audit.importantMissingSymbols.join(", ")}`);
  }
  const marketCapSymbols = [...audit.importantMissingMarketCapSymbols, ...audit.importantLowMarketCapSymbols];
  if (marketCapSymbols.length > 0) {
    issues.push(`bad market-cap weights: ${Array.from(new Set(marketCapSymbols)).join(", ")}`);
  }
  if (audit.missingSymbolCount > 0 && issues.length === 0) {
    issues.push(`${audit.missingSymbolCount} universe names missing`);
  }
  return `Map audit ${audit.status}: ${issues.join("; ")}.`;
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
        <p className="text-xs font-medium uppercase tracking-[0.22em] text-emerald-200/80">{data.status.replace("-", " ")}</p>
        <h2 className="mt-3 text-xl font-medium text-white">{copy.title}</h2>
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
  const universeDetails = data.capabilities.universeDetails?.[data.universe];
  const items = [
    ["Bullish", data.summary.bullishCount],
    ["Bearish", data.summary.bearishCount],
    ["Conflicted", data.summary.conflictedCount],
    ["Hidden Accumulation", data.summary.hiddenAccumulationCount],
    ["Fragile Winners", data.summary.fragileWinnerCount],
  ] as const;
  return (
    <section className="grid gap-2 border-y border-white/10 bg-slate-950/35 px-1 py-3 md:grid-cols-[1fr_auto] md:items-center">
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-5">
        {items.map(([label, value]) => (
          <div key={label} className="min-w-0 rounded-md border border-white/10 bg-slate-900/55 px-3 py-2">
            <div className="text-[10px] font-medium uppercase tracking-[0.14em] text-slate-400">{label}</div>
            <div className="mt-1 text-lg font-medium text-white">{value}</div>
          </div>
        ))}
      </div>
      <div className="text-xs leading-5 text-slate-400 md:max-w-xs md:text-right">
        <div>Price as of {formatDate(data.priceAsOf)}</div>
        <div>Confirmation as of {formatDate(data.confirmationAsOf)}</div>
        {universeDetails?.status === "stale" ? <div className="font-medium text-amber-200">Membership data is stale</div> : null}
      </div>
    </section>
  );
}

function TileMarkers({ tile }: { tile: MarketPressureTile }) {
  if (tile.divergence === "hidden_accumulation") {
    return <span className="rounded-sm border border-emerald-100/70 bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wide text-emerald-100">Accumulation</span>;
  }
  if (tile.divergence === "fragile_winner") {
    return <span className="rounded-sm border border-amber-100/70 bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wide text-amber-100">Fragile</span>;
  }
  if (tile.divergence === "conflicted") {
    return <span className="rounded-sm border border-violet-100/50 bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-medium uppercase tracking-wide text-violet-100">Conflicted</span>;
  }
  return null;
}

function MarketTile({
  tile,
  period,
  colorMode,
  onOpen,
  rect,
  displayRect,
}: {
  tile: MarketPressureTile;
  period: MarketPressureTimeRange;
  colorMode: MarketPressureColorMode;
  onOpen: (tile: MarketPressureTile) => void;
  rect?: TreemapRect;
  displayRect?: TreemapRect;
}) {
  const label = accessibleTileLabel(tile, period);
  const labelRect = displayRect ?? rect;
  const tileArea = labelRect ? labelRect.width * labelRect.height : 1000;
  const hideLabel = labelRect ? labelRect.width < 2.4 || labelRect.height < 2.5 || tileArea < 10 : false;
  const showPrice = labelRect ? !hideLabel && labelRect.width >= 3.2 && labelRect.height >= 3.8 && tileArea >= 18 : true;
  const compact = labelRect ? labelRect.width < 4.4 || labelRect.height < 5 || tileArea < 32 : false;
  const medium = labelRect ? labelRect.width < 7 || labelRect.height < 7 || tileArea < 70 : false;
  const feature = labelRect ? labelRect.width >= 8.5 && labelRect.height >= 8.5 && tileArea >= 110 : false;
  const hero = labelRect ? labelRect.width >= 13 && labelRect.height >= 13 && tileArea >= 210 : false;
  const showDiagnostics = labelRect ? colorMode === "price" && !hideLabel && !feature && labelRect.width >= 10 && labelRect.height >= 10 && tileArea >= 130 : colorMode === "price";
  const tileClassName = rect
    ? `group absolute flex flex-col overflow-hidden rounded-none px-1.5 py-1 text-left shadow-none transition hover:z-20 hover:brightness-110 focus-visible:z-30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/80 focus-visible:ring-offset-0 ${feature ? "justify-center" : "justify-start"} ${tileFillClass(tile, colorMode)} ${confirmationFrameClass(tile)}`
    : `group relative min-h-[5.7rem] overflow-hidden rounded-md p-2 text-left shadow-sm transition hover:-translate-y-0.5 hover:shadow-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/70 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-950 ${tileFillClass(tile, colorMode)} ${confirmationFrameClass(tile)}`;
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
      data-symbol={tile.symbol}
      aria-label={label}
      title={`${label} ${explainTile(tile, period)}`}
      onClick={() => onOpen(tile)}
      onKeyDown={handleKeyDown}
    >
      <span className={`pointer-events-none absolute right-1 top-1 rounded-sm bg-slate-950/55 px-1 py-0.5 text-[7px] font-medium uppercase tracking-wide text-white/85 sm:text-[8px] ${hideLabel || compact ? "hidden" : ""}`}>
        {directionGlyph[tile.confirmationDirection]}
      </span>
      {!hideLabel ? (
        <span className={`${compact ? "text-[7px] sm:text-[9px]" : medium ? "text-[8px] sm:text-[11px]" : hero ? "text-lg sm:text-2xl" : feature ? "text-sm sm:text-xl" : "text-[10px] sm:text-sm"} block max-w-full truncate text-center font-mono font-semibold leading-tight tracking-normal drop-shadow-[0_1px_1px_rgba(0,0,0,0.7)]`}>
          {tile.symbol}
        </span>
      ) : null}
      {showPrice ? (
        <span className={`${compact ? "mt-0.5 text-[7px] sm:text-[8px]" : medium ? "mt-0.5 text-[8px] sm:text-[10px]" : hero ? "mt-1 text-sm sm:text-lg" : feature ? "mt-0.5 text-xs sm:mt-1 sm:text-sm" : "mt-0.5 text-[9px] sm:text-xs"} block text-center font-medium leading-tight drop-shadow-[0_1px_1px_rgba(0,0,0,0.7)]`}>
          {tileMetricLabel(tile, colorMode)}
        </span>
      ) : null}
      <span className={`${showDiagnostics ? "mt-0.5 block text-[8px] sm:mt-1 sm:text-[10px]" : "hidden"} text-center font-normal text-white/80`}>CS {formatScore(tile.confirmationScore)}</span>
      <span className={`${showDiagnostics ? "mt-1 flex" : "hidden"} min-h-5 flex-wrap gap-1`}>
        <TileMarkers tile={tile} />
        {tile.dataState === "complete" ? null : <span className="rounded-sm bg-slate-950/65 px-1.5 py-0.5 text-[9px] font-medium uppercase text-slate-200">{tile.dataState}</span>}
      </span>
    </button>
  );
}

function SectorMap({
  sectors,
  period,
  colorMode,
  onOpen,
}: {
  sectors: MarketPressureSector[];
  period: MarketPressureTimeRange;
  colorMode: MarketPressureColorMode;
  onOpen: (tile: MarketPressureTile) => void;
}) {
  const [hoveredSector, setHoveredSector] = useState<SectorHoverState | null>(null);
  const sectorLayouts = useMemo(
    () =>
      layoutTreemap(sectors.map((sectorGroup) => ({ item: sectorGroup, weight: sectorWeight(sectorGroup) }))).map((sectorLayout) => ({
        ...sectorLayout,
        tileLayouts: layoutTreemap(sectorLayout.item.tiles.map((tile) => ({ item: tile, weight: tileWeight(tile) }))),
      })),
    [sectors],
  );

  return (
    <div
      className="relative min-h-[34rem] overflow-hidden rounded-md border border-slate-950 bg-slate-950 shadow-inner sm:min-h-[42rem] xl:min-h-[48rem]"
      data-market-pressure-map
      data-sector-treemap
      onMouseLeave={() => setHoveredSector(null)}
    >
      {sectorLayouts.map(({ item: sectorGroup, rect, tileLayouts }) => {
        const showHeader = rect.width >= 8 && rect.height >= 7;
        return (
          <section
            key={sectorGroup.sector}
            className="absolute overflow-hidden border border-slate-950 bg-slate-900/50"
            style={rectStyle(rect)}
            aria-label={`${sectorGroup.sector} sector pressure`}
            onMouseEnter={(event) => setHoveredSector({ sector: sectorGroup, x: event.clientX, y: event.clientY })}
            onMouseMove={(event) => setHoveredSector({ sector: sectorGroup, x: event.clientX, y: event.clientY })}
            onFocus={() => setHoveredSector(null)}
          >
            {showHeader ? (
              <div className="absolute inset-x-0 top-0 z-10 flex h-5 min-w-0 items-center justify-between gap-2 border-b border-slate-950 bg-slate-800/85 px-1.5 text-[9px] font-medium uppercase tracking-normal text-slate-100">
                <span className="truncate">{sectorGroup.sector}</span>
                <span className="shrink-0 text-slate-300">{formatPct(sectorGroup.summary.averagePriceChangePct, true)}</span>
              </div>
            ) : null}
            <div className={showHeader ? "absolute inset-x-0 bottom-0 top-5" : "absolute inset-0"}>
              {tileLayouts.map(({ item: tile, rect: tileRect }) => {
                const displayRect = {
                  x: rect.x + (rect.width * tileRect.x) / 100,
                  y: rect.y + (rect.height * tileRect.y) / 100,
                  width: (rect.width * tileRect.width) / 100,
                  height: (rect.height * tileRect.height) / 100,
                };
                return <MarketTile key={`${tile.sector}:${tile.symbol}`} tile={tile} period={period} colorMode={colorMode} onOpen={onOpen} rect={tileRect} displayRect={displayRect} />;
              })}
            </div>
            {showHeader ? (
              <div className="pointer-events-none absolute bottom-1 left-1 z-10 hidden rounded-sm bg-slate-950/70 px-1.5 py-0.5 text-[9px] font-medium text-slate-200 sm:block">
                {sectorGroup.summary.symbolCount} names
              </div>
            ) : null}
          </section>
        );
      })}
      <SectorHoverTooltip hover={hoveredSector} period={period} />
    </div>
  );
}

function SectorHoverTooltip({ hover, period }: { hover: SectorHoverState | null; period: MarketPressureTimeRange }) {
  if (!hover) return null;
  const rows = [...hover.sector.tiles].sort(compareSectorHoverRows);
  return (
    <div
      className="fixed z-50 w-[27rem] max-w-[calc(100vw-1.5rem)] rounded-2xl border border-white/10 bg-slate-950/95 px-3 py-3 text-sm shadow-xl backdrop-blur"
      style={{
        left: `clamp(0.75rem, ${hover.x + 16}px, calc(100vw - 28rem))`,
        top: `clamp(0.75rem, ${hover.y + 16}px, calc(100vh - 27rem))`,
      }}
      role="tooltip"
      data-sector-hover-tooltip
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{hover.sector.sector}</div>
          <div className="mt-1 text-xs text-slate-400">{rows.length} names - avg {formatPct(hover.sector.summary.averagePriceChangePct, true)}</div>
        </div>
        <div className="text-right text-[10px] font-medium uppercase tracking-[0.14em] text-slate-500">{period}</div>
      </div>
      <div className="mt-3 max-h-80 overflow-y-auto rounded-lg border border-white/10">
        <table className="min-w-full text-xs">
          <thead className="bg-slate-900/95 text-[10px] uppercase tracking-[0.16em] text-slate-500">
            <tr>
              <th className="px-2 py-2 text-left font-medium">Ticker</th>
              <th className="px-2 py-2 text-right font-medium">1D</th>
              <th className="px-2 py-2 text-right font-medium">Score</th>
              <th className="px-2 py-2 text-left font-medium">Direction</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/5">
            {rows.map((tile) => (
              <tr key={`${hover.sector.sector}:hover:${tile.symbol}`} className="text-slate-200">
                <td className="px-2 py-1.5 font-mono font-medium text-emerald-100">{tile.symbol}</td>
                <td className={`px-2 py-1.5 text-right font-medium tabular-nums ${tile.priceChangePct == null ? "text-slate-500" : tile.priceChangePct >= 0 ? "text-emerald-200" : "text-rose-200"}`}>{formatPct(tile.priceChangePct, true)}</td>
                <td className="px-2 py-1.5 text-right tabular-nums text-slate-100">{formatScore(tile.confirmationScore)}</td>
                <td className="px-2 py-1.5 text-slate-300">{statusTitle(tile.confirmationDirection)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function InfoHelp({ label }: { label: string }) {
  return (
    <span className="group relative inline-flex">
      <span className="inline-flex h-4 w-4 items-center justify-center rounded-full border border-white/20 bg-slate-950/70 text-[10px] font-medium text-slate-300">?</span>
      <span className="pointer-events-none absolute left-1/2 top-6 z-50 hidden w-56 -translate-x-1/2 rounded-md border border-white/10 bg-slate-950 px-2.5 py-2 text-xs font-normal leading-5 text-slate-200 shadow-xl group-hover:block group-focus-within:block">
        {label}
      </span>
    </span>
  );
}

function ColorLegend({ colorMode }: { colorMode: MarketPressureColorMode }) {
  const items = legendItemsForColorMode(colorMode);
  return (
    <div className="flex flex-wrap items-center gap-1" aria-label={`${colorMode} color legend`} data-market-pressure-color-legend>
      {items.map(([label, color]) => (
        <div key={label} className="min-w-14 px-2 py-1 text-center text-xs font-medium text-white shadow-sm" style={{ backgroundColor: color }}>
          {label}
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
        <div className="truncate text-sm font-normal text-white">{label}</div>
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
      <div className="space-y-3">
        <div className="grid gap-2 sm:grid-cols-4">
          <Metric label={`${period} price`} value={formatPct(tile.priceChangePct)} />
          <Metric label="Confirmation" value={tile.confirmationScore == null ? "NA" : `${Math.round(tile.confirmationScore)}/100`} />
          <Metric label="Direction" value={statusTitle(tile.confirmationDirection)} />
          <Metric label="Divergence" value={divergenceLabel[tile.divergence]} />
        </div>
        <div className="rounded-md border border-white/10 bg-slate-950/45 p-3">
          <h3 className="text-sm font-medium text-white">Why it stands out</h3>
          <p className="mt-1.5 text-sm leading-5 text-slate-300">{explainTile(tile, period)}</p>
          <p className="mt-1.5 text-xs text-slate-500">
            Price as of {formatDate(tile.priceEndAt)}. Confirmation as of {formatDate(tile.confirmationAsOf)}. Data state: {statusTitle(tile.dataState)}.
          </p>
        </div>
        <div className="rounded-md border border-white/10 bg-slate-950/45 p-3">
          <h3 className="text-sm font-medium text-white">Evidence summary</h3>
          <div className="mt-2 grid gap-x-3 sm:grid-cols-2">
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
    <div className="rounded-md border border-white/10 bg-slate-900/55 px-2.5 py-2">
      <div className="text-[10px] font-medium uppercase tracking-[0.14em] text-slate-500">{label}</div>
      <div className="mt-1 truncate text-sm font-medium text-white">{value}</div>
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

function confirmationFillHex(tile: MarketPressureTile) {
  const score = tile.confirmationScore;
  if (score == null || !Number.isFinite(score) || tile.confirmationDirection === "unavailable") return "#334155";
  if (tile.confirmationDirection === "bullish") {
    if (score >= 80) return "#0b63ce";
    if (score >= 65) return "#1d7fe5";
    if (score >= 50) return "#3b9af3";
    return "#5eb2ff";
  }
  if (tile.confirmationDirection === "bearish") {
    if (score >= 80) return "#c2410c";
    if (score >= 65) return "#ea580c";
    if (score >= 50) return "#f97316";
    return "#fb923c";
  }
  if (tile.confirmationDirection === "conflicted") {
    return "#6d28d9";
  }
  return "#3f4b5d";
}

function tileFillHex(tile: MarketPressureTile, colorMode: MarketPressureColorMode) {
  return colorMode === "confirmation" ? confirmationFillHex(tile) : priceFillHex(tile.priceChangePct);
}

function tileSvg(tile: MarketPressureTile, x: number, y: number, width: number, height: number, colorMode: MarketPressureColorMode) {
  const fill = tileFillHex(tile, colorMode);
  const marker = tile.divergence === "hidden_accumulation" ? "ACC" : tile.divergence === "fragile_winner" ? "FRG" : "";
  const showLabel = width >= 48 && height >= 26;
  const showPrice = showLabel && width >= 64 && height >= 38;
  const showScore = colorMode === "price" && width >= 92 && height >= 66;
  const fontSize = width >= 150 && height >= 96 ? 28 : width >= 110 && height >= 68 ? 20 : width >= 70 && height >= 42 ? 12 : 9;
  const textAnchor = width >= 110 && height >= 68 ? "middle" : "start";
  const labelX = textAnchor === "middle" ? x + width / 2 : x + 6;
  const labelY = textAnchor === "middle" ? y + height / 2 - 2 : y + Math.max(12, fontSize + 4);
  const priceY = textAnchor === "middle" ? labelY + Math.max(14, fontSize * 0.68) : y + fontSize + 22;
  return `<g><rect x="${x}" y="${y}" width="${width}" height="${height}" rx="0" fill="${fill}" stroke="#020617" stroke-width="1.5"/>${showLabel ? `<text x="${labelX}" y="${labelY}" text-anchor="${textAnchor}" fill="#fff" font-size="${fontSize}" font-weight="600" font-family="Arial">${svgEscape(tile.symbol)}</text>` : ""}${showPrice ? `<text x="${labelX}" y="${priceY}" text-anchor="${textAnchor}" fill="#f8fafc" font-size="${Math.max(8, Math.round(fontSize * 0.55))}" font-weight="500" font-family="Arial">${svgEscape(tileMetricLabel(tile, colorMode))}</text>` : ""}${showScore && textAnchor === "start" ? `<text x="${x + 6}" y="${y + fontSize + 40}" fill="#cbd5e1" font-size="10" font-family="Arial">CS ${svgEscape(formatScore(tile.confirmationScore))}</text>` : ""}${marker && width >= 68 && height >= 34 ? `<text x="${x + width - 30}" y="${y + 15}" fill="#fff7ed" font-size="9" font-weight="600" font-family="Arial">${marker}</text>` : ""}</g>`;
}

function renderShareSvg(data: MarketPressureMapResult, sectors: MarketPressureSector[], query: QueryState, colorMode: MarketPressureColorMode) {
  const width = 1200;
  const height = 675;
  const mapX = 44;
  const mapY = 150;
  const mapWidth = 1112;
  const mapHeight = 432;
  const universe = marketPressureUniverses.find((item) => item.value === query.universe)?.label ?? "S&P 500";
  const view = marketPressureViewModes.find((item) => item.value === query.viewMode)?.label ?? "Market Pressure";
  const legendItems = legendItemsForColorMode(colorMode);
  const legendChipWidth = colorMode === "confirmation" ? 124 : 66;
  const legendMarkup = legendItems
    .map(([label, color], index) => {
      const x = mapX + index * (legendChipWidth + 6);
      return `<g><rect x="${x}" y="608" width="${legendChipWidth}" height="24" fill="${color}"/><text x="${x + legendChipWidth / 2}" y="624" text-anchor="middle" fill="#fff" font-size="11" font-weight="500" font-family="Arial">${svgEscape(label)}</text></g>`;
    })
    .join("");
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
        .map(({ item: tile, rect: tileRect }) => tileSvg(tile, x + (tileRect.x / 100) * sectorWidth, y + headerHeight + (tileRect.y / 100) * tileHeight, (tileRect.width / 100) * sectorWidth, (tileRect.height / 100) * tileHeight, colorMode))
        .join("");
      const headerMarkup = showHeader
        ? `<rect x="${x}" y="${y}" width="${sectorWidth}" height="${headerHeight}" fill="#1e293b"/><text x="${x + 6}" y="${y + 13}" fill="#e2e8f0" font-size="10" font-weight="600" font-family="Arial">${svgEscape(sector.sector)} ${svgEscape(formatPct(sector.summary.averagePriceChangePct, true))}</text>`
        : "";
      return `<g><rect x="${x}" y="${y}" width="${sectorWidth}" height="${sectorHeight}" fill="#020617" stroke="#020617" stroke-width="2"/>${headerMarkup}${tileMarkup}</g>`;
    })
    .join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}"><rect width="${width}" height="${height}" fill="#020617"/><text x="44" y="54" fill="#ecfdf5" font-size="28" font-weight="600" font-family="Arial">Walnut Market Pressure Map</text><text x="44" y="86" fill="#94a3b8" font-size="15" font-family="Arial">${svgEscape(universe)} - ${svgEscape(query.timeRange)} - ${svgEscape(view)} - ${svgEscape(colorMode === "confirmation" ? "Confirmation mode" : "Price mode")} - Generated ${svgEscape(formatDate(data.generatedAt))}</text><text x="44" y="120" fill="#67e8f9" font-size="16" font-weight="600" font-family="Arial">Most heatmaps show where the market has been. Walnut shows where pressure is building.</text><g>${sectorMarkup}</g><rect x="44" y="596" width="1112" height="1" fill="#1e293b"/><g>${legendMarkup}</g><text x="44" y="652" fill="#cbd5e1" font-size="13" font-family="Arial">${svgEscape(colorMode === "confirmation" ? "Mode uses the canonical confirmation classification. Conflicted is one state." : "Mode uses price performance.")}</text><text x="1018" y="652" fill="#34d399" font-size="16" font-weight="600" font-family="Arial">walnutmarkets.com</text></svg>`;
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
  colorMode,
}: {
  data: MarketPressureMapResult;
  sectors: MarketPressureSector[];
  shareUrl: string;
  query: QueryState;
  colorMode: MarketPressureColorMode;
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
      const svg = renderShareSvg(data, sectors, query, colorMode);
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
  }, [colorMode, data, query, sectors]);

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
  const [colorMode, setColorMode] = useState<MarketPressureColorMode>("price");
  const [selectedTile, setSelectedTile] = useState<MarketPressureTile | null>(null);
  const renderStartRef = useRef<number>(typeof performance !== "undefined" ? performance.now() : 0);

  const query = useMemo(() => ({ timeRange, universe, viewMode }), [timeRange, universe, viewMode]);
  const sectors = useMemo(() => sortSectors(initialData.sectors), [initialData.sectors]);
  const selectedUniverseLabel = marketPressureUniverses.find((option) => option.value === universe)?.label ?? "S&P 500";
  const selectedViewLabel = marketPressureViewModes.find((option) => option.value === viewMode)?.label ?? "Market Pressure";
  const fallbackNotice = unavailableUniverseNotice(initialData);
  const auditNotice = marketPressureAuditNotice(initialData);
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
          <h1 className="text-2xl font-medium tracking-normal text-white sm:text-3xl">Market Pressure Map</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-300">See where price movement and Walnut&apos;s complete confirmation stack are aligning-or diverging-across the market.</p>
          <p className="mt-2 text-xs font-medium uppercase tracking-[0.16em] text-cyan-200/80">Most heatmaps show where the market has been. Walnut shows where pressure is building.</p>
        </div>
        <ShareMapButton data={initialData} sectors={sectors} shareUrl={currentShareUrl} query={query} colorMode={colorMode} />
      </section>

      {fallbackNotice ? (
        <div className="rounded-md border border-amber-300/25 bg-amber-300/10 px-3 py-2 text-xs font-medium text-amber-100">
          {fallbackNotice}
        </div>
      ) : null}

      {auditNotice ? (
        <div className="rounded-md border border-rose-300/30 bg-rose-400/10 px-3 py-2 text-xs font-medium text-rose-100">
          {auditNotice}
        </div>
      ) : null}

      <section className="rounded-md border border-white/10 bg-slate-900/55 p-3 shadow-card">
        <div className="grid gap-3 xl:grid-cols-[auto_auto_auto_minmax(18rem,1fr)] xl:items-start">
          <div>
            <div className="mb-2 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">Universe</div>
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
            <div className="mb-2 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">Period</div>
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
            <div className="mb-2 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">Mode</div>
            <div className="flex flex-wrap gap-2" role="group" aria-label="Market Pressure data mode">
              {(["price", "confirmation"] as const).map((option) => (
                <AnalyticsButton
                  key={option}
                  active={colorMode === option}
                  ariaLabel={`Set data mode to ${option}`}
                  onClick={() => {
                    setColorMode(option);
                    recordProductEvent({ event_name: "market_pressure_colour_mode_changed", properties: { colour_mode: option } });
                  }}
                >
                  {option === "price" ? "Price" : "Confirmation"}
                </AnalyticsButton>
              ))}
            </div>
          </div>
          <div>
            <div className="mb-2 text-xs font-medium uppercase tracking-[0.16em] text-slate-400">View</div>
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
                  <span className="inline-flex items-center gap-1.5">
                    {option.label}
                    {option.value === "hidden_accumulation" ? <InfoHelp label="Price is weak while Walnut confirmation remains bullish." /> : null}
                    {option.value === "fragile_winners" ? <InfoHelp label="Price is strong while Walnut confirmation is bearish or conflicted." /> : null}
                  </span>
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
            <h2 className="text-lg font-medium text-white">{selectedViewLabel}</h2>
            <p className="mt-1 text-xs text-slate-400">{selectedUniverseLabel} - {timeRange} - {initialData.summary.symbolCount} symbols</p>
          </div>
          <ColorLegend colorMode={colorMode} />
        </div>
        {initialData.status === "ready" && initialData.tiles.length > 0 ? (
          <SectorMap sectors={sectors} period={timeRange} colorMode={colorMode} onOpen={openTile} />
        ) : (
          <MarketPressureStatusState data={initialData.status === "ready" ? { ...initialData, status: "no-data" } : initialData} />
        )}
      </section>

      <TickerFlyout tile={selectedTile} period={timeRange} onClose={() => setSelectedTile(null)} />
    </div>
  );
}
