"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  getMacroPositioningFeed,
  getInsightsMacroPositioning,
  type InsightsMacroPositioningMarket,
  type InsightsMacroPositioningResponse,
  type MacroPositioningFeedItem,
  type MacroPositioningFeedResponse,
} from "@/lib/api";
import { cardClassName, ghostButtonClassName, selectClassName } from "@/lib/styles";

const pageSizeOptions = [5, 10, 25] as const;

type MacroPositioningTab = "overview" | "feed";
type MacroFeedView = "significant" | "all";

function biasLabel(value?: string | null): string {
  if (value === "bullish") return "Bullish";
  if (value === "bearish") return "Bearish";
  return "Neutral";
}

function biasClassName(value?: string | null): string {
  if (value === "bullish") return "text-emerald-300";
  if (value === "bearish") return "text-rose-300";
  if (value === "neutral") return "text-amber-300";
  return "text-slate-400";
}

function trendLabel(value?: string | null): string {
  if (value === "increasing") return "Increasing";
  if (value === "decreasing") return "Decreasing";
  if (value === "stable") return "Stable";
  return "Unavailable";
}

function percentileText(value?: number | null): string | null {
  if (typeof value !== "number" || Number.isNaN(value)) return null;
  return `${Math.round(value)}th percentile`;
}

function updatedText(value?: string | null): string {
  if (!value) return "Updated weekly";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Updated weekly";
  const formatted = new Intl.DateTimeFormat("en-US", { year: "numeric", month: "short", day: "2-digit", timeZone: "UTC" }).format(date);
  return `Updated ${formatted}`;
}

function convictionText(value?: number | null): string {
  const rating = Math.max(1, Math.min(5, Math.round(value ?? 3)));
  return `${rating}/5 conviction`;
}

function formatFeedDate(value?: string | null): string {
  if (!value) return "Unavailable";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", { year: "numeric", month: "short", day: "2-digit", timeZone: "UTC" }).format(date);
}

function feedPositioningLabel(item: MacroPositioningFeedItem): string {
  if (item.crowded) return `${biasLabel(item.positioning)} / Crowded`;
  return biasLabel(item.positioning);
}

function feedPositioningClass(value?: string | null): string {
  if (value === "bullish") return "text-emerald-300";
  if (value === "bearish") return "text-rose-300";
  return "text-slate-300";
}

function feedTrendLabel(item: MacroPositioningFeedItem): string {
  const label = trendLabel(item.trend);
  if (label === "Unavailable") return label;
  return item.trend_weeks ? `${label} ${item.trend_weeks}w` : label;
}

function feedPercentileLabel(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "Unavailable";
  return `${Math.round(value)}th percentile`;
}

function feedEventLabel(value?: string | null): string {
  if (value === "crowded") return "Crowded positioning";
  if (value === "trend_change") return "Trend change";
  if (value === "extreme") return "Historical extreme";
  if (value === "weekly_move") return "Weekly move";
  return "Positioning update";
}

function SectionSkeleton() {
  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <div className="flex flex-col gap-2">
        <div className="h-5 w-44 animate-pulse rounded bg-white/10" />
        <div className="h-4 w-full max-w-md animate-pulse rounded bg-white/10" />
      </div>
      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="min-h-[9rem] rounded-lg border border-white/10 bg-slate-950/45 p-4">
            <div className="h-4 w-24 animate-pulse rounded bg-white/10" />
            <div className="mt-4 h-5 w-20 animate-pulse rounded bg-white/10" />
            <div className="mt-4 h-3 w-full animate-pulse rounded bg-white/10" />
          </div>
        ))}
      </div>
    </section>
  );
}

function LockedSection({ data }: { data: InsightsMacroPositioningResponse }) {
  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="max-w-3xl">
          <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
          <p className="mt-2 text-sm leading-6 text-slate-400">
            See whether institutional futures positioning is bullish, bearish, crowded, or shifting across major markets.
          </p>
          <p className="mt-3 text-sm font-semibold text-emerald-100">{data.subtitle ?? "Included with Walnut Pro."}</p>
        </div>
        <Link href="/pricing" className={ghostButtonClassName}>
          Upgrade to Pro
        </Link>
      </div>
      <div className="mt-5 rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
        <p className="text-sm font-semibold text-white">Macro Positioning</p>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          Institutional futures positioning across major markets.
        </p>
      </div>
    </section>
  );
}

function EmptyState({ status, message }: { status?: string | null; message?: string | null }) {
  const text =
    status === "awaiting_first_refresh"
      ? "Macro positioning will appear after the next weekly data refresh."
      : message ?? "Macro positioning is temporarily unavailable.";
  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
      <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning across major markets.</p>
      <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/45 p-4 text-sm text-slate-300">{text}</div>
    </section>
  );
}

function MacroCard({ market, onOpen }: { market: InsightsMacroPositioningMarket; onOpen: () => void }) {
  const percentile = percentileText(market.percentile);
  return (
    <button
      type="button"
      onClick={onOpen}
      className="min-h-[10rem] rounded-lg border border-white/10 bg-slate-950/45 p-4 text-left transition hover:border-emerald-300/35 hover:bg-slate-950/65 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-300/25"
    >
      <div className="flex items-start justify-between gap-3">
        <h3 className="min-w-0 text-sm font-semibold leading-5 text-white">{market.name}</h3>
        <span className={`shrink-0 text-[10px] font-semibold uppercase tracking-[0.14em] ${biasClassName(market.bias)}`}>
          {biasLabel(market.bias)}
        </span>
      </div>
      <p className="mt-4 text-sm leading-5 text-slate-300">{market.headline ?? "Institutional positioning is available for the latest weekly report."}</p>
      <div className="mt-4 flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
        <span>{percentile ?? trendLabel(market.trend)}</span>
        <span>{updatedText(market.updated_at)}</span>
      </div>
    </button>
  );
}

function MacroFlyout({ market, onClose }: { market: InsightsMacroPositioningMarket; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/70 backdrop-blur-sm" role="dialog" aria-modal="true" aria-label={`${market.name} Positioning`}>
      <button type="button" aria-label="Close Macro Positioning" className="absolute inset-0 cursor-default" onClick={onClose} />
      <aside className="relative z-10 flex h-full w-full max-w-md flex-col overflow-y-auto border-l border-white/10 bg-slate-950 p-5 shadow-2xl shadow-black/50">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">{market.name} Positioning</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning across major markets.</p>
          </div>
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-2.5 py-1.5 text-xs font-semibold text-slate-300 transition hover:border-white/25 hover:text-white">
            Close
          </button>
        </div>

        <div className="mt-8 space-y-7">
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Overall Bias</p>
            <p className={`mt-3 text-lg font-semibold ${biasClassName(market.bias)}`}>{biasLabel(market.bias)}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Rating</p>
            <p className="mt-3 text-sm font-semibold text-slate-100">{convictionText(market.rating)}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section className="grid gap-4 sm:grid-cols-2">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Positioning</p>
              <p className="mt-3 text-sm font-semibold text-slate-100">{percentileText(market.percentile) ?? "Latest available"}</p>
            </div>
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Weekly Trend</p>
              <p className="mt-3 text-sm font-semibold text-slate-100">
                {trendLabel(market.trend)}
                {market.trend_weeks ? ` for ${market.trend_weeks} weeks` : ""}
              </p>
            </div>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Interpretation</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">{market.interpretation ?? "Latest interpretation is unavailable."}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Updated</p>
            <p className="mt-3 text-sm font-semibold text-slate-100">{updatedText(market.updated_at)}</p>
          </section>
        </div>
      </aside>
    </div>
  );
}

function MacroFeedFlyout({ item, onClose }: { item: MacroPositioningFeedItem; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/70 backdrop-blur-sm" role="dialog" aria-modal="true" aria-label={`${item.market_name ?? "Market"} Positioning Detail`}>
      <button type="button" aria-label="Close Positioning Detail" className="absolute inset-0 cursor-default" onClick={onClose} />
      <aside className="relative z-10 flex h-full w-full max-w-md flex-col overflow-y-auto border-l border-white/10 bg-slate-950 p-5 shadow-2xl shadow-black/50">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">{item.market_name ?? "Market"} Detail</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">{formatFeedDate(item.report_date)}</p>
          </div>
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-2.5 py-1.5 text-xs font-semibold text-slate-300 transition hover:border-white/25 hover:text-white">
            Close
          </button>
        </div>
        <div className="mt-8 space-y-7">
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Positioning</p>
            <p className={`mt-3 text-lg font-semibold ${feedPositioningClass(item.positioning)}`}>{feedPositioningLabel(item)}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section className="grid gap-4 sm:grid-cols-2">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Weekly Change</p>
              <p className="mt-3 text-sm font-semibold text-slate-100">{item.weekly_change ?? "Unavailable"}</p>
            </div>
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Historical Range</p>
              <p className="mt-3 text-sm font-semibold text-slate-100">{feedPercentileLabel(item.percentile)}</p>
            </div>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Trend</p>
            <p className="mt-3 text-sm font-semibold text-slate-100">{feedTrendLabel(item)}</p>
          </section>
          <div className="h-px bg-white/10" />
          <section>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Insight</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">{item.insight ?? feedEventLabel(item.event_kind)}</p>
          </section>
        </div>
      </aside>
    </div>
  );
}

function MacroFeedPanel({ overviewSummary }: { overviewSummary?: string | null }) {
  const [data, setData] = useState<MacroPositioningFeedResponse | null>(null);
  const [failed, setFailed] = useState(false);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState<(typeof pageSizeOptions)[number]>(5);
  const [view, setView] = useState<MacroFeedView>("significant");
  const [market, setMarket] = useState("all");
  const [positioning, setPositioning] = useState("all");
  const [event, setEvent] = useState("all");
  const [sort, setSort] = useState("report_date_desc");
  const [selectedItem, setSelectedItem] = useState<MacroPositioningFeedItem | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setFailed(false);
    getMacroPositioningFeed({
      page,
      page_size: pageSize,
      view,
      market,
      positioning,
      event,
      sort,
      signal: controller.signal,
    })
      .then((payload) => {
        if (!controller.signal.aborted) setData(payload);
      })
      .catch((error) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        setFailed(true);
      });
    return () => controller.abort();
  }, [event, market, page, pageSize, positioning, sort, view]);

  const items = data?.items ?? [];
  const total = data?.pagination?.total ?? items.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const showingStart = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const showingEnd = Math.min(total, page * pageSize);
  const summary = data?.summary ?? overviewSummary ?? "Latest institutional positioning activity is available below.";
  const isLocked = data ? !data.entitlement.unlocked || data.status === "locked" : false;

  function resetPageWith(update: () => void) {
    update();
    setPage(1);
  }

  if (isLocked) {
    return (
      <div className="mt-5 rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-sm font-semibold text-white">Macro positioning requires Pro.</p>
            <p className="mt-2 text-sm leading-6 text-slate-300">{data?.locked_copy ?? "Unlock detailed positioning activity across major markets."}</p>
          </div>
          <Link href="/pricing" className={ghostButtonClassName}>
            Upgrade to Pro
          </Link>
        </div>
      </div>
    );
  }

  return (
    <div className="mt-5 space-y-4">
      <div className="rounded-lg border border-white/10 bg-slate-950/45 p-4">
        <div className="flex flex-col gap-2 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Changes This Past Week</p>
            <p className="mt-3 text-sm leading-6 text-slate-200">{summary}</p>
          </div>
          <div className="text-xs text-slate-500 lg:text-right">
            <p>{data?.report_date ? `Report ${formatFeedDate(data.report_date)}` : "Latest report"}</p>
            <p className="mt-1">{data?.updated_at ? updatedText(data.updated_at) : "Updated weekly"}</p>
          </div>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <label className="space-y-1 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
          View
          <select className={selectClassName} value={view} onChange={(event) => resetPageWith(() => setView(event.target.value as MacroFeedView))}>
            <option value="significant">Significant Changes</option>
            <option value="all">All Markets</option>
          </select>
        </label>
        <label className="space-y-1 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
          Market
          <select className={selectClassName} value={market} onChange={(event) => resetPageWith(() => setMarket(event.target.value))}>
            <option value="all">All Markets</option>
            <option value="equity_index">Equity Indexes</option>
            <option value="rates">Rates</option>
            <option value="fx">Currencies</option>
            <option value="metals">Metals</option>
            <option value="energy">Energy</option>
            <option value="crypto">Crypto</option>
          </select>
        </label>
        <label className="space-y-1 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
          Positioning
          <select className={selectClassName} value={positioning} onChange={(event) => resetPageWith(() => setPositioning(event.target.value))}>
            <option value="all">All Positioning</option>
            <option value="bullish">Bullish</option>
            <option value="bearish">Bearish</option>
            <option value="neutral">Neutral</option>
          </select>
        </label>
        <label className="space-y-1 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
          Event
          <select className={selectClassName} value={event} onChange={(event) => resetPageWith(() => setEvent(event.target.value))}>
            <option value="all">All Events</option>
            <option value="crowded">Crowded</option>
            <option value="trend_change">Trend Changes</option>
            <option value="extreme">Extremes</option>
            <option value="weekly_move">Weekly Moves</option>
          </select>
        </label>
        <label className="space-y-1 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
          Sort
          <select className={selectClassName} value={sort} onChange={(event) => resetPageWith(() => setSort(event.target.value))}>
            <option value="report_date_desc">Newest</option>
            <option value="market_asc">Market A-Z</option>
            <option value="percentile_desc">Highest Range</option>
          </select>
        </label>
      </div>

      <div className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/45">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-white/10 text-left text-sm">
            <thead className="bg-slate-950/70 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
              <tr>
                {["Report Date", "Market", "Positioning", "Weekly Change", "Historical Range", "Trend", "Insight"].map((heading) => (
                  <th key={heading} className="px-4 py-3">
                    {heading}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10">
              {!data && !failed ? (
                <tr>
                  <td colSpan={7} className="px-4 py-6 text-slate-400">
                    Loading weekly reports.
                  </td>
                </tr>
              ) : failed ? (
                <tr>
                  <td colSpan={7} className="px-4 py-6 text-slate-400">
                    Positioning activity is temporarily unavailable.
                  </td>
                </tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-6 text-slate-400">
                    No positioning activity matched these filters.
                  </td>
                </tr>
              ) : (
                items.map((item) => (
                  <tr key={item.event_id} className="transition hover:bg-white/[0.03]">
                    <td className="whitespace-nowrap px-4 py-4 text-slate-400">{formatFeedDate(item.report_date)}</td>
                    <td className="px-4 py-4 font-semibold text-white">
                      <button type="button" className="text-left transition hover:text-emerald-200" onClick={() => setSelectedItem(item)}>
                        {item.market_name ?? "Market"}
                      </button>
                      {item.market_group ? <p className="mt-1 text-xs font-normal text-slate-500">{item.market_group}</p> : null}
                    </td>
                    <td className={`whitespace-nowrap px-4 py-4 font-semibold ${feedPositioningClass(item.positioning)}`}>{feedPositioningLabel(item)}</td>
                    <td className="whitespace-nowrap px-4 py-4 text-slate-300">{item.weekly_change ?? "Unavailable"}</td>
                    <td className="whitespace-nowrap px-4 py-4 text-slate-300">{feedPercentileLabel(item.percentile)}</td>
                    <td className="whitespace-nowrap px-4 py-4 text-slate-300">{feedTrendLabel(item)}</td>
                    <td className="min-w-[16rem] px-4 py-4 text-slate-300">{item.insight ?? feedEventLabel(item.event_kind)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="flex flex-col gap-3 text-sm text-slate-400 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-wrap items-center gap-3">
          <span>
            Showing {showingStart}-{showingEnd} of {total}
          </span>
          <select
            className={`${selectClassName} w-auto min-w-[8rem]`}
            value={pageSize}
            onChange={(event) => resetPageWith(() => setPageSize(Number(event.target.value) as (typeof pageSizeOptions)[number]))}
          >
            {pageSizeOptions.map((option) => (
              <option key={option} value={option}>
                {option} / page
              </option>
            ))}
          </select>
        </div>
        <div className="flex items-center gap-2">
          <button type="button" className={ghostButtonClassName} disabled={page <= 1} onClick={() => setPage((current) => Math.max(1, current - 1))}>
            Previous
          </button>
          <span className="text-xs text-slate-500">
            Page {page} of {totalPages}
          </span>
          <button type="button" className={ghostButtonClassName} disabled={page >= totalPages} onClick={() => setPage((current) => Math.min(totalPages, current + 1))}>
            Next
          </button>
        </div>
      </div>

      {selectedItem ? <MacroFeedFlyout item={selectedItem} onClose={() => setSelectedItem(null)} /> : null}
    </div>
  );
}

export function InsightsMacroPositioningClient() {
  const [data, setData] = useState<InsightsMacroPositioningResponse | null>(null);
  const [failed, setFailed] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<MacroPositioningTab>("overview");

  useEffect(() => {
    const controller = new AbortController();
    getInsightsMacroPositioning({ signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setData(payload);
      })
      .catch((error) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        setFailed(true);
        setData({
          status: "unavailable",
          entitlement: { required_plan: "pro", unlocked: true },
          markets: [],
          summary: null,
          message: "Macro positioning is temporarily unavailable.",
        });
      });
    return () => controller.abort();
  }, []);

  const selectedMarket = useMemo(
    () => data?.markets.find((market) => market.id === selectedId) ?? null,
    [data?.markets, selectedId],
  );

  if (!data) return <SectionSkeleton />;
  if (!data.entitlement.unlocked || data.status === "locked") return <LockedSection data={data} />;
  if (failed || data.markets.length === 0 || ["unavailable", "awaiting_first_refresh"].includes(data.status)) {
    return <EmptyState status={data.status} message={data.message} />;
  }

  return (
    <section id="macro-positioning" className={`${cardClassName} scroll-mt-24`}>
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="max-w-4xl">
          <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
          <p className="mt-2 text-sm leading-6 text-slate-400">Institutional futures positioning across major markets.</p>
          {data.stale ? (
            <p className="mt-3 rounded-lg border border-amber-300/20 bg-amber-300/10 px-3 py-2 text-sm text-amber-100">
              Latest weekly positioning data is delayed.
            </p>
          ) : null}
        </div>
        {data.updated_at ? <p className="text-xs text-slate-500 lg:text-right">{updatedText(data.updated_at)}</p> : null}
      </div>

      <div className="mt-5 inline-flex rounded-lg border border-white/10 bg-slate-950/55 p-1">
        {[
          ["overview", "Overview"],
          ["feed", "Weekly Reports"],
        ].map(([value, label]) => (
          <button
            key={value}
            type="button"
            onClick={() => setActiveTab(value as MacroPositioningTab)}
            className={`rounded-md px-3 py-2 text-sm font-semibold transition ${
              activeTab === value ? "bg-emerald-300/15 text-emerald-100" : "text-slate-400 hover:text-white"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {data.summary ? (
        <div className={activeTab === "overview" ? "mt-5 rounded-lg border border-white/10 bg-slate-950/45 p-4" : "hidden"}>
          <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Macro Position Summary</p>
          <p className="mt-3 text-sm leading-6 text-slate-200">{data.summary}</p>
        </div>
      ) : null}

      {activeTab === "overview" ? (
        <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {data.markets.map((market) => (
            <MacroCard key={market.id} market={market} onOpen={() => setSelectedId(market.id)} />
          ))}
        </div>
      ) : (
        <MacroFeedPanel overviewSummary={data.summary} />
      )}

      {selectedMarket ? <MacroFlyout market={selectedMarket} onClose={() => setSelectedId(null)} /> : null}
    </section>
  );
}
