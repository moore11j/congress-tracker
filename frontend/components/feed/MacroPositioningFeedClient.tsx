"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  getMacroPositioningFeed,
  type MacroPositioningFeedItem,
  type MacroPositioningFeedResponse,
} from "@/lib/api";
import { cardClassName, ghostButtonClassName, selectClassName } from "@/lib/styles";

const pageSizeOptions = [25, 50, 100] as const;

function param(sp: URLSearchParams, key: string, fallback = "") {
  return sp.get(key) ?? fallback;
}

function parsePage(value: string, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(1, Math.floor(parsed)) : fallback;
}

function parsePageSize(value: string) {
  const parsed = Number(value);
  return pageSizeOptions.includes(parsed as 25 | 50 | 100) ? parsed : 25;
}

function formatDate(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(date);
}

function positioningLabel(item: MacroPositioningFeedItem) {
  const value = item.positioning;
  const base = value === "bullish" ? "Bullish" : value === "bearish" ? "Bearish" : value === "neutral" ? "Neutral" : "Unavailable";
  return item.crowded && base !== "Unavailable" ? `${base} / Crowded` : base;
}

function positioningClass(value?: string | null) {
  if (value === "bullish") return "text-emerald-300";
  if (value === "bearish") return "text-rose-300";
  if (value === "neutral") return "text-amber-300";
  return "text-slate-400";
}

function trendLabel(item: MacroPositioningFeedItem) {
  if (item.trend === "increasing") return item.trend_weeks ? `Increasing · ${item.trend_weeks} weeks` : "Increasing";
  if (item.trend === "decreasing") return item.trend_weeks ? `Decreasing · ${item.trend_weeks} weeks` : "Decreasing";
  if (item.trend === "stable") return "Little changed";
  return "Trend unavailable";
}

function percentileLabel(value?: number | null) {
  if (typeof value !== "number" || Number.isNaN(value)) return "-";
  const rounded = Math.round(value);
  if (rounded >= 95) return "Upper 5%";
  if (rounded >= 90) return "Upper 10%";
  if (rounded <= 5) return "Lower 5%";
  if (rounded <= 10) return "Lower 10%";
  return `${rounded}th percentile`;
}

function eventLabel(value?: string | null) {
  if (value === "historical_extreme") return "Historical extreme";
  if (value === "trend_milestone") return "Trend milestone";
  if (value === "major_shift") return "Major shift";
  if (value === "crowding") return "Crowding";
  if (value === "current_state") return "Current state";
  return "Positioning";
}

const queryDefaults: Record<string, string> = {
  event: "all",
  market: "all",
  page: "1",
  page_size: "25",
  positioning: "all",
  sort: "latest",
  view: "significant",
};

const lockedMacroPositioningFallback: MacroPositioningFeedResponse = {
  status: "locked",
  entitlement: { required_plan: "pro", unlocked: false },
  locked_copy: "Track major shifts, trends, and historical extremes in institutional futures positioning.",
  items: [],
  pagination: { page: 1, page_size: 25, total: 0 },
};

function updateParams(router: ReturnType<typeof useRouter>, sp: URLSearchParams, updates: Record<string, string | null>) {
  const next = new URLSearchParams(sp.toString());
  Object.entries(updates).forEach(([key, value]) => {
    if (value === null || value === "" || value === queryDefaults[key]) next.delete(key);
    else next.set(key, value);
  });
  if (!("page" in updates)) next.set("page", "1");
  router.replace(`/feed/macro-positioning${next.toString() ? `?${next.toString()}` : ""}`, { scroll: false });
}

const proButtonClassName = "inline-flex w-fit items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-1.5 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15";

function LockedView({ data }: { data: MacroPositioningFeedResponse }) {
  return (
    <section className={`${cardClassName} space-y-5`}>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h2 className="text-xl font-semibold text-white">Macro Positioning</h2>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">
            {data.locked_copy ?? "Track major shifts, trends, and historical extremes in institutional futures positioning."}
          </p>
          <p className="mt-3 text-sm font-semibold text-emerald-100">Macro positioning requires pro</p>
          <Link href="/pricing" className={`mt-3 ${proButtonClassName}`}>
            View Pro
          </Link>
        </div>
      </div>
      <div className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/45">
        <div className="hidden grid-cols-[1fr_1fr_1fr_1fr_1fr_2fr] gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-3 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500 md:grid">
          <span>Report Date</span>
          <span>Market</span>
          <span>Positioning</span>
          <span>Weekly Change</span>
          <span>Trend</span>
          <span>Insight</span>
        </div>
        <div className="grid gap-3 px-4 py-4 text-sm text-slate-300 md:grid-cols-[1fr_1fr_1fr_1fr_1fr_2fr]">
          <span>Weekly</span>
          <span>Pro feature</span>
          <span>Locked</span>
          <span>Locked</span>
          <span>Locked</span>
          <span>Upgrade to access institutional macro positioning.</span>
        </div>
      </div>
    </section>
  );
}

function DetailDrawer({ item, onClose }: { item: MacroPositioningFeedItem; onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-slate-950/70 backdrop-blur-sm" role="dialog" aria-modal="true" aria-label={`${item.market_name} Positioning`}>
      <button type="button" aria-label="Close Macro Positioning Detail" className="absolute inset-0 cursor-default" onClick={onClose} />
      <aside className="relative z-10 flex h-full w-full max-w-md flex-col overflow-y-auto border-l border-white/10 bg-slate-950 p-5 shadow-2xl shadow-black/50">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">{item.market_name} Positioning</h2>
            <p className="mt-2 text-sm leading-6 text-slate-400">Weekly institutional futures positioning.</p>
          </div>
          <button type="button" onClick={onClose} className="rounded-md border border-white/10 px-2.5 py-1.5 text-xs font-semibold text-slate-300 transition hover:border-white/25 hover:text-white">
            Close
          </button>
        </div>
        <div className="mt-8 space-y-6">
          {[
            ["Overall Positioning", positioningLabel(item)],
            ["Weekly Change", item.weekly_change ?? "Unavailable"],
            ["Historical Range", percentileLabel(item.percentile)],
            ["Trend", trendLabel(item)],
            ["Why it matters", item.insight ?? "Latest weekly positioning is available."],
            ["Report Period", `Week ending ${formatDate(item.report_date)}`],
            ["Updated in Walnut", formatDate(item.updated_at)],
          ].map(([label, value]) => (
            <section key={label}>
              <p className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
              <p className="mt-3 text-sm font-semibold leading-6 text-slate-100">{value}</p>
            </section>
          ))}
          <Link href="/insights#macro-positioning" className={ghostButtonClassName}>
            View Macro Overview
          </Link>
        </div>
      </aside>
    </div>
  );
}

export function MacroPositioningFeedClient({ initialData = null }: { initialData?: MacroPositioningFeedResponse | null }) {
  const router = useRouter();
  const sp = useSearchParams();
  const [data, setData] = useState<MacroPositioningFeedResponse | null>(initialData);
  const [failed, setFailed] = useState(false);
  const [selected, setSelected] = useState<MacroPositioningFeedItem | null>(null);

  const page = parsePage(param(sp, "page", "1"), 1);
  const pageSize = parsePageSize(param(sp, "page_size", "25"));
  const view = param(sp, "view", "significant") === "all" ? "all" : "significant";
  const market = param(sp, "market", "all");
  const positioning = param(sp, "positioning", "all");
  const event = param(sp, "event", "all");
  const sort = param(sp, "sort", "latest");

  useEffect(() => {
    const controller = new AbortController();
    setFailed(false);
    getMacroPositioningFeed({ page, page_size: pageSize, view, market, positioning, event, sort, signal: controller.signal })
      .then((payload) => {
        if (!controller.signal.aborted) setData(payload);
      })
      .catch((error) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        setFailed(true);
        setData(null);
      });
    return () => controller.abort();
  }, [event, market, page, pageSize, positioning, sort, view]);

  const total = data?.pagination?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const items = data?.items ?? [];
  const showing = useMemo(() => `${total === 0 ? 0 : (page - 1) * pageSize + 1}-${Math.min(page * pageSize, total)} of ${total}`, [page, pageSize, total]);

  if (data === null) return <LockedView data={lockedMacroPositioningFallback} />;
  if (data.status === "locked" || data.entitlement?.unlocked === false) return <LockedView data={data} />;

  return (
    <div className="space-y-6">
      <section className="flex flex-col gap-5">
        <div className="flex flex-col gap-2">
          <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Macro Positioning</p>
          <h1 className="text-4xl font-semibold text-white sm:text-5xl">Weekly institutional positioning feed.</h1>
          <p className="max-w-2xl text-sm leading-6 text-slate-400">Track major shifts, trends, and historical extremes across supported macro markets.</p>
          <div className="mt-2 flex flex-wrap gap-2">
            <Link href="/feed" className={ghostButtonClassName}>Market Activity</Link>
            <span className="rounded-md border border-emerald-300/30 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100">Macro Positioning</span>
          </div>
        </div>
      </section>

      <section className={`${cardClassName} space-y-4`}>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h2 className="text-lg font-semibold text-white">Weekly Positioning Summary</h2>
            <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-300">
              {failed ? "Macro positioning is temporarily unavailable." : data?.summary ?? "Weekly summary will appear after the next complete refresh."}
            </p>
            <p className="mt-3 text-xs font-semibold text-slate-500">Report period: {data?.report_date ? `Week ending ${formatDate(data.report_date)}` : "Awaiting weekly data"}</p>
          </div>
          <Link href="/insights#macro-positioning" className={ghostButtonClassName}>View Macro Overview</Link>
        </div>
      </section>

      <section className={`${cardClassName} space-y-4`}>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
          <select className={selectClassName} value={view} onChange={(e) => updateParams(router, sp, { view: e.target.value, page: "1" })}>
            <option value="significant">Significant Changes</option>
            <option value="all">All Markets</option>
          </select>
          <select className={selectClassName} value={market} onChange={(e) => updateParams(router, sp, { market: e.target.value, page: "1" })}>
            <option value="all">All markets</option>
            <option value="equity_indexes">Equity indexes</option>
            <option value="commodities">Commodities</option>
            <option value="currencies">Currencies</option>
            <option value="rates">Rates</option>
            <option value="crypto">Crypto</option>
          </select>
          <select className={selectClassName} value={positioning} onChange={(e) => updateParams(router, sp, { positioning: e.target.value, page: "1" })}>
            <option value="all">All positioning</option>
            <option value="bullish">Bullish</option>
            <option value="bearish">Bearish</option>
            <option value="neutral">Neutral</option>
            <option value="crowded">Crowded</option>
          </select>
          <select className={selectClassName} value={event} onChange={(e) => updateParams(router, sp, { event: e.target.value, page: "1" })}>
            <option value="all">All events</option>
            <option value="major_shift">Major shift</option>
            <option value="trend_milestone">Trend milestone</option>
            <option value="historical_extreme">Historical extreme</option>
            <option value="crowding">Crowding</option>
          </select>
          <select className={selectClassName} value={sort} onChange={(e) => updateParams(router, sp, { sort: e.target.value, page: "1" })}>
            <option value="latest">Newest report</option>
            <option value="oldest">Oldest report</option>
            <option value="market">Market</option>
            <option value="percentile">Historical percentile</option>
          </select>
          <select className={selectClassName} value={String(pageSize)} onChange={(e) => updateParams(router, sp, { page_size: e.target.value, page: "1" })}>
            {pageSizeOptions.map((value) => <option key={value} value={value}>{value} / page</option>)}
          </select>
        </div>

        <div className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/35">
          <div className="hidden grid-cols-[0.9fr_1fr_1fr_1fr_1fr_1fr_2fr] gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-3 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500 lg:grid">
            <span>Report Date</span>
            <span>Market</span>
            <span>Positioning</span>
            <span>Weekly Change</span>
            <span>Historical Range</span>
            <span>Trend</span>
            <span>Insight</span>
          </div>
          {items.length === 0 ? (
            <div className="px-4 py-8 text-sm text-slate-400">No macro positioning rows match the current view.</div>
          ) : (
            <div className="divide-y divide-white/5">
              {items.map((item) => (
                <button key={item.event_id} type="button" onClick={() => setSelected(item)} className="grid w-full gap-3 px-4 py-4 text-left transition hover:bg-emerald-400/[0.06] lg:grid-cols-[0.9fr_1fr_1fr_1fr_1fr_1fr_2fr]">
                  <span className="text-sm text-slate-300">{formatDate(item.report_date)}<span className="mt-1 block text-xs text-slate-500">Weekly data</span></span>
                  <span className="font-semibold text-white">{item.market_name}</span>
                  <span className={`text-xs font-semibold ${positioningClass(item.positioning)}`}>{positioningLabel(item)}</span>
                  <span className="text-sm text-slate-300">{item.weekly_change ?? "-"}</span>
                  <span className="text-sm text-slate-300">{percentileLabel(item.percentile)}</span>
                  <span className="text-sm text-slate-300">{trendLabel(item)}</span>
                  <span className="text-sm leading-5 text-slate-300"><span className="font-semibold text-slate-100">{eventLabel(item.event_kind)}.</span> {item.insight}</span>
                </button>
              ))}
            </div>
          )}
        </div>

        <div className="flex flex-col gap-3 text-sm text-slate-400 sm:flex-row sm:items-center sm:justify-between">
          <span>{showing}</span>
          <div className="flex gap-2">
            <button type="button" disabled={page <= 1} className={ghostButtonClassName} onClick={() => updateParams(router, sp, { page: String(page - 1) })}>Previous</button>
            <button type="button" disabled={page >= totalPages} className={ghostButtonClassName} onClick={() => updateParams(router, sp, { page: String(page + 1) })}>Next</button>
          </div>
        </div>
      </section>

      {selected ? <DetailDrawer item={selected} onClose={() => setSelected(null)} /> : null}
    </div>
  );
}
