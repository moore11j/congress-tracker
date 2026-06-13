"use client";

import { useEffect, useMemo, useState } from "react";
import { getAdminPageAnalytics, type AdminPageAnalyticsPeriod, type AdminPageAnalyticsResponse } from "@/lib/api";

const PERIODS: Array<{ value: AdminPageAnalyticsPeriod; label: string }> = [
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
  { value: "30d", label: "30d" },
];

const PAGE_SIZE_OPTIONS = [10, 25];

function formatPercent(value: number) {
  return `${Number.isFinite(value) ? value.toFixed(1) : "0.0"}%`;
}

function formatDate(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

export function PageAnalyticsReport() {
  const [period, setPeriod] = useState<AdminPageAnalyticsPeriod>("7d");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [data, setData] = useState<AdminPageAnalyticsResponse | null>(null);
  const [status, setStatus] = useState("Loading page analytics.");

  useEffect(() => {
    let ignore = false;
    setStatus("Loading page analytics.");
    getAdminPageAnalytics({ period, limit: 30 })
      .then((next) => {
        if (ignore) return;
        setData(next);
        setStatus("");
      })
      .catch((error) => {
        if (!ignore) setStatus(error instanceof Error ? error.message : "Unable to load page analytics.");
      });
    return () => {
      ignore = true;
    };
  }, [period]);

  const pageCount = Math.max(1, Math.ceil((data?.top_pages.length ?? 0) / pageSize));
  const currentPage = Math.min(page, pageCount);
  const visiblePages = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return data?.top_pages.slice(start, start + pageSize) ?? [];
  }, [currentPage, data, pageSize]);
  const firstVisibleRow = data?.top_pages.length ? (currentPage - 1) * pageSize + 1 : 0;
  const lastVisibleRow = data?.top_pages.length ? Math.min(currentPage * pageSize, data.top_pages.length) : 0;
  const totalViews = useMemo(() => data?.top_pages.reduce((sum, row) => sum + row.views, 0) ?? 0, [data]);
  const maxTrend = useMemo(() => Math.max(1, ...(data?.trend_by_day.map((row) => row.views) ?? [1])), [data]);

  useEffect(() => {
    setPage((current) => Math.min(current, pageCount));
  }, [pageCount]);

  return (
    <section className="rounded-2xl border border-white/10 bg-slate-950/70 p-5 shadow-2xl shadow-black/20">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-white">Page analytics</h2>
          <p className="mt-1 text-sm text-slate-400">First-party usage by route, audience, plan, and device.</p>
        </div>
        <div className="flex rounded-lg border border-white/10 bg-slate-900/70 p-1">
          {PERIODS.map((option) => (
            <button
              key={option.value}
              type="button"
              onClick={() => {
                setPeriod(option.value);
                setPage(1);
              }}
              className={`rounded-md px-3 py-1.5 text-sm font-semibold ${period === option.value ? "bg-emerald-300 text-slate-950" : "text-slate-300 hover:text-white"}`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      {status ? <p className="mt-4 text-sm text-slate-400">{status}</p> : null}

      {data ? (
        <>
          <div className="mt-5 grid gap-3 sm:grid-cols-3">
            <div className="rounded-lg border border-white/10 bg-slate-900/60 p-3">
              <div className="text-xs font-semibold uppercase text-slate-500">Views</div>
              <div className="mt-1 text-2xl font-semibold text-white">{totalViews.toLocaleString()}</div>
            </div>
            <div className="rounded-lg border border-white/10 bg-slate-900/60 p-3">
              <div className="text-xs font-semibold uppercase text-slate-500">Tracked pages</div>
              <div className="mt-1 text-2xl font-semibold text-white">{data.top_pages.length.toLocaleString()}</div>
            </div>
            <div className="rounded-lg border border-white/10 bg-slate-900/60 p-3">
              <div className="text-xs font-semibold uppercase text-slate-500">Generated</div>
              <div className="mt-1 text-sm font-semibold text-slate-200">{formatDate(data.generated_at)}</div>
            </div>
          </div>

          <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
            <table className="min-w-full divide-y divide-white/10 text-left text-sm">
              <thead className="bg-white/5 text-xs uppercase tracking-wide text-slate-400">
                <tr>
                  <th className="px-3 py-3">Page</th>
                  <th className="px-3 py-3">Views</th>
                  <th className="px-3 py-3">Unique users</th>
                  <th className="px-3 py-3">Auth %</th>
                  <th className="px-3 py-3">Premium/Pro %</th>
                  <th className="px-3 py-3">Mobile %</th>
                  <th className="px-3 py-3">Last viewed</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-white/10">
                {visiblePages.map((row) => (
                  <tr key={row.page} className="text-slate-300">
                    <td className="whitespace-nowrap px-3 py-3 font-mono text-slate-100">{row.page}</td>
                    <td className="whitespace-nowrap px-3 py-3 tabular-nums">{row.views.toLocaleString()}</td>
                    <td className="whitespace-nowrap px-3 py-3 tabular-nums">{row.unique_users.toLocaleString()}</td>
                    <td className="whitespace-nowrap px-3 py-3 tabular-nums">{formatPercent(row.auth_percent)}</td>
                    <td className="whitespace-nowrap px-3 py-3 tabular-nums">{formatPercent(row.paid_percent)}</td>
                    <td className="whitespace-nowrap px-3 py-3 tabular-nums">{formatPercent(row.mobile_percent)}</td>
                    <td className="whitespace-nowrap px-3 py-3">{formatDate(row.last_viewed_at)}</td>
                  </tr>
                ))}
                {data.top_pages.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-3 py-6 text-center text-slate-500">No page views tracked for this period.</td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
            <div className="text-sm text-slate-400">
              Page {currentPage} of {pageCount}
              {data.top_pages.length ? ` - Showing ${firstVisibleRow}-${lastVisibleRow} of ${data.top_pages.length}` : ""}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-2 text-sm text-slate-400">
                <span>Rows</span>
                <select
                  value={pageSize}
                  onChange={(event) => {
                    setPageSize(Number(event.target.value));
                    setPage(1);
                  }}
                  className="rounded-lg border border-white/10 bg-slate-950 px-2 py-1 text-sm text-white outline-none focus:border-emerald-300/50"
                >
                  {PAGE_SIZE_OPTIONS.map((option) => (
                    <option key={option} value={option}>
                      {option}
                    </option>
                  ))}
                </select>
              </label>
              <button
                type="button"
                onClick={() => setPage(1)}
                disabled={currentPage <= 1}
                className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
              >
                First
              </button>
              <button
                type="button"
                onClick={() => setPage((current) => Math.max(1, current - 1))}
                disabled={currentPage <= 1}
                className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
              >
                Previous
              </button>
              <button
                type="button"
                onClick={() => setPage((current) => Math.min(pageCount, current + 1))}
                disabled={currentPage >= pageCount}
                className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
              >
                Next
              </button>
              <button
                type="button"
                onClick={() => setPage(pageCount)}
                disabled={currentPage >= pageCount}
                className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 disabled:opacity-50"
              >
                Last
              </button>
            </div>
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-[1fr_1.2fr]">
            <div className="rounded-lg border border-white/10 bg-slate-900/50 p-4">
              <h3 className="text-sm font-semibold text-slate-200">Low usage</h3>
              <div className="mt-3 space-y-2">
                {data.low_usage_pages.map((row) => (
                  <div key={row.page} className="flex items-center justify-between gap-3 text-sm">
                    <span className="truncate font-mono text-slate-300">{row.page}</span>
                    <span className="shrink-0 tabular-nums text-slate-400">{row.views}</span>
                  </div>
                ))}
                {data.low_usage_pages.length === 0 ? <p className="text-sm text-slate-500">No low-usage pages yet.</p> : null}
              </div>
            </div>
            <div className="rounded-lg border border-white/10 bg-slate-900/50 p-4">
              <h3 className="text-sm font-semibold text-slate-200">Trend by day</h3>
              <div className="mt-4 flex h-28 items-end gap-2">
                {data.trend_by_day.map((row) => (
                  <div key={row.day} className="flex min-w-8 flex-1 flex-col items-center gap-2">
                    <div className="w-full rounded-t bg-emerald-300/70" style={{ height: `${Math.max(6, (row.views / maxTrend) * 100)}%` }} />
                    <span className="text-[10px] text-slate-500">{row.day.slice(5)}</span>
                  </div>
                ))}
                {data.trend_by_day.length === 0 ? <p className="text-sm text-slate-500">No daily trend yet.</p> : null}
              </div>
            </div>
          </div>
        </>
      ) : null}
    </section>
  );
}
