"use client";

import { useEffect, useState } from "react";
import { getAdminProviderUsageFmp, type AdminProviderUsageResponse } from "@/lib/api";

function formatPercent(value?: number | null) {
  return value == null ? "n/a" : `${value.toFixed(1)}%`;
}

function statusClasses(status?: string) {
  if (status === "critical") return "border-rose-300/30 bg-rose-300/10 text-rose-100";
  if (status === "warning") return "border-amber-300/30 bg-amber-300/10 text-amber-100";
  return "border-emerald-300/30 bg-emerald-300/10 text-emerald-100";
}

export function ProviderUsageReport() {
  const [data, setData] = useState<AdminProviderUsageResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    getAdminProviderUsageFmp()
      .then((next) => {
        if (!active) return;
        setData(next);
        setError(null);
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : "Unable to load provider usage.");
      });
    return () => {
      active = false;
    };
  }, []);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold text-white">Provider Usage</h2>
          <p className="mt-1 text-sm text-slate-400">FMP Premium guardrails, cache pressure, throttles, and top consumers.</p>
        </div>
        <span className={`rounded-md border px-3 py-1 text-xs font-semibold uppercase tracking-wide ${statusClasses(data?.status)}`}>
          {data?.status ?? "loading"}
        </span>
      </div>

      {error ? <p className="mt-4 rounded-lg border border-rose-300/20 bg-rose-300/10 p-3 text-sm text-rose-100">{error}</p> : null}

      {data ? (
        <>
          {data.warnings.length ? (
            <div className="mt-4 rounded-lg border border-amber-300/20 bg-amber-300/10 p-3 text-sm text-amber-100">
              {data.recommendation || "Approaching FMP Premium limit. Reduce refresh frequency or upgrade bandwidth."}
            </div>
          ) : null}

          <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="Plan assumption" value={`Premium / ${data.configured_calls_per_minute} calls per minute`} />
            <Metric label="Calls last minute" value={String(data.calls_last_minute)} />
            <Metric label="Calls today" value={String(data.calls_today)} />
            <Metric label="Cache hit rate" value={formatPercent(data.cache_hit_rate)} />
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="Cache mode" value={data.cache_mode} />
            <Metric label="Live page fetch" value={data.live_page_fetch_enabled ? "enabled" : "blocked"} />
            <Metric label="Throttles" value={String(data.totals.throttles)} />
            <Metric label="Fallbacks" value={String(data.totals.fallbacks)} />
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            <UsageList title="Top sources" rows={data.top_routes.slice(0, 8)} />
            <UsageList title="Top categories" rows={data.top_categories.slice(0, 8)} />
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            <EventList title="Recent throttles" rows={data.recent_throttles} />
            <EventList title="Recent errors" rows={data.recent_errors} />
          </div>
        </>
      ) : (
        <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Metric label="Plan assumption" value="Premium / 750 calls per minute" />
          <Metric label="Calls last minute" value="loading" />
          <Metric label="Calls today" value="loading" />
          <Metric label="Cache hit rate" value="loading" />
        </div>
      )}
    </section>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 break-words text-sm font-semibold text-slate-100">{value}</div>
    </div>
  );
}

function UsageList({ title, rows }: { title: string; rows: Array<{ name: string; kind: string; count: number }> }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      <div className="mt-3 space-y-2">
        {rows.length ? rows.map((row) => (
          <div key={`${row.name}-${row.kind}`} className="flex items-center justify-between gap-3 text-sm">
            <span className="min-w-0 truncate text-slate-300">{row.name}</span>
            <span className="shrink-0 text-slate-500">{row.kind}: {row.count}</span>
          </div>
        )) : <p className="text-sm text-slate-500">No usage yet.</p>}
      </div>
    </div>
  );
}

function EventList({ title, rows }: { title: string; rows: Array<{ category?: string | null; route?: string | null; error?: string | null; reason?: string | null; created_at?: string | null; ts?: string | null }> }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      <div className="mt-3 space-y-2">
        {rows.length ? rows.slice(0, 6).map((row, index) => (
          <div key={`${row.category}-${row.created_at ?? row.ts ?? index}`} className="rounded-md border border-white/10 bg-slate-900/60 p-2 text-xs text-slate-400">
            <div className="font-medium text-slate-200">{row.category || "unknown"} · {row.error || row.reason || "event"}</div>
            <div className="mt-1 truncate">{row.route || "background"}</div>
          </div>
        )) : <p className="text-sm text-slate-500">None recorded.</p>}
      </div>
    </div>
  );
}
