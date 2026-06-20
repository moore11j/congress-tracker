"use client";

import { useEffect, useState } from "react";
import { getAdminProviderUsageFmp, type AdminProviderUsageResponse } from "@/lib/api";

function formatPercent(value?: number | null) {
  return value == null ? "n/a" : `${value.toFixed(1)}%`;
}

function statusClasses(status?: string) {
  if (status === "critical") return "border-rose-300/30 bg-rose-300/10 text-rose-100";
  if (status === "warning" || status === "partial" || status === "stale") return "border-amber-300/30 bg-amber-300/10 text-amber-100";
  if (status === "unavailable" || status === "error") return "border-rose-300/30 bg-rose-300/10 text-rose-100";
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
            <Metric label="Calls last 5 min" value={String(data.call_windows?.last_5_min ?? "n/a")} />
            <Metric label="Calls last hour" value={String(data.call_windows?.last_1_hour ?? "n/a")} />
            <Metric label="Calls last 24h" value={String(data.call_windows?.last_24_hours ?? "n/a")} />
            <Metric label="Provider errors" value={String(data.totals.provider_errors)} />
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="Cache mode" value={data.cache_mode} />
            <Metric label="Live page fetch" value={data.live_page_fetch_enabled ? "enabled" : "blocked"} />
            <Metric label="Throttles" value={String(data.totals.throttles)} />
            <Metric label="Fallbacks" value={String(data.totals.fallbacks)} />
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="Budget used" value={`${data.budget?.used_last_minute ?? data.calls_last_minute} / ${data.budget?.throttle_limit_per_minute ?? data.configured_calls_per_minute}`} />
            <Metric label="Soft budget" value={String(data.budget?.soft_limit_per_minute ?? "n/a")} />
            <Metric label="Hard budget" value={String(data.budget?.hard_limit_per_minute ?? "n/a")} />
            <Metric label="Budget state" value={data.budget?.hard_exceeded ? "hard limit exceeded" : data.budget?.soft_exceeded ? "soft limit exceeded" : "within limits"} />
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <Metric label="Fundamentals rows" value={String(data.cache_coverage?.fundamentals_ok_rows ?? data.cache_coverage?.fundamentals_rows ?? "n/a")} />
            <Metric label="Avg volume coverage" value={String(data.cache_coverage?.fundamentals_avg_volume_rows ?? "n/a")} />
            <Metric label="Technical symbols" value={String(data.cache_coverage?.technical_price_history_symbols ?? "n/a")} />
            <Metric
              label="Queued enrichments"
              value={String((data.enrichment_queue?.by_type_status ?? []).filter((row) => row.status === "queued").reduce((sum, row) => sum + (row.count ?? 0), 0))}
            />
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            <UsageList title="Top sources" rows={data.top_routes.slice(0, 8)} />
            <UsageList title="Top categories" rows={data.top_categories.slice(0, 8)} />
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            <ReasonList title="Fallback reasons" rows={data.fallback_reasons ?? data.reasons ?? []} />
            <ContentWriteList title="Ticker content writes" rows={data.content_writes ?? []} />
          </div>

          <div className="mt-5">
            <ContentDiagnostics rows={data.content_diagnostics ?? []} />
          </div>

          <div className="mt-5">
            <FredDiagnostics diagnostics={data.fred_macro_cache ?? null} />
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            <QueueList title="Enrichment queue" rows={data.enrichment_queue?.by_type_status ?? []} />
            <QueueList title="Failed enrichments" rows={data.enrichment_queue?.failed_by_reason ?? []} />
          </div>

          <div className="mt-5 grid gap-4 lg:grid-cols-2">
            <QueueList title="Recent successful enrichments" rows={(data.enrichment_queue?.recent_successes_by_type ?? []).map((row) => ({ ...row, status: "done" }))} />
            <OldestPending title="Oldest pending content job" job={data.enrichment_queue?.oldest_pending_content_job ?? null} />
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

function FredDiagnostics({
  diagnostics,
}: {
  diagnostics: {
    status: string;
    last_refresh_at?: string | null;
    missing_series?: string[];
    stale_series?: string[];
    error_series?: string[];
    series?: Array<{
      series_id: string;
      label?: string | null;
      block?: string | null;
      status?: string | null;
      cache_status?: string | null;
      last_refreshed_at?: string | null;
      latest_observation_date?: string | null;
      observation_count?: number;
      error?: string | null;
    }>;
  } | null;
}) {
  const rows = diagnostics?.series ?? [];
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="font-semibold text-white">FRED macro cache</h3>
          <p className="mt-1 text-sm text-slate-500">US Macro and Treasury source state.</p>
        </div>
        <span className={`rounded-md border px-2 py-1 text-[10px] font-semibold uppercase tracking-wide ${statusClasses(diagnostics?.status)}`}>
          {diagnostics?.status ?? "unavailable"}
        </span>
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-3">
        <Metric label="Last FRED refresh" value={diagnostics?.last_refresh_at ?? "none"} />
        <Metric label="Missing series" value={(diagnostics?.missing_series ?? []).join(", ") || "none"} />
        <Metric label="Stale series" value={(diagnostics?.stale_series ?? []).join(", ") || "none"} />
      </div>
      <div className="mt-3 grid gap-2 lg:grid-cols-2 xl:grid-cols-3">
        {rows.length ? rows.map((row) => (
          <div key={row.series_id} className="rounded-md border border-white/10 bg-slate-900/60 p-3 text-xs text-slate-400">
            <div className="flex items-center justify-between gap-2">
              <span className="font-semibold text-slate-100">{row.series_id}</span>
              <span>{row.cache_status ?? row.status ?? "unknown"}</span>
            </div>
            <div className="mt-1 truncate">{row.label ?? row.block ?? "FRED series"}</div>
            <div className="mt-1">latest: {row.latest_observation_date ?? "missing"}</div>
            <div className="mt-1">rows: {row.observation_count ?? 0}</div>
            {row.error ? <div className="mt-1 truncate text-amber-200">{row.error}</div> : null}
          </div>
        )) : <p className="text-sm text-slate-500">No FRED diagnostics recorded yet.</p>}
      </div>
    </div>
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

function QueueList({ title, rows }: { title: string; rows: Array<{ job_type: string; status?: string | null; reason?: string | null; error?: string | null; count?: number }> }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      <div className="mt-3 space-y-2">
        {rows.length ? rows.slice(0, 8).map((row, index) => (
          <div key={`${row.job_type}-${row.status ?? row.reason ?? index}`} className="flex items-center justify-between gap-3 text-sm">
            <span className="min-w-0 truncate text-slate-300">{row.job_type}</span>
            <span className="shrink-0 text-slate-500">{row.status || row.reason || row.error || "job"}: {row.count ?? 0}</span>
          </div>
        )) : <p className="text-sm text-slate-500">None recorded.</p>}
      </div>
    </div>
  );
}

function ReasonList({ title, rows }: { title: string; rows: Array<{ reason: string; count: number }> }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      <div className="mt-3 space-y-2">
        {rows.length ? rows.slice(0, 8).map((row) => (
          <div key={row.reason} className="flex items-center justify-between gap-3 text-sm">
            <span className="min-w-0 truncate text-slate-300">{row.reason}</span>
            <span className="shrink-0 text-slate-500">{row.count}</span>
          </div>
        )) : <p className="text-sm text-slate-500">None recorded.</p>}
      </div>
    </div>
  );
}

function ContentWriteList({ title, rows }: { title: string; rows: Array<{ category: string; symbol?: string | null; writes: number; items_written: number }> }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      <div className="mt-3 space-y-2">
        {rows.length ? rows.slice(0, 8).map((row) => (
          <div key={`${row.category}-${row.symbol ?? "all"}`} className="flex items-center justify-between gap-3 text-sm">
            <span className="min-w-0 truncate text-slate-300">{row.category}{row.symbol ? ` - ${row.symbol}` : ""}</span>
            <span className="shrink-0 text-slate-500">{row.items_written} items / {row.writes} writes</span>
          </div>
        )) : <p className="text-sm text-slate-500">No content writes yet.</p>}
      </div>
    </div>
  );
}

function ContentDiagnostics({
  rows,
}: {
  rows: Array<{
    content_type: string;
    category: string;
    cache_hits: number;
    cache_misses: number;
    jobs_done: number;
    jobs_queued: number;
    jobs_failed: number;
    items_written: number;
    oldest_pending_at?: string | null;
  }>;
}) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">Ticker content diagnostics</h3>
      <div className="mt-3 grid gap-3 lg:grid-cols-3">
        {rows.length ? rows.map((row) => (
          <div key={row.content_type} className="rounded-md border border-white/10 bg-slate-900/60 p-3 text-sm">
            <div className="font-semibold text-slate-100">{row.content_type}</div>
            <div className="mt-2 space-y-1 text-xs text-slate-400">
              <div>jobs done/queued/failed: {row.jobs_done} / {row.jobs_queued} / {row.jobs_failed}</div>
              <div>items written: {row.items_written}</div>
              <div>cache hits/misses: {row.cache_hits} / {row.cache_misses}</div>
              <div>oldest pending: {row.oldest_pending_at ?? "none"}</div>
            </div>
          </div>
        )) : <p className="text-sm text-slate-500">No ticker content diagnostics yet.</p>}
      </div>
    </div>
  );
}

function OldestPending({ title = "Oldest pending enrichment", job }: { title?: string; job: { job_type: string; symbol?: string | null; reason?: string | null; created_at?: string | null } | null }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      {job ? (
        <div className="mt-3 rounded-md border border-white/10 bg-slate-900/60 p-2 text-xs text-slate-400">
          <div className="font-medium text-slate-200">{job.job_type}{job.symbol ? ` - ${job.symbol}` : ""}</div>
          <div className="mt-1">{job.reason || "queued"}</div>
          <div className="mt-1 truncate">{job.created_at || "created time unavailable"}</div>
        </div>
      ) : <p className="mt-3 text-sm text-slate-500">No pending jobs.</p>}
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
