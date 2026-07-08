"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  getAdminDataArchitecture,
  type AdminArchitecturePipeline,
  type AdminArchitectureProvider,
  type AdminArchitectureRoute,
  type AdminArchitectureStatus,
  type AdminDataArchitectureResponse,
} from "@/lib/api";

const STATUS_LABELS: Record<string, string> = {
  healthy: "Healthy",
  degraded: "Degraded",
  down: "Down",
  unknown: "Unknown",
  configured: "Configured",
  missing: "Missing",
};

const ARCHITECTURE_NODES = [
  {
    id: "frontend",
    label: "Vercel / Next.js Frontend",
    description: "Admin UI and public app pages",
    x: 44,
    y: 26,
    statusKey: "frontend",
  },
  {
    id: "backend",
    label: "Fly.io / FastAPI Backend",
    description: "API routes and operational services",
    x: 44,
    y: 130,
    statusKey: "backend",
  },
  {
    id: "auth",
    label: "Auth + Entitlements",
    description: "Session, plan, and admin-only gates",
    x: 44,
    y: 242,
    statusKey: "backend",
  },
  {
    id: "routes",
    label: "API Routes",
    description: "/events, /quotes, /signals-summary",
    x: 292,
    y: 242,
    statusKey: "routes",
  },
  {
    id: "observability",
    label: "Admin Observability",
    description: "/api/admin/data-architecture",
    x: 540,
    y: 242,
    statusKey: "routes",
  },
  {
    id: "cache",
    label: "Cache Layer",
    description: "Quotes, prices, fundamentals, content",
    x: 44,
    y: 372,
    statusKey: "cache",
  },
  {
    id: "database",
    label: "Production DB",
    description: "Normalized events, caches, users",
    x: 292,
    y: 372,
    statusKey: "database",
  },
  {
    id: "jobs",
    label: "Background Jobs / Ingestion",
    description: "Scheduled refresh and queue workers",
    x: 540,
    y: 372,
    statusKey: "jobs",
  },
  {
    id: "providers",
    label: "Provider Layer",
    description: "Congress, SEC, market, contracts, 13F, options, email",
    x: 292,
    y: 506,
    statusKey: "providers",
  },
] as const;

const ARCHITECTURE_LINES = [
  ["frontend", "backend"],
  ["backend", "auth"],
  ["backend", "routes"],
  ["backend", "observability"],
  ["backend", "cache"],
  ["backend", "database"],
  ["backend", "jobs"],
  ["jobs", "providers"],
  ["cache", "database"],
] as const;

function normalizeStatus(value?: string | null): AdminArchitectureStatus {
  const normalized = (value || "unknown").toLowerCase();
  if (normalized === "ok") return "healthy";
  if (normalized === "unavailable") return "down";
  return normalized;
}

function statusLabel(value?: string | null) {
  const normalized = normalizeStatus(value);
  return STATUS_LABELS[normalized] ?? normalized.replaceAll("_", " ");
}

function statusClass(value?: string | null) {
  const normalized = normalizeStatus(value);
  if (normalized === "healthy" || normalized === "configured") return "border-emerald-300/35 bg-emerald-300/10 text-emerald-100";
  if (normalized === "degraded") return "border-amber-300/35 bg-amber-300/10 text-amber-100";
  if (normalized === "down" || normalized === "missing") return "border-rose-300/35 bg-rose-300/10 text-rose-100";
  return "border-slate-500/40 bg-slate-900/80 text-slate-300";
}

function formatDate(value?: string | null) {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatLatency(value?: number | null) {
  if (value === null || value === undefined) return "Unknown";
  return `${Math.round(value).toLocaleString()} ms`;
}

function formatRate(value?: number | null) {
  if (value === null || value === undefined) return "Unknown";
  return `${(value * 100).toFixed(value < 0.01 && value > 0 ? 2 : 1)}%`;
}

function formatCount(value?: number | null) {
  if (value === null || value === undefined) return "Unknown";
  return value.toLocaleString();
}

function shortError(value?: string | null) {
  if (!value) return "None";
  return value.replace(/\s+/g, " ").trim().slice(0, 180);
}

function nodeStatus(data: AdminDataArchitectureResponse, key: string) {
  if (key === "frontend") return data.frontend.status;
  if (key === "backend") return data.backend.status;
  if (key === "cache") return data.cache.status;
  if (key === "database") return data.database.status;
  if (key === "jobs") return data.background_jobs?.status ?? "unknown";
  if (key === "providers") return data.summary.providers.unavailable > 0 ? "down" : data.summary.providers.degraded > 0 ? "degraded" : data.summary.providers.healthy > 0 ? "healthy" : "unknown";
  if (key === "routes") return data.summary.backend_routes.down > 0 ? "down" : data.summary.backend_routes.degraded > 0 ? "degraded" : data.summary.backend_routes.healthy > 0 ? "healthy" : "unknown";
  return "unknown";
}

function statusFill(value?: string | null) {
  const normalized = normalizeStatus(value);
  if (normalized === "healthy") return "#34d399";
  if (normalized === "degraded") return "#fbbf24";
  if (normalized === "down") return "#fb7185";
  return "#94a3b8";
}

function StatusPill({ status }: { status?: string | null }) {
  return (
    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide ${statusClass(status)}`}>
      {statusLabel(status)}
    </span>
  );
}

function Section({
  title,
  children,
  description,
}: {
  title: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <section className="space-y-4">
      <div>
        <h3 className="text-lg font-semibold text-white">{title}</h3>
        {description ? <p className="mt-1 max-w-3xl text-sm text-slate-400">{description}</p> : null}
      </div>
      {children}
    </section>
  );
}

function SummaryCard({ title, status, rows }: { title: string; status?: string | null; rows: Array<[string, string]> }) {
  return (
    <article className="rounded-lg border border-white/10 bg-slate-950/50 p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-sm font-semibold text-white">{title}</h3>
        <StatusPill status={status} />
      </div>
      <dl className="mt-4 grid gap-2 text-sm">
        {rows.map(([label, value]) => (
          <div key={label} className="flex items-center justify-between gap-4">
            <dt className="text-slate-400">{label}</dt>
            <dd className="text-right font-medium text-slate-100">{value}</dd>
          </div>
        ))}
      </dl>
    </article>
  );
}

function Summary({ data }: { data: AdminDataArchitectureResponse }) {
  const providerStatus = data.summary.providers.unavailable > 0 ? "down" : data.summary.providers.degraded > 0 ? "degraded" : data.summary.providers.healthy > 0 ? "healthy" : "unknown";
  const routeStatus = data.summary.backend_routes.down > 0 ? "down" : data.summary.backend_routes.degraded > 0 ? "degraded" : data.summary.backend_routes.healthy > 0 ? "healthy" : "unknown";
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
      <SummaryCard
        title="Overall Data Health"
        status={data.overall_status}
        rows={[
          ["Snapshot", data.stale ? "Stale" : "Fresh"],
          ["Generated", formatDate(data.snapshot_generated_at)],
        ]}
      />
      <SummaryCard
        title="Backend API Health"
        status={routeStatus}
        rows={[
          ["Healthy routes", String(data.summary.backend_routes.healthy)],
          ["Degraded", String(data.summary.backend_routes.degraded)],
          ["Down", String(data.summary.backend_routes.down)],
          ["p95 latency", formatLatency(data.summary.backend_routes.p95_latency_ms)],
        ]}
      />
      <SummaryCard
        title="Provider Health"
        status={providerStatus}
        rows={[
          ["Healthy", String(data.summary.providers.healthy)],
          ["Degraded", String(data.summary.providers.degraded)],
          ["Unavailable", String(data.summary.providers.unavailable)],
          ["Last snapshot", formatDate(data.summary.providers.last_snapshot_at)],
        ]}
      />
      <SummaryCard
        title="Cache / DB Health"
        status={data.summary.cache_db.db_status === "down" ? "down" : data.summary.cache_db.cache_status}
        rows={[
          ["Cache", statusLabel(data.summary.cache_db.cache_status)],
          ["DB", statusLabel(data.summary.cache_db.db_status)],
          ["Jobs", statusLabel(data.summary.cache_db.background_jobs_status)],
          ["Last refresh", formatDate(data.summary.cache_db.last_successful_refresh_at)],
        ]}
      />
    </div>
  );
}

function ArchitectureMap({ data }: { data: AdminDataArchitectureResponse }) {
  const nodeById = Object.fromEntries(ARCHITECTURE_NODES.map((node) => [node.id, node]));
  return (
    <div className="overflow-hidden rounded-lg border border-white/10 bg-slate-950/40">
      <div className="overflow-x-auto">
        <svg className="min-w-[760px]" viewBox="0 0 760 620" role="img" aria-label="Walnut data architecture map">
          <defs>
            <marker id="arrow" markerHeight="8" markerWidth="8" orient="auto" refX="7" refY="4">
              <path d="M0,0 L8,4 L0,8 Z" fill="#64748b" />
            </marker>
          </defs>
          {ARCHITECTURE_LINES.map(([from, to]) => {
            const a = nodeById[from];
            const b = nodeById[to];
            return (
              <line
                key={`${from}-${to}`}
                x1={a.x + 88}
                y1={a.y + 64}
                x2={b.x + 88}
                y2={b.y}
                stroke="#64748b"
                strokeWidth="2"
                markerEnd="url(#arrow)"
              />
            );
          })}
          {ARCHITECTURE_NODES.map((node) => {
            const status = nodeStatus(data, node.statusKey);
            return (
              <g key={node.id}>
                <rect x={node.x} y={node.y} width="176" height="76" rx="8" fill="#020617" stroke="#334155" />
                <circle cx={node.x + 18} cy={node.y + 18} r="5" fill={statusFill(status)} />
                <text x={node.x + 32} y={node.y + 22} fill="#f8fafc" fontSize="12" fontWeight="700">
                  {node.label}
                </text>
                <foreignObject x={node.x + 14} y={node.y + 34} width="148" height="34">
                  <p className="text-[10px] leading-snug text-slate-400">{node.description}</p>
                </foreignObject>
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}

function PipelineCard({ pipeline }: { pipeline: AdminArchitecturePipeline }) {
  return (
    <article className="rounded-lg border border-white/10 bg-slate-950/50 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h4 className="text-base font-semibold text-white">{pipeline.name}</h4>
          <p className="mt-1 text-sm text-slate-400">Source: {pipeline.source}</p>
        </div>
        <StatusPill status={pipeline.health} />
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        {pipeline.flow.map((step, index) => (
          <span key={`${pipeline.id}-${step}`} className="inline-flex items-center gap-2 text-xs text-slate-300">
            <span className="rounded-md border border-white/10 bg-slate-900 px-2 py-1">{step}</span>
            {index < pipeline.flow.length - 1 ? <span className="text-slate-500">-&gt;</span> : null}
          </span>
        ))}
      </div>
      <dl className="mt-4 grid gap-2 text-sm sm:grid-cols-2">
        <Metric label="Last ingest" value={formatDate(pipeline.last_ingest_at)} />
        <Metric label="Last successful ingest" value={formatDate(pipeline.last_success_at)} />
        <Metric label="Rows/events available" value={formatCount(pipeline.record_count)} />
        <Metric label="Latest error" value={shortError(pipeline.latest_error)} />
      </dl>
      {pipeline.notes ? <p className="mt-3 text-xs text-slate-400">{pipeline.notes}</p> : null}
    </article>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-xs uppercase tracking-wide text-slate-500">{label}</dt>
      <dd className="mt-1 text-slate-100">{value}</dd>
    </div>
  );
}

function TableShell({ children }: { children: ReactNode }) {
  return <div className="overflow-x-auto rounded-lg border border-white/10 bg-slate-950/40">{children}</div>;
}

function Th({ children }: { children: ReactNode }) {
  return <th className="whitespace-nowrap px-3 py-3 text-left text-xs font-semibold uppercase tracking-wide text-slate-400">{children}</th>;
}

function Td({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <td className={`align-top px-3 py-3 text-sm text-slate-200 ${className}`}>{children}</td>;
}

function SecretStatus({ provider }: { provider: AdminArchitectureProvider }) {
  return (
    <div className="space-y-1">
      <StatusPill status={provider.secret_status} />
      <div className="text-xs text-slate-500">{provider.secret_names.length ? provider.secret_names.join(", ") : "No secret name"}</div>
    </div>
  );
}

function ProviderEndpoints({ providers }: { providers: AdminArchitectureProvider[] }) {
  return (
    <TableShell>
      <table className="min-w-[1040px] w-full border-collapse">
        <thead className="border-b border-white/10 bg-slate-950/70">
          <tr>
            <Th>Provider</Th>
            <Th>Purpose</Th>
            <Th>Endpoint URL / route template</Th>
            <Th>Secret name</Th>
            <Th>Secret status</Th>
            <Th>Health</Th>
            <Th>p95 latency</Th>
            <Th>Last checked</Th>
            <Th>Last successful refresh</Th>
            <Th>Latest error</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10">
          {providers.map((provider) => (
            <tr key={provider.id}>
              <Td className="font-medium text-white">{provider.name}</Td>
              <Td>{provider.purpose}</Td>
              <Td className="max-w-[260px] break-words font-mono text-xs text-slate-300">{provider.safe_endpoint_url ?? "Unknown"}</Td>
              <Td>{provider.secret_names.length ? provider.secret_names.join(", ") : "None"}</Td>
              <Td>
                <SecretStatus provider={provider} />
              </Td>
              <Td>
                <StatusPill status={provider.health} />
              </Td>
              <Td>{formatLatency(provider.p95_latency_ms)}</Td>
              <Td>{formatDate(provider.last_checked_at)}</Td>
              <Td>{formatDate(provider.last_success_at)}</Td>
              <Td>{shortError(provider.latest_error)}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </TableShell>
  );
}

function InternalApiHealth({ routes }: { routes: AdminArchitectureRoute[] }) {
  return (
    <TableShell>
      <table className="min-w-[900px] w-full border-collapse">
        <thead className="border-b border-white/10 bg-slate-950/70">
          <tr>
            <Th>Route</Th>
            <Th>Method</Th>
            <Th>Consumer</Th>
            <Th>Health</Th>
            <Th>p95 latency</Th>
            <Th>Error rate</Th>
            <Th>Last seen</Th>
            <Th>Notes</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10">
          {routes.map((route) => (
            <tr key={`${route.method}-${route.route}`}>
              <Td className="font-mono text-xs text-slate-100">{route.route}</Td>
              <Td>{route.method}</Td>
              <Td>{route.consumer}</Td>
              <Td>
                <StatusPill status={route.health} />
              </Td>
              <Td>{formatLatency(route.p95_latency_ms)}</Td>
              <Td>{formatRate(route.error_rate)}</Td>
              <Td>{formatDate(route.last_seen_at)}</Td>
              <Td>{route.notes ?? "None"}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </TableShell>
  );
}

function RecentHealthEvents({ data }: { data: AdminDataArchitectureResponse }) {
  const events = data.recent_events ?? [];
  if (!events.length) {
    return <p className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">No recent health events or errors are available in the cached snapshot.</p>;
  }
  return (
    <div className="grid gap-3 md:grid-cols-2">
      {events.map((event, index) => (
        <article key={`${event.source}-${index}`} className="rounded-lg border border-white/10 bg-slate-950/50 p-4">
          <div className="flex items-start justify-between gap-3">
            <h4 className="text-sm font-semibold text-white">{event.source}</h4>
            <StatusPill status={event.health} />
          </div>
          <p className="mt-2 text-sm text-slate-400">Last updated: {formatDate(event.last_checked_at)}</p>
          {event.latest_error ? (
            <details className="mt-3 text-sm text-slate-300">
              <summary className="cursor-pointer text-slate-200">Latest error summary</summary>
              <p className="mt-2 break-words rounded-md border border-white/10 bg-slate-900 p-3 font-mono text-xs text-slate-300">{shortError(event.latest_error)}</p>
            </details>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function LoadingState() {
  return (
    <div className="space-y-4">
      <div className="h-24 animate-pulse rounded-lg border border-white/10 bg-slate-950/50" />
      <div className="h-72 animate-pulse rounded-lg border border-white/10 bg-slate-950/50" />
    </div>
  );
}

function ErrorState({ message }: { message: string }) {
  return (
    <div className="rounded-lg border border-rose-300/25 bg-rose-950/30 p-4 text-sm text-rose-100">
      Unable to load the data architecture snapshot. {message}
    </div>
  );
}

export function DataSourcesReport() {
  const [data, setData] = useState<AdminDataArchitectureResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    getAdminDataArchitecture()
      .then((next) => {
        if (!active) return;
        setData(next);
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : "Unknown error.");
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  const providerCount = useMemo(() => data?.providers.length ?? 0, [data]);

  return (
    <div className="space-y-8">
      <header className="space-y-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <h2 className="text-2xl font-semibold text-white">Data Architecture</h2>
            <p className="mt-2 max-w-4xl text-sm text-slate-400">
              Read-only overview of Walnut's data pipelines, provider connectivity, cache health, backend routes, and operational status.
            </p>
          </div>
          {data ? <StatusPill status={data.overall_status} /> : null}
        </div>
        <p className="rounded-lg border border-cyan-300/20 bg-cyan-300/10 px-4 py-3 text-sm text-cyan-100">
          Read-only architecture view. Configuration changes live in Settings or environment secrets.
        </p>
        {data ? (
          <div className="flex flex-wrap gap-x-6 gap-y-2 text-xs text-slate-400">
            <span>Last updated: {formatDate(data.snapshot_generated_at)}</span>
            <span>Snapshot freshness: {data.stale ? "Stale" : "Fresh"}</span>
            <span>Provider dependencies: {providerCount}</span>
          </div>
        ) : null}
      </header>

      {loading ? <LoadingState /> : null}
      {error ? <ErrorState message={error} /> : null}

      {data ? (
        <>
          <Section title="Summary">
            <Summary data={data} />
          </Section>

          <Section title="Architecture Map" description="Current production data flow from frontend surfaces through backend, auth, storage, jobs, and provider dependencies.">
            <ArchitectureMap data={data} />
          </Section>

          <Section title="Data Pipelines">
            <div className="grid gap-3 xl:grid-cols-2">
              {data.pipelines.map((pipeline) => (
                <PipelineCard key={pipeline.id} pipeline={pipeline} />
              ))}
            </div>
          </Section>

          <Section title="Provider Endpoints" description="Safe base URLs and route templates only. Secret values, headers, query tokens, and credential-like fragments are not rendered.">
            <ProviderEndpoints providers={data.providers} />
          </Section>

          <Section title="Internal API Health">
            <InternalApiHealth routes={data.internal_routes} />
          </Section>

          <Section title="Recent Health Events / Errors">
            <RecentHealthEvents data={data} />
          </Section>
        </>
      ) : null}
    </div>
  );
}
