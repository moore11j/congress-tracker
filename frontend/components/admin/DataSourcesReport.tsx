"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  getAdminDataSourcesStatus,
  runAdminDataSource,
  updateAdminDataSourceSetting,
  type AdminDataSourceDomain,
  type AdminDataSourcesStatusResponse,
} from "@/lib/api";

function badgeClass(label: string) {
  const normalized = label.toLowerCase();
  if (normalized.includes("error") || normalized.includes("unsafe")) return "border-rose-300/30 bg-rose-300/10 text-rose-100";
  if (normalized.includes("stale") || normalized.includes("warning") || normalized.includes("risk")) return "border-amber-300/30 bg-amber-300/10 text-amber-100";
  if (normalized.includes("disabled") || normalized.includes("missing")) return "border-white/10 bg-slate-950/60 text-slate-300";
  if (normalized.includes("shadow") || normalized.includes("dry")) return "border-cyan-300/30 bg-cyan-300/10 text-cyan-100";
  return "border-emerald-300/30 bg-emerald-300/10 text-emerald-100";
}

function formatDate(value?: string | null) {
  if (!value) return "none";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "none";
  if (typeof value === "number") return value.toLocaleString();
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (Array.isArray(value)) return value.length ? value.map(formatValue).join(", ") : "none";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function domainMatchesFilter(domain: AdminDataSourceDomain, filter: string) {
  if (filter === "All") return true;
  if (filter === "Safe") return domain.builder_safe_status === "safe";
  if (filter === "Warning") return domain.builder_safe_status === "warning";
  if (filter === "Unsafe") return domain.builder_safe_status === "unsafe";
  if (filter === "External APIs") return domain.source_type === "external API";
  if (filter === "Official Sources") return domain.source_type === "public official source";
  if (filter === "Cache-only") return domain.source_type === "local cache";
  if (filter === "Errors") return Boolean(domain.last_error);
  if (filter === "Stale") return domain.stale_status === "stale" || domain.stale_status === "missing";
  if (filter === "Disabled") return domain.mode === "disabled" || !domain.settings.is_enabled;
  return true;
}

export function DataSourcesReport() {
  const [data, setData] = useState<AdminDataSourcesStatusResponse | null>(null);
  const [filter, setFilter] = useState("All");
  const [error, setError] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);

  const refresh = async () => {
    try {
      const next = await getAdminDataSourcesStatus();
      setData(next);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load data sources.");
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const rows = useMemo(
    () => (data?.domains ?? []).filter((domain) => domainMatchesFilter(domain, filter)),
    [data?.domains, filter],
  );

  const updateDomain = async (domain: AdminDataSourceDomain, patch: Record<string, unknown>) => {
    setBusyKey(domain.domain_key);
    setStatus(null);
    try {
      await updateAdminDataSourceSetting(domain.domain_key, {
        ...patch,
        reason: "admin_data_sources_ui",
      });
      await refresh();
      setStatus(`${domain.data_domain} updated.`);
    } catch (err) {
      setStatus(err instanceof Error ? err.message : "Unable to update provider setting.");
    } finally {
      setBusyKey(null);
    }
  };

  const runDomain = async (domain: AdminDataSourceDomain, mode = "dry_run") => {
    setBusyKey(domain.domain_key);
    setStatus(null);
    try {
      const result = await runAdminDataSource(domain.domain_key, { mode, reason: "admin_data_sources_ui" });
      await refresh();
      setStatus(`${domain.data_domain} queued as ${result.job?.job_type ?? "job"}.`);
    } catch (err) {
      setStatus(err instanceof Error ? err.message : "Unable to queue data source run.");
    } finally {
      setBusyKey(null);
    }
  };

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold text-white">Data Sources</h2>
          <p className="mt-1 text-sm text-slate-400">Provider, cache, job, and official-pipeline status.</p>
        </div>
        <button
          type="button"
          onClick={refresh}
          className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200"
        >
          Refresh
        </button>
      </div>

      {error ? <p className="mt-4 rounded-lg border border-rose-300/20 bg-rose-300/10 p-3 text-sm text-rose-100">{error}</p> : null}
      {status ? <p className="mt-4 rounded-lg border border-white/10 bg-slate-950/50 p-3 text-sm text-slate-300">{status}</p> : null}

      <div className="mt-5 flex flex-wrap gap-2">
        {(data?.filters ?? ["All", "Safe", "Warning", "Unsafe", "External APIs", "Official Sources", "Cache-only", "Errors", "Stale", "Disabled"]).map((item) => (
          <button
            key={item}
            type="button"
            onClick={() => setFilter(item)}
            className={`rounded-md border px-3 py-1.5 text-xs font-semibold ${
              filter === item ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-300"
            }`}
          >
            {item}
          </button>
        ))}
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <Metric label="Domains" value={String(data?.domains.length ?? "loading")} />
        <Metric label="Official shadow rows" value={String((data?.diagnostics.congress.normalized_transactions as number | undefined) ?? 0)} />
        <Metric label="SEC normalized rows" value={String((data?.diagnostics.insider.form4_filings_discovered as number | undefined) ?? 0)} />
        <Metric label="Generated" value={formatDate(data?.generated_at)} />
      </div>

      <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
        <table className="min-w-[1500px] w-full border-collapse text-left text-xs">
          <thead className="bg-slate-950/80 text-slate-400">
            <tr>
              <Th>Domain</Th>
              <Th>Provider</Th>
              <Th>Fallback</Th>
              <Th>Mode</Th>
              <Th>Enabled</Th>
              <Th>Type</Th>
              <Th>Status</Th>
              <Th>Endpoint / job</Th>
              <Th>Refresh</Th>
              <Th>Cache</Th>
              <Th>Rows</Th>
              <Th>Calls 24h</Th>
              <Th>Queue</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {rows.length ? rows.map((domain) => (
              <tr key={domain.domain_key} className="bg-slate-950/30 align-top text-slate-300">
                <Td>
                  <div className="font-semibold text-slate-100">{domain.data_domain}</div>
                  <div className="mt-1 text-[11px] text-slate-500">{domain.domain_key}</div>
                  {domain.notes ? <div className="mt-1 max-w-64 text-[11px] leading-4 text-slate-500">{domain.notes}</div> : null}
                </Td>
                <Td>
                  <select
                    value={domain.active_provider}
                    disabled={busyKey === domain.domain_key}
                    onChange={(event) => updateDomain(domain, { active_provider: event.target.value })}
                    className="w-40 rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100"
                  >
                    {(data?.provider_options ?? []).map((provider) => <option key={provider} value={provider}>{provider}</option>)}
                  </select>
                </Td>
                <Td>
                  <select
                    value={domain.fallback_provider ?? "none"}
                    disabled={busyKey === domain.domain_key}
                    onChange={(event) => updateDomain(domain, { fallback_provider: event.target.value === "none" ? null : event.target.value })}
                    className="w-40 rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100"
                  >
                    <option value="none">none</option>
                    {(data?.provider_options ?? []).filter((provider) => provider !== "none").map((provider) => <option key={provider} value={provider}>{provider}</option>)}
                  </select>
                </Td>
                <Td>
                  <select
                    value={domain.settings.mode}
                    disabled={busyKey === domain.domain_key}
                    onChange={(event) => updateDomain(domain, { mode: event.target.value, is_enabled: event.target.value !== "disabled" })}
                    className="w-28 rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100"
                  >
                    {(data?.mode_options ?? []).map((mode) => <option key={mode} value={mode}>{mode}</option>)}
                  </select>
                </Td>
                <Td>
                  <label className="inline-flex items-center gap-2 text-xs text-slate-300">
                    <input
                      type="checkbox"
                      checked={domain.settings.is_enabled}
                      disabled={busyKey === domain.domain_key}
                      onChange={(event) => updateDomain(domain, { is_enabled: event.target.checked, mode: event.target.checked ? domain.settings.mode === "disabled" ? "shadow" : domain.settings.mode : "disabled" })}
                      className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                    />
                    {domain.settings.is_enabled ? "on" : "off"}
                  </label>
                </Td>
                <Td>{domain.source_type}</Td>
                <Td>
                  <div className="flex max-w-60 flex-wrap gap-1">
                    {domain.badges.map((badge) => (
                      <span key={badge} className={`rounded-md border px-2 py-1 text-[10px] font-semibold uppercase ${badgeClass(badge)}`}>{badge}</span>
                    ))}
                  </div>
                  {domain.last_error ? <div className="mt-2 max-w-56 truncate text-rose-200">{domain.last_error}</div> : null}
                </Td>
                <Td>
                  <div className="max-w-64 space-y-1">
                    {domain.endpoint_names.map((endpoint) => <div key={endpoint} className="truncate text-slate-400">{endpoint}</div>)}
                  </div>
                </Td>
                <Td>
                  <div>{formatDate(domain.last_successful_refresh)}</div>
                  <div className="mt-1 text-slate-500">{domain.stale_status}</div>
                </Td>
                <Td>{domain.cache_table ?? "none"}</Td>
                <Td>{domain.row_count?.toLocaleString() ?? "n/a"}</Td>
                <Td>{domain.call_count_24h?.toLocaleString() ?? "n/a"}</Td>
                <Td>{domain.queue_depth?.toLocaleString() ?? "0"}</Td>
                <Td>
                  <div className="flex flex-col gap-2">
                    {domain.admin_actions.can_run_dry_run ? (
                      <button
                        type="button"
                        disabled={busyKey === domain.domain_key}
                        onClick={() => runDomain(domain, "dry_run")}
                        className="rounded-md border border-cyan-300/30 px-2 py-1.5 text-xs font-semibold text-cyan-100"
                      >
                        Run dry-run
                      </button>
                    ) : null}
                    {domain.admin_actions.can_refresh_cache ? (
                      <button
                        type="button"
                        disabled={busyKey === domain.domain_key}
                        onClick={() => runDomain(domain, "dry_run")}
                        className="rounded-md border border-emerald-300/30 px-2 py-1.5 text-xs font-semibold text-emerald-100"
                      >
                        Refresh cache
                      </button>
                    ) : null}
                    <button
                      type="button"
                      onClick={() => setFilter("All")}
                      className="rounded-md border border-white/10 px-2 py-1.5 text-xs font-semibold text-slate-200"
                    >
                      Diagnostics
                    </button>
                  </div>
                </Td>
              </tr>
            )) : (
              <tr>
                <td colSpan={14} className="bg-slate-950/30 p-4 text-sm text-slate-500">No data source rows match this filter.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {data ? (
        <div className="mt-5 grid gap-4 xl:grid-cols-2">
          <Diagnostics title="Congress official vs current" rows={data.diagnostics.congress} />
          <Diagnostics title="SEC Form 4 vs current" rows={data.diagnostics.insider} />
          <Diagnostics title="Current data source map" rows={data.current_data_source_map} />
          <Diagnostics title="Production source counts" rows={data.diagnostics.production_source_counts} />
        </div>
      ) : null}

      {data ? (
        <div className="mt-5 grid gap-4 lg:grid-cols-2">
          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <h3 className="font-semibold text-white">Dry-run commands</h3>
            <div className="mt-3 space-y-2 text-xs text-slate-400">
              {Object.entries(data.dry_run_commands).map(([key, value]) => (
                <div key={key} className="rounded-md border border-white/10 bg-slate-900/60 p-2">
                  <div className="font-semibold text-slate-200">{key}</div>
                  <code className="mt-1 block break-words text-slate-400">{value}</code>
                </div>
              ))}
            </div>
          </div>
          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <h3 className="font-semibold text-white">Risk list</h3>
            <div className="mt-3 space-y-2 text-sm text-slate-400">
              {data.risks.map((risk) => <div key={risk} className="rounded-md border border-white/10 bg-slate-900/60 p-2">{risk}</div>)}
            </div>
          </div>
        </div>
      ) : null}
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

function Diagnostics({ title, rows }: { title: string; rows: Record<string, unknown> }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      <div className="mt-3 grid gap-2">
        {Object.entries(rows).map(([key, value]) => (
          <div key={key} className="grid gap-2 rounded-md border border-white/10 bg-slate-900/60 p-2 text-xs sm:grid-cols-[13rem_1fr]">
            <span className="font-semibold text-slate-300">{key.replaceAll("_", " ")}</span>
            <span className="break-words text-slate-400">{formatValue(value)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function Th({ children }: { children: ReactNode }) {
  return <th className="px-3 py-3 font-semibold">{children}</th>;
}

function Td({ children }: { children: ReactNode }) {
  return <td className="px-3 py-3">{children}</td>;
}
