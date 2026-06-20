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

const PROVIDER_LABELS: Record<string, string> = {
  fmp: "FMP",
  fred: "FRED",
  treasury_gov: "Treasury.gov",
  sec_edgar: "SEC EDGAR",
  official_house: "Official House Disclosures",
  official_senate: "Official Senate Disclosures",
  walnut_official: "Walnut Official Pipeline",
  walnut_cache: "Local Walnut Cache",
  internal_computed: "Internal Computed",
  manual_admin_override: "Manual Admin Override",
  future_vendor_fallback: "Future Vendor Fallback",
  disabled: "Disabled",
  none: "None",
};

const MODE_HELP: Record<string, string> = {
  primary: "This is the selected production provider for this domain.",
  fallback: "This provider is used only if the primary provider is unavailable or disabled.",
  shadow: "This provider can ingest or compare data in the background, but it does not power public user-facing data yet.",
  dry_run: "This can run test/staging jobs without writing to production event tables.",
  disabled: "This domain is intentionally turned off.",
};

const HEADER_HELP: Record<string, string> = {
  Domain: "The dataset or product area, such as prices, fundamentals, Congress trades, insider trades, or Insights macro.",
  Provider: "The currently selected source for this domain.",
  Fallback: "The backup source Walnut may use if the primary source is unavailable. Fallback should not trigger live user-route fetches.",
  Mode: "Controls whether this provider is production, fallback, shadow, dry-run, or disabled.",
  Enabled: "Whether this domain is enabled in provider settings.",
  Type: "External API, official public source, local cache, or internal computed data.",
  Status: "Latest health/risk state based on refresh jobs, entitlement checks, cache state, and errors.",
  "Endpoint/job": "The backend endpoint, scheduled job, or cache process responsible for this data.",
  Refresh: "The latest known refresh/check time and freshness state.",
  Cache: "The local Walnut table or cache used by the app.",
  Rows: "Approximate number of local rows available for this domain.",
};

const ISSUE_HELP: Record<string, { label: string; detail: string }> = {
  provider_entitlement: {
    label: "Provider entitlement",
    detail:
      "This provider is selected, but the latest refresh/check failed because the current provider plan or API key may not be entitled to one or more endpoints in this domain.",
  },
};

const ADD_ON_RISK_HELP =
  "This may require an FMP add-on or exchange/provider entitlement depending on the endpoint used. Builder-safe mode should avoid this for launch unless explicitly enabled.";

const PROVIDER_ENTITLEMENT_HELP =
  "The provider returned or was flagged with an entitlement/access issue. This may affect one endpoint in the domain, not necessarily the entire domain. Check endpoint/job details.";

const CACHE_PROVIDER_HELP =
  "Local Walnut Cache means the app reads from Walnut's database/cache instead of calling an external API during page render.";

function badgeClass(label: string) {
  const normalized = label.toLowerCase();
  if (normalized.includes("error") || normalized.includes("unsafe")) return "border-rose-300/30 bg-rose-300/10 text-rose-100";
  if (normalized.includes("stale") || normalized.includes("warning") || normalized.includes("risk")) return "border-amber-300/30 bg-amber-300/10 text-amber-100";
  if (normalized.includes("disabled") || normalized.includes("missing") || normalized.includes("not checked")) return "border-white/10 bg-slate-950/60 text-slate-300";
  if (normalized.includes("shadow") || normalized.includes("dry")) return "border-cyan-300/30 bg-cyan-300/10 text-cyan-100";
  if (normalized.includes("external")) return "border-sky-300/30 bg-sky-300/10 text-sky-100";
  if (normalized.includes("official")) return "border-indigo-300/30 bg-indigo-300/10 text-indigo-100";
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

function friendlyLabel(value?: string | null, labels?: Record<string, string>) {
  if (!value) return "None";
  if (labels?.[value]) return labels[value];
  return PROVIDER_LABELS[value] ?? value.replaceAll("_", " ");
}

function titleLabel(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
    .replace(/\bFmp\b/g, "FMP")
    .replace(/\bSec\b/g, "SEC")
    .replace(/\bUs\b/g, "US");
}

function modeLabel(value?: string | null) {
  if (!value) return "Not checked";
  if (value === "dry_run") return "Dry-run";
  return titleLabel(value);
}

function sourceTypeLabel(value: string) {
  if (value === "external API") return "External API";
  if (value === "public official source") return "Official public source";
  if (value === "local cache") return "Local cache";
  if (value === "internal computed") return "Internal computed";
  return titleLabel(value);
}

function issueMeta(error?: string | null) {
  if (!error) return null;
  return ISSUE_HELP[error] ?? { label: titleLabel(error), detail: error };
}

function healthState(domain: AdminDataSourceDomain) {
  if (domain.last_error) return "Error";
  if (!domain.settings.is_enabled || domain.mode === "disabled") return "Not checked";
  if (domain.stale_status === "missing") return "Missing";
  if (domain.stale_status === "stale") return "Stale";
  if (domain.stale_status === "fresh") return "Healthy";
  return "Not checked";
}

function riskStates(domain: AdminDataSourceDomain) {
  const states: string[] = [];
  if (domain.builder_safe_status === "safe") states.push("Builder-safe");
  if (domain.builder_safe_status === "warning") states.push("Add-on risk");
  if (domain.source_type === "external API") states.push("External API");
  if (domain.source_type === "public official source") states.push("Official source");
  if (domain.source_type === "local cache" || domain.active_provider === "walnut_cache") states.push("Cache-only");
  if (!states.length) states.push(sourceTypeLabel(domain.source_type));
  return states;
}

function configStates(domain: AdminDataSourceDomain) {
  return [domain.settings.is_enabled ? "Enabled" : "Disabled", modeLabel(domain.settings.mode)];
}

function cacheState(domain: AdminDataSourceDomain) {
  if (domain.stale_status === "missing") return "Cache missing";
  if (domain.stale_status === "stale") return "Cache stale";
  if (domain.stale_status === "fresh" && typeof domain.row_count === "number" && domain.row_count > 0) return "Cache populated";
  if (domain.stale_status === "fresh") return "Cache refreshed";
  return "Cache not checked";
}

function isShadowExplainedDomain(domain: AdminDataSourceDomain) {
  return domain.domain_key === "congress_trades" || domain.domain_key === "insider_trades";
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

function getNestedNumber(source: unknown, path: string[]) {
  let current = source;
  for (const key of path) {
    if (!current || typeof current !== "object" || !(key in current)) return 0;
    current = (current as Record<string, unknown>)[key];
  }
  return typeof current === "number" ? current : 0;
}

function optionsWithSavedValue(allowed: string[] | undefined, current: string | null | undefined, fallbackOptions: string[]) {
  const base = allowed?.length ? allowed : fallbackOptions;
  const saved = current || "none";
  return saved && !base.includes(saved) ? [saved, ...base] : base;
}

function isInvalidSavedValue(allowed: string[] | undefined, current: string | null | undefined) {
  const saved = current || "none";
  return Boolean(allowed?.length && saved && !allowed.includes(saved));
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

  const officialShadowRows = getNestedNumber(data?.diagnostics.congress, ["normalized_transactions"]);
  const secNormalizedRows = getNestedNumber(data?.diagnostics.insider, ["comparison", "sec_vs_current_feed_count", "sec_normalized"]);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold text-white">Data Sources</h2>
          <p className="mt-1 text-sm text-slate-400">Provider configuration, health, cache state, and official-pipeline diagnostics.</p>
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

      <div className="mt-5 rounded-lg border border-emerald-300/20 bg-emerald-300/10 p-4">
        <h3 className="text-sm font-semibold text-emerald-100">How to read this panel</h3>
        <p className="mt-2 text-sm leading-6 text-emerald-50/80">
          This page shows provider configuration and health. A domain can be enabled but still show an error if the latest refresh/check
          failed. Shadow mode means the pipeline is being staged or compared in the background and does not power public pages yet. Local
          Walnut Cache means user-facing pages read from Walnut's database/cache instead of calling an external API live.
        </p>
      </div>

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
        <Metric
          label="Official shadow rows"
          value={officialShadowRows.toLocaleString()}
          helper={officialShadowRows === 0 ? "No official-source shadow rows staged yet." : undefined}
        />
        <Metric
          label="SEC normalized rows"
          value={secNormalizedRows.toLocaleString()}
          helper={secNormalizedRows === 0 ? "No SEC Form 4 normalized rows staged yet." : undefined}
        />
        <Metric label="Generated" value={formatDate(data?.generated_at)} />
      </div>

      <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
        <table className="w-full min-w-[1500px] border-collapse text-left text-xs">
          <thead className="bg-slate-950/80 text-slate-400">
            <tr>
              <Th help={HEADER_HELP.Domain}>Domain</Th>
              <Th help={HEADER_HELP.Provider}>Provider</Th>
              <Th help={HEADER_HELP.Fallback}>Fallback</Th>
              <Th help={HEADER_HELP.Mode}>Mode</Th>
              <Th help={HEADER_HELP.Enabled}>Enabled</Th>
              <Th help={HEADER_HELP.Type}>Type</Th>
              <Th help={HEADER_HELP.Status}>Status</Th>
              <Th help={HEADER_HELP["Endpoint/job"]}>Endpoint/job</Th>
              <Th help={HEADER_HELP.Refresh}>Refresh</Th>
              <Th help={HEADER_HELP.Cache}>Cache</Th>
              <Th help={HEADER_HELP.Rows}>Rows</Th>
              <Th>Calls 24h</Th>
              <Th>Queue</Th>
              <Th>Actions</Th>
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {rows.length ? rows.map((domain) => (
              <DataSourceRow
                key={domain.domain_key}
                domain={domain}
                providerOptions={data?.provider_options ?? []}
                modeOptions={data?.mode_options ?? []}
                busy={busyKey === domain.domain_key}
                updateDomain={updateDomain}
                runDomain={runDomain}
                clearFilter={() => setFilter("All")}
              />
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
          <DataSourceMap rows={data.current_data_source_map} domains={data.domains} />
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
                  <div className="font-semibold text-slate-200">{titleLabel(key)}</div>
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

function DataSourceRow({
  domain,
  providerOptions,
  modeOptions,
  busy,
  updateDomain,
  runDomain,
  clearFilter,
}: {
  domain: AdminDataSourceDomain;
  providerOptions: string[];
  modeOptions: string[];
  busy: boolean;
  updateDomain: (domain: AdminDataSourceDomain, patch: Record<string, unknown>) => Promise<void>;
  runDomain: (domain: AdminDataSourceDomain, mode?: string) => Promise<void>;
  clearFilter: () => void;
}) {
  const issue = issueMeta(domain.last_error);
  const health = healthState(domain);
  const providerIsCache = domain.active_provider === "walnut_cache";
  const fallbackIsCache = domain.fallback_provider === "walnut_cache";
  const allowedProviders = domain.allowed_providers ?? providerOptions;
  const allowedFallbacks = domain.allowed_fallbacks ?? ["none", ...providerOptions];
  const allowedModes = domain.allowed_modes ?? modeOptions;
  const providerSelectOptions = optionsWithSavedValue(allowedProviders, domain.active_provider, providerOptions);
  const fallbackSelectOptions = optionsWithSavedValue(allowedFallbacks, domain.fallback_provider ?? "none", ["none", ...providerOptions]);
  const modeSelectOptions = optionsWithSavedValue(allowedModes, domain.settings.mode, modeOptions);
  const providerLabels = domain.provider_labels;
  const validationWarnings = domain.validation_warnings ?? [];

  return (
    <tr className="bg-slate-950/30 align-top text-slate-300">
      <Td>
        <div className="font-semibold text-slate-100">{domain.data_domain}</div>
        <div className="mt-1 text-[11px] text-slate-500">{domain.domain_key}</div>
        {domain.notes ? <div className="mt-2 max-w-64 text-[11px] leading-4 text-slate-500">{domain.notes}</div> : null}
        {domain.domain_help_text && domain.domain_help_text !== domain.notes ? <div className="mt-2 max-w-64 text-[11px] leading-4 text-slate-500">{domain.domain_help_text}</div> : null}
        {validationWarnings.length ? (
          <div className="mt-2 rounded-md border border-amber-300/20 bg-amber-300/10 p-2 text-[11px] leading-4 text-amber-100">
            <div className="font-semibold uppercase">Invalid saved value</div>
            {validationWarnings.map((warning) => <div key={warning} className="mt-1">{warning}</div>)}
            <div className="mt-1 text-amber-100/70">Choose a valid provider, fallback, and mode before making other changes.</div>
          </div>
        ) : null}
      </Td>
      <Td>
        <ProviderDisplay provider={domain.active_provider} labels={providerLabels} helper={domain.provider_help_text?.[domain.active_provider] ?? (providerIsCache ? CACHE_PROVIDER_HELP : undefined)} />
        <select
          value={domain.active_provider}
          disabled={busy}
          onChange={(event) =>
            updateDomain(
              domain,
              event.target.value === "disabled"
                ? { active_provider: event.target.value, mode: "disabled", is_enabled: false }
                : { active_provider: event.target.value },
            )
          }
          className="mt-2 w-44 rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100"
        >
          {providerSelectOptions.map((provider) => (
            <option key={provider} value={provider} disabled={isInvalidSavedValue(allowedProviders, provider)}>
              {isInvalidSavedValue(allowedProviders, provider) ? `Invalid saved value: ${optionLabel(provider, providerLabels)}` : optionLabel(provider, providerLabels)}
            </option>
          ))}
        </select>
      </Td>
      <Td>
        <ProviderDisplay provider={domain.fallback_provider ?? "none"} labels={providerLabels} helper={domain.provider_help_text?.[domain.fallback_provider ?? "none"] ?? (fallbackIsCache ? CACHE_PROVIDER_HELP : undefined)} />
        <select
          value={domain.fallback_provider ?? "none"}
          disabled={busy}
          onChange={(event) => updateDomain(domain, { fallback_provider: event.target.value === "none" ? null : event.target.value })}
          className="mt-2 w-44 rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100"
        >
          {fallbackSelectOptions.map((provider) => (
            <option key={provider} value={provider} disabled={isInvalidSavedValue(allowedFallbacks, provider)}>
              {isInvalidSavedValue(allowedFallbacks, provider) ? `Invalid saved value: ${optionLabel(provider, providerLabels)}` : optionLabel(provider, providerLabels)}
            </option>
          ))}
        </select>
      </Td>
      <Td>
        <div className="flex items-center gap-2">
          <Badge label={modeLabel(domain.settings.mode)} title={MODE_HELP[domain.settings.mode]} />
          {MODE_HELP[domain.settings.mode] ? (
            <InfoTooltip id={`mode-${domain.domain_key}`} label="Mode help" description={MODE_HELP[domain.settings.mode]} />
          ) : null}
        </div>
        <select
          value={domain.settings.mode}
          disabled={busy}
          onChange={(event) => updateDomain(domain, { mode: event.target.value, is_enabled: event.target.value !== "disabled" })}
          className="mt-2 w-32 rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100"
        >
          {modeSelectOptions.map((mode) => (
            <option key={mode} value={mode} disabled={isInvalidSavedValue(allowedModes, mode)}>
              {isInvalidSavedValue(allowedModes, mode) ? `Invalid saved value: ${modeLabel(mode)}` : modeLabel(mode)}
            </option>
          ))}
        </select>
        {isShadowExplainedDomain(domain) && domain.settings.mode === "shadow" ? (
          <p className="mt-2 max-w-44 text-[11px] leading-4 text-cyan-100">
            Shadow mode: staging/comparison only. Not powering public feed.
          </p>
        ) : null}
      </Td>
      <Td>
        <label className="inline-flex items-center gap-2 text-xs text-slate-300">
          <input
            type="checkbox"
            checked={domain.settings.is_enabled}
            disabled={busy}
            onChange={(event) => updateDomain(domain, { is_enabled: event.target.checked, mode: event.target.checked ? domain.settings.mode === "disabled" ? "shadow" : domain.settings.mode : "disabled" })}
            className="h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
          />
          {domain.settings.is_enabled ? "Enabled" : "Disabled"}
        </label>
      </Td>
      <Td>
        <div className="text-slate-200">{sourceTypeLabel(domain.source_type)}</div>
        {providerIsCache || domain.source_type === "local cache" ? <p className="mt-1 max-w-44 text-[11px] leading-4 text-slate-500">{CACHE_PROVIDER_HELP}</p> : null}
      </Td>
      <Td>
        <StatusSummary domain={domain} />
        {issue ? (
          <div className="mt-3 rounded-md border border-rose-300/20 bg-rose-300/10 p-2">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-[11px] font-semibold uppercase text-rose-100">Issue</span>
              <Badge label={issue.label} title={domain.last_error === "provider_entitlement" ? PROVIDER_ENTITLEMENT_HELP : issue.detail} />
            </div>
            <p className="mt-1 max-w-64 text-[11px] leading-4 text-rose-100/80">{issue.detail}</p>
          </div>
        ) : null}
        {health === "Error" ? <p className="mt-2 text-[11px] text-slate-500">Enabled does not mean healthy; it only means this domain is configured on.</p> : null}
      </Td>
      <Td>
        <div className="max-w-64 space-y-1">
          {domain.endpoint_names.map((endpoint) => (
            <div key={endpoint} className="rounded-md border border-white/10 bg-slate-900/60 px-2 py-1 text-slate-400">
              <span className="break-words">{endpoint}</span>
            </div>
          ))}
          {issue ? (
            <div className="rounded-md border border-amber-300/20 bg-amber-300/10 px-2 py-1 text-[11px] leading-4 text-amber-100">
              Domain check issue: {issue.label}. Check the endpoint/job above before treating the whole domain as unavailable.
            </div>
          ) : null}
        </div>
      </Td>
      <Td>
        <div>{formatDate(domain.last_successful_refresh)}</div>
        <div className="mt-1"><Badge label={cacheState(domain)} /></div>
        {domain.active_provider === "fred" ? (
          <p className="mt-2 max-w-44 text-[11px] leading-4 text-slate-500">FRED cache is {cacheState(domain).replace("Cache ", "").toLowerCase()}.</p>
        ) : null}
      </Td>
      <Td>
        <div className="break-words text-slate-200">{domain.cache_table ?? "none"}</div>
        {domain.cache_table ? <p className="mt-1 max-w-44 text-[11px] leading-4 text-slate-500">{CACHE_PROVIDER_HELP}</p> : null}
      </Td>
      <Td>{domain.row_count?.toLocaleString() ?? "n/a"}</Td>
      <Td>{domain.call_count_24h?.toLocaleString() ?? "n/a"}</Td>
      <Td>{domain.queue_depth?.toLocaleString() ?? "0"}</Td>
      <Td>
        <div className="flex flex-col gap-2">
          {domain.admin_actions.can_run_dry_run ? (
            <button
              type="button"
              disabled={busy}
              onClick={() => runDomain(domain, "dry_run")}
              className="rounded-md border border-cyan-300/30 px-2 py-1.5 text-xs font-semibold text-cyan-100"
            >
              Run dry-run
            </button>
          ) : null}
          {domain.admin_actions.can_refresh_cache ? (
            <button
              type="button"
              disabled={busy}
              onClick={() => runDomain(domain, "dry_run")}
              className="rounded-md border border-emerald-300/30 px-2 py-1.5 text-xs font-semibold text-emerald-100"
            >
              Refresh cache
            </button>
          ) : null}
          <button
            type="button"
            onClick={clearFilter}
            className="rounded-md border border-white/10 px-2 py-1.5 text-xs font-semibold text-slate-200"
          >
            Diagnostics
          </button>
        </div>
      </Td>
    </tr>
  );
}

function StatusSummary({ domain }: { domain: AdminDataSourceDomain }) {
  return (
    <div className="space-y-2">
      <StatusLine label="Configuration" badges={configStates(domain)} />
      <StatusLine label="Health" badges={[healthState(domain)]} />
      <StatusLine label="Risk" badges={riskStates(domain)} />
    </div>
  );
}

function StatusLine({ label, badges }: { label: string; badges: string[] }) {
  return (
    <div>
      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 flex max-w-64 flex-wrap gap-1">
        {badges.map((badge) => (
          <Badge
            key={badge}
            label={badge}
            title={badge === "Add-on risk" ? ADD_ON_RISK_HELP : undefined}
          />
        ))}
      </div>
    </div>
  );
}

function ProviderDisplay({ provider, labels, helper }: { provider: string; labels?: Record<string, string>; helper?: string }) {
  return (
    <div>
      <div className="font-semibold text-slate-100">{friendlyLabel(provider, labels)}</div>
      <code className="mt-0.5 block text-[11px] text-slate-500">{provider}</code>
      {helper ? <p className="mt-1 max-w-44 text-[11px] leading-4 text-slate-500">{helper}</p> : null}
    </div>
  );
}

function optionLabel(provider: string, labels?: Record<string, string>) {
  return `${friendlyLabel(provider, labels)} (${provider})`;
}

function Metric({ label, value, helper }: { label: string; value: string; helper?: string }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 break-words text-sm font-semibold text-slate-100">{value}</div>
      {helper ? <p className="mt-1 text-[11px] leading-4 text-slate-500">{helper}</p> : null}
    </div>
  );
}

function Diagnostics({ title, rows }: { title: string; rows: Record<string, unknown> }) {
  const comparison = rows.comparison && typeof rows.comparison === "object" ? rows.comparison as Record<string, unknown> : null;
  const otherRows = Object.entries(rows).filter(([key]) => key !== "comparison");

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      {comparison ? <ComparisonBlock comparison={comparison} /> : null}
      <div className="mt-3 grid gap-2">
        {otherRows.map(([key, value]) => (
          <DiagnosticRow key={key} label={titleLabel(key)} value={value} />
        ))}
      </div>
      {comparison ? (
        <details className="mt-3 rounded-md border border-white/10 bg-slate-900/60 p-2 text-xs text-slate-400">
          <summary className="cursor-pointer font-semibold text-slate-200">View raw comparison</summary>
          <pre className="mt-2 max-w-full whitespace-pre-wrap break-words text-[11px] leading-4 text-slate-400">{JSON.stringify(comparison, null, 2)}</pre>
        </details>
      ) : null}
    </div>
  );
}

function ComparisonBlock({ comparison }: { comparison: Record<string, unknown> }) {
  const countObjectKey = Object.keys(comparison).find((key) => key.endsWith("_feed_count"));
  const countObject = countObjectKey && typeof comparison[countObjectKey] === "object" && comparison[countObjectKey] !== null
    ? comparison[countObjectKey] as Record<string, unknown>
    : {};
  const entries = [
    ...Object.entries(countObject),
    ...Object.entries(comparison).filter(([key]) => key !== countObjectKey),
  ];

  return (
    <div className="mt-3 rounded-md border border-white/10 bg-slate-900/60 p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Comparison</div>
      <div className="mt-2 grid gap-2 sm:grid-cols-2">
        {entries.map(([key, value]) => (
          <div key={key} className="min-w-0 rounded-md border border-white/10 bg-slate-950/50 p-2">
            <div className="text-[11px] font-semibold text-slate-300">{titleLabel(key)}</div>
            <div className="mt-1 break-words text-sm font-semibold text-slate-100 [overflow-wrap:anywhere]">{formatValue(value)}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function DiagnosticRow({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="grid min-w-0 gap-2 rounded-md border border-white/10 bg-slate-900/60 p-2 text-xs sm:grid-cols-[13rem_1fr]">
      <span className="font-semibold text-slate-300">{label}</span>
      <DiagnosticValue value={value} />
    </div>
  );
}

function DiagnosticValue({ value }: { value: unknown }) {
  if (Array.isArray(value)) {
    if (!value.length) return <span className="break-words text-slate-400">none</span>;
    return (
      <div className="grid min-w-0 gap-1">
        {value.slice(0, 12).map((item, index) => (
          <span key={index} className="min-w-0 break-words rounded border border-white/10 bg-slate-950/50 px-2 py-1 text-slate-400">
            {typeof item === "object" && item !== null ? Object.entries(item as Record<string, unknown>).map(([key, itemValue]) => `${titleLabel(key)}: ${formatValue(itemValue)}`).join(" | ") : formatValue(item)}
          </span>
        ))}
        {value.length > 12 ? <span className="text-slate-500">+{value.length - 12} more</span> : null}
      </div>
    );
  }
  if (value && typeof value === "object") {
    return (
      <div className="grid min-w-0 gap-1">
        {Object.entries(value as Record<string, unknown>).map(([key, itemValue]) => (
          <span key={key} className="min-w-0 break-words text-slate-400">
            <span className="font-semibold text-slate-300">{titleLabel(key)}:</span> {formatValue(itemValue)}
          </span>
        ))}
      </div>
    );
  }
  return <span className="break-words text-slate-400">{formatValue(value)}</span>;
}

function DataSourceMap({ rows, domains }: { rows: Record<string, string>; domains: AdminDataSourceDomain[] }) {
  const domainByKey = new Map(domains.map((domain) => [domain.domain_key, domain]));
  const orderedKeys = [
    ...domains.map((domain) => domain.domain_key),
    ...Object.keys(rows).filter((key) => !domainByKey.has(key)),
  ];
  const groups = orderedKeys.reduce<Record<string, string[]>>((acc, key) => {
    const group = sourceMapGroup(key);
    acc[group] = [...(acc[group] ?? []), key];
    return acc;
  }, {});

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">Current data source map</h3>
      <div className="mt-3 space-y-4">
        {Object.entries(groups).map(([group, keys]) => (
          <div key={group}>
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{group}</div>
            <div className="mt-2 grid min-w-0 gap-2 md:grid-cols-2">
              {keys.map((key) => {
                const domain = domainByKey.get(key);
                const provider = rows[key] ?? domain?.active_provider ?? "none";
                return (
                  <div key={key} className="min-w-0 rounded-md border border-white/10 bg-slate-900/60 p-3">
                    <div className="font-semibold text-slate-100">{domain?.data_domain ?? titleLabel(key)}</div>
                    <div className="mt-1 text-sm text-slate-300">{friendlyLabel(provider, domain?.provider_labels)}</div>
                    <div className="mt-1 break-words text-[11px] text-slate-500">{key} / {provider}</div>
                    {domain ? (
                      <div className="mt-2 flex flex-wrap gap-1">
                        <Badge label={modeLabel(domain.settings.mode)} title={MODE_HELP[domain.settings.mode]} />
                        <Badge label={healthState(domain)} />
                      </div>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function sourceMapGroup(key: string) {
  if (key.startsWith("prices_") || ["fundamentals", "ratios", "technicals", "profiles", "earnings", "analyst_estimates", "institutional_13f"].includes(key)) return "Market Data";
  if (["congress_trades", "house_disclosures", "senate_disclosures", "insider_trades"].includes(key)) return "Alternative Data";
  if (key.startsWith("insights_")) return "Insights";
  if (key.startsWith("screener_")) return "Screener";
  return "Internal/Computed";
}

function Badge({ label, title }: { label: string; title?: string }) {
  return (
    <span title={title} className={`rounded-md border px-2 py-1 text-[10px] font-semibold uppercase ${badgeClass(label)}`}>
      {label}
    </span>
  );
}

function InfoTooltip({ id, label, description, align = "left" }: { id: string; label: string; description: string; align?: "left" | "right" }) {
  return (
    <span className="group/header-tip relative inline-flex items-center">
      <button
        type="button"
        aria-label={label}
        aria-describedby={id}
        className="inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-slate-700/70 bg-slate-900/70 text-[10px] font-semibold leading-none text-slate-500 transition hover:border-emerald-400/40 hover:text-emerald-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-400/30"
      >
        i
      </button>
      <span
        id={id}
        role="tooltip"
        className={`pointer-events-none invisible absolute top-full z-40 mt-2 w-64 rounded-lg border border-white/10 bg-slate-950/95 p-2.5 text-left text-[11px] font-medium normal-case leading-4 tracking-normal text-slate-200 opacity-0 shadow-2xl shadow-black/40 backdrop-blur transition delay-75 group-hover/header-tip:visible group-hover/header-tip:opacity-100 group-focus-within/header-tip:visible group-focus-within/header-tip:opacity-100 ${align === "right" ? "right-0" : "left-0"}`}
      >
        {description}
      </span>
    </span>
  );
}

function HeaderTooltip({ id, label, description }: { id: string; label: ReactNode; description: string }) {
  const ariaLabel = typeof label === "string" ? `${label} help` : "Column help";
  return (
    <span className="group/header-tip relative inline-flex max-w-full items-center gap-1.5">
      <span className="min-w-0 truncate underline decoration-slate-600/70 decoration-dotted underline-offset-4">{label}</span>
      <InfoTooltip id={id} label={ariaLabel} description={description} align="left" />
    </span>
  );
}

function Th({ children, help }: { children: ReactNode; help?: string }) {
  const label = typeof children === "string" ? children : "column";
  return (
    <th className="px-3 py-3 font-semibold">
      {help ? <HeaderTooltip id={`data-sources-header-${label.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`} label={children} description={help} /> : children}
    </th>
  );
}

function Td({ children }: { children: ReactNode }) {
  return <td className="px-3 py-3">{children}</td>;
}
