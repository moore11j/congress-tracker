"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  getAdminDataSourcesStatus,
  runAdminDataSource,
  testAdminDataSourceEndpoint,
  updateAdminDataSourceSetting,
  type AdminDataSourceDomain,
  type AdminDataSourceEndpointTest,
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

const MODE_HELP_ITEMS = [
  ["Primary", "This provider is the selected production source for this data domain."],
  ["Fallback", "This provider is used only if the primary provider is unavailable or disabled."],
  ["Shadow", "This provider can ingest, stage, or compare data in the background, but it does not power public user-facing pages yet."],
  ["Dry-run", "This mode can run test/staging jobs without writing to production event tables."],
  ["Disabled", "This data domain is intentionally turned off."],
] as const;

const MODE_HELP: Record<string, string> = {
  primary: MODE_HELP_ITEMS[0][1],
  fallback: "This provider is used only if the primary provider is unavailable or disabled.",
  shadow: MODE_HELP_ITEMS[2][1],
  dry_run: MODE_HELP_ITEMS[3][1],
  disabled: MODE_HELP_ITEMS[4][1],
};

const HEADER_HELP: Record<string, string> = {
  Domain: "The dataset or product area, such as prices, fundamentals, Congress trades, insider trades, or Insights macro.",
  Provider: "The currently selected source for this domain.",
  Fallback: "The backup source Walnut may use if the primary source is unavailable. Fallback should not trigger live user-route fetches.",
  Enabled:
    "Enabled means this domain is configured for use. It does not guarantee the latest refresh/check is healthy. Check the Health column for errors, stale data, or missing data.",
  Type: "External API, official public source, local cache, or internal computed data.",
  Health: "Latest refresh/check condition for this data domain, such as healthy, stale, missing, or error.",
  Risk: "Provider or licensing/runtime risk, such as Builder-safe, add-on risk, external API, official source, or cache-only.",
  "Endpoint/job": "The backend endpoint, scheduled job, or cache process responsible for this data.",
  Refresh: "The latest known refresh/check time and freshness state.",
  Cache: "The local Walnut table or cache used by the app.",
  Rows: "Approximate number of local rows available for this domain.",
};

const SWITCH_READINESS_HELP =
  "Provider switching affects future ingest jobs only. Existing Walnut records remain stored. This panel checks whether the shadow provider is healthy and whether it would create duplicates if promoted.";

const PROVIDER_SWITCH_SAFETY_HELP =
  "Changing a provider controls which source future ingest jobs use. It does not delete or replace existing records. Historical backfills are separate admin actions. Walnut uses stable deduplication keys to avoid duplicate events when multiple providers report the same trade or filing.";

const HISTORICAL_COVERAGE_HELP =
  "This does not need to match before switching providers for future ingests.";

const HISTORICAL_GAP_HELP =
  "Large historical gaps are expected before a backfill is run. They do not mean existing production data will be removed.";

const READINESS_LABELS: Record<string, string> = {
  mode: "Mode",
  public_feed_impact: "Public feed impact",
  existing_data_preserved: "Existing data preserved",
  latest_source_check: "Latest source check",
  sec_latest_check: "Latest SEC check",
  form4_filings_discovered: "Form 4 filings discovered",
  parser_failures: "Parser failures",
  duplicate_candidates: "Duplicate candidates",
  potential_duplicate_insert_risk: "Potential duplicate insert risk",
  would_insert_count: "Would insert count",
  would_skip_duplicate_count: "Would skip duplicate count",
  potential_conflicts_count: "Potential conflicts count",
  promoted_events: "Promoted events",
  pnl_pending: "Gain / Loss pending",
  last_successful_shadow_ingest: "Last successful shadow ingest",
  last_successful_sec_ingest: "Last successful SEC ingest",
  readiness_status: "Readiness status",
  normalized_hash_coverage_percent: "Normalized hash coverage percent",
  unresolved_ciks_tickers: "Unresolved CIK/ticker mappings",
  grants_options_exercises: "Grants/options/exercises",
};

const SECONDARY_DIAGNOSTIC_KEYS = new Set(["comparison", "house_latest_source_check", "senate_latest_source_check", "safe_to_promote"]);

const ISSUE_HELP: Record<string, { label: string; detail: string }> = {
  provider_entitlement: {
    label: "Provider entitlement",
    detail:
      "This provider is selected, but the latest refresh/check failed because the current provider plan or API key may not be entitled to one or more endpoints in this domain.",
  },
  missing_cache: {
    label: "Missing cache",
    detail: "The latest check could not find populated local cache rows for this domain.",
  },
  stale_cache: {
    label: "Stale cache",
    detail: "The local cache exists, but the latest check considers it stale.",
  },
  missing_refresh: {
    label: "Missing refresh",
    detail: "No successful refresh/check has been recorded for this domain.",
  },
  unknown_error: {
    label: "Unknown error",
    detail: "The latest refresh/check failed without a more specific issue label.",
  },
  missing_api_key: {
    label: "Missing API key",
    detail: "The endpoint test could not run because FMP_API_KEY is not configured on the backend.",
  },
  provider_rate_limited: {
    label: "Provider rate limit",
    detail: "The endpoint test reached the provider but was rate-limited.",
  },
  provider_error: {
    label: "Provider error",
    detail: "The endpoint test reached the provider but the response was not healthy.",
  },
};

const ADD_ON_RISK_HELP =
  "This may require an FMP add-on or exchange/provider entitlement depending on the endpoint used. Builder-safe mode should avoid this for launch unless explicitly enabled.";

const CACHE_PROVIDER_HELP =
  "Local Walnut Cache means the app reads from Walnut's database/cache instead of calling an external API during page render.";

const CONGRESS_SOURCE_HIERARCHY =
  "House disclosures + Senate disclosures \u2192 Walnut Official Pipeline \u2192 normalized Congress trades.";

const CONGRESS_SOURCE_HELP: Record<string, string> = {
  congress_trades:
    "Aggregate output combining House + Senate disclosures into normalized Congress trade events. In shadow mode, this does not power the public feed.",
  house_disclosures: "Raw official House disclosure discovery and parsing source. Feeds the Walnut Official Congress Pipeline.",
  senate_disclosures: "Raw official Senate disclosure discovery and parsing source. Feeds the Walnut Official Congress Pipeline.",
};

const SHADOW_PIPELINE_STATUS_HELP =
  "Configured, but not production. This pipeline is not considered ready until filings discovered, filings parsed, and normalized transactions are greater than zero with acceptable duplicate risk.";

const OFFICIAL_CONGRESS_RELATIONSHIP_HELP =
  "House disclosures and Senate disclosures are raw official source layers. Walnut Official Congress Pipeline is the aggregate pipeline that combines those sources into normalized Congress trades. In shadow mode, the official pipeline is staged for comparison and does not power public pages yet.";

const ANALYTICS_LAYER_HELP =
  "Screener, Leaderboards, and Backtesting consume normalized events and cached market/fundamental data. They should not read raw House/Senate/SEC filings directly.";

const PIPELINE_FLOW_ROWS = [
  "Official House Disclosures + Official Senate Disclosures \u2192 Walnut Official Congress Pipeline \u2192 Normalized Congress Trades \u2192 Unified Event Layer / Walnut Cache \u2192 Feed / Ticker Pages / Member Pages / Signals / Watchlists",
  "SEC Form 4 Filings \u2192 Walnut Official Insider Pipeline \u2192 Normalized Insider Trades \u2192 Unified Event Layer / Walnut Cache \u2192 Feed / Ticker Pages / Insider Pages / Signals / Watchlists",
  "FMP Market Data \u2192 Walnut Market Data / Cache Layer \u2192 Local Walnut Cache \u2192 Ticker Pages / Screener / Leaderboards / Backtesting / Gain-Loss / Signal Inputs",
  "FRED Macro Data \u2192 FRED Macro/Treasury Cache \u2192 Insights Snapshots \u2192 Insights",
];

const SOURCE_MAP_GROUP_ORDER = ["Market Data", "Official / Alternative Data", "Insights", "Internal / Computed"];

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

function formatDiagnosticValue(value: unknown): string {
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "string" && /^[a-z][a-z0-9_]*$/.test(value)) return titleLabel(value);
  return formatValue(value);
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
  if (domain.endpoint_tests?.primary?.status === "healthy") return "Healthy";
  if (domain.endpoint_tests?.fallback?.status === "healthy" && domain.endpoint_tests?.primary?.status !== "error") return "Healthy";
  if (!domain.settings.is_enabled || domain.mode === "disabled") return "Not checked";
  if (domain.stale_status === "missing") return "Missing";
  if (domain.stale_status === "stale") return "Stale";
  if (domain.stale_status === "warning") return "Warning";
  if (domain.stale_status === "fresh") return "Healthy";
  return "Not checked";
}

function riskStates(domain: AdminDataSourceDomain) {
  const states: string[] = [];
  if (!domain.settings.is_enabled || domain.mode === "disabled" || domain.active_provider === "disabled") states.push("Disabled");
  if (domain.builder_safe_status === "safe") states.push("Builder-safe");
  if (domain.builder_safe_status === "warning") states.push("Add-on risk");
  if (domain.source_type === "external API") states.push("External API");
  if (domain.source_type === "public official source") states.push("Official source");
  if (domain.source_type === "local cache" || domain.active_provider === "walnut_cache") states.push("Cache-only");
  if (!states.length) states.push(sourceTypeLabel(domain.source_type));
  return states;
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

function isCongressOfficialSourceDomain(domain: AdminDataSourceDomain) {
  return domain.domain_key === "congress_trades" || domain.domain_key === "house_disclosures" || domain.domain_key === "senate_disclosures";
}

function domainRowHelperText(domain: AdminDataSourceDomain) {
  return CONGRESS_SOURCE_HELP[domain.domain_key] ?? domain.notes ?? null;
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

function providerSupportsEndpointUrl(provider?: string | null) {
  return Boolean(provider && !["none", "disabled", "walnut_cache", "internal_computed", "walnut_official"].includes(provider));
}

function endpointValue(domain: AdminDataSourceDomain, role: "primary" | "fallback") {
  if (role === "primary") return domain.settings.primary_endpoint_url ?? domain.endpoint_urls?.primary ?? "";
  return domain.settings.fallback_endpoint_url ?? domain.endpoint_urls?.fallback ?? "";
}

function endpointContractValue(domain: AdminDataSourceDomain, role: "primary" | "fallback") {
  if (role === "primary") return domain.settings.primary_endpoint_contract_json ?? domain.endpoint_contracts?.primary ?? "";
  return domain.settings.fallback_endpoint_contract_json ?? domain.endpoint_contracts?.fallback ?? "";
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

  const testDomain = async (domain: AdminDataSourceDomain) => {
    setBusyKey(domain.domain_key);
    setStatus(null);
    try {
      const result = await testAdminDataSourceEndpoint(domain.domain_key, { symbol: "AAPL", reason: "admin_data_sources_ui" });
      await refresh();
      const primary = result.results.primary?.status ?? "skipped";
      const fallback = result.results.fallback?.status ?? "skipped";
      setStatus(`${domain.data_domain} endpoint test complete. Primary: ${primary}. Fallback: ${fallback}.`);
    } catch (err) {
      setStatus(err instanceof Error ? err.message : "Unable to test endpoint.");
    } finally {
      setBusyKey(null);
    }
  };

  const officialShadowRows = getNestedNumber(data?.diagnostics.congress, ["normalized_transactions"]);
  const secNormalizedRows = getNestedNumber(data?.diagnostics.insider, ["normalized_transactions"]);

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
          {SWITCH_READINESS_HELP} Shadow mode means the pipeline is being staged or compared in the background and does not power public
          pages yet. Local Walnut Cache means user-facing pages read from Walnut's database/cache instead of calling an external API live.
        </p>
      </div>

      <div className="mt-3 rounded-lg border border-cyan-300/20 bg-cyan-300/10 p-4">
        <h3 className="text-sm font-semibold text-cyan-100">Provider switch safety</h3>
        <p className="mt-2 text-sm leading-6 text-cyan-50/80">{PROVIDER_SWITCH_SAFETY_HELP}</p>
      </div>

      <PipelineOverview
        domains={data?.domains ?? []}
        officialShadowRows={officialShadowRows}
        secNormalizedRows={secNormalizedRows}
      />

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
          label="Congress shadow rows"
          value={officialShadowRows.toLocaleString()}
          helper={officialShadowRows === 0 ? "No official-source shadow rows staged yet." : undefined}
        />
        <Metric
          label="SEC Form 4 shadow rows"
          value={secNormalizedRows.toLocaleString()}
          helper={secNormalizedRows === 0 ? "No SEC Form 4 normalized rows staged yet." : undefined}
        />
        <Metric label="Generated" value={formatDate(data?.generated_at)} />
      </div>

      <div className="mt-5 overflow-x-auto rounded-lg border border-white/10">
        <table className="w-full min-w-[1550px] border-collapse text-left text-xs">
          <thead className="bg-slate-950/80 text-slate-400">
            <tr>
              <Th help={HEADER_HELP.Domain}>Domain</Th>
              <Th help={HEADER_HELP.Provider}>Provider</Th>
              <Th help={HEADER_HELP.Fallback}>Fallback</Th>
              <Th help={<ModeHelpList />}>Mode</Th>
              <Th help={HEADER_HELP.Enabled}>Enabled</Th>
              <Th help={HEADER_HELP.Type}>Type</Th>
              <Th help={HEADER_HELP.Health}>Health</Th>
              <Th help={HEADER_HELP.Risk}>Risk</Th>
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
                testDomain={testDomain}
                clearFilter={() => setFilter("All")}
              />
            )) : (
              <tr>
                <td colSpan={15} className="bg-slate-950/30 p-4 text-sm text-slate-500">No data source rows match this filter.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {data ? (
        <div className="mt-5 grid gap-4 xl:grid-cols-2">
          <Diagnostics title="Congress Official Pipeline Readiness" rows={data.diagnostics.congress} />
          <Diagnostics title="SEC Form 4 Pipeline Readiness" rows={data.diagnostics.insider} />
          <div className="xl:col-span-2">
            <DataSourceMap rows={data.current_data_source_map} domains={data.domains} />
          </div>
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

function PipelineOverview({
  domains,
  officialShadowRows,
  secNormalizedRows,
}: {
  domains: AdminDataSourceDomain[];
  officialShadowRows: number;
  secNormalizedRows: number;
}) {
  const congressMode = domains.find((domain) => domain.domain_key === "congress_trades")?.settings.mode;
  const insiderMode = domains.find((domain) => domain.domain_key === "insider_trades")?.settings.mode;
  const hasLoadedStatus = domains.length > 0;
  const congressNotPopulated = hasLoadedStatus && officialShadowRows === 0;
  const insiderNotPopulated = hasLoadedStatus && secNormalizedRows === 0;

  return (
    <div className="mt-5 rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-white">Pipeline Overview</h3>
          <p className="mt-1 text-sm leading-6 text-slate-400">
            How raw official sources, licensed data, and Walnut caches flow into product features.
          </p>
        </div>
        <Badge label="Admin map" />
      </div>

      <p className="mt-3 rounded-md border border-cyan-300/20 bg-cyan-300/10 p-3 text-xs leading-5 text-cyan-50/90">
        {OFFICIAL_CONGRESS_RELATIONSHIP_HELP}
      </p>

      <div className="mt-4 grid min-w-0 gap-3 md:grid-cols-2 xl:grid-cols-5">
        <PipelineLayer
          title="Raw Sources"
          items={[
            { title: "Official House Disclosures", chips: ["Official Source"] },
            { title: "Official Senate Disclosures", chips: ["Official Source"] },
            { title: "SEC Form 4 Filings", chips: ["Official Source"] },
            { title: "FMP Market Data", chips: ["Licensed Provider"] },
            { title: "FRED Macro Data", chips: ["Official Source"] },
          ]}
        />
        <PipelineLayer
          title="Walnut Pipelines"
          items={[
            {
              title: "Walnut Official Congress Pipeline",
              helper: "Combines House + Senate disclosure sources, then stages, parses, normalizes, and deduplicates Congress trade records.",
              chips: congressMode === "shadow" ? ["Shadow"] : congressMode ? [modeLabel(congressMode)] : [],
              emptyNotice: congressNotPopulated,
            },
            {
              title: "Walnut Official Insider Pipeline",
              helper: "Stages, parses, normalizes, and deduplicates SEC Form 4 insider transaction records.",
              chips: insiderMode === "shadow" ? ["Shadow"] : insiderMode ? [modeLabel(insiderMode)] : [],
              emptyNotice: insiderNotPopulated,
            },
            {
              title: "Walnut Market Data / Cache Layer",
              helper: "Caches licensed market/fundamental data and stores internal computed values.",
              chips: ["Cache"],
            },
            {
              title: "FRED Macro/Treasury Cache",
              helper: "Stores macro and treasury observations for Insights.",
              chips: ["Cache"],
            },
          ]}
        />
        <PipelineLayer
          title="Normalized / Cached Outputs"
          items={[
            { title: "Normalized Congress Trades", chips: ["Cache"], emptyNotice: congressNotPopulated },
            { title: "Normalized Insider Trades", chips: ["Cache"], emptyNotice: insiderNotPopulated },
            { title: "Unified Event Layer / Local Walnut Cache", chips: ["Cache"] },
            { title: "Insights Snapshots", chips: ["Cache"] },
            { title: "Screener Caches", chips: ["Cache"] },
            { title: "Signal Inputs", chips: ["Internal Computed"] },
            { title: "Gain / Loss Enrichment", chips: ["Internal Computed"] },
            { title: "Trade Outcomes", chips: ["Internal Computed"] },
          ]}
        />
        <PipelineLayer
          title="Analytics / Intelligence Layer"
          helper={ANALYTICS_LAYER_HELP}
          items={[
            { title: "Screener", helper: "Filters cached fundamentals, technicals, signal scores, and event activity." },
            { title: "Leaderboards", helper: "Ranks members and insiders using normalized events, trade outcomes, and simulations." },
            { title: "Backtesting", helper: "Simulates strategies using historical normalized events and cached EOD prices." },
            { title: "Signal Scoring", helper: "Computes Walnut's signal and confirmation scores from cached inputs.", chips: ["Internal Computed"] },
            { title: "Portfolio Simulation", helper: "Models disclosure-lag portfolios and benchmark comparisons.", chips: ["Internal Computed"] },
          ]}
        />
        <PipelineLayer
          title="Product Surfaces"
          items={["Feed", "Ticker Pages", "Member Pages", "Insider Pages", "Signals", "Watchlists / Monitoring", "Insights"].map((title) => ({
            title,
            chips: ["Product Surface"],
          }))}
        />
      </div>

      <div className="mt-4 grid gap-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Key flows</div>
        {PIPELINE_FLOW_ROWS.map((flow) => (
          <div key={flow} className="min-w-0 rounded-md border border-white/10 bg-slate-900/60 px-3 py-2 text-xs leading-5 text-slate-300">
            <span className="break-words">{flow}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function PipelineLayer({
  title,
  helper,
  items,
}: {
  title: string;
  helper?: string;
  items: Array<{ title: string; helper?: string; chips?: string[]; emptyNotice?: boolean }>;
}) {
  return (
    <div className="min-w-0 rounded-lg border border-white/10 bg-slate-900/60 p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{title}</div>
      {helper ? <p className="mt-2 text-[11px] leading-4 text-slate-400">{helper}</p> : null}
      <div className="mt-3 grid gap-2">
        {items.map((item) => (
          <PipelineCard key={item.title} {...item} />
        ))}
      </div>
    </div>
  );
}

function PipelineCard({
  title,
  helper,
  chips = [],
  emptyNotice = false,
}: {
  title: string;
  helper?: string;
  chips?: string[];
  emptyNotice?: boolean;
}) {
  return (
    <div className="min-w-0 rounded-md border border-white/10 bg-slate-950/50 p-2">
      <div className="break-words text-xs font-semibold text-slate-100">{title}</div>
      {chips.length ? (
        <div className="mt-1 flex flex-wrap gap-1">
          {chips.map((chip) => <Badge key={chip} label={chip} />)}
        </div>
      ) : null}
      {helper ? <p className="mt-1 text-[11px] leading-4 text-slate-500">{helper}</p> : null}
      {emptyNotice ? <p className="mt-1 text-[11px] leading-4 text-amber-100">Configured but not populated yet.</p> : null}
    </div>
  );
}

function DataSourceRow({
  domain,
  providerOptions,
  modeOptions,
  busy,
  updateDomain,
  runDomain,
  testDomain,
  clearFilter,
}: {
  domain: AdminDataSourceDomain;
  providerOptions: string[];
  modeOptions: string[];
  busy: boolean;
  updateDomain: (domain: AdminDataSourceDomain, patch: Record<string, unknown>) => Promise<void>;
  runDomain: (domain: AdminDataSourceDomain, mode?: string) => Promise<void>;
  testDomain: (domain: AdminDataSourceDomain) => Promise<void>;
  clearFilter: () => void;
}) {
  const issue = issueMeta(domain.last_error);
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
  const helperText = domainRowHelperText(domain);
  const isCongressOfficialSource = isCongressOfficialSourceDomain(domain);
  const isCongressShadowRow = isCongressOfficialSource && domain.settings.mode === "shadow";
  const isConfiguredButEmpty = isCongressShadowRow && typeof domain.row_count === "number" && domain.row_count === 0;
  const primaryEndpointSupported = domain.provider_endpoint_support?.primary ?? providerSupportsEndpointUrl(domain.active_provider);
  const fallbackEndpointSupported = domain.provider_endpoint_support?.fallback ?? providerSupportsEndpointUrl(domain.fallback_provider);
  const [primaryEndpoint, setPrimaryEndpoint] = useState(endpointValue(domain, "primary"));
  const [fallbackEndpoint, setFallbackEndpoint] = useState(endpointValue(domain, "fallback"));
  const [primaryEndpointContract, setPrimaryEndpointContract] = useState(endpointContractValue(domain, "primary"));
  const [fallbackEndpointContract, setFallbackEndpointContract] = useState(endpointContractValue(domain, "fallback"));

  useEffect(() => {
    setPrimaryEndpoint(endpointValue(domain, "primary"));
    setFallbackEndpoint(endpointValue(domain, "fallback"));
    setPrimaryEndpointContract(endpointContractValue(domain, "primary"));
    setFallbackEndpointContract(endpointContractValue(domain, "fallback"));
  }, [
    domain.domain_key,
    domain.settings.primary_endpoint_url,
    domain.settings.fallback_endpoint_url,
    domain.settings.primary_endpoint_contract_json,
    domain.settings.fallback_endpoint_contract_json,
    domain.endpoint_urls?.primary,
    domain.endpoint_urls?.fallback,
    domain.endpoint_contracts?.primary,
    domain.endpoint_contracts?.fallback,
  ]);

  const endpointsDirty =
    primaryEndpoint !== endpointValue(domain, "primary") ||
    fallbackEndpoint !== endpointValue(domain, "fallback") ||
    primaryEndpointContract !== endpointContractValue(domain, "primary") ||
    fallbackEndpointContract !== endpointContractValue(domain, "fallback");

  const saveEndpoints = () =>
    updateDomain(domain, {
      primary_endpoint_url: primaryEndpointSupported ? primaryEndpoint.trim() || null : null,
      fallback_endpoint_url: fallbackEndpointSupported ? fallbackEndpoint.trim() || null : null,
      primary_endpoint_contract_json: primaryEndpointSupported ? primaryEndpointContract.trim() || null : null,
      fallback_endpoint_contract_json: fallbackEndpointSupported ? fallbackEndpointContract.trim() || null : null,
    });

  return (
    <tr className="bg-slate-950/30 align-top text-slate-300">
      <Td>
        <div className="font-semibold text-slate-100">{domain.data_domain}</div>
        <div className="mt-1 text-[11px] text-slate-500">{domain.domain_key}</div>
        {helperText ? <div className="mt-2 max-w-64 text-[11px] leading-4 text-slate-500">{helperText}</div> : null}
        {domain.domain_key === "congress_trades" ? (
          <div className="mt-2 max-w-64 rounded-md border border-cyan-300/20 bg-cyan-300/10 p-2 text-[11px] leading-4 text-cyan-100">
            {CONGRESS_SOURCE_HIERARCHY}
          </div>
        ) : null}
        {domain.domain_help_text && domain.domain_help_text !== domain.notes && domain.domain_help_text !== helperText ? <div className="mt-2 max-w-64 text-[11px] leading-4 text-slate-500">{domain.domain_help_text}</div> : null}
        {validationWarnings.length ? (
          <div className="mt-2 rounded-md border border-amber-300/20 bg-amber-300/10 p-2 text-[11px] leading-4 text-amber-100">
            <div className="font-semibold uppercase">Provider validation warning</div>
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
              {isInvalidSavedValue(allowedProviders, provider) ? `Invalid provider: ${optionLabel(provider, providerLabels)}` : optionLabel(provider, providerLabels)}
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
              {isInvalidSavedValue(allowedFallbacks, provider) ? `Invalid fallback: ${optionLabel(provider, providerLabels)}` : optionLabel(provider, providerLabels)}
            </option>
          ))}
        </select>
      </Td>
      <Td>
        <div className="flex items-center gap-2">
          <Badge label={modeLabel(domain.settings.mode)} title={MODE_HELP[domain.settings.mode]} />
          {MODE_HELP[domain.settings.mode] ? (
            <InfoTooltip id={`mode-${domain.domain_key}`} label="Mode help" description={<ModeHelpList />} />
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
              {isInvalidSavedValue(allowedModes, mode) ? `Invalid mode: ${modeLabel(mode)}` : modeLabel(mode)}
            </option>
          ))}
        </select>
        {isShadowExplainedDomain(domain) && domain.settings.mode === "shadow" ? (
          <p className="mt-2 max-w-44 text-[11px] leading-4 text-cyan-100">
            Shadow mode: staging/comparison only. Not powering public feed.
          </p>
        ) : null}
        {isCongressShadowRow ? (
          <p className="mt-2 max-w-44 text-[11px] leading-4 text-cyan-100">
            {SHADOW_PIPELINE_STATUS_HELP}
          </p>
        ) : null}
        {isConfiguredButEmpty ? (
          <p className="mt-2 max-w-44 rounded-md border border-amber-300/20 bg-amber-300/10 p-2 text-[11px] leading-4 text-amber-100">
            Configured but not populated yet.
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
        <HealthBadge domain={domain} issue={issue} />
      </Td>
      <Td>
        <RiskBadges domain={domain} />
      </Td>
      <Td>
        <div className="max-w-64 space-y-1">
          {domain.endpoint_names.map((endpoint) => (
            <div key={endpoint} className="rounded-md border border-white/10 bg-slate-900/60 px-2 py-1 text-slate-400">
              <span className="break-words">{endpoint}</span>
            </div>
          ))}
          <EndpointEditor
            id={`${domain.domain_key}-primary-endpoint`}
            label="Primary endpoint"
            provider={domain.active_provider}
            supported={primaryEndpointSupported}
            value={primaryEndpoint}
            defaultValue={domain.default_primary_endpoint_url}
            contractValue={primaryEndpointContract}
            defaultContractValue={domain.default_primary_endpoint_contract_json}
            busy={busy}
            onChange={setPrimaryEndpoint}
            onContractChange={setPrimaryEndpointContract}
            test={domain.endpoint_tests?.primary}
          />
          <EndpointEditor
            id={`${domain.domain_key}-fallback-endpoint`}
            label="Fallback endpoint"
            provider={domain.fallback_provider ?? "none"}
            supported={fallbackEndpointSupported}
            value={fallbackEndpoint}
            defaultValue={domain.default_fallback_endpoint_url}
            contractValue={fallbackEndpointContract}
            defaultContractValue={domain.default_fallback_endpoint_contract_json}
            busy={busy}
            onChange={setFallbackEndpoint}
            onContractChange={setFallbackEndpointContract}
            test={domain.endpoint_tests?.fallback}
          />
          {primaryEndpointSupported || fallbackEndpointSupported ? (
            <button
              type="button"
              disabled={busy || !endpointsDirty}
              onClick={saveEndpoints}
              className="rounded-md border border-white/10 px-2 py-1.5 text-xs font-semibold text-slate-200 disabled:opacity-50"
            >
              Save endpoints
            </button>
          ) : null}
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
          {domain.admin_actions.can_test_endpoint ? (
            <button
              type="button"
              disabled={busy}
              onClick={() => testDomain(domain)}
              className="rounded-md border border-amber-300/30 px-2 py-1.5 text-xs font-semibold text-amber-100"
            >
              Test endpoint
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

function HealthBadge({ domain, issue }: { domain: AdminDataSourceDomain; issue: { label: string; detail: string } | null }) {
  const state = healthState(domain);
  const label = issue ? `${state} · ${issue.label}` : state;
  const title = issue
    ? `${issue.detail} Raw issue key: ${domain.last_error ?? "unknown_error"}`
    : undefined;
  return (
    <div className="flex max-w-56 flex-wrap gap-1">
      <Badge label={label} title={title} />
    </div>
  );
}

function RiskBadges({ domain }: { domain: AdminDataSourceDomain }) {
  return (
    <div className="flex max-w-56 flex-wrap gap-1">
      {riskStates(domain).map((badge) => (
        <Badge
          key={badge}
          label={badge}
          title={badge === "Add-on risk" ? ADD_ON_RISK_HELP : undefined}
        />
      ))}
    </div>
  );
}

function ModeHelpList() {
  return (
    <dl className="grid gap-1.5">
      {MODE_HELP_ITEMS.map(([label, detail]) => (
        <div key={label} className="grid gap-0.5">
          <dt className="font-semibold text-slate-100">{label}</dt>
          <dd className="text-slate-300">{detail}</dd>
        </div>
      ))}
    </dl>
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

function EndpointEditor({
  id,
  label,
  provider,
  supported,
  value,
  defaultValue,
  contractValue,
  defaultContractValue,
  busy,
  onChange,
  onContractChange,
  test,
}: {
  id: string;
  label: string;
  provider: string;
  supported: boolean;
  value: string;
  defaultValue?: string | null;
  contractValue: string;
  defaultContractValue?: string | null;
  busy: boolean;
  onChange: (value: string) => void;
  onContractChange: (value: string) => void;
  test?: AdminDataSourceEndpointTest | null;
}) {
  if (!supported) {
    return (
      <div className="rounded-md border border-white/10 bg-slate-950/40 px-2 py-1.5 text-[11px] leading-4 text-slate-500">
        {label}: {friendlyLabel(provider)} uses no endpoint URL.
      </div>
    );
  }
  const placeholder = defaultValue ?? "https://financialmodelingprep.com/stable/...";
  const exampleEndpoint = defaultValue ?? "https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={symbol}";
  const contractId = `${id}-contract`;
  return (
    <div className="block rounded-md border border-white/10 bg-slate-950/40 p-2">
      <div className="flex items-center justify-between gap-2 text-[11px] font-semibold uppercase text-slate-500">
        <label htmlFor={id}>{label}</label>
        <span className="flex items-center gap-1">
          {defaultValue ? (
            <button
              type="button"
              disabled={busy || value === defaultValue}
              onClick={() => {
                onChange(defaultValue);
                if (defaultContractValue) onContractChange(defaultContractValue);
              }}
              className="rounded-md border border-white/10 px-1.5 py-0.5 text-[10px] font-semibold text-slate-300 disabled:opacity-50"
            >
              Use default
            </button>
          ) : null}
          {test ? <Badge label={test.status === "healthy" ? "Test healthy" : test.status === "error" ? "Test error" : titleLabel(test.status)} title={test.error ?? undefined} /> : <Badge label="Not tested" />}
        </span>
      </div>
      <input
        id={id}
        type="text"
        value={value}
        disabled={busy}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        className="mt-1 w-full rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 text-xs text-slate-100 placeholder:text-slate-600 disabled:opacity-60"
      />
      <p className="mt-1 text-[10px] leading-4 text-slate-500">
        Use {"{symbol}"} or [symbol] for ticker endpoints. Example: {exampleEndpoint}
      </p>
      <label htmlFor={contractId} className="mt-2 block text-[10px] font-semibold uppercase text-slate-500">
        Request/response contract
      </label>
      <textarea
        id={contractId}
        value={contractValue}
        disabled={busy}
        onChange={(event) => onContractChange(event.target.value)}
        placeholder={defaultContractValue ?? '{"response":{"price_field":"close","date_field":"date","date_format":"YYYY-MM-DD HH:MM:SS"}}'}
        rows={5}
        className="mt-1 w-full rounded-md border border-white/10 bg-slate-950 px-2 py-1.5 font-mono text-[10px] leading-4 text-slate-100 placeholder:text-slate-600 disabled:opacity-60"
      />
      <p className="mt-1 text-[10px] leading-4 text-slate-500">
        Configure request params and response fields. Intraday chart uses date as YYYY-MM-DD HH:MM:SS and close as price; EOD light uses YYYY-MM-DD and price.
      </p>
      {test?.tested_at ? <span className="mt-1 block text-[10px] text-slate-500">Tested {formatDate(test.tested_at)}</span> : null}
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
  const otherRows = Object.entries(rows).filter(([key]) => !SECONDARY_DIAGNOSTIC_KEYS.has(key));

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">{title}</h3>
      {comparison ? <p className="mt-2 text-xs leading-5 text-slate-400">{SWITCH_READINESS_HELP}</p> : null}
      <div className="mt-3 grid gap-2">
        {otherRows.map(([key, value]) => (
          <DiagnosticRow key={key} label={diagnosticLabel(key)} value={value} />
        ))}
      </div>
      {comparison ? <HistoricalComparisonDetails comparison={comparison} /> : null}
    </div>
  );
}

function HistoricalComparisonDetails({ comparison }: { comparison: Record<string, unknown> }) {
  const countObjectKey = Object.keys(comparison).find((key) => key.endsWith("_feed_count"));
  const countObject = countObjectKey && typeof comparison[countObjectKey] === "object" && comparison[countObjectKey] !== null
    ? comparison[countObjectKey] as Record<string, unknown>
    : {};
  const entries = [
    ...Object.entries(countObject),
    ...Object.entries(comparison).filter(([key]) => key !== countObjectKey),
  ];

  return (
    <details className="mt-3 rounded-md border border-white/10 bg-slate-900/60 p-3 text-xs text-slate-400">
      <summary className="cursor-pointer font-semibold text-slate-200">Historical coverage comparison (optional)</summary>
      <p className="mt-2 leading-5">{HISTORICAL_COVERAGE_HELP}</p>
      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        {entries.map(([key, value]) => (
          <div key={key} className="min-w-0 rounded-md border border-white/10 bg-slate-950/50 p-2">
            <div className="text-[11px] font-semibold text-slate-300">{historicalComparisonLabel(key)}</div>
            <div className="mt-1 break-words text-sm font-semibold text-slate-100 [overflow-wrap:anywhere]">{formatDiagnosticValue(value)}</div>
          </div>
        ))}
      </div>
      <p className="mt-3 leading-5 text-slate-500">{HISTORICAL_GAP_HELP} Backfill is optional. Existing production data remains intact.</p>
    </details>
  );
}

function diagnosticLabel(key: string) {
  return READINESS_LABELS[key] ?? titleLabel(key);
}

function historicalComparisonLabel(key: string) {
  if (key === "current_feed") return "Current production feed count";
  if (key === "official_normalized" || key === "sec_normalized") return "Shadow normalized count";
  if (key === "delta") return "Historical gap";
  if (key === "missing_in_official" || key === "missing_in_sec") return "Missing in shadow";
  if (key === "missing_in_current") return "Missing in current";
  if (key === "potential_duplicates") return "Duplicate candidates";
  if (key === "parse_confidence_warnings") return "Parse confidence warnings";
  if (key === "fmp_raw_rows") return "Legacy FMP raw rows";
  return titleLabel(key);
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
            {typeof item === "object" && item !== null ? Object.entries(item as Record<string, unknown>).map(([key, itemValue]) => `${titleLabel(key)}: ${formatDiagnosticValue(itemValue)}`).join(" | ") : formatDiagnosticValue(item)}
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
            <span className="font-semibold text-slate-300">{titleLabel(key)}:</span> {formatDiagnosticValue(itemValue)}
          </span>
        ))}
      </div>
    );
  }
  return <span className="break-words text-slate-400">{formatDiagnosticValue(value)}</span>;
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
  const groupNames = [
    ...SOURCE_MAP_GROUP_ORDER,
    ...Object.keys(groups).filter((group) => !SOURCE_MAP_GROUP_ORDER.includes(group)),
  ];

  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <h3 className="font-semibold text-white">Current data source map</h3>
      <div className="mt-3 grid min-w-0 gap-3 md:grid-cols-2 xl:grid-cols-4">
        {groupNames.map((group) => {
          const keys = groups[group] ?? [];
          return (
            <div key={group} className="min-w-0 rounded-lg border border-white/10 bg-slate-900/50 p-3">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{group}</div>
              <div className="mt-2 grid min-w-0 gap-2">
                {keys.length ? (
                  keys.map((key) => {
                    const domain = domainByKey.get(key);
                    const provider = rows[key] ?? domain?.active_provider ?? "none";
                    const risk = domain ? riskStates(domain)[0] : null;
                    return (
                      <div key={key} className="min-w-0 rounded-md border border-white/10 bg-slate-900/60 p-3">
                        <div className="font-semibold text-slate-100">{domain?.data_domain ?? titleLabel(key)}</div>
                        <div className="mt-1 text-sm text-slate-300">{friendlyLabel(provider, domain?.provider_labels)}</div>
                        <div className="mt-1 break-words text-[11px] text-slate-500">{key} / {provider}</div>
                        {domain ? (
                          <div className="mt-2 flex flex-wrap gap-1">
                            <Badge label={modeLabel(domain.settings.mode)} title={MODE_HELP[domain.settings.mode]} />
                            <Badge label={healthState(domain)} />
                            {risk ? <Badge label={risk} title={risk === "Add-on risk" ? ADD_ON_RISK_HELP : undefined} /> : null}
                          </div>
                        ) : null}
                      </div>
                    );
                  })
                ) : (
                  <div className="rounded-md border border-white/10 bg-slate-950/50 p-3 text-xs text-slate-500">No domains configured in this group.</div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function sourceMapGroup(key: string) {
  if (
    key.startsWith("prices_")
    || ["fundamentals", "ratios", "technicals", "profiles", "earnings", "analyst_estimates", "institutional_13f"].includes(key)
  ) {
    return "Market Data";
  }
  if (["congress_trades", "house_disclosures", "senate_disclosures", "insider_trades", "form4_filings"].includes(key) || key.includes("government_contract")) {
    return "Official / Alternative Data";
  }
  if (key.startsWith("insights_")) return "Insights";
  if (
    key.startsWith("screener_")
    || ["pnl_enrichment", "signal_inputs", "confirmation_score", "trade_outcomes", "portfolio_simulation", "backtesting_inputs", "watchlist_alerts"].includes(key)
    || key.includes("portfolio")
    || key.includes("backtesting")
  ) {
    return "Internal / Computed";
  }
  return "Internal / Computed";
}

function Badge({ label, title }: { label: string; title?: string }) {
  return (
    <span title={title} className={`rounded-md border px-1.5 py-0.5 text-[10px] font-semibold uppercase ${badgeClass(label)}`}>
      {label}
    </span>
  );
}

function InfoTooltip({ id, label, description, align = "left" }: { id: string; label: string; description: ReactNode; align?: "left" | "right" }) {
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

function HeaderTooltip({ id, label, description }: { id: string; label: ReactNode; description: ReactNode }) {
  const ariaLabel = typeof label === "string" ? `${label} help` : "Column help";
  return (
    <span className="group/header-tip relative inline-flex max-w-full items-center gap-1.5">
      <span className="min-w-0 truncate underline decoration-slate-600/70 decoration-dotted underline-offset-4">{label}</span>
      <InfoTooltip id={id} label={ariaLabel} description={description} align="left" />
    </span>
  );
}

function Th({ children, help }: { children: ReactNode; help?: ReactNode }) {
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
