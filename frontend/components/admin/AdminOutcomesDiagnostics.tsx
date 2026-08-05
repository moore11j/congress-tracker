"use client";

import { useEffect, useState } from "react";
import {
  ApiError,
  getAdminOutcomeLedgerStatus,
  getAdminOutcomeSnapshots,
  type AdminOutcomeLedgerStatus,
  type OutcomeSnapshot,
  type OutcomeSnapshotsResponse,
} from "@/lib/api";

function text(value: unknown) {
  if (value === null || value === undefined || value === "") return "-";
  return String(value);
}

function formatDateTime(value?: string | null) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function loadError(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 403) return "Admin access required.";
  }
  return error instanceof Error ? error.message : "Unable to load Outcomes diagnostics.";
}

function DiagnosticMetric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/50 p-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
      <p className="mt-2 text-lg font-semibold text-white">{value}</p>
    </div>
  );
}

export function AdminOutcomesDiagnostics() {
  const [status, setStatus] = useState<AdminOutcomeLedgerStatus | null>(null);
  const [snapshots, setSnapshots] = useState<OutcomeSnapshotsResponse | null>(null);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [ticker, setTicker] = useState("");
  const [methodology, setMethodology] = useState("");
  const [calculationType, setCalculationType] = useState("live");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  function load() {
    setLoading(true);
    Promise.all([
      getAdminOutcomeLedgerStatus(),
      getAdminOutcomeSnapshots({
        ticker: ticker || undefined,
        methodology: methodology || undefined,
        calculation_type: calculationType || undefined,
        start_date: startDate || undefined,
        end_date: endDate || undefined,
        limit: 50,
      }),
    ])
      .then(([nextStatus, nextSnapshots]) => {
        setStatus(nextStatus);
        setSnapshots(nextSnapshots);
        setError(null);
      })
      .catch((nextError) => setError(loadError(nextError)))
      .finally(() => setLoading(false));
  }

  useEffect(() => {
    load();
  }, []);

  const expanded = snapshots?.items.find((item) => item.id === expandedId) ?? null;

  return (
    <div className="space-y-6">
      {error ? <div className="rounded-lg border border-rose-300/20 bg-rose-400/10 px-4 py-3 text-sm text-rose-100">{error}</div> : null}
      <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Diagnostics</p>
            <h2 className="mt-1 text-xl font-semibold text-white">Outcome Ledger capture health</h2>
          </div>
          <button
            type="button"
            onClick={load}
            className="rounded-md border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-emerald-300/30 hover:text-white"
          >
            Refresh
          </button>
        </div>
        <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <DiagnosticMetric label="Methodology" value={status?.current_methodology_version ?? "-"} />
          <DiagnosticMetric label="Total live snapshots" value={status?.total_live_snapshots ?? 0} />
          <DiagnosticMetric label="Unique securities" value={status?.unique_securities_captured ?? 0} />
          <DiagnosticMetric label="Created in 24h" value={status?.snapshots_created_past_24h ?? 0} />
          <DiagnosticMetric label="First snapshot" value={formatDateTime(status?.first_live_snapshot_date)} />
          <DiagnosticMetric label="Latest snapshot" value={formatDateTime(status?.most_recent_snapshot_timestamp)} />
          <DiagnosticMetric label="Duplicates ignored" value={status?.duplicate_attempts_ignored ?? 0} />
          <DiagnosticMetric label="Persistence errors" value={status?.persistence_errors ?? 0} />
          <DiagnosticMetric label="Missing prices" value={status?.missing_reference_prices ?? 0} />
          <DiagnosticMetric label="Missing security IDs" value={status?.missing_security_ids ?? 0} />
          <DiagnosticMetric label="Missing source payloads" value={status?.missing_source_contribution_payloads ?? 0} />
          <DiagnosticMetric label="Data quality" value={status?.data_quality_status ?? "unknown"} />
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
        <div className="grid gap-3 md:grid-cols-5">
          <input value={ticker} onChange={(event) => setTicker(event.target.value.toUpperCase())} placeholder="Ticker" className="rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50" />
          <input value={methodology} onChange={(event) => setMethodology(event.target.value)} placeholder="Methodology" className="rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50" />
          <select value={calculationType} onChange={(event) => setCalculationType(event.target.value)} className="rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50">
            <option value="">All capture types</option>
            <option value="live">Live</option>
            <option value="historical_reconstruction">Historical reconstruction</option>
            <option value="data_correction">Data correction</option>
            <option value="manual_test">Manual test</option>
          </select>
          <input value={startDate} onChange={(event) => setStartDate(event.target.value)} type="date" className="rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50" />
          <input value={endDate} onChange={(event) => setEndDate(event.target.value)} type="date" className="rounded-md border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50" />
        </div>
        <div className="mt-3">
          <button type="button" onClick={load} className="rounded-md bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200">
            Apply filters
          </button>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-xl font-semibold text-white">Latest snapshots</h2>
          <p className="text-sm text-slate-400">{loading ? "Loading..." : `${snapshots?.total ?? 0} matching records`}</p>
        </div>
        <div className="overflow-x-auto rounded-lg border border-white/10">
          <table className="min-w-[1080px] w-full divide-y divide-white/10 text-left text-sm">
            <thead className="bg-slate-950/70 text-[11px] uppercase tracking-[0.16em] text-slate-500">
              <tr>
                {["Ticker", "Calculated", "Score", "Direction", "Strength", "Ref price", "Sources", "Methodology", "Type", "Detail"].map((label) => (
                  <th key={label} className="px-4 py-3 font-semibold">{label}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10 bg-slate-900/30">
              {(snapshots?.items ?? []).map((item) => (
                <tr key={item.id} className="hover:bg-white/[0.03]">
                  <td className="px-4 py-3 font-mono font-semibold text-emerald-100">{item.ticker}</td>
                  <td className="px-4 py-3 text-slate-300">{formatDateTime(item.calculated_at)}</td>
                  <td className="px-4 py-3 text-white">{item.score}</td>
                  <td className="px-4 py-3 capitalize text-slate-300">{item.direction}</td>
                  <td className="px-4 py-3 capitalize text-slate-300">{item.strength}</td>
                  <td className="px-4 py-3 text-slate-300">{text(item.reference_price)}</td>
                  <td className="px-4 py-3 text-slate-300">{item.active_source_count}</td>
                  <td className="px-4 py-3 text-slate-300">{text(item.methodology)}</td>
                  <td className="px-4 py-3 text-slate-300">{item.calculation_type}</td>
                  <td className="px-4 py-3">
                    <button type="button" onClick={() => setExpandedId(expandedId === item.id ? null : item.id)} className="rounded-md border border-white/10 px-3 py-1 text-xs font-semibold text-slate-200 hover:text-white">
                      {expandedId === item.id ? "Hide" : "Inspect"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {!loading && !(snapshots?.items ?? []).length ? (
          <div className="mt-4 rounded-lg border border-dashed border-white/15 bg-slate-950/40 p-6 text-sm text-slate-400">
            No snapshots match the current filters.
          </div>
        ) : null}
      </section>

      {expanded ? (
        <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">Snapshot detail</p>
          <h2 className="mt-1 text-xl font-semibold text-white">{expanded.ticker} raw capture</h2>
          <pre className="mt-4 max-h-[32rem] overflow-auto rounded-lg border border-white/10 bg-slate-950 p-4 text-xs leading-5 text-slate-300">
            {JSON.stringify(expanded, null, 2)}
          </pre>
        </section>
      ) : null}
    </div>
  );
}
