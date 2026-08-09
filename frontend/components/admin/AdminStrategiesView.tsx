"use client";

import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  activateAdminStrategyVersion,
  approveAdminStrategyVersion,
  createAdminStrategyVersion,
  getAdminStrategies,
  getAdminStrategy,
  getAdminStrategySchedulerStatus,
  getAdminStrategyVersions,
  previewAdminStrategyVersion,
  setAdminStrategyPublication,
  type StrategyDefinitionPayload,
  type StrategyDetailPayload,
  type StrategyHolding,
  type StrategyPerformanceSnapshot,
  type StrategyVersionPayload,
  type StrategyVersionPreview,
  type StrategySchedulerStatus,
} from "@/lib/api";
import type { AdminToastApi } from "@/components/admin/AdminToast";

const PERIODS = ["max", "30d", "1y", "2y", "3y"] as const;
const DEFAULT_PROSPECTIVE_RULES = JSON.stringify(
  {
    candidate_source: "confirmation_score_snapshots",
    direction: "bullish",
    min_score: 60,
    min_active_sources: 3,
    max_positions: 10,
    max_snapshot_age_days: 3,
  },
  null,
  2,
);

type PeriodKey = (typeof PERIODS)[number];

function formatNumber(value: number | null | undefined, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function formatPct(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "n/a";
  return `${formatNumber(value)}%`;
}

function statusMessage(error: unknown) {
  if (error instanceof ApiError) {
    if (error.status === 401) return "Sign in required.";
    if (error.status === 403) return "Admin access required.";
  }
  return error instanceof Error ? error.message : "Unable to load strategies.";
}

function metricRows(performance?: StrategyPerformanceSnapshot | null) {
  return [
    ["Walnut Score", formatNumber(performance?.walnutStrategyScore)],
    ["CAGR", formatPct(performance?.cagrPct)],
    ["Return", formatPct(performance?.totalReturnPct)],
    ["SPY CAGR", formatPct(performance?.benchmarkCagrPct)],
    ["Alpha", formatPct(performance?.alphaCagrPct)],
    ["Sharpe", formatNumber(performance?.sharpe)],
    ["Max drawdown", formatPct(performance?.maxDrawdownPct)],
    ["Volatility", formatPct(performance?.annualizedVolatilityPct)],
    ["Win rate", formatPct(performance?.winRatePct)],
    ["Trades", formatNumber(performance?.tradeCount, 0)],
    ["Signals", formatNumber(performance?.independentSignalCount, 0)],
    ["Avg holdings", formatNumber(performance?.avgHoldings)],
  ] as const;
}

function JsonBlock({ value }: { value: unknown }) {
  return (
    <pre className="max-h-96 overflow-auto rounded-lg border border-white/10 bg-slate-950/70 p-3 text-xs leading-5 text-slate-300">
      {JSON.stringify(value ?? {}, null, 2)}
    </pre>
  );
}

function StrategyPill({ children, tone = "neutral" }: { children: string; tone?: "neutral" | "good" | "warn" }) {
  const classes =
    tone === "good"
      ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
      : tone === "warn"
        ? "border-amber-300/30 bg-amber-300/10 text-amber-100"
        : "border-white/10 bg-slate-950/60 text-slate-300";
  return <span className={`rounded-md border px-2 py-1 text-xs font-semibold ${classes}`}>{children}</span>;
}

function StrategyListItem({
  strategy,
  selected,
  onSelect,
}: {
  strategy: StrategyDefinitionPayload;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`w-full rounded-lg border p-4 text-left transition ${
        selected ? "border-emerald-300/40 bg-emerald-300/10" : "border-white/10 bg-slate-950/40 hover:border-white/20"
      }`}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="truncate font-semibold text-white">{strategy.name}</h3>
          <p className="mt-1 truncate text-xs text-slate-500">{strategy.slug}</p>
        </div>
        <StrategyPill tone={strategy.status === "published" ? "good" : "warn"}>{strategy.status}</StrategyPill>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        <StrategyPill>{strategy.category}</StrategyPill>
        <StrategyPill>{strategy.dataQualityConfidence}</StrategyPill>
        <StrategyPill>{`Score ${formatNumber(strategy.latestRun?.walnutStrategyScore)}`}</StrategyPill>
      </div>
    </button>
  );
}

function HoldingsTable({ holdings }: { holdings: StrategyHolding[] }) {
  const rows = holdings.slice(0, 30);
  if (!rows.length) {
    return <p className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">No current holdings stored.</p>;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-white/10">
      <table className="min-w-full divide-y divide-white/10 text-sm">
        <thead className="bg-slate-950/70 text-left text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-3 py-2">Rank</th>
            <th className="px-3 py-2">Symbol</th>
            <th className="px-3 py-2">Weight</th>
            <th className="px-3 py-2">Entry</th>
            <th className="px-3 py-2">Return</th>
            <th className="px-3 py-2">Signals</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/10 bg-slate-950/30">
          {rows.map((holding) => (
            <tr key={`${holding.rank ?? "x"}-${holding.symbol}`}>
              <td className="px-3 py-2 text-slate-400">{holding.rank ?? "n/a"}</td>
              <td className="px-3 py-2 font-semibold text-white">{holding.symbol}</td>
              <td className="px-3 py-2 text-slate-300">{formatPct(holding.weightPct)}</td>
              <td className="px-3 py-2 text-slate-400">{holding.entryDate ?? "n/a"}</td>
              <td className="px-3 py-2 text-slate-300">{formatPct(holding.returnPct)}</td>
              <td className="px-3 py-2 text-slate-400">{holding.sourceSignalCount ?? 0}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function AdminStrategiesView({ showToast }: { showToast?: AdminToastApi["showToast"] }) {
  const [period, setPeriod] = useState<PeriodKey>("max");
  const [strategies, setStrategies] = useState<StrategyDefinitionPayload[]>([]);
  const [selectedSlug, setSelectedSlug] = useState<string | null>(null);
  const [detail, setDetail] = useState<StrategyDetailPayload | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [publicationBusy, setPublicationBusy] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const [versions, setVersions] = useState<StrategyVersionPayload[]>([]);
  const [versionBusy, setVersionBusy] = useState(false);
  const [versionRefreshKey, setVersionRefreshKey] = useState(0);
  const [versionRules, setVersionRules] = useState(DEFAULT_PROSPECTIVE_RULES);
  const [previewDate, setPreviewDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [preview, setPreview] = useState<StrategyVersionPreview | null>(null);
  const [scheduler, setScheduler] = useState<StrategySchedulerStatus | null>(null);

  const selected = useMemo(
    () => strategies.find((strategy) => strategy.slug === selectedSlug) ?? strategies[0] ?? null,
    [selectedSlug, strategies],
  );

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      setBusy(true);
      setStatus(null);
      try {
        const response = await getAdminStrategies({ period, sort: "cagr", signal: controller.signal });
        setStrategies(response.items);
        setSelectedSlug((current) => current ?? response.items[0]?.slug ?? null);
      } catch (error) {
        if (controller.signal.aborted) return;
        const message = statusMessage(error);
        setStatus(message);
        showToast?.({ message, tone: "error" });
      } finally {
        if (!controller.signal.aborted) setBusy(false);
      }
    }
    load();
    return () => controller.abort();
  }, [period, refreshKey, showToast]);

  useEffect(() => {
    if (!selected?.slug) {
      setDetail(null);
      return;
    }
    const controller = new AbortController();
    async function loadDetail() {
      setBusy(true);
      setStatus(null);
      try {
        setDetail(await getAdminStrategy(selected.slug, { period, equityLimit: 1500, signal: controller.signal }));
      } catch (error) {
        if (controller.signal.aborted) return;
        const message = statusMessage(error);
        setStatus(message);
        showToast?.({ message, tone: "error" });
      } finally {
        if (!controller.signal.aborted) setBusy(false);
      }
    }
    loadDetail();
    return () => controller.abort();
  }, [period, refreshKey, selected?.slug, showToast]);

  useEffect(() => {
    if (!selected?.slug) {
      setVersions([]);
      setPreview(null);
      return;
    }
    let active = true;
    getAdminStrategyVersions(selected.slug)
      .then((response) => {
        if (!active) return;
        setVersions(response.items);
      })
      .catch((error) => {
        if (!active) return;
        const message = statusMessage(error);
        setStatus(message);
        showToast?.({ message, tone: "error" });
      });
    return () => {
      active = false;
    };
  }, [selected?.slug, showToast, versionRefreshKey]);

  useEffect(() => {
    let active = true;
    getAdminStrategySchedulerStatus()
      .then((response) => {
        if (active) setScheduler(response);
      })
      .catch(() => {
        // Scheduler status is ancillary to strategy review; retain the review UI if it is unavailable.
      });
    return () => {
      active = false;
    };
  }, [versionRefreshKey]);

  const activeDetail = detail?.slug === selected?.slug ? detail : null;
  const performance = activeDetail?.performance ?? selected?.performance ?? null;
  const run = activeDetail?.latestRun ?? selected?.latestRun ?? null;
  const diagnostics = run?.diagnostics as Record<string, unknown> | undefined;

  async function updatePublication() {
    if (!selected || publicationBusy) return;
    const publish = selected.status !== "published";
    setPublicationBusy(true);
    setStatus(null);
    try {
      const updated = await setAdminStrategyPublication(selected.slug, publish);
      setDetail(updated);
      setRefreshKey((value) => value + 1);
      showToast?.({ message: `${updated.name} ${publish ? "published" : "unpublished"}.`, tone: "success" });
    } catch (error) {
      const message = statusMessage(error);
      setStatus(message);
      showToast?.({ message, tone: "error" });
    } finally {
      setPublicationBusy(false);
    }
  }

  async function createVersion() {
    if (!selected || versionBusy) return;
    let rules: Record<string, unknown>;
    try {
      const parsed = JSON.parse(versionRules);
      if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error("Rules must be a JSON object.");
      rules = parsed as Record<string, unknown>;
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Rules must be valid JSON.");
      return;
    }
    setVersionBusy(true);
    setPreview(null);
    try {
      const version = await createAdminStrategyVersion(selected.slug, { rules });
      setVersionRefreshKey((value) => value + 1);
      showToast?.({ message: `Created draft version ${version.version}.`, tone: "success" });
    } catch (error) {
      const message = statusMessage(error);
      setStatus(message);
      showToast?.({ message, tone: "error" });
    } finally {
      setVersionBusy(false);
    }
  }

  async function previewVersion(version: StrategyVersionPayload) {
    if (!selected || versionBusy) return;
    setVersionBusy(true);
    setPreview(null);
    try {
      setPreview(await previewAdminStrategyVersion(selected.slug, version.id, previewDate));
    } catch (error) {
      const message = statusMessage(error);
      setStatus(message);
      showToast?.({ message, tone: "error" });
    } finally {
      setVersionBusy(false);
    }
  }

  async function approveVersion(version: StrategyVersionPayload) {
    if (!selected || versionBusy) return;
    setVersionBusy(true);
    try {
      const approved = await approveAdminStrategyVersion(selected.slug, version.id);
      setVersionRefreshKey((value) => value + 1);
      showToast?.({ message: `Approved version ${approved.version}.`, tone: "success" });
    } catch (error) {
      const message = statusMessage(error);
      setStatus(message);
      showToast?.({ message, tone: "error" });
    } finally {
      setVersionBusy(false);
    }
  }

  async function activateVersion(version: StrategyVersionPayload) {
    if (!selected || versionBusy) return;
    setVersionBusy(true);
    try {
      const activated = await activateAdminStrategyVersion(selected.slug, version.id);
      setVersionRefreshKey((value) => value + 1);
      showToast?.({ message: `Activated version ${activated.version}.`, tone: "success" });
    } catch (error) {
      const message = statusMessage(error);
      setStatus(message);
      showToast?.({ message, tone: "error" });
    } finally {
      setVersionBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">Strategy drafts</h2>
            <p className="mt-2 text-sm text-slate-400">
              {strategies.length} persisted strategy {strategies.length === 1 ? "draft" : "drafts"} in review.
            </p>
          </div>
          <div className="inline-flex rounded-lg border border-white/10 bg-slate-950/50 p-1">
            {PERIODS.map((nextPeriod) => (
              <button
                key={nextPeriod}
                type="button"
                onClick={() => setPeriod(nextPeriod)}
                className={`rounded-md px-3 py-2 text-sm font-semibold ${
                  period === nextPeriod ? "bg-emerald-300/10 text-emerald-100" : "text-slate-300 hover:text-white"
                }`}
              >
                {nextPeriod}
              </button>
            ))}
          </div>
        </div>
        {status ? <p className="mt-3 text-sm text-amber-200">{status}</p> : null}
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(18rem,24rem)_minmax(0,1fr)]">
        <section className="space-y-3">
          {strategies.map((strategy) => (
            <StrategyListItem
              key={strategy.slug}
              strategy={strategy}
              selected={strategy.slug === selected?.slug}
              onSelect={() => setSelectedSlug(strategy.slug)}
            />
          ))}
          {!strategies.length && !busy ? (
            <p className="rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">No strategy drafts found.</p>
          ) : null}
        </section>

        <section className="min-w-0 space-y-5 rounded-lg border border-white/10 bg-slate-900/70 p-5">
          {selected ? (
            <>
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="min-w-0">
                  <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">{selected.category}</p>
                  <h2 className="mt-1 text-2xl font-semibold text-white">{selected.name}</h2>
                  <p className="mt-2 break-all text-xs text-slate-500">{selected.slug}</p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <StrategyPill tone={selected.status === "published" ? "good" : "warn"}>{selected.status}</StrategyPill>
                  <StrategyPill>{selected.dataQualityConfidence}</StrategyPill>
                  <StrategyPill>{selected.accessTier}</StrategyPill>
                  <button
                    type="button"
                    onClick={updatePublication}
                    disabled={publicationBusy}
                    className="rounded-md border border-emerald-300/35 bg-emerald-300/10 px-3 py-1 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-300/20 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    {publicationBusy ? "Updating..." : selected.status === "published" ? "Unpublish strategy" : "Publish strategy"}
                  </button>
                </div>
              </div>

              <p className="-mt-2 text-xs text-slate-500">Publishing changes catalogue visibility only. A successful stored run and max-period snapshot are required.</p>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                {metricRows(performance).map(([label, value]) => (
                  <div key={label} className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
                    <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
                    <div className="mt-1 text-lg font-semibold text-white">{value}</div>
                  </div>
                ))}
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                  <h3 className="font-semibold text-white">Methodology</h3>
                  <p className="mt-2 text-sm text-slate-300">{selected.methodology ?? "No methodology text stored."}</p>
                  <dl className="mt-4 space-y-2 text-sm">
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Backtest</dt>
                      <dd className="text-right text-slate-200">{run?.backtestStartDate ?? "n/a"} to {run?.backtestEndDate ?? "n/a"}</dd>
                    </div>
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Benchmark</dt>
                      <dd className="text-right text-slate-200">{run?.benchmark ?? "n/a"}</dd>
                    </div>
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Code version</dt>
                      <dd className="text-right text-slate-200">{run?.codeVersion ?? "n/a"}</dd>
                    </div>
                    <div className="flex justify-between gap-3">
                      <dt className="text-slate-500">Run key</dt>
                      <dd className="max-w-52 break-all text-right text-slate-200">{run?.runKey ?? "n/a"}</dd>
                    </div>
                  </dl>
                </div>

                <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                  <h3 className="font-semibold text-white">Review flags</h3>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {(selected.riskNotes?.length ? selected.riskNotes : ["none"]).map((note) => (
                      <StrategyPill key={note} tone={note === "none" ? "good" : "warn"}>{note}</StrategyPill>
                    ))}
                  </div>
                  <p className="mt-4 text-sm text-slate-400">{selected.walnutTake ?? "No Walnut take stored."}</p>
                </div>
              </div>

              <section className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <h3 className="font-semibold text-white">Prospective versions</h3>
                    <p className="mt-1 text-sm text-slate-400">Draft from explicit point-in-time rules, preview candidates, then approve for future scheduling.</p>
                  </div>
                  <label className="text-xs font-medium text-slate-400">Preview date
                    <input type="date" value={previewDate} onChange={(event) => setPreviewDate(event.target.value)} className="ml-2 rounded-md border border-white/10 bg-slate-950 px-2 py-1 text-sm text-white" />
                  </label>
                </div>
                <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-2 rounded-md border border-white/10 bg-slate-950/60 px-3 py-2 text-xs text-slate-400">
                  <span>Scheduler: <strong className={scheduler?.enabled ? "text-emerald-200" : "text-amber-200"}>{scheduler?.enabled ? "Enabled" : "Disabled"}</strong></span>
                  <span>Run cap: {scheduler?.maxStrategiesPerRun ?? "--"}</span>
                  <span>Last result: {scheduler?.lastRun?.status ?? "Not run"}</span>
                  {scheduler?.lastRun?.failed ? <span className="text-rose-200">Failures: {scheduler.lastRun.failed}</span> : null}
                </div>
                <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(18rem,0.9fr)]">
                  <div>
                    <label className="text-xs font-semibold uppercase tracking-wide text-slate-500">New version rules</label>
                    <textarea value={versionRules} onChange={(event) => setVersionRules(event.target.value)} spellCheck={false} className="mt-2 min-h-52 w-full rounded-md border border-white/10 bg-slate-950/70 p-3 font-mono text-xs leading-5 text-slate-200 outline-none focus:border-emerald-300/50" />
                    <button type="button" onClick={createVersion} disabled={versionBusy} className="mt-3 rounded-md border border-emerald-300/35 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20 disabled:cursor-not-allowed disabled:opacity-50">
                      Create draft version
                    </button>
                  </div>
                  <div className="space-y-2">
                    {versions.length ? versions.map((version) => (
                      <div key={version.id} className="rounded-md border border-white/10 bg-slate-950/60 p-3">
                        <div className="flex items-center justify-between gap-2"><span className="font-semibold text-white">Version {version.version}</span><StrategyPill tone={version.status === "approved" ? "good" : "warn"}>{version.status}</StrategyPill></div>
                        <p className="mt-1 text-xs text-slate-500">{version.createdAt ?? "Pending timestamp"}</p>
                        <div className="mt-3 flex flex-wrap gap-2">
                          <button type="button" onClick={() => previewVersion(version)} disabled={versionBusy} className="rounded-md border border-white/15 px-2 py-1 text-xs font-semibold text-slate-200 hover:border-emerald-300/40 disabled:opacity-50">Preview</button>
                          {version.status === "draft" ? <button type="button" onClick={() => approveVersion(version)} disabled={versionBusy} className="rounded-md border border-emerald-300/35 px-2 py-1 text-xs font-semibold text-emerald-100 hover:bg-emerald-300/10 disabled:opacity-50">Approve</button> : null}
                          {version.status === "approved" ? <button type="button" onClick={() => activateVersion(version)} disabled={versionBusy} className="rounded-md border border-sky-300/35 px-2 py-1 text-xs font-semibold text-sky-100 hover:bg-sky-300/10 disabled:opacity-50">Activate</button> : null}
                        </div>
                      </div>
                    )) : <p className="rounded-md border border-white/10 p-3 text-sm text-slate-500">No prospective versions yet.</p>}
                  </div>
                </div>
                {preview ? <div className="mt-4 overflow-x-auto rounded-md border border-white/10"><div className="border-b border-white/10 px-3 py-2 text-sm text-slate-300">Dry run: {preview.qualifyingCount} qualifying from {preview.universeCount} visible candidates.</div><table className="min-w-full text-sm"><thead className="bg-slate-950/70 text-left text-xs uppercase tracking-wide text-slate-500"><tr><th className="px-3 py-2">Symbol</th><th className="px-3 py-2">Score</th><th className="px-3 py-2">Sources</th><th className="px-3 py-2">Weight</th></tr></thead><tbody className="divide-y divide-white/10">{preview.candidates.map((candidate) => <tr key={candidate.symbol}><td className="px-3 py-2 font-semibold text-white">{candidate.symbol}</td><td className="px-3 py-2 text-slate-300">{formatNumber(candidate.score)}</td><td className="px-3 py-2 text-slate-300">{candidate.sourceCount ?? "n/a"}</td><td className="px-3 py-2 text-slate-300">{formatPct(candidate.weightPct)}</td></tr>)}</tbody></table></div> : null}
              </section>

              <div>
                <div className="mb-3 flex items-center justify-between gap-3">
                  <h3 className="font-semibold text-white">Current holdings</h3>
                  <span className="text-sm text-slate-500">{activeDetail?.currentHoldings?.length ?? 0} stored</span>
                </div>
                <HoldingsTable holdings={activeDetail?.currentHoldings ?? []} />
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <details className="rounded-lg border border-white/10 bg-slate-950/40 p-4" open>
                  <summary className="cursor-pointer font-semibold text-white">Rule and parameters</summary>
                  <div className="mt-3 grid gap-3">
                    <JsonBlock value={{ rule: selected.rule, parameters: selected.parameters, universe: selected.universe }} />
                  </div>
                </details>

                <details className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                  <summary className="cursor-pointer font-semibold text-white">Diagnostics</summary>
                  <div className="mt-3">
                    <JsonBlock value={diagnostics ?? {}} />
                  </div>
                </details>
              </div>
            </>
          ) : (
            <p className="text-sm text-slate-400">Select a strategy draft.</p>
          )}
        </section>
      </div>
    </div>
  );
}
