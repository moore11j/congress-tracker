"use client";

import { useEffect, useState } from "react";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { getAdminReportsSummary, type AdminReportsSummary } from "@/lib/api";

function formatInteger(value: number) {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatCurrency(value: number, currency: string) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: currency || "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatUpdatedAt(value?: string) {
  if (!value) return "";
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) return value;
  return parsed.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</p>
      <p className="mt-3 text-2xl font-semibold text-white">{value}</p>
      {detail ? <p className="mt-1 text-xs text-slate-500">{detail}</p> : null}
    </div>
  );
}

function BusinessOverviewSkeleton() {
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-6">
      {Array.from({ length: 6 }).map((_, index) => (
        <div key={index} className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
          <SkeletonBlock className="h-3 w-24" />
          <SkeletonBlock className="mt-3 h-7 w-28" />
          <SkeletonBlock className="mt-2 h-3 w-20" />
        </div>
      ))}
    </div>
  );
}

export function BusinessOverviewReport() {
  const [summary, setSummary] = useState<AdminReportsSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let ignore = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const next = await getAdminReportsSummary();
        if (!ignore) setSummary(next);
      } catch (loadError) {
        if (!ignore) {
          setSummary(null);
          setError(loadError instanceof Error ? loadError.message : "Unable to load business overview.");
        }
      } finally {
        if (!ignore) setLoading(false);
      }
    };
    load();
    return () => {
      ignore = true;
    };
  }, []);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Business Overview</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Business Overview</h2>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">User activity and subscription revenue at a glance.</p>
        </div>
        {summary?.generated_at ? <p className="text-xs text-slate-500">Updated {formatUpdatedAt(summary.generated_at)}</p> : null}
      </div>

      {loading ? <div className="mt-5"><BusinessOverviewSkeleton /></div> : null}
      {!loading && error ? <p className="mt-5 rounded-lg border border-rose-300/20 bg-rose-400/[0.07] px-4 py-3 text-sm text-rose-100">{error}</p> : null}

      {!loading && !error && summary ? (
        <>
          <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-6">
            <MetricCard label="Active Free Users" value={formatInteger(summary.active_free_users)} />
            <MetricCard label="Active Premium Users" value={formatInteger(summary.active_premium_users)} />
            <MetricCard
              label="Monthly Recurring Revenue"
              value={formatCurrency(summary.monthly_recurring_revenue, summary.currency)}
              detail="/ month"
            />
            <MetricCard label="Revenue — YTD" value={formatCurrency(summary.revenue_ytd, summary.currency)} />
            <MetricCard label="New Users — Last 30 Days" value={formatInteger(summary.new_users_last_30_days)} />
            <MetricCard label="Total Users" value={formatInteger(summary.total_users)} />
          </div>

          {summary.notes && summary.notes.length > 0 ? (
            <div className="mt-4 rounded-lg border border-white/10 bg-slate-950/35 px-4 py-3">
              {summary.notes.map((note) => (
                <p key={note} className="text-sm text-slate-400">
                  {note}
                </p>
              ))}
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  );
}
