"use client";

import Link from "next/link";
import { useState } from "react";
import { clearWatchlistConfirmationEvents, refreshWatchlistConfirmationMonitoring } from "@/lib/api";
import type { ConfirmationMonitoringEvent } from "@/lib/types";

type Props = {
  watchlistId: number;
  initialEvents: ConfirmationMonitoringEvent[];
};

function eventScoreDelta(event: ConfirmationMonitoringEvent) {
  if (typeof event.score_before !== "number" || typeof event.score_after !== "number") return null;
  const delta = event.score_after - event.score_before;
  if (delta === 0) return null;
  return `${delta > 0 ? "+" : ""}${delta}`;
}

function compactDate(value: string) {
  const ts = new Date(value);
  if (Number.isNaN(ts.getTime())) return "";
  return ts.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function ConfirmationMonitoringPanel({ watchlistId, initialEvents }: Props) {
  const [events, setEvents] = useState(initialEvents);
  const [status, setStatus] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [clearing, setClearing] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  async function refresh() {
    setPending(true);
    setStatus(null);
    try {
      const result = await refreshWatchlistConfirmationMonitoring(watchlistId);
      const generated = Math.max(result.generated ?? 0, 0);
      const initialized = Math.max(result.initialized ?? 0, 0);
      if (result.items?.length) setEvents(result.items);
      setStatus(
        generated > 0
          ? `${generated} change${generated === 1 ? "" : "s"} found`
          : initialized > 0
            ? "Monitor baseline set"
            : "No material change",
      );
    } catch {
      setStatus("Refresh failed");
    } finally {
      setPending(false);
    }
  }

  async function clearAll() {
    setClearing(true);
    setStatus(null);
    try {
      const result = await clearWatchlistConfirmationEvents(watchlistId);
      setEvents([]);
      setConfirmOpen(false);
      setStatus(`${Math.max(result.cleared ?? 0, 0)} change${result.cleared === 1 ? "" : "s"} cleared`);
    } catch {
      setStatus("Clear failed");
    } finally {
      setClearing(false);
    }
  }

  return (
    <div className="border-y border-white/10 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">Confirmation monitor</h2>
          <p className="text-sm text-slate-400">Material confirmation changes for saved tickers. Auto-refreshes after scheduled ingest.</p>
        </div>
        <div className="flex flex-wrap items-center justify-end gap-2">
          <button
            type="button"
            onClick={refresh}
            disabled={pending || clearing}
            className="inline-flex h-9 items-center justify-center rounded-lg border border-emerald-300/30 bg-emerald-300/10 px-3 text-xs font-semibold text-emerald-100 transition hover:bg-emerald-300/20 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {pending ? "Checking..." : "Refresh monitor"}
          </button>
          <button
            type="button"
            onClick={() => setConfirmOpen(true)}
            disabled={pending || clearing || events.length === 0}
            className="inline-flex h-9 items-center justify-center rounded-lg border border-white/10 bg-white/[0.03] px-3 text-xs font-semibold text-slate-200 transition hover:border-rose-300/35 hover:bg-rose-300/10 hover:text-rose-100 disabled:cursor-not-allowed disabled:opacity-40"
          >
            Clear all
          </button>
          {status ? <span className="text-xs text-slate-400">{status}</span> : null}
        </div>
      </div>

      <div className="mt-4 divide-y divide-white/10">
        {events.length === 0 ? (
          <div className="py-3 text-sm text-slate-400">No confirmation changes recorded yet.</div>
        ) : (
          events.map((event) => {
            const delta = eventScoreDelta(event);
            return (
              <Link
                key={event.id}
                href={`/ticker/${encodeURIComponent(event.ticker)}`}
                prefetch={false}
                className="grid gap-2 py-3 transition hover:bg-white/[0.03] sm:grid-cols-[4.25rem_minmax(0,1fr)_8.75rem] sm:items-center sm:gap-x-2"
              >
                <span className="font-mono text-sm font-semibold text-emerald-200">{event.ticker}</span>
                <span className="min-w-0">
                  <span className="block truncate text-sm font-semibold text-white">{event.title}</span>
                  {event.body ? <span className="block truncate text-xs text-slate-500">{event.body}</span> : null}
                </span>
                <span className="flex min-w-[8.75rem] shrink-0 flex-nowrap items-center gap-1.5 whitespace-nowrap text-xs text-slate-500 sm:justify-end">
                  {delta ? (
                    <span className={`rounded-lg border px-2 py-0.5 font-semibold ${delta.startsWith("+") ? "border-emerald-300/25 text-emerald-100" : "border-rose-300/25 text-rose-100"}`}>
                      {delta}
                    </span>
                  ) : null}
                  <span>{compactDate(event.created_at)}</span>
                </span>
              </Link>
            );
          })
        )}
      </div>

      {confirmOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/70 px-4" role="dialog" aria-modal="true" aria-label="Clear confirmation changes">
          <div className="w-full max-w-md rounded-2xl border border-white/10 bg-slate-900 p-5 text-slate-100 shadow-2xl shadow-black/50">
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-rose-200">Clear changes</p>
            <h3 className="mt-2 text-lg font-semibold text-white">Clear all confirmation changes?</h3>
            <p className="mt-2 text-sm leading-6 text-slate-300">
              This removes the visible confirmation monitor history for this watchlist. Future monitor refreshes can still create new changes.
            </p>
            <div className="mt-5 flex flex-wrap justify-end gap-3">
              <button
                type="button"
                onClick={() => setConfirmOpen(false)}
                disabled={clearing}
                className="inline-flex h-10 items-center justify-center rounded-xl border border-white/10 px-4 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white disabled:opacity-60"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={clearAll}
                disabled={clearing}
                className="inline-flex h-10 items-center justify-center rounded-xl border border-rose-300/40 bg-rose-500/10 px-4 text-sm font-semibold text-rose-100 transition hover:bg-rose-500/20 disabled:opacity-60"
              >
                {clearing ? "Clearing..." : "Clear changes"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
