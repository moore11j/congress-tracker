"use client";

import { useEffect, useState } from "react";
import { getAccountBillingHistory, getEntitlements, type BillingHistoryItem } from "@/lib/api";
import {
  defaultEntitlements,
  type Entitlements,
} from "@/lib/entitlements";

export function BillingAccountPanel() {
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [history, setHistory] = useState<BillingHistoryItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [historyStatus, setHistoryStatus] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getEntitlements()
      .then((next) => {
        if (!cancelled) setEntitlements(next);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setHistoryLoading(true);
    setHistoryStatus(null);
    getAccountBillingHistory()
      .then((response) => {
        if (cancelled) return;
        setHistory(response.items);
      })
      .catch((error) => {
        if (cancelled) return;
        const message = error instanceof Error ? error.message : "";
        setHistoryStatus(message.includes("HTTP 401") ? "Sign in to view billing documents." : "Billing history is unavailable.");
      })
      .finally(() => {
        if (!cancelled) setHistoryLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Account</p>
          <h1 className="mt-1 text-3xl font-semibold text-white">
            {entitlements.tier === "premium" ? "Premium" : "Free"}
          </h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-300">
            Free stays useful for research. Premium raises workflow limits and unlocks alert-first digests.
          </p>
        </div>
        <a
          href="/pricing"
          className="inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
        >
          Compare plans
        </a>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <Metric label="Watchlists" value={entitlements.limits.watchlists} />
        <Metric label="Tickers per list" value={entitlements.limits.watchlist_tickers} />
        <Metric label="Saved views" value={entitlements.limits.saved_views} />
        <Metric label="Inbox sources" value={entitlements.limits.monitoring_sources} />
      </div>

      <div className="mt-8 border-t border-white/10 pt-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Billing history</p>
            <h2 className="mt-1 text-xl font-semibold text-white">Invoices and receipts</h2>
            <p className="mt-1 max-w-2xl text-sm text-slate-400">
              Stripe-hosted documents stay attached to each paid transaction.
            </p>
          </div>
        </div>

        {historyStatus ? <p className="mt-4 text-sm text-slate-400">{historyStatus}</p> : null}
        {historyLoading ? <p className="mt-4 text-sm text-slate-400">Loading billing history.</p> : null}

        {!historyLoading && !historyStatus && history.length === 0 ? (
          <p className="mt-4 rounded-lg border border-white/10 bg-slate-950/40 p-4 text-sm text-slate-400">
            No paid billing history is attached to this account yet.
          </p>
        ) : null}

        {history.length > 0 ? (
          <div className="mt-4 grid gap-3">
            {history.map((item) => (
              <BillingHistoryCard key={item.id} item={item} />
            ))}
          </div>
        ) : null}
      </div>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{value}</div>
    </div>
  );
}

function BillingHistoryCard({ item }: { item: BillingHistoryItem }) {
  const documents = item.documents;
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-semibold text-white">{item.description}</h3>
            <span className="rounded border border-white/10 px-2 py-0.5 text-xs font-semibold uppercase text-slate-400">
              {compactStatus(item.status_refund_state)}
            </span>
          </div>
          <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-sm text-slate-400">
            <span>{formatDate(item.date_charged)}</span>
            <span>{item.total_display}</span>
            <span>{formatServicePeriod(item)}</span>
            <span className="font-mono text-xs">{documents.invoice_number || item.transaction_id}</span>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          {documents.hosted_invoice_url ? (
            <DocumentAction href={documents.hosted_invoice_url} label="View invoice" primary />
          ) : null}
          {documents.invoice_pdf_url ? (
            <DocumentAction href={documents.invoice_pdf_url} label="Download invoice PDF" />
          ) : null}
          {documents.receipt_url ? (
            <DocumentAction href={documents.receipt_url} label="View receipt" />
          ) : null}
        </div>
      </div>

      {!documents.has_stripe_document ? (
        <p className="mt-3 text-sm text-slate-500">
          {documents.fallback_message || "Stripe has not provided a hosted document for this transaction yet."}
        </p>
      ) : null}
    </div>
  );
}

function DocumentAction({ href, label, primary = false }: { href: string; label: string; primary?: boolean }) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noreferrer"
      className={
        primary
          ? "inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/15"
          : "inline-flex items-center justify-center rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white"
      }
    >
      {label}
    </a>
  );
}

function formatDate(value?: string | null) {
  if (!value) return "Date unavailable";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Date unavailable";
  return date.toLocaleDateString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatServicePeriod(item: BillingHistoryItem) {
  if (!item.service_period_start || !item.service_period_end) {
    return item.billing_period_type ? compactStatus(item.billing_period_type) : "Service period unavailable";
  }
  return `${formatDate(item.service_period_start)} to ${formatDate(item.service_period_end)}`;
}

function compactStatus(value?: string | null) {
  return (value || "unknown").replaceAll("_", " ");
}
