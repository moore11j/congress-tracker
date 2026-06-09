"use client";

import { useCallback, useEffect, useState } from "react";
import { ApiError, createCustomerPortalSession, deleteAccount, getAccountBillingHistory, getMe, refreshBillingSubscription, type AccountUser, type BillingHistoryItem } from "@/lib/api";
import { accountPlanSummary, formatInteger } from "@/lib/accountDisplay";
import {
  defaultEntitlements,
  type Entitlements,
} from "@/lib/entitlements";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { WalnutConfirmDialog } from "@/components/ui/WalnutConfirmDialog";

function BillingAccountSkeleton() {
  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5" aria-busy="true" aria-live="polite">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <SkeletonBlock className="h-3 w-20" />
          <SkeletonBlock className="mt-3 h-8 w-44" />
          <SkeletonBlock className="mt-3 h-4 w-full max-w-2xl" />
        </div>
        <SkeletonBlock className="h-10 w-28" />
      </div>
      <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <SkeletonBlock className="h-3 w-24" />
            <SkeletonBlock className="mt-3 h-7 w-16" />
          </div>
        ))}
      </div>
      <div className="mt-8 border-t border-white/10 pt-5">
        <SkeletonBlock className="h-3 w-32" />
        <SkeletonBlock className="mt-3 h-6 w-48" />
        <SkeletonBlock className="mt-3 h-4 w-full max-w-xl" />
      </div>
    </section>
  );
}

export function BillingAccountPanel() {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [authLoading, setAuthLoading] = useState(true);
  const [entitlementLoading, setEntitlementLoading] = useState(true);
  const [accountStatus, setAccountStatus] = useState<string | null>(null);
  const [history, setHistory] = useState<BillingHistoryItem[]>([]);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [historyStatus, setHistoryStatus] = useState<string | null>(null);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [deleteConfirmation, setDeleteConfirmation] = useState("");
  const [deleteStatus, setDeleteStatus] = useState<string | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [returnSyncStatus, setReturnSyncStatus] = useState<"syncing" | "synced" | "delayed" | null>(null);
  const [portalStatus, setPortalStatus] = useState<string | null>(null);

  const loadBillingHistory = useCallback(async () => {
    setHistoryLoading(true);
    setHistoryStatus(null);
    try {
      const response = await getAccountBillingHistory();
      setHistory(response.items);
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      setHistoryStatus(message.includes("HTTP 401") ? "Sign in to view billing documents." : "Billing history is unavailable.");
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        let response = await getMe({ force: true, source: "Billing" });
        if (response.user) {
          try {
            await refreshBillingSubscription();
            response = await getMe({ force: true, source: "BillingRefresh" });
          } catch {
            // Account loading should not fail just because Stripe is briefly unavailable.
          }
        }
        if (cancelled) return;
        setUser(response.user);
        setEntitlements(response.entitlements);
      } catch (error) {
        if (cancelled) return;
        setUser(null);
        setEntitlements(defaultEntitlements);
        setAccountStatus(error instanceof ApiError && error.status === 401 ? "Sign in to view account plan and entitlement limits." : "Account details are temporarily unavailable.");
      } finally {
        if (cancelled) return;
        setAuthLoading(false);
        setEntitlementLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined" || !/[?&](checkout=success|portal_return=1)\b/.test(window.location.search)) return;
    let cancelled = false;
    const fromCheckout = /[?&]checkout=success\b/.test(window.location.search);
    const paidTier = (responseUser: AccountUser | null, responseTier?: string | null) => {
      const tier = (responseTier || responseUser?.entitlement_tier || responseUser?.subscription_plan || "").toLowerCase();
      return tier === "premium" || tier === "pro";
    };
    const refresh = async () => {
      setReturnSyncStatus("syncing");
      const deadline = Date.now() + 30000;
      while (!cancelled && Date.now() <= deadline) {
        let refreshedFromStripe = false;
        try {
          const refreshResponse = await refreshBillingSubscription();
          refreshedFromStripe = refreshResponse.status === "refreshed";
        } catch {
          // Webhooks may still win the race; polling /me below covers that path.
        }
        try {
          const response = await getMe({ force: true, source: "BillingReturnPoll" });
          if (cancelled) return;
          setUser(response.user);
          setEntitlements(response.entitlements);
          await loadBillingHistory();
          if ((fromCheckout && paidTier(response.user, response.entitlements.tier)) || (!fromCheckout && refreshedFromStripe)) {
            setReturnSyncStatus("synced");
            return;
          }
        } catch {
          // Keep polling briefly; checkout returns can arrive before webhooks finish.
        }
        await new Promise((resolve) => window.setTimeout(resolve, 3000));
      }
      if (!cancelled) setReturnSyncStatus("delayed");
    };
    void refresh();
    return () => {
      cancelled = true;
    };
  }, [loadBillingHistory]);

  useEffect(() => {
    loadBillingHistory().catch(() => undefined);
  }, [loadBillingHistory]);

  const plan = accountPlanSummary(user, entitlements);
  const paidThrough = paidAccessThrough(user);
  const paidThroughLabel = formatDate(paidThrough);
  const hasPaidAccess = Boolean(paidThrough);
  const nonRenewing = isNonRenewingPaid(user);
  const checkoutSyncPending = returnSyncStatus === "syncing" && plan.label === "Free";
  const checkoutSyncDelayed = returnSyncStatus === "delayed" && plan.label === "Free";
  const displayedPlan = checkoutSyncPending || checkoutSyncDelayed
    ? {
        ...plan,
        label: returnSyncStatus === "delayed" ? "Still syncing" : "Payment received",
        description:
          returnSyncStatus === "delayed"
            ? "Still syncing. Refresh in a moment or contact support."
            : "Payment received. Updating your plan...",
      }
    : plan;

  const runDeleteAccount = async () => {
    if (deleteConfirmation !== "DELETE") return;
    setDeleteBusy(true);
    setDeleteStatus(null);
    try {
      await deleteAccount(deleteConfirmation);
      window.location.href = "/login?account_deleted=1";
    } catch (error) {
      setDeleteStatus(error instanceof Error ? error.message : "Unable to delete account.");
      setDeleteBusy(false);
    }
  };

  const openBillingPortal = async () => {
    setPortalStatus(null);
    try {
      const session = await createCustomerPortalSession();
      if (session.url) {
        window.location.href = session.url;
        return;
      }
      setPortalStatus("Stripe did not return a billing portal URL.");
    } catch (error) {
      setPortalStatus(error instanceof Error ? error.message : "Unable to open billing portal.");
    }
  };

  if (authLoading || entitlementLoading) {
    return <BillingAccountSkeleton />;
  }

  if (!user) {
    const signInRequired = accountStatus?.toLowerCase().startsWith("sign in");
    return (
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Account</p>
          <h1 className="mt-1 text-3xl font-semibold text-white">{signInRequired ? "Sign in required" : "Account unavailable"}</h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-300">
            {accountStatus ?? "Sign in to view account plan and entitlement limits."}
          </p>
        </div>
      </section>
    );
  }

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Account</p>
          <h1 className="mt-1 text-3xl font-semibold text-white">
            {displayedPlan.label}
          </h1>
          <p className="mt-2 max-w-2xl text-sm text-slate-300">
            {displayedPlan.description}
          </p>
          {returnSyncStatus === "synced" ? (
            <p className="mt-2 text-sm font-medium text-emerald-200">Plan updated.</p>
          ) : null}
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

      {nonRenewing ? (
        <div className="mt-5 rounded-lg border border-amber-300/30 bg-amber-300/10 p-4 text-sm text-amber-50">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p>
              Your subscription is set to end on {formatDate(user.access_expires_at)}. You will keep {displayPlanName(user)} access until then. Renew in billing to keep access.
            </p>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={openBillingPortal}
                className="inline-flex items-center justify-center rounded-lg border border-amber-200/40 bg-amber-200/10 px-3 py-2 text-sm font-semibold text-amber-50 transition hover:bg-amber-200/15"
              >
                Manage billing
              </button>
              <a
                href="/pricing"
                className="inline-flex items-center justify-center rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/20 hover:text-white"
              >
                View plans
              </a>
            </div>
          </div>
          {portalStatus ? <p className="mt-2 text-sm text-amber-100">{portalStatus}</p> : null}
        </div>
      ) : null}

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

      <div className="mt-8 border-t border-white/10 pt-5">
        <p className="text-xs font-semibold uppercase tracking-wide text-rose-300">Danger zone</p>
        <h2 className="mt-1 text-xl font-semibold text-white">Delete account</h2>
        <p className="mt-1 max-w-2xl text-sm text-slate-400">
          Deactivate your Walnut account and mark it as deleted. Admins can still see the deleted record for audit and support.
        </p>
        <button
          type="button"
          onClick={() => {
            setDeleteDialogOpen(true);
            setDeleteConfirmation("");
            setDeleteStatus(null);
          }}
          className="mt-4 inline-flex items-center justify-center rounded-lg border border-rose-300/30 bg-rose-500/10 px-4 py-2 text-sm font-semibold text-rose-100 transition hover:bg-rose-500/15"
        >
          Delete account
        </button>
      </div>

      <WalnutConfirmDialog
        open={deleteDialogOpen}
        eyebrow="Delete account"
        title="Delete your account?"
        description={
          <div className="space-y-3">
            <p>
              You're about to delete your Walnut account. Your account access will be disabled and your account will be marked as deleted.
            </p>
            {hasPaidAccess ? (
              <p>
                Deleting your account does not issue a refund. Walnut will set your subscription not to renew, and your paid billing period remains active until {paidThroughLabel}. You can reactivate your account before that date to restore access.
              </p>
            ) : null}
            <p>To confirm, type DELETE below.</p>
          </div>
        }
        confirmLabel={deleteBusy ? "Deleting..." : "Delete account"}
        tone="danger"
        isBusy={deleteBusy}
        confirmDisabled={deleteConfirmation !== "DELETE"}
        onClose={() => {
          if (deleteBusy) return;
          setDeleteDialogOpen(false);
          setDeleteConfirmation("");
          setDeleteStatus(null);
        }}
        onConfirm={runDeleteAccount}
      >
        <label className="block text-sm font-medium text-slate-200">
          Confirmation
          <input
            value={deleteConfirmation}
            onChange={(event) => setDeleteConfirmation(event.target.value)}
            className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-rose-300/50"
            autoComplete="off"
          />
        </label>
        {deleteStatus ? <p className="mt-3 text-sm text-rose-300">{deleteStatus}</p> : null}
      </WalnutConfirmDialog>
    </section>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{formatInteger(value)}</div>
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

function paidAccessThrough(user: AccountUser | null) {
  if (!user?.access_expires_at) return null;
  const date = new Date(user.access_expires_at);
  if (Number.isNaN(date.getTime()) || date <= new Date()) return null;
  const status = (user.subscription_status || "").toLowerCase();
  const tier = (user.subscription_plan || user.entitlement_tier || "").toLowerCase();
  if (["active", "trialing"].includes(status) || tier === "premium" || tier === "pro") return user.access_expires_at;
  return null;
}

function isNonRenewingPaid(user: AccountUser | null) {
  if (!user?.subscription_cancel_at_period_end || !user.access_expires_at) return false;
  const date = new Date(user.access_expires_at);
  if (Number.isNaN(date.getTime()) || date <= new Date()) return false;
  const status = (user.subscription_status || "").toLowerCase();
  const tier = (user.subscription_plan || user.entitlement_tier || "").toLowerCase();
  return ["active", "trialing"].includes(status) && (tier === "premium" || tier === "pro");
}

function displayPlanName(user: AccountUser | null) {
  const plan = (user?.subscription_plan || user?.entitlement_tier || "paid").toLowerCase();
  if (plan === "pro") return "Pro";
  if (plan === "premium") return "Premium";
  return "paid";
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
