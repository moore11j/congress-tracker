"use client";

import { useEffect, useMemo, useState } from "react";
import {
  adminDeleteUser,
  adminSetPremium,
  adminSuspendUser,
  adminUpdateFeatureGate,
  adminUpdatePlanLimit,
  adminUpdatePlanPrice,
  getAdminSettings,
  type AccountUser,
  type AdminSettings,
  type FeatureGate,
  type PlanLimit,
  type PlanPrice,
} from "@/lib/api";

function formatDate(value?: string | null) {
  if (!value) return "never";
  return new Date(value).toLocaleString();
}

export function AdminSettingsPanel() {
  const [settings, setSettings] = useState<AdminSettings | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [limitDrafts, setLimitDrafts] = useState<Record<string, string>>({});
  const [priceDrafts, setPriceDrafts] = useState<Record<string, string>>({});

  const users = useMemo(() => settings?.users ?? [], [settings]);
  const gates = useMemo(() => settings?.feature_gates ?? [], [settings]);
  const planLimits = useMemo(() => settings?.plan_config.plan_limits ?? [], [settings]);
  const planPrices = useMemo(() => settings?.plan_config.plan_prices ?? [], [settings]);
  const editableLimits = useMemo(
    () => planLimits.filter((limit) => ["watchlists", "watchlist_tickers"].includes(limit.feature_key)),
    [planLimits],
  );

  const refresh = async () => {
    setBusy(true);
    setStatus(null);
    try {
      setSettings(await getAdminSettings());
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to load admin settings.");
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    const nextLimits: Record<string, string> = {};
    for (const limit of planLimits) {
      nextLimits[limitDraftKey(limit)] = String(limit.limit_value);
    }
    setLimitDrafts(nextLimits);

    const nextPrices: Record<string, string> = {};
    for (const price of planPrices) {
      nextPrices[priceDraftKey(price)] = centsToDollars(price.amount_cents);
    }
    setPriceDrafts(nextPrices);
  }, [planLimits, planPrices]);

  const replaceUser = (next: AccountUser) => {
    setSettings((current) =>
      current ? { ...current, users: current.users.map((user) => (user.id === next.id ? next : user)) } : current,
    );
  };

  const replaceGate = (next: FeatureGate) => {
    setSettings((current) =>
      current
        ? { ...current, feature_gates: current.feature_gates.map((gate) => (gate.feature_key === next.feature_key ? next : gate)) }
        : current,
    );
  };

  const setPremium = async (user: AccountUser, tier: "free" | "premium" | null) => {
    setBusy(true);
    try {
      replaceUser(await adminSetPremium(user.id, tier));
      setStatus(tier ? `${user.email} set to ${tier}.` : `${user.email} manual override cleared.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update user.");
    } finally {
      setBusy(false);
    }
  };

  const suspend = async (user: AccountUser, suspended: boolean) => {
    if (suspended && !window.confirm(`Suspend ${user.email}?`)) return;
    setBusy(true);
    try {
      replaceUser(await adminSuspendUser(user.id, suspended));
      setStatus(suspended ? `${user.email} suspended.` : `${user.email} unsuspended.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update suspension.");
    } finally {
      setBusy(false);
    }
  };

  const deleteUser = async (user: AccountUser) => {
    if (!window.confirm(`Delete ${user.email}? This removes the account record.`)) return;
    setBusy(true);
    try {
      await adminDeleteUser(user.id);
      setSettings((current) => (current ? { ...current, users: current.users.filter((item) => item.id !== user.id) } : current));
      setStatus(`${user.email} deleted.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to delete user.");
    } finally {
      setBusy(false);
    }
  };

  const updateGate = async (gate: FeatureGate, requiredTier: "free" | "premium") => {
    setBusy(true);
    try {
      replaceGate(await adminUpdateFeatureGate(gate.feature_key, requiredTier));
      setStatus(`${gate.feature_key} now requires ${requiredTier}.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update feature gate.");
    } finally {
      setBusy(false);
    }
  };

  const updateLimit = async (limit: PlanLimit) => {
    const raw = limitDrafts[limitDraftKey(limit)] ?? String(limit.limit_value);
    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed < 0) {
      setStatus("Enter a non-negative plan limit.");
      return;
    }
    setBusy(true);
    try {
      await adminUpdatePlanLimit(limit.feature_key, limit.tier, Math.floor(parsed));
      await refresh();
      setStatus(`${limit.label ?? limit.feature_key} ${limit.tier} limit updated.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update plan limit.");
    } finally {
      setBusy(false);
    }
  };

  const updatePrice = async (price: PlanPrice) => {
    const raw = priceDrafts[priceDraftKey(price)] ?? centsToDollars(price.amount_cents);
    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed < 0) {
      setStatus("Enter a non-negative price.");
      return;
    }
    setBusy(true);
    try {
      await adminUpdatePlanPrice(price.tier, price.billing_interval, Math.round(parsed * 100), price.currency);
      await refresh();
      setStatus(`${price.tier} ${price.billing_interval} price updated.`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update plan price.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Admin</p>
            <h1 className="mt-1 text-3xl font-semibold text-white">Settings</h1>
            <p className="mt-2 text-sm text-slate-400">Stripe status, account controls, and backend feature gates.</p>
          </div>
          <button
            type="button"
            onClick={refresh}
            disabled={busy}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Refresh
          </button>
        </div>
        {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h2 className="text-xl font-semibold text-white">Stripe setup</h2>
        {settings?.stripe ? (
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            <StripeRow label="Configured" value={settings.stripe.configured ? "yes" : "no"} />
            <StripeRow label="Secret key" value={settings.stripe.secret_key} />
            <StripeRow label="Price id" value={settings.stripe.price_id} />
            <StripeRow label="Webhook secret" value={settings.stripe.webhook_secret} />
            <StripeRow label="Webhook URL" value={settings.stripe.webhook_url} />
            <StripeRow label="Success URL" value={settings.stripe.success_url} />
          </div>
        ) : (
          <p className="mt-3 text-sm text-slate-400">Sign in as admin to load Stripe setup.</p>
        )}
        <p className="mt-4 text-sm text-slate-400">
          Secrets are not editable here. Set `STRIPE_SECRET_KEY`, `STRIPE_PRICE_ID`, and `STRIPE_WEBHOOK_SECRET` in the deployment environment.
        </p>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-xl font-semibold text-white">Plan configuration</h2>
            <p className="mt-2 text-sm text-slate-400">
              These backend settings drive entitlement limits and the public pricing page.
            </p>
          </div>
          <a href="/pricing" className="rounded-lg border border-white/10 px-3 py-2 text-sm font-semibold text-slate-200">
            View pricing
          </a>
        </div>

        <div className="mt-5 grid gap-4 lg:grid-cols-2">
          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <h3 className="font-semibold text-white">Watchlist limits</h3>
            <div className="mt-4 space-y-3">
              {editableLimits.map((limit) => (
                <div key={limitDraftKey(limit)} className="grid gap-3 md:grid-cols-[1fr_8rem_auto] md:items-end">
                  <label className="text-sm">
                    <span className="block font-medium text-slate-200">
                      {limit.label ?? limit.feature_key} - {limit.tier}
                    </span>
                    <span className="text-xs text-slate-500">{limit.feature_key}</span>
                  </label>
                  <input
                    type="number"
                    min={0}
                    value={limitDrafts[limitDraftKey(limit)] ?? ""}
                    onChange={(event) =>
                      setLimitDrafts((current) => ({ ...current, [limitDraftKey(limit)]: event.target.value }))
                    }
                    className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
                  />
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() => updateLimit(limit)}
                    className="rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100"
                  >
                    Save
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
            <h3 className="font-semibold text-white">Subscription prices</h3>
            <div className="mt-4 space-y-3">
              {planPrices
                .filter((price) => price.tier === "premium")
                .map((price) => (
                  <div key={priceDraftKey(price)} className="grid gap-3 md:grid-cols-[1fr_8rem_auto] md:items-end">
                    <label className="text-sm">
                      <span className="block font-medium text-slate-200">Premium - {price.billing_interval}</span>
                      <span className="text-xs text-slate-500">{price.currency}</span>
                    </label>
                    <input
                      type="number"
                      min={0}
                      step="0.01"
                      value={priceDrafts[priceDraftKey(price)] ?? ""}
                      onChange={(event) =>
                        setPriceDrafts((current) => ({ ...current, [priceDraftKey(price)]: event.target.value }))
                      }
                      className="rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
                    />
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => updatePrice(price)}
                      className="rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100"
                    >
                      Save
                    </button>
                  </div>
                ))}
            </div>
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h2 className="text-xl font-semibold text-white">Registered accounts</h2>
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-full text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="py-2 pr-4">Email</th>
                <th className="py-2 pr-4">Name</th>
                <th className="py-2 pr-4">Registered</th>
                <th className="py-2 pr-4">Last seen</th>
                <th className="py-2 pr-4">Access</th>
                <th className="py-2 pr-4">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-white/10">
              {users.map((user) => (
                <tr key={user.id}>
                  <td className="py-3 pr-4 text-white">{user.email}</td>
                  <td className="py-3 pr-4 text-slate-300">{user.name || "-"}</td>
                  <td className="py-3 pr-4 text-slate-400">{formatDate(user.created_at)}</td>
                  <td className="py-3 pr-4 text-slate-400">{formatDate(user.last_seen_at)}</td>
                  <td className="py-3 pr-4 text-slate-300">
                    {user.is_admin ? "admin" : user.manual_tier_override || user.entitlement_tier || "free"}
                    {user.is_suspended ? " suspended" : ""}
                  </td>
                  <td className="flex flex-wrap gap-2 py-3 pr-4">
                    <button className="rounded-lg border border-white/10 px-2 py-1 text-slate-200" onClick={() => setPremium(user, "premium")}>
                      Premium
                    </button>
                    <button className="rounded-lg border border-white/10 px-2 py-1 text-slate-200" onClick={() => setPremium(user, "free")}>
                      Downgrade
                    </button>
                    <button className="rounded-lg border border-white/10 px-2 py-1 text-slate-200" onClick={() => setPremium(user, null)}>
                      Clear
                    </button>
                    <button className="rounded-lg border border-white/10 px-2 py-1 text-slate-200" onClick={() => suspend(user, !user.is_suspended)}>
                      {user.is_suspended ? "Unsuspend" : "Suspend"}
                    </button>
                    <button className="rounded-lg border border-rose-300/30 px-2 py-1 text-rose-200" onClick={() => deleteUser(user)}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <h2 className="text-xl font-semibold text-white">Feature gates</h2>
        <div className="mt-4 grid gap-3">
          {gates.map((gate) => (
            <div key={gate.feature_key} className="grid gap-3 rounded-lg border border-white/10 bg-slate-950/40 p-4 md:grid-cols-[1fr_auto] md:items-center">
              <div>
                <div className="font-semibold text-white">{gate.feature_key}</div>
                <p className="text-sm text-slate-400">{gate.description}</p>
              </div>
              <div className="flex gap-2">
                <button
                  type="button"
                  onClick={() => updateGate(gate, "free")}
                  className={`rounded-lg border px-3 py-2 text-sm font-semibold ${
                    gate.required_tier === "free" ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-200"
                  }`}
                >
                  Free
                </button>
                <button
                  type="button"
                  onClick={() => updateGate(gate, "premium")}
                  className={`rounded-lg border px-3 py-2 text-sm font-semibold ${
                    gate.required_tier === "premium" ? "border-emerald-300/40 bg-emerald-300/10 text-emerald-100" : "border-white/10 text-slate-200"
                  }`}
                >
                  Premium
                </button>
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function StripeRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-white/10 bg-slate-950/40 p-3">
      <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-1 break-all text-sm text-slate-200">{value}</div>
    </div>
  );
}

function limitDraftKey(limit: PlanLimit) {
  return `${limit.feature_key}:${limit.tier}`;
}

function priceDraftKey(price: PlanPrice) {
  return `${price.tier}:${price.billing_interval}`;
}

function centsToDollars(cents: number) {
  return (cents / 100).toFixed(2);
}
