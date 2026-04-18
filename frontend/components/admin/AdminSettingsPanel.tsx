"use client";

import { useEffect, useMemo, useState } from "react";
import {
  adminDeleteUser,
  adminSetPremium,
  adminSuspendUser,
  adminUpdateFeatureGate,
  adminUpdateOAuthSettings,
  adminUpdatePlanLimit,
  adminUpdatePlanPrice,
  adminUpdateStripeTaxSettings,
  getAdminSettings,
  type AccountUser,
  type AdminSettings,
  type FeatureGate,
  type PlanLimit,
  type PlanPrice,
  type StripeTaxSettingsPayload,
} from "@/lib/api";

type AdminTab = "settings" | "reports" | "users";

const ADMIN_TABS: Array<{ key: AdminTab; label: string; description: string }> = [
  {
    key: "settings",
    label: "Settings",
    description: "Stripe setup, Stripe Tax readiness, OAuth setup, plan configuration, and feature gates.",
  },
  {
    key: "reports",
    label: "Reports",
    description: "Sales Ledger and admin exports will live here.",
  },
  {
    key: "users",
    label: "Users",
    description: "Registered accounts and access controls.",
  },
];

function formatDate(value?: string | null) {
  if (!value) return "never";
  return new Date(value).toLocaleString();
}

export function AdminSettingsPanel() {
  const [activeTab, setActiveTab] = useState<AdminTab>("settings");
  const [settings, setSettings] = useState<AdminSettings | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [limitDrafts, setLimitDrafts] = useState<Record<string, string>>({});
  const [priceDrafts, setPriceDrafts] = useState<Record<string, string>>({});
  const [googleClientIdDraft, setGoogleClientIdDraft] = useState("");
  const [stripeTaxDraft, setStripeTaxDraft] = useState<StripeTaxSettingsPayload>({
    automatic_tax_enabled: false,
    require_billing_address: true,
    product_tax_code: "",
    price_tax_behavior: "unspecified",
  });

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
      setStatus(error instanceof Error ? error.message : "Unable to load admin panel.");
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

  useEffect(() => {
    setGoogleClientIdDraft(settings?.oauth?.google_client_id ?? "");
  }, [settings?.oauth?.google_client_id]);

  useEffect(() => {
    if (!settings?.stripe_tax) return;
    setStripeTaxDraft({
      automatic_tax_enabled: settings.stripe_tax.automatic_tax_enabled,
      require_billing_address: settings.stripe_tax.require_billing_address,
      product_tax_code: settings.stripe_tax.product_tax_code ?? "",
      price_tax_behavior: settings.stripe_tax.price_tax_behavior,
    });
  }, [settings?.stripe_tax]);

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

  const updateOAuthSettings = async () => {
    setBusy(true);
    setStatus(null);
    try {
      const next = await adminUpdateOAuthSettings(googleClientIdDraft);
      setSettings((current) => (current ? { ...current, oauth: next } : current));
      setStatus("Google Client ID updated.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update Google Client ID.");
    } finally {
      setBusy(false);
    }
  };

  const updateStripeTaxSettings = async () => {
    setBusy(true);
    setStatus(null);
    try {
      const next = await adminUpdateStripeTaxSettings({
        ...stripeTaxDraft,
        product_tax_code: stripeTaxDraft.product_tax_code?.trim() || null,
      });
      setSettings((current) => (current ? { ...current, stripe_tax: next } : current));
      setStatus("Stripe Tax readiness settings updated.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to update Stripe Tax settings.");
    } finally {
      setBusy(false);
    }
  };

  const activeTabConfig = ADMIN_TABS.find((tab) => tab.key === activeTab) ?? ADMIN_TABS[0];

  return (
    <div className="flex flex-col gap-6">
      <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Admin panel</p>
            <h2 className="mt-1 text-2xl font-semibold text-white">{activeTabConfig.label}</h2>
            <p className="mt-2 text-sm text-slate-400">{activeTabConfig.description}</p>
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

        <div className="mt-5 inline-flex flex-wrap gap-1 rounded-lg border border-white/10 bg-slate-950/40 p-1">
          {ADMIN_TABS.map((tab) => (
            <button
              key={tab.key}
              type="button"
              onClick={() => setActiveTab(tab.key)}
              className={`rounded-md px-4 py-2 text-sm font-semibold transition ${
                activeTab === tab.key
                  ? "border border-emerald-300/40 bg-emerald-300/10 text-emerald-100"
                  : "border border-transparent text-slate-300 hover:border-white/10 hover:text-white"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}
      </section>

      {activeTab === "settings" ? (
        <>
          <section className="order-[1] rounded-lg border border-white/10 bg-slate-900/70 p-5">
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

          <section className="order-[2] rounded-lg border border-white/10 bg-slate-900/70 p-5">
            <h2 className="text-xl font-semibold text-white">OAuth setup</h2>
            <p className="mt-2 text-sm text-slate-400">Google sign-in uses this Client ID with the Google Client Secret from the deployment environment.</p>
            <div className="mt-4 grid gap-3 md:grid-cols-[1fr_auto] md:items-end">
              <label className="text-sm">
                <span className="block font-medium text-slate-200">Google Client ID</span>
                <input
                  value={googleClientIdDraft}
                  onChange={(event) => setGoogleClientIdDraft(event.target.value)}
                  placeholder="Google OAuth client ID"
                  className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                />
              </label>
              <button
                type="button"
                disabled={busy}
                onClick={updateOAuthSettings}
                className="rounded-lg border border-emerald-300/30 px-3 py-2 text-sm font-semibold text-emerald-100"
              >
                Save
              </button>
            </div>
          </section>

          <section className="order-[3] rounded-lg border border-white/10 bg-slate-900/70 p-5">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <h2 className="text-xl font-semibold text-white">Stripe Tax / Billing readiness</h2>
                <p className="mt-2 text-sm text-slate-400">
                  Stripe Tax will calculate tax from customer location and your Stripe registrations. These app settings prepare billing integration only.
                </p>
              </div>
              {settings?.stripe_tax ? (
                <span
                  className={`rounded-md border px-3 py-2 text-sm font-semibold ${
                    settings.stripe_tax.configured
                      ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
                      : "border-white/10 bg-slate-950/50 text-slate-300"
                  }`}
                >
                  {settings.stripe_tax.configured ? "Ready in app" : "Not ready"}
                </span>
              ) : null}
            </div>

            {settings?.stripe_tax ? (
              <>
                <p className="mt-4 rounded-lg border border-white/10 bg-slate-950/40 p-3 text-sm text-slate-400">
                  {settings.stripe_tax.notes}
                </p>

                <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <StripeRow label="Stripe Tax status" value={settings.stripe_tax.stripe_tax_status.replaceAll("_", " ")} />
                  <StripeRow label="Automatic tax target" value={settings.stripe_tax.automatic_tax_enabled ? "enabled for future billing flows" : "disabled"} />
                  <StripeRow label="Stripe price id" value={settings.stripe_tax.price_id} />
                  <StripeRow label="Price tax behavior" value={settings.stripe_tax.price_tax_behavior} />
                  <StripeRow label="Product tax code" value={settings.stripe_tax.product_tax_code || "not set"} />
                  <StripeRow label="Customer location" value={settings.stripe_tax.require_billing_address ? "required before checkout" : "not required by app flag"} />
                  <StripeRow label="Business/support info" value={settings.stripe_tax.business_support.configured ? "present" : "not detected"} />
                  <StripeRow label="Stripe dashboard" value="registrations and tax settings live in Stripe" />
                </div>

                <div className="mt-5 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                  <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                    <h3 className="font-semibold text-white">App billing flags</h3>
                    <div className="mt-4 space-y-4">
                      <label className="flex items-start gap-3 text-sm text-slate-300">
                        <input
                          type="checkbox"
                          checked={stripeTaxDraft.automatic_tax_enabled}
                          onChange={(event) =>
                            setStripeTaxDraft((current) => ({ ...current, automatic_tax_enabled: event.target.checked }))
                          }
                          className="mt-1 h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                        />
                        <span>
                          <span className="block font-medium text-slate-200">Use Stripe automatic tax in future billing flows</span>
                          <span className="text-xs text-slate-500">This does not change checkout yet.</span>
                        </span>
                      </label>

                      <label className="flex items-start gap-3 text-sm text-slate-300">
                        <input
                          type="checkbox"
                          checked={stripeTaxDraft.require_billing_address}
                          onChange={(event) =>
                            setStripeTaxDraft((current) => ({ ...current, require_billing_address: event.target.checked }))
                          }
                          className="mt-1 h-4 w-4 rounded border-white/10 bg-slate-950 accent-emerald-300"
                        />
                        <span>
                          <span className="block font-medium text-slate-200">Require customer billing location before checkout</span>
                          <span className="text-xs text-slate-500">Future billing can use this to prompt before creating a Stripe session.</span>
                        </span>
                      </label>

                      <label className="block text-sm">
                        <span className="block font-medium text-slate-200">Product tax code</span>
                        <input
                          value={stripeTaxDraft.product_tax_code ?? ""}
                          onChange={(event) => setStripeTaxDraft((current) => ({ ...current, product_tax_code: event.target.value }))}
                          placeholder="Optional, for example txcd_10000000"
                          className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none placeholder:text-slate-500 focus:border-emerald-300/50"
                        />
                      </label>

                      <label className="block text-sm">
                        <span className="block font-medium text-slate-200">Default price tax behavior</span>
                        <select
                          value={stripeTaxDraft.price_tax_behavior}
                          onChange={(event) =>
                            setStripeTaxDraft((current) => ({
                              ...current,
                              price_tax_behavior: event.target.value as StripeTaxSettingsPayload["price_tax_behavior"],
                            }))
                          }
                          className="mt-1 w-full rounded-lg border border-white/10 bg-slate-950 px-3 py-2 text-sm text-white outline-none focus:border-emerald-300/50"
                        >
                          <option value="unspecified">Unspecified</option>
                          <option value="exclusive">Exclusive</option>
                          <option value="inclusive">Inclusive</option>
                        </select>
                      </label>

                      <button
                        type="button"
                        disabled={busy}
                        onClick={updateStripeTaxSettings}
                        className="rounded-lg border border-emerald-300/30 px-4 py-2 text-sm font-semibold text-emerald-100"
                      >
                        Save Stripe Tax settings
                      </button>
                    </div>
                  </div>

                  <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
                    <h3 className="font-semibold text-white">Readiness checks</h3>
                    <div className="mt-4 space-y-3">
                      {settings.stripe_tax.checks.map((check) => (
                        <div key={check.key} className="rounded-lg border border-white/10 bg-slate-900/60 p-3">
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <span className="font-medium text-slate-100">{check.label}</span>
                            <span
                              className={`rounded-md border px-2 py-1 text-xs font-semibold ${
                                check.status === "ready"
                                  ? "border-emerald-300/30 bg-emerald-300/10 text-emerald-100"
                                  : check.required
                                    ? "border-rose-300/30 bg-rose-300/10 text-rose-100"
                                    : "border-white/10 bg-slate-950/50 text-slate-300"
                              }`}
                            >
                              {check.status}
                            </span>
                          </div>
                          <p className="mt-2 text-sm text-slate-400">{check.detail}</p>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </>
            ) : (
              <p className="mt-3 text-sm text-slate-400">Sign in as admin to load Stripe Tax readiness.</p>
            )}
          </section>

          <section className="order-[4] rounded-lg border border-white/10 bg-slate-900/70 p-5">
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

          <section className="order-[5] rounded-lg border border-white/10 bg-slate-900/70 p-5">
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
        </>
      ) : null}

      {activeTab === "reports" ? (
        <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Reports</p>
          <h2 className="mt-1 text-xl font-semibold text-white">Sales Ledger and admin exports</h2>
          <p className="mt-2 max-w-2xl text-sm text-slate-400">
            This area will host the Sales Ledger, reconciliation views, and admin export workflows for finance operations.
          </p>
          <div className="mt-5 grid gap-3 md:grid-cols-2">
            <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Coming soon</div>
              <h3 className="mt-2 font-semibold text-white">Sales Ledger</h3>
              <p className="mt-2 text-sm text-slate-400">Revenue, subscription events, and ledger-ready activity will be organized here.</p>
            </div>
            <div className="rounded-lg border border-white/10 bg-slate-950/40 p-4">
              <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">Coming soon</div>
              <h3 className="mt-2 font-semibold text-white">Admin exports</h3>
              <p className="mt-2 text-sm text-slate-400">Operational exports for billing and account review will be available from this panel.</p>
            </div>
          </div>
        </section>
      ) : null}

      {activeTab === "users" ? (
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
      ) : null}
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
