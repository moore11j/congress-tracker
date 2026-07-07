"use client";

import { useEffect, useMemo, useState } from "react";
import { createCustomerPortalSession, getMe, getPlanConfig, refreshBillingSubscription, type AccountUser, type PlanConfig, type PlanConfigFeature, type PlanConfigTier, type PlanPrice } from "@/lib/api";
import { PricingActions } from "@/components/billing/PricingActions";
import type { Entitlements } from "@/lib/entitlements";

type BillingInterval = "monthly" | "annual";
type PlanTier = "free" | "premium" | "pro";

const planOrder: PlanTier[] = ["free", "premium", "pro"];
const tierRank: Record<PlanTier, number> = { free: 0, premium: 10, pro: 20 };

const headlineLimitKeys = [
  "screener_results",
  "screener_saved_screens",
  "watchlists",
  "watchlist_tickers",
  "saved_views",
  "monitoring_sources",
] as const;

const categoryOrder = ["Market feeds", "Screener & signals", "Watchlists & monitoring", "Data export & workflow", "Advanced / Coming Soon"] as const;

const featureOrderByCategory: Record<string, Record<string, number>> = {
  "Market feeds": {
    congress_feed: 10,
    insider_feed: 20,
    government_contracts_feed: 30,
    government_contracts_filters: 40,
    premium_feed_metrics: 50,
  },
  "Screener & signals": {
    screener: 10,
    screener_results: 20,
    screener_intelligence: 30,
    screener_presets: 40,
    signals: 50,
    ticker_confirmation: 60,
    leaderboards: 70,
    options_flow_filters: 80,
  },
  "Watchlists & monitoring": {
    inbox_alerts: 10,
    inbox_alert_retention: 20,
    monitoring_sources: 30,
    watchlists: 40,
    watchlist_tickers: 50,
    saved_views: 60,
    screener_saved_screens: 70,
    screener_monitoring: 80,
    notification_digests: 90,
  },
  "Advanced / Coming Soon": {
    institutional_feed: 10,
    institutional_filters: 20,
    options_flow_feed: 30,
    api_webhooks: 40,
  },
};

function categoryFor(featureKey: string) {
  if (["congress_feed", "insider_feed", "government_contracts_feed", "government_contracts_filters", "premium_feed_metrics"].includes(featureKey)) return "Market feeds";
  if (["screener", "screener_intelligence", "screener_presets", "screener_results", "signals", "ticker_confirmation", "leaderboards", "options_flow_filters"].includes(featureKey)) return "Screener & signals";
  if (["watchlists", "watchlist_tickers", "screener_saved_screens", "screener_monitoring", "monitoring_sources", "inbox_alerts", "inbox_alert_retention", "notification_digests", "saved_views"].includes(featureKey)) return "Watchlists & monitoring";
  if (["screener_csv_export", "backtesting"].includes(featureKey)) return "Data export & workflow";
  return "Advanced / Coming Soon";
}

function sortFeaturesForCategory(category: string, features: PlanConfigFeature[]) {
  const order = featureOrderByCategory[category] ?? {};
  return [...features].sort((a, b) => {
    const aOrder = order[a.feature_key] ?? 999;
    const bOrder = order[b.feature_key] ?? 999;
    if (aOrder !== bOrder) return aOrder - bOrder;
    if (a.sort_order !== b.sort_order) return a.sort_order - b.sort_order;
    return a.feature_key.localeCompare(b.feature_key);
  });
}

function priceFor(config: PlanConfig, tier: PlanTier, interval: BillingInterval): PlanPrice | undefined {
  return config.plan_prices.find((price) => price.tier === tier && price.billing_interval === interval);
}

function tierFor(config: PlanConfig, tier: PlanTier): PlanConfigTier | undefined {
  return config.tiers.find((item) => item.tier === tier);
}

function formatMoney(price?: PlanPrice) {
  const amount = (price?.amount_cents ?? 0) / 100;
  const currency = price?.currency || "USD";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    minimumFractionDigits: amount % 1 === 0 ? 0 : 2,
    maximumFractionDigits: 2,
  }).format(amount);
}

function annualSavingsLabel(monthly?: PlanPrice, annual?: PlanPrice) {
  const monthlyYear = (monthly?.amount_cents ?? 0) * 12;
  const annualAmount = annual?.amount_cents ?? 0;
  const monthlyAmount = monthly?.amount_cents ?? 0;
  if (monthlyYear <= 0 || monthlyAmount <= 0 || annualAmount <= 0 || annualAmount >= monthlyYear) return null;
  const monthsFree = Math.ceil(((monthlyYear - annualAmount) / monthlyAmount) * 2) / 2;
  const formattedMonths = Number.isInteger(monthsFree) ? monthsFree.toFixed(0) : monthsFree.toFixed(1);
  return `${formattedMonths} ${monthsFree === 1 ? "month" : "months"} free`;
}

function formatLimit(feature: PlanConfigFeature | undefined, value: number) {
  if (!feature) return value.toLocaleString();
  const unit = value === 1 ? feature.unit_singular : feature.unit_plural;
  return unit ? `${value.toLocaleString()} ${unit}` : value.toLocaleString();
}

function publicFeatureCopy(value?: string | null) {
  return (value ?? "")
    .replaceAll("Capitol Ledger", "Walnut")
    .replaceAll("Congress Tracker", "Walnut Market Terminal")
    .replace(/Smart money signal/gi, "Signal conviction");
}

function featureIncluded(feature: PlanConfigFeature, tier: PlanTier) {
  return tierRank[tier] >= tierRank[feature.required_tier as PlanTier];
}

function featureCell(feature: PlanConfigFeature, tier: PlanTier) {
  if (feature.kind === "limit") return formatLimit(feature, feature.limits[tier] ?? 0);
  if (featureIncluded(feature, tier)) {
    if (["options_flow_feed", "api_webhooks"].includes(feature.feature_key)) return "Coming soon";
    return (
      <span aria-label="Included" title="Included" className="inline-flex text-lg font-semibold leading-none text-emerald-300">
        ✓
      </span>
    );
  }
  return "-";
}

function limitFeature(config: PlanConfig, featureKey: string) {
  return config.features.find((feature) => feature.feature_key === featureKey);
}

export function PricingPlanner({ config }: { config: PlanConfig }) {
  const [activeConfig, setActiveConfig] = useState(config);
  const [billingInterval, setBillingInterval] = useState<BillingInterval>("monthly");
  const [accountUser, setAccountUser] = useState<AccountUser | null>(null);
  const [accountEntitlements, setAccountEntitlements] = useState<Entitlements | null>(null);
  const [accountLoading, setAccountLoading] = useState(true);
  const [portalStatus, setPortalStatus] = useState<string | null>(null);

  useEffect(() => {
    setActiveConfig(config);
  }, [config]);

  useEffect(() => {
    let cancelled = false;
    const loadAccount = async () => {
      let response = await getMe({ force: true, source: "Pricing" });
      if (response.user) {
        try {
          await refreshBillingSubscription();
          response = await getMe({ force: true, source: "PricingRefresh" });
        } catch {
          // Keep pricing usable when Stripe refresh is temporarily unavailable.
        }
      }
      return response;
    };
    void Promise.allSettled([
      getPlanConfig(),
      loadAccount(),
    ]).then(([configResult, accountResult]) => {
      if (cancelled) return;
      if (configResult.status === "fulfilled" && configResult.value.tiers.length > 0 && configResult.value.features.length > 0) {
        setActiveConfig(configResult.value);
      }
      if (accountResult.status === "fulfilled") {
        setAccountUser(accountResult.value.user);
        setAccountEntitlements(accountResult.value.entitlements);
      }
      setAccountLoading(false);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const featuresByCategory = useMemo(() => {
    const grouped = new Map<string, PlanConfigFeature[]>();
    for (const feature of [...activeConfig.features].sort((a, b) => a.sort_order - b.sort_order)) {
      const category = categoryFor(feature.feature_key);
      grouped.set(category, [...(grouped.get(category) ?? []), feature]);
    }
    return categoryOrder.map((category) => ({ category, features: sortFeaturesForCategory(category, grouped.get(category) ?? []) }));
  }, [activeConfig.features]);

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

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-950/60 p-4 shadow-2xl shadow-black/25">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Plans & Pricing</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">Walnut Market Terminal plans</h1>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
              Compact access, limits, and workflow capacity for Free, Premium, and Pro.
            </p>
          </div>
          <div className="inline-grid grid-cols-2 rounded-lg border border-white/10 bg-slate-900 p-1 text-sm font-semibold text-slate-300">
            {(["monthly", "annual"] as BillingInterval[]).map((interval) => (
              <button
                key={interval}
                type="button"
                onClick={() => setBillingInterval(interval)}
                className={`rounded-md px-4 py-2 transition ${billingInterval === interval ? "bg-emerald-300 text-slate-950" : "hover:text-white"}`}
              >
                {interval === "monthly" ? "Monthly" : "Annual"}
              </button>
            ))}
          </div>
        </div>

        {isNonRenewingPaid(accountUser) ? (
          <div className="mt-5 rounded-lg border border-amber-300/30 bg-amber-300/10 p-4 text-sm text-amber-50">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p>
                Your Walnut {displayPlanName(accountUser)} subscription is active until {formatDate(accountUser?.access_expires_at)}, but it is set not to renew. Renew before that date to keep access.
              </p>
              <button
                type="button"
                onClick={openBillingPortal}
                className="inline-flex items-center justify-center rounded-lg border border-amber-200/40 bg-amber-200/10 px-3 py-2 text-sm font-semibold text-amber-50 transition hover:bg-amber-200/15"
              >
                Manage billing
              </button>
            </div>
            {portalStatus ? <p className="mt-2 text-sm text-amber-100">{portalStatus}</p> : null}
          </div>
        ) : null}

        <div className="mt-5 grid gap-3 lg:grid-cols-3">
          {planOrder.map((tier) => (
            <PlanCard
              key={tier}
              config={activeConfig}
              tier={tier}
              billingInterval={billingInterval}
              plan={tierFor(activeConfig, tier)}
              accountUser={accountUser}
              accountEntitlements={accountEntitlements}
              accountLoading={accountLoading}
            />
          ))}
        </div>
      </section>

      <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-900/70">
        <div className="overflow-x-auto">
          <div className="min-w-[760px]">
            <div className="grid grid-cols-[1.25fr_0.75fr_0.75fr_0.75fr] gap-3 bg-slate-950/80 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500">
              <div>Feature</div>
              <div>Free</div>
              <div>Premium</div>
              <div>Pro</div>
            </div>
            {featuresByCategory.map(({ category, features }) => (
              <div key={category}>
                <div className="border-t border-white/10 bg-slate-950/40 px-4 py-2 text-xs font-semibold uppercase tracking-wide text-emerald-300">
                  {category}
                </div>
                {features.map((feature) => (
                  <div key={feature.feature_key} className="grid grid-cols-[1.25fr_0.75fr_0.75fr_0.75fr] gap-3 border-t border-white/10 px-4 py-3 text-sm">
                    <div>
                      <div className="font-semibold text-white">{feature.label}</div>
                      <p className="mt-1 text-xs leading-5 text-slate-500">{publicFeatureCopy(feature.description)}</p>
                    </div>
                    {planOrder.map((tier) => {
                      const included = feature.kind === "limit" ? (feature.limits[tier] ?? 0) > 0 : featureIncluded(feature, tier);
                      return (
                        <div key={tier} className={`font-medium ${included ? "text-slate-100" : "text-slate-600"}`}>
                          {featureCell(feature, tier)}
                        </div>
                      );
                    })}
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

function PlanCard({
  config,
  tier,
  billingInterval,
  plan,
  accountUser,
  accountEntitlements,
  accountLoading,
}: {
  config: PlanConfig;
  tier: PlanTier;
  billingInterval: BillingInterval;
  plan?: PlanConfigTier;
  accountUser: AccountUser | null;
  accountEntitlements: Entitlements | null;
  accountLoading: boolean;
}) {
  const price = priceFor(config, tier, billingInterval);
  const monthly = priceFor(config, tier, "monthly");
  const annual = priceFor(config, tier, "annual");
  const savings = billingInterval === "annual" ? annualSavingsLabel(monthly, annual) : null;
  const highlighted = tier === "premium";

  return (
    <article className={`rounded-lg border p-4 ${highlighted ? "border-emerald-300/35 bg-emerald-300/[0.06]" : "border-white/10 bg-slate-900/70"}`}>
      <div className="flex items-center justify-between gap-3">
        <h2 className="text-xl font-semibold text-white">{plan?.name ?? tier}</h2>
        {tier === "pro" ? <span className="rounded-md border border-cyan-300/30 px-2 py-1 text-xs font-semibold text-cyan-100">Highest limits</span> : null}
        {tier === "premium" ? <span className="rounded-md border border-emerald-300/30 px-2 py-1 text-xs font-semibold text-emerald-100">Popular</span> : null}
      </div>
      <p className="mt-2 min-h-[48px] text-sm leading-6 text-slate-400">{plan?.description}</p>
      <div className="mt-4 flex items-end gap-2">
        <span className="text-4xl font-semibold text-white">{formatMoney(price)}</span>
        <span className="pb-1 text-sm text-slate-500">{tier === "free" ? "forever" : billingInterval === "annual" ? "/yr" : "/mo"}</span>
      </div>
      {savings ? <div className="mt-2 inline-flex rounded-md border border-emerald-300/25 bg-emerald-300/10 px-2 py-1 text-xs font-semibold text-emerald-100">{savings}</div> : null}

      <div className="mt-4 grid grid-cols-2 gap-2">
        {headlineLimitKeys.map((featureKey) => {
          const feature = limitFeature(config, featureKey);
          const value = feature?.limits[tier] ?? plan?.limits[featureKey] ?? 0;
          return (
            <div key={featureKey} className="rounded-md border border-white/10 bg-slate-950/50 p-2">
              <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">{feature?.label ?? featureKey}</div>
              <div className="mt-1 whitespace-nowrap text-sm font-semibold text-slate-100">{formatLimit(feature, value)}</div>
            </div>
          );
        })}
      </div>

      <div className="mt-4">
        <PricingActions
          billingInterval={billingInterval}
          tier={tier}
          ctaLabel={tier === "free" ? "Get started" : `Upgrade to ${tier === "pro" ? "Pro" : "Premium"}`}
          user={accountUser}
          entitlements={accountEntitlements}
          accountLoading={accountLoading}
        />
      </div>
    </article>
  );
}

function isNonRenewingPaid(user: AccountUser | null) {
  if (!user?.subscription_cancel_at_period_end || !user.access_expires_at) return false;
  const date = new Date(user.access_expires_at);
  if (Number.isNaN(date.getTime()) || date <= new Date()) return false;
  const status = (user.subscription_status || "").toLowerCase();
  const tier = (user.current_plan || user.subscription_plan || user.entitlement_tier || "").toLowerCase();
  return ["active", "trialing"].includes(status) && (tier === "premium" || tier === "pro");
}

function displayPlanName(user: AccountUser | null) {
  const plan = (user?.current_plan || user?.subscription_plan || user?.entitlement_tier || "paid").toLowerCase();
  if (plan === "pro") return "Pro";
  if (plan === "premium") return "Premium";
  return "paid";
}

function formatDate(value?: string | null) {
  if (!value) return "the end of your billing period";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "the end of your billing period";
  return date.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
}
