"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import type { PlanConfig, PlanConfigFeature, PlanConfigTier, PlanPrice } from "@/lib/api";
import { PricingActions } from "@/components/billing/PricingActions";

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

function categoryFor(featureKey: string) {
  if (["congress_feed", "insider_feed", "government_contracts_feed", "government_contracts_filters"].includes(featureKey)) return "Market feeds";
  if (["screener", "screener_intelligence", "screener_presets", "screener_results", "signals", "leaderboards"].includes(featureKey)) return "Screener & signals";
  if (["watchlists", "watchlist_tickers", "screener_saved_screens", "screener_monitoring", "monitoring_sources", "inbox_alerts", "inbox_alert_retention", "notification_digests", "saved_views"].includes(featureKey)) return "Watchlists & monitoring";
  if (["screener_csv_export", "backtesting"].includes(featureKey)) return "Data export & workflow";
  return "Advanced / Coming Soon";
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

function annualSavingsLabel(monthly?: PlanPrice, annual?: PlanPrice, tier?: PlanTier) {
  const monthlyYear = (monthly?.amount_cents ?? 0) * 12;
  const annualAmount = annual?.amount_cents ?? 0;
  if (monthlyYear <= 0 || annualAmount <= 0 || annualAmount >= monthlyYear) return null;
  const savedMonths = Math.round((monthlyYear - annualAmount) / (monthly?.amount_cents || 1));
  if (tier === "pro" && savedMonths >= 2) return "2 months free";
  const percent = Math.round(((monthlyYear - annualAmount) / monthlyYear) * 100);
  return `Save ${percent}%`;
}

function formatLimit(feature: PlanConfigFeature | undefined, value: number) {
  if (!feature) return value.toLocaleString();
  const unit = value === 1 ? feature.unit_singular : feature.unit_plural;
  return unit ? `${value.toLocaleString()} ${unit}` : value.toLocaleString();
}

function featureIncluded(feature: PlanConfigFeature, tier: PlanTier) {
  return tierRank[tier] >= tierRank[feature.required_tier as PlanTier];
}

function featureCell(feature: PlanConfigFeature, tier: PlanTier) {
  if (feature.kind === "limit") return formatLimit(feature, feature.limits[tier] ?? 0);
  if (featureIncluded(feature, tier)) {
    if (["options_flow_feed", "institutional_feed", "api_webhooks"].includes(feature.feature_key)) return "Coming soon";
    return "Included";
  }
  return "-";
}

function limitFeature(config: PlanConfig, featureKey: string) {
  return config.features.find((feature) => feature.feature_key === featureKey);
}

export function PricingPlanner({ config }: { config: PlanConfig }) {
  const [billingInterval, setBillingInterval] = useState<BillingInterval>("monthly");
  const featuresByCategory = useMemo(() => {
    const grouped = new Map<string, PlanConfigFeature[]>();
    for (const feature of [...config.features].sort((a, b) => a.sort_order - b.sort_order)) {
      const category = categoryFor(feature.feature_key);
      grouped.set(category, [...(grouped.get(category) ?? []), feature]);
    }
    return categoryOrder.map((category) => ({ category, features: grouped.get(category) ?? [] }));
  }, [config.features]);

  return (
    <div className="space-y-6">
      <section className="rounded-lg border border-white/10 bg-slate-950/60 p-4 shadow-2xl shadow-black/25">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.24em] text-emerald-300">Plans & Pricing</p>
            <h1 className="mt-2 text-3xl font-semibold text-white">Capitol Ledger intelligence plans</h1>
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

        <div className="mt-5 grid gap-3 lg:grid-cols-3">
          {planOrder.map((tier) => (
            <PlanCard key={tier} config={config} tier={tier} billingInterval={billingInterval} plan={tierFor(config, tier)} />
          ))}
        </div>
      </section>

      <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-900/70">
        <div className="grid min-w-[760px] grid-cols-[1.25fr_0.75fr_0.75fr_0.75fr] gap-3 bg-slate-950/80 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-500">
          <div>Feature</div>
          <div>Free</div>
          <div>Premium</div>
          <div>Pro</div>
        </div>
        <div className="overflow-x-auto">
          <div className="min-w-[760px]">
            {featuresByCategory.map(({ category, features }) => (
              <div key={category}>
                <div className="border-t border-white/10 bg-slate-950/40 px-4 py-2 text-xs font-semibold uppercase tracking-wide text-emerald-300">
                  {category}
                </div>
                {features.map((feature) => (
                  <div key={feature.feature_key} className="grid grid-cols-[1.25fr_0.75fr_0.75fr_0.75fr] gap-3 border-t border-white/10 px-4 py-3 text-sm">
                    <div>
                      <div className="font-semibold text-white">{feature.label}</div>
                      <p className="mt-1 text-xs leading-5 text-slate-500">{feature.description}</p>
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
}: {
  config: PlanConfig;
  tier: PlanTier;
  billingInterval: BillingInterval;
  plan?: PlanConfigTier;
}) {
  const price = priceFor(config, tier, billingInterval);
  const monthly = priceFor(config, tier, "monthly");
  const annual = priceFor(config, tier, "annual");
  const savings = billingInterval === "annual" ? annualSavingsLabel(monthly, annual, tier) : null;
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
        {tier === "free" ? (
          <Link href="/login?return_to=/pricing" className="inline-flex w-full items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-100 transition hover:border-white/25">
            Get started
          </Link>
        ) : (
          <PricingActions billingInterval={billingInterval} tier={tier} ctaLabel={`Upgrade to ${tier === "pro" ? "Pro" : "Premium"}`} />
        )}
      </div>
    </article>
  );
}
