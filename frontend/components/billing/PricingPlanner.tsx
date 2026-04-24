"use client";

import { useMemo, useState } from "react";
import type { PlanConfig, PlanConfigFeature, PlanPrice } from "@/lib/api";
import { PricingActions } from "@/components/billing/PricingActions";

type BillingInterval = "monthly" | "annual";

const tierRank = {
  free: 0,
  premium: 10,
};

function priceFor(config: PlanConfig, tier: "free" | "premium", interval: BillingInterval): PlanPrice | undefined {
  return config.plan_prices.find((price) => price.tier === tier && price.billing_interval === interval);
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

function annualDiscountPercent(monthly?: PlanPrice, annual?: PlanPrice) {
  const monthlyYear = (monthly?.amount_cents ?? 0) * 12;
  const annualAmount = annual?.amount_cents ?? 0;
  if (monthlyYear <= 0 || annualAmount <= 0 || annualAmount >= monthlyYear) return 0;
  return Math.round(((monthlyYear - annualAmount) / monthlyYear) * 100);
}

function formatLimit(feature: PlanConfigFeature, value: number) {
  const unit = value === 1 ? feature.unit_singular : feature.unit_plural;
  return unit ? `${value.toLocaleString()} ${unit}` : value.toLocaleString();
}

function featureValue(feature: PlanConfigFeature, tier: "free" | "premium") {
  if (feature.kind === "limit") {
    return formatLimit(feature, feature.limits[tier] ?? 0);
  }
  const included = tierRank[tier] >= tierRank[feature.required_tier];
  if (included) return "Included";
  return feature.required_tier === "premium" ? "Premium only" : "Not included";
}

function limitValue(config: PlanConfig, featureKey: string, tier: "free" | "premium") {
  const feature = config.features.find((item) => item.feature_key === featureKey);
  return feature?.limits?.[tier] ?? 0;
}

export function PricingPlanner({ config }: { config: PlanConfig }) {
  const [billingInterval, setBillingInterval] = useState<BillingInterval>("monthly");
  const premiumMonthly = priceFor(config, "premium", "monthly");
  const premiumAnnual = priceFor(config, "premium", "annual");
  const premiumPrice = priceFor(config, "premium", billingInterval);
  const freePrice = priceFor(config, "free", billingInterval);
  const annualDiscount = annualDiscountPercent(premiumMonthly, premiumAnnual);
  const features = useMemo(
    () => [...config.features].sort((a, b) => a.sort_order - b.sort_order),
    [config.features],
  );
  const freeScreenLimit = limitValue(config, "screener_saved_screens", "free");
  const premiumScreenLimit = limitValue(config, "screener_saved_screens", "premium");
  const freeResultLimit = limitValue(config, "screener_results", "free");
  const premiumResultLimit = limitValue(config, "screener_results", "premium");

  const billingCopy =
    billingInterval === "annual"
      ? `${formatMoney(premiumAnnual)} billed annually${annualDiscount ? `, ${annualDiscount}% below monthly` : ""}`
      : `${formatMoney(premiumMonthly)} billed monthly`;

  return (
    <div className="space-y-8">
      <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-900/80 shadow-2xl shadow-black/30">
        <div className="grid gap-8 p-6 lg:grid-cols-[1.05fr_0.95fr] lg:p-8">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.3em] text-emerald-300">Plans & Pricing</p>
            <h1 className="mt-4 max-w-3xl text-4xl font-semibold leading-tight text-white">
              Political trading intelligence for investors who monitor the tape before it becomes consensus.
            </h1>
            <p className="mt-4 max-w-2xl text-base leading-7 text-slate-300">
              Track congressional trading, insider activity, premium signals, leaderboards, and watchlists in one research workflow built for faster decision support.
            </p>
            <div className="mt-6 flex flex-wrap gap-2 text-sm text-slate-300">
              {["Congress trading tracker", "Insider trading tracker", "Political stock trading alerts"].map((item) => (
                <span key={item} className="rounded-lg border border-white/10 bg-slate-950/40 px-3 py-2">
                  {item}
                </span>
              ))}
            </div>
          </div>

          <div className="rounded-lg border border-emerald-300/25 bg-slate-950/60 p-5">
            <div className="inline-grid rounded-lg border border-white/10 bg-slate-950 p-1 text-sm font-semibold text-slate-300 grid-cols-2">
              {(["monthly", "annual"] as BillingInterval[]).map((interval) => (
                <button
                  key={interval}
                  type="button"
                  onClick={() => setBillingInterval(interval)}
                  className={`rounded-md px-4 py-2 transition ${
                    billingInterval === interval ? "bg-emerald-300 text-slate-950" : "hover:text-white"
                  }`}
                >
                  {interval === "monthly" ? "Monthly" : "Annual"}
                </button>
              ))}
            </div>
            <div className="mt-6">
              <div className="text-sm font-semibold uppercase tracking-wide text-emerald-300">Premium</div>
              <div className="mt-2 flex items-end gap-2">
                <span className="text-5xl font-semibold text-white">{formatMoney(premiumPrice)}</span>
                <span className="pb-2 text-sm text-slate-400">/{billingInterval === "annual" ? "yr" : "mo"}</span>
              </div>
              <p className="mt-3 text-sm leading-6 text-slate-300">{billingCopy}</p>
              {billingInterval === "annual" && annualDiscount > 0 ? (
                <p className="mt-3 inline-flex rounded-lg border border-emerald-300/25 bg-emerald-300/10 px-3 py-2 text-sm font-semibold text-emerald-100">
                  Save {annualDiscount}% versus paying monthly for 12 months.
                </p>
              ) : null}
              <div className="mt-5">
                <PricingActions billingInterval={billingInterval} ctaLabel={billingInterval === "annual" ? "Upgrade annually" : "Upgrade monthly"} />
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <PlanCard
          name="Free"
          price={formatMoney(freePrice)}
          cadence="forever"
          description="For investors who want a focused way to follow congressional trades, screen for ideas, and keep one tight watchlist close."
          points={[
            `Stock screener with core market filters and up to ${freeResultLimit.toLocaleString()} results per screen`,
            `${freeScreenLimit.toLocaleString()} saved screen${freeScreenLimit === 1 ? "" : "s"} plus premium intelligence previews`,
            "Core tracking without Premium signal screens, exports, or saved-screen monitoring",
          ]}
        />
        <PlanCard
          name="Premium"
          price={formatMoney(premiumPrice)}
          cadence={billingInterval === "annual" ? "per year" : "per month"}
          description="For active research workflows that need signals, leaderboards, intelligence filters, exports, and more room to monitor political and insider activity."
          points={[
            "Premium signals plus full Congress, insider, confirmation, Why Now, and freshness screener filters",
            `${premiumScreenLimit.toLocaleString()} saved screens with monitoring, presets, and CSV export`,
            `Up to ${premiumResultLimit.toLocaleString()} screener results per query with deeper monitoring workflows`,
          ]}
          highlighted
        />
      </section>

      <section className="overflow-hidden rounded-lg border border-white/10 bg-slate-900/70">
        <div className="grid gap-3 bg-slate-950/70 px-4 py-3 text-xs font-semibold uppercase tracking-wide text-slate-400 sm:grid-cols-[1.15fr_0.8fr_0.8fr]">
          <div>Plan comparison</div>
          <div>Free</div>
          <div>Premium</div>
        </div>
        {features.map((feature) => (
          <div key={feature.feature_key} className="grid gap-3 border-t border-white/10 px-4 py-4 text-sm sm:grid-cols-[1.15fr_0.8fr_0.8fr]">
            <div>
              <div className="font-semibold text-white">{feature.label}</div>
              <p className="mt-1 text-xs leading-5 text-slate-400">{feature.description}</p>
            </div>
            <div className="font-medium text-slate-300">{featureValue(feature, "free")}</div>
            <div className="font-medium text-emerald-100">{featureValue(feature, "premium")}</div>
          </div>
        ))}
        <div className="grid gap-3 border-t border-white/10 px-4 py-4 text-sm sm:grid-cols-[1.15fr_0.8fr_0.8fr]">
          <div>
            <div className="font-semibold text-white">Billing options</div>
            <p className="mt-1 text-xs leading-5 text-slate-400">Monthly and annual premium billing shown from admin plan settings.</p>
          </div>
          <div className="font-medium text-slate-300">Free</div>
          <div className="font-medium text-emerald-100">
            {formatMoney(premiumMonthly)}/mo or {formatMoney(premiumAnnual)}/yr
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-white/10 bg-slate-900/60 p-5">
        <h2 className="text-2xl font-semibold text-white">Built for sharper monitoring, not noise.</h2>
        <p className="mt-3 max-w-4xl text-sm leading-7 text-slate-300">
          Capitol Ledger brings congressional trading tracker data, insider trading tracker context, political stock trading alerts, premium signals, leaderboards, and watchlists into a single workflow. Free keeps the core research loop accessible. Premium adds the higher-conviction layers for investors who want earlier context, faster triage, and deeper decision support.
        </p>
      </section>
    </div>
  );
}

function PlanCard({
  name,
  price,
  cadence,
  description,
  points,
  highlighted = false,
}: {
  name: string;
  price: string;
  cadence: string;
  description: string;
  points: string[];
  highlighted?: boolean;
}) {
  return (
    <article className={`rounded-lg border p-5 ${highlighted ? "border-emerald-300/30 bg-emerald-300/[0.06]" : "border-white/10 bg-slate-900/60"}`}>
      <div className="flex items-start justify-between gap-4">
        <div>
          <h2 className="text-2xl font-semibold text-white">{name}</h2>
          <p className="mt-2 text-sm leading-6 text-slate-300">{description}</p>
        </div>
        {highlighted ? <span className="rounded-lg border border-emerald-300/30 px-2 py-1 text-xs font-semibold text-emerald-100">Best for daily research</span> : null}
      </div>
      <div className="mt-5 flex items-end gap-2">
        <span className="text-4xl font-semibold text-white">{price}</span>
        <span className="pb-1 text-sm text-slate-400">{cadence}</span>
      </div>
      <ul className="mt-5 space-y-3 text-sm text-slate-300">
        {points.map((point) => (
          <li key={point} className="flex gap-3">
            <span className="mt-2 h-1.5 w-1.5 shrink-0 rounded-full bg-emerald-300" />
            <span>{point}</span>
          </li>
        ))}
      </ul>
    </article>
  );
}
