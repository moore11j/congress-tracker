"use client";

import Link from "next/link";
import { useState } from "react";
import { createCheckoutSession, createCustomerPortalSession, type AccountUser } from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";

type PricingActionsProps = {
  billingInterval?: "monthly" | "annual";
  tier?: "free" | "premium" | "pro";
  ctaLabel?: string;
  user: AccountUser | null;
  entitlements: Entitlements | null;
  accountLoading?: boolean;
};

const tierRank: Record<"free" | "premium" | "pro", number> = { free: 0, premium: 10, pro: 20 };
const managedSubscriptionStatuses = new Set(["active", "trialing", "past_due"]);

export function PricingActions({ billingInterval = "monthly", tier = "premium", ctaLabel, user, entitlements, accountLoading = false }: PricingActionsProps) {
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  const currentTier = entitlements?.tier === "admin" ? "pro" : entitlements?.tier;
  const targetRank = tierRank[tier];
  const currentRank = currentTier === "premium" || currentTier === "pro" ? tierRank[currentTier] : 0;
  const isCurrentPlan = currentTier === tier;
  const subscriptionStatus = (user?.subscription_status || "").toLowerCase();
  const hasManagedSubscription = managedSubscriptionStatuses.has(subscriptionStatus) && currentRank > 0;
  const opensBillingPortal = hasManagedSubscription && !isCurrentPlan;
  const disabled = loading || accountLoading || isCurrentPlan;
  const buttonLabel = isCurrentPlan
    ? "Current plan"
    : accountLoading
      ? "Checking plan"
    : opensBillingPortal
      ? "Change plan"
      : ctaLabel ?? (tier === "free" ? "Get started" : billingInterval === "annual" ? "Upgrade annually" : "Upgrade monthly");

  const runAction = async () => {
    if (user?.email_verification_required || user?.email_verified === false) {
      setStatus("Please verify your email before upgrading with Stripe.");
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      if (opensBillingPortal || (tier === "free" && hasManagedSubscription)) {
        const session = await createCustomerPortalSession();
        if (session.url) {
          window.location.href = session.url;
          return;
        }
        setStatus("Stripe did not return a billing portal URL.");
        return;
      }
      if (tier === "free") {
        window.location.href = "/account/billing";
        return;
      }
      const session = await createCheckoutSession(billingInterval, tier);
      if (session.url) {
        window.location.href = session.url;
        return;
      }
      setStatus("Stripe did not return a checkout URL.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to start checkout.");
    } finally {
      setLoading(false);
    }
  };

  if (accountLoading) {
    return (
      <button
        type="button"
        disabled
        className="inline-flex w-full cursor-default items-center justify-center rounded-lg border border-white/10 bg-slate-900/70 px-4 py-2 text-sm font-semibold text-slate-400"
      >
        Checking plan
      </button>
    );
  }

  if (!user) {
    return (
      <Link
        href="/login?return_to=/pricing"
        className="inline-flex w-full items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
      >
        {ctaLabel ?? "Login / Register"}
      </Link>
    );
  }

  return (
    <div>
      <button
        type="button"
        onClick={runAction}
        disabled={disabled}
        className={`inline-flex w-full items-center justify-center rounded-lg border px-4 py-2 text-sm font-semibold transition ${
          disabled
            ? "cursor-default border-white/10 bg-slate-900/70 text-slate-400"
            : "border-emerald-300/40 bg-emerald-300/15 text-emerald-100 hover:bg-emerald-300/20"
        }`}
      >
        {buttonLabel}
      </button>
      {status ? <p className="mt-2 text-sm text-slate-400">{status}</p> : null}
    </div>
  );
}
