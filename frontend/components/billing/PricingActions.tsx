"use client";

import Link from "next/link";
import { useState } from "react";
import { ApiError, createCheckoutSession, createCustomerPortalSession, type AccountUser } from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";

type PricingActionsProps = {
  billingInterval?: "monthly" | "annual";
  tier?: "free" | "premium" | "pro";
  ctaLabel?: string;
  user: AccountUser | null;
  entitlements: Entitlements | null;
  accountLoading?: boolean;
};

type PlanTier = "free" | "premium" | "pro";
type PlanAction = "current" | "downgrade" | "upgrade";

const tierRank: Record<PlanTier, number> = { free: 0, premium: 10, pro: 20 };
const managedSubscriptionStatuses = new Set(["active", "trialing", "past_due"]);
const billingLocationRequiredMessage = "Complete billing location before starting taxable checkout.";
const planNames: Record<PlanTier, string> = { free: "Free", premium: "Premium", pro: "Pro" };

function normalizedPlanTier(value?: string | null): PlanTier | null {
  if (value === "admin") return "pro";
  if (value === "free" || value === "premium" || value === "pro") return value;
  return null;
}

function currentPlanTier(user: AccountUser | null, entitlements: Entitlements | null): PlanTier {
  return (
    normalizedPlanTier(entitlements?.effective_tier) ??
    normalizedPlanTier(entitlements?.tier) ??
    normalizedPlanTier(user?.entitlement_tier) ??
    normalizedPlanTier(user?.current_plan) ??
    normalizedPlanTier(user?.subscription_plan) ??
    normalizedPlanTier(user?.plan) ??
    "free"
  );
}

function actionForPlan(currentTier: PlanTier, targetTier: PlanTier): PlanAction {
  if (currentTier === targetTier) return "current";
  return tierRank[targetTier] < tierRank[currentTier] ? "downgrade" : "upgrade";
}

function labelForAction(action: PlanAction, targetTier: PlanTier) {
  if (action === "current") return "Current plan";
  return `${action === "downgrade" ? "Downgrade" : "Upgrade"} to ${planNames[targetTier]}`;
}

function checkoutConflictRedirectPath(error: unknown): string | null {
  if (!(error instanceof ApiError) || error.status !== 409) return null;
  const detail = error.detail;
  if (!detail || typeof detail !== "object") return null;
  const payload = detail as { code?: unknown; redirect_path?: unknown };
  if (payload.code !== "active_subscription_exists") return null;
  return typeof payload.redirect_path === "string" && payload.redirect_path.startsWith("/")
    ? payload.redirect_path
    : "/account/billing";
}

export function PricingActions({ billingInterval = "monthly", tier = "premium", ctaLabel, user, entitlements, accountLoading = false }: PricingActionsProps) {
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  const currentTier = currentPlanTier(user, entitlements);
  const planAction = actionForPlan(currentTier, tier);
  const currentRank = tierRank[currentTier];
  const isCurrentPlan = planAction === "current";
  const isDowngrade = planAction === "downgrade";
  const subscriptionStatus = (user?.subscription_status || "").toLowerCase();
  const hasManagedSubscription = managedSubscriptionStatuses.has(subscriptionStatus) && currentRank > 0;
  const opensBillingPortal = !isCurrentPlan && (hasManagedSubscription || isDowngrade);
  const disabled = loading || accountLoading || isCurrentPlan;
  const buttonLabel = isCurrentPlan
    ? "Current plan"
    : accountLoading
      ? "Checking plan"
      : labelForAction(planAction, tier);

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
      const redirectPath = checkoutConflictRedirectPath(error);
      if (redirectPath) {
        window.location.href = redirectPath;
        return;
      }
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
            : isDowngrade
              ? "border-white/10 bg-slate-900/70 text-slate-200 hover:border-white/20 hover:text-white"
              : "border-emerald-300/40 bg-emerald-300/15 text-emerald-100 hover:bg-emerald-300/20"
        }`}
      >
        {buttonLabel}
      </button>
      {status ? <CheckoutStatus status={status} /> : null}
    </div>
  );
}

function CheckoutStatus({ status }: { status: string }) {
  if (status !== billingLocationRequiredMessage) {
    return <p className="mt-2 text-sm text-slate-400">{status}</p>;
  }

  return (
    <p className="mt-2 text-sm text-slate-400">
      {status}{" "}
      <Link
        href="/account/settings"
        className="font-semibold text-emerald-200 underline decoration-emerald-200/50 underline-offset-4 transition hover:text-emerald-100"
      >
        Add it in Account Settings.
      </Link>
    </p>
  );
}
