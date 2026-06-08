"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { createCheckoutSession, createCustomerPortalSession, getMe, type AccountUser } from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";

type PricingActionsProps = {
  billingInterval?: "monthly" | "annual";
  tier?: "free" | "premium" | "pro";
  ctaLabel?: string;
};

export function PricingActions({ billingInterval = "monthly", tier = "premium", ctaLabel }: PricingActionsProps) {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [entitlements, setEntitlements] = useState<Entitlements | null>(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMe({ force: true, source: "Pricing" })
      .then((response) => {
        if (!cancelled) {
          setUser(response.user);
          setEntitlements(response.entitlements);
        }
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  const currentTier = entitlements?.tier === "admin" ? "pro" : entitlements?.tier;
  const tierRank: Record<"free" | "premium" | "pro", number> = { free: 0, premium: 10, pro: 20 };
  const targetRank = tierRank[tier];
  const currentRank = currentTier === "premium" || currentTier === "pro" ? tierRank[currentTier] : 0;
  const isCurrentPlan = currentTier === tier;
  const opensBillingPortal = Boolean(user?.subscription_status) && currentRank > targetRank;
  const disabled = loading || isCurrentPlan;
  const buttonLabel = isCurrentPlan
    ? "Current plan"
    : opensBillingPortal
      ? "Manage billing"
      : ctaLabel ?? (tier === "free" ? "Get started" : billingInterval === "annual" ? "Upgrade annually" : "Upgrade monthly");

  const runAction = async () => {
    if (user?.email_verification_required || user?.email_verified === false) {
      setStatus("Please verify your email before upgrading with Stripe.");
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      if (opensBillingPortal || tier === "free") {
        const session = await createCustomerPortalSession();
        if (session.url) {
          window.location.href = session.url;
          return;
        }
        setStatus("Stripe did not return a billing portal URL.");
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
