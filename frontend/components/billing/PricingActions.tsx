"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { createCheckoutSession, getMe, type AccountUser } from "@/lib/api";

type PricingActionsProps = {
  billingInterval?: "monthly" | "annual";
  ctaLabel?: string;
};

export function PricingActions({ billingInterval = "monthly", ctaLabel }: PricingActionsProps) {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (!cancelled) setUser(response.user);
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, []);

  const upgrade = async () => {
    setLoading(true);
    setStatus(null);
    try {
      const session = await createCheckoutSession();
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
        className="inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
      >
        {ctaLabel ?? "Login / Register"}
      </Link>
    );
  }

  return (
    <div>
      <button
        type="button"
        onClick={upgrade}
        disabled={loading}
        className="inline-flex items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20"
      >
        {ctaLabel ?? (billingInterval === "annual" ? "Upgrade annually" : "Upgrade monthly")}
      </button>
      {status ? <p className="mt-2 text-sm text-slate-400">{status}</p> : null}
    </div>
  );
}
