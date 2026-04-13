"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  createCheckoutSession,
  createCustomerPortalSession,
  getMe,
  logout,
  type AccountUser,
} from "@/lib/api";
import type { Entitlements } from "@/lib/entitlements";

export function AccountAccessPanel() {
  const [user, setUser] = useState<AccountUser | null>(null);
  const [entitlements, setEntitlements] = useState<Entitlements | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getMe()
      .then((response) => {
        if (cancelled) return;
        setUser(response.user);
        setEntitlements(response.entitlements);
      })
      .catch(() => {
        if (!cancelled) setStatus("Account status is unavailable.");
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const signOut = async () => {
    setLoading(true);
    try {
      await logout();
      setUser(null);
      setEntitlements(null);
      setStatus("Signed out.");
      window.location.replace("/login");
    } finally {
      setLoading(false);
    }
  };

  const startCheckout = async () => {
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

  const openPortal = async () => {
    setLoading(true);
    setStatus(null);
    try {
      const session = await createCustomerPortalSession();
      if (session.url) {
        window.location.href = session.url;
        return;
      }
      setStatus("Stripe did not return a portal URL.");
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Unable to open billing portal.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="rounded-lg border border-white/10 bg-slate-900/70 p-5">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-emerald-300">Sign in</p>
          <h2 className="mt-1 text-xl font-semibold text-white">
            {user ? user.email : "Use an account for billing and entitlements."}
          </h2>
          <p className="mt-1 text-sm text-slate-400">
            {user
              ? `Current access: ${entitlements?.tier ?? "free"}${user.is_admin ? " admin" : ""}.`
              : "Sign in to manage billing, subscription state, and account access."}
          </p>
        </div>
        {user ? (
          <button
            type="button"
            onClick={signOut}
            disabled={loading}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Sign out
          </button>
        ) : null}
      </div>

      {!user ? (
        <div className="mt-5 space-y-4">
          <a
            href="/login?return_to=/account/billing"
            className="inline-flex w-full items-center justify-center rounded-lg border border-emerald-300/40 bg-emerald-300/15 px-4 py-2 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-300/20 md:w-auto"
          >
            Login / Register
          </a>
        </div>
      ) : (
        <div className="mt-5 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={startCheckout}
            disabled={loading}
            className="rounded-lg border border-emerald-300/40 bg-emerald-300/10 px-4 py-2 text-sm font-semibold text-emerald-100"
          >
            Upgrade with Stripe
          </button>
          <button
            type="button"
            onClick={openPortal}
            disabled={loading}
            className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200"
          >
            Manage billing
          </button>
          <Link href="/account/settings" prefetch={false} className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
            Account settings
          </Link>
          {user.is_admin ? (
            <a href="/admin/settings" className="rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200">
              Admin settings
            </a>
          ) : null}
        </div>
      )}

      {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}
    </section>
  );
}
