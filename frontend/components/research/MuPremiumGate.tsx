"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { ApiError, createCheckoutSession, recordProductEvent } from "@/lib/api";
import { currentCampaignProperties, registerHref } from "@/lib/campaignAttribution";

type MuPremiumGateProps = {
  authState: "logged_out" | "free";
  entitlement: string;
  returnTo: string;
};

type PremiumResearchGateProps = MuPremiumGateProps & {
  articleSlug: string;
  tickers?: string[];
  requiredPlan?: "premium" | "pro" | string | null;
  heading: string;
  description: string;
};

const primaryClassName =
  "inline-flex min-h-11 items-center justify-center rounded-lg bg-emerald-300 px-5 py-2.5 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-wait disabled:opacity-70";
const secondaryClassName =
  "inline-flex min-h-11 items-center justify-center rounded-lg border border-white/15 px-5 py-2.5 text-sm font-semibold text-slate-100 transition hover:border-emerald-300/50 hover:text-emerald-100";

function paywallProperties(
  entitlement: string,
  articleSlug: string,
  tickers: string[],
  extra: Record<string, string | number | boolean | null> = {},
) {
  return currentCampaignProperties({
    article_slug: articleSlug,
    ticker: tickers[0] || null,
    tickers: tickers.join(","),
    user_entitlement: entitlement,
    ...extra,
  });
}

export function PremiumResearchGate({
  authState,
  entitlement,
  returnTo,
  articleSlug,
  tickers = [],
  requiredPlan = "premium",
  heading,
  description,
}: PremiumResearchGateProps) {
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const trackedRef = useRef(false);
  const signupHref = useMemo(() => registerHref(returnTo), [returnTo]);
  const plansHref = useMemo(() => `/pricing?returnTo=${encodeURIComponent(returnTo)}`, [returnTo]);
  const checkoutPlan = requiredPlan === "pro" ? "pro" : "premium";
  const primaryLabel = checkoutPlan === "pro" ? "Unlock with Pro" : "Unlock with Premium";

  useEffect(() => {
    if (trackedRef.current) return;
    trackedRef.current = true;
    recordProductEvent({
      event_name: "research_preview_viewed",
      path: returnTo,
      properties: paywallProperties(entitlement, articleSlug, tickers, { gate_state: authState, required_plan: checkoutPlan }),
    });
    recordProductEvent({
      event_name: "research_paywall_viewed",
      path: returnTo,
      properties: paywallProperties(entitlement, articleSlug, tickers, { gate_state: authState, required_plan: checkoutPlan }),
    });
  }, [articleSlug, authState, checkoutPlan, entitlement, returnTo, tickers]);

  const startCheckout = async () => {
    if (loading) return;
    setLoading(true);
    setStatus(null);
    recordProductEvent({
      event_name: "research_paywall_cta_clicked",
      path: returnTo,
      properties: paywallProperties(entitlement, articleSlug, tickers, { cta: checkoutPlan === "pro" ? "unlock_with_pro" : "unlock_with_premium", required_plan: checkoutPlan }),
    });
    recordProductEvent({
      event_name: "research_checkout_started",
      path: returnTo,
      properties: paywallProperties(entitlement, articleSlug, tickers, { plan: checkoutPlan, billing_interval: "monthly", required_plan: checkoutPlan }),
    });
    try {
      const session = await createCheckoutSession("monthly", checkoutPlan, returnTo);
      if (session.url) {
        window.location.href = session.url;
        return;
      }
      setStatus("Stripe did not return a checkout URL.");
    } catch (error) {
      const message = error instanceof ApiError && error.status === 403
        ? "Verify your email before upgrading with Stripe."
        : error instanceof Error
          ? error.message
          : "Unable to start checkout.";
      setStatus(message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="relative overflow-hidden rounded-lg border border-emerald-300/25 bg-slate-950/80 p-5 shadow-[0_18px_70px_-48px_rgba(16,185,129,0.7)] sm:p-6">
      <div className="pointer-events-none absolute inset-x-0 -top-12 h-16 bg-gradient-to-b from-transparent to-slate-950/80" aria-hidden="true" />
      <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-300">{checkoutPlan === "pro" ? "Pro Research" : "Premium Research"}</p>
      <h2 className="mt-3 text-2xl font-semibold text-white">{heading}</h2>
      <p className="mt-3 max-w-2xl text-sm leading-7 text-slate-300">
        {description}
      </p>
      <div className="mt-5 flex flex-wrap gap-3">
        {authState === "logged_out" ? (
          <Link
            href={signupHref}
            className={primaryClassName}
            onClick={() => {
              recordProductEvent({
                event_name: "research_paywall_cta_clicked",
                path: returnTo,
                properties: paywallProperties(entitlement, articleSlug, tickers, { cta: "create_account_to_continue", required_plan: checkoutPlan }),
              });
              recordProductEvent({
                event_name: "research_signup_started",
                path: returnTo,
                properties: paywallProperties(entitlement, articleSlug, tickers, { cta: "create_account_to_continue", required_plan: checkoutPlan }),
              });
            }}
          >
            Create an Account to Continue
          </Link>
        ) : (
          <button type="button" onClick={startCheckout} disabled={loading} className={primaryClassName}>
            {loading ? "Starting Checkout" : primaryLabel}
          </button>
        )}
        <Link
          href={plansHref}
          className={secondaryClassName}
          onClick={() => {
            recordProductEvent({
              event_name: "research_paywall_cta_clicked",
              path: returnTo,
              properties: paywallProperties(entitlement, articleSlug, tickers, { cta: "view_premium_plans", required_plan: checkoutPlan }),
            });
          }}
        >
          View Premium Plans
        </Link>
      </div>
      {status ? <p className="mt-3 text-sm text-slate-400">{status}</p> : null}
    </section>
  );
}

export function MuPremiumGate({ authState, entitlement, returnTo }: MuPremiumGateProps) {
  return (
    <PremiumResearchGate
      authState={authState}
      entitlement={entitlement}
      returnTo={returnTo}
      articleSlug="mu-dd"
      tickers={["MU"]}
      requiredPlan="premium"
      heading="Unlock Walnut's Full MU Conclusion"
      description="See the confirmation score, directional judgment, supporting evidence, catalysts, risks, and what could change the outlook."
    />
  );
}
