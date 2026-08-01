"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { useEffect, useMemo, useRef, useState } from "react";
import { getEntitlements } from "@/lib/api";
import { normalizeTier, type EntitlementTier } from "@/lib/entitlements";
import { preserveCurrentPath, registerHref } from "@/lib/campaignAttribution";
import { trackCampaignEvent } from "@/components/campaign/CampaignAnalytics";
import { WatchlistQuickAddButton } from "@/components/campaign/WatchlistQuickAddButton";

type Props = {
  ticker: string;
  companyName?: string | null;
  researchSlug: string;
};

type LoadedState = {
  authState: "logged_out" | "authenticated";
  plan: EntitlementTier;
};

const primaryButtonClassName =
  "inline-flex min-h-10 items-center justify-center rounded-lg bg-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:bg-emerald-200 disabled:cursor-wait disabled:opacity-70";
const secondaryButtonClassName =
  "inline-flex min-h-10 items-center justify-center rounded-lg border border-white/10 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-white/20 hover:text-white";

export function ResearchBriefContextualCta({ ticker, companyName, researchSlug }: Props) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [state, setState] = useState<LoadedState | null>(null);
  const trackedViewRef = useRef(false);
  const normalizedTicker = ticker.trim().toUpperCase();
  const currentPath = useMemo(() => preserveCurrentPath(pathname || `/research/${researchSlug}`, searchParams), [pathname, researchSlug, searchParams]);
  const secondaryHref = useMemo(() => {
    const params = new URLSearchParams();
    for (const key of ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"]) {
      const value = searchParams.get(key);
      if (value) params.set(key, value);
    }
    const query = params.toString();
    return `/search${query ? `?${query}` : ""}`;
  }, [searchParams]);
  const signupReturnTo = useMemo(() => {
    const [path, query = ""] = currentPath.split("?", 2);
    const params = new URLSearchParams(query);
    params.set("cta_ticker", normalizedTicker);
    params.set("research_slug", researchSlug);
    const nextQuery = params.toString();
    return `${path}${nextQuery ? `?${nextQuery}` : ""}`;
  }, [currentPath, normalizedTicker, researchSlug]);
  const properties = useMemo(() => {
    const params = new URLSearchParams(searchParams.toString());
    return {
      ticker: normalizedTicker,
      research_slug: researchSlug,
      auth_state: state?.authState ?? "unknown",
      plan: state?.plan ?? "free",
      page_path: currentPath,
      utm_source: params.get("utm_source"),
      utm_medium: params.get("utm_medium"),
      utm_campaign: params.get("utm_campaign"),
      utm_content: params.get("utm_content"),
      utm_term: params.get("utm_term"),
    };
  }, [currentPath, normalizedTicker, researchSlug, searchParams, state]);

  useEffect(() => {
    let cancelled = false;
    getEntitlements(undefined, { source: "ResearchBriefContextualCta" })
      .then((entitlements) => {
        if (cancelled) return;
        setState({
          authState: entitlements.user ? "authenticated" : "logged_out",
          plan: normalizeTier(entitlements.effective_tier ?? entitlements.tier),
        });
      })
      .catch(() => {
        if (!cancelled) setState({ authState: "logged_out", plan: "free" });
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!state || trackedViewRef.current) return;
    trackedViewRef.current = true;
    trackCampaignEvent("research_brief_contextual_cta_view", properties, currentPath);
  }, [currentPath, properties, state]);

  if (!state || !normalizedTicker) return null;

  const isLoggedOut = state.authState === "logged_out";
  const headline = isLoggedOut
    ? `Keep following ${normalizedTicker}.`
    : state.plan === "free"
      ? "Keep this research on your radar."
      : `Save ${normalizedTicker} to a watchlist.`;
  const copy = isLoggedOut
    ? `Create a free account to save this research, track ${companyName || normalizedTicker} and return when the catalysts, risks or supporting data change.`
    : state.plan === "free"
      ? `Add ${normalizedTicker} to your watchlist and keep track of what changes after this brief.`
      : `Add ${normalizedTicker} to a watchlist so it is easy to revisit after this brief.`;

  return (
    <section className="rounded-lg border border-emerald-300/20 bg-emerald-300/[0.06] p-5">
      <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-300">Follow-up workflow</p>
          <h2 className="mt-2 text-lg font-semibold text-white">{headline}</h2>
          <p className="mt-1 max-w-2xl text-sm leading-6 text-slate-300">{copy}</p>
        </div>
        <div className="flex shrink-0 flex-wrap gap-2">
          {isLoggedOut ? (
            <Link
              href={registerHref(signupReturnTo)}
              className={primaryButtonClassName}
              onClick={() => {
                trackCampaignEvent("research_brief_signup_start", properties, currentPath);
                trackCampaignEvent("research_brief_contextual_cta_click", { ...properties, cta: "create_free_account" }, currentPath);
              }}
            >
              Create Free Account
            </Link>
          ) : (
            <WatchlistQuickAddButton
              symbols={[normalizedTicker]}
              label={`Add ${normalizedTicker} to Watchlist`}
              eventName="research_brief_watchlist_click"
              properties={properties}
              className={primaryButtonClassName}
            />
          )}
          <Link
            href={secondaryHref}
            className={secondaryButtonClassName}
            onClick={() => trackCampaignEvent("research_brief_contextual_cta_click", { ...properties, cta: "research_another_stock" }, currentPath)}
          >
            Research Another Stock
          </Link>
        </div>
      </div>
    </section>
  );
}
