"use client";

import { useEffect, useState } from "react";
import {
  ApiError,
  deleteStrategySubscription,
  getEntitlements,
  getStrategySubscription,
  updateStrategySubscription,
} from "@/lib/api";
import { UpgradePrompt } from "@/components/billing/UpgradePrompt";
import { defaultEntitlements, hasEntitlement, type Entitlements } from "@/lib/entitlements";

type Props = {
  slug: string;
  compact?: boolean;
};

const defaultSubscription = {
  email_enabled: true,
  delivery_mode: "realtime" as const,
  event_types: ["trade_added", "trade_exited", "rebalance_completed"],
};

export function StrategyFollowButton({ slug, compact = false }: Props) {
  const [loading, setLoading] = useState(true);
  const [following, setFollowing] = useState(false);
  const [signedOut, setSignedOut] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [upgradeOpen, setUpgradeOpen] = useState(false);

  useEffect(() => {
    let active = true;
    getEntitlements().then((result) => active && setEntitlements(result)).catch(() => active && setEntitlements(defaultEntitlements));
    getStrategySubscription(slug)
      .then((result) => {
        if (!active) return;
        setFollowing(Boolean(result.subscription?.isActive));
      })
      .catch((error) => {
        if (!active) return;
        if (error instanceof ApiError && error.status === 401) setSignedOut(true);
        else setMessage(error instanceof Error ? error.message : "Unable to load follow status.");
      })
      .finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [slug]);

  async function toggleFollowing() {
    if (!hasEntitlement(entitlements, "notification_digests")) {
      setUpgradeOpen(true);
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      if (following) {
        await deleteStrategySubscription(slug);
        setFollowing(false);
      } else {
        await updateStrategySubscription(slug, defaultSubscription);
        setFollowing(true);
      }
    } catch (error) {
      if (error instanceof ApiError && (error.status === 401 || error.status === 402)) setUpgradeOpen(true);
      else setMessage(error instanceof Error ? error.message : "Unable to update strategy alerts.");
    } finally {
      setLoading(false);
    }
  }

  return <div className={compact ? "" : "w-full"}>
    <button type="button" onClick={toggleFollowing} disabled={loading} className={`${compact ? "" : "w-full"} rounded-md px-3 py-2 text-sm font-semibold disabled:cursor-wait disabled:opacity-60 ${following ? "border border-white/15 text-slate-200 hover:border-rose-300/50 hover:text-rose-100" : "bg-emerald-400/20 text-emerald-50 ring-1 ring-emerald-300/35 hover:bg-emerald-400/30"}`}>
      {loading ? "Loading..." : following ? "Following strategy" : signedOut ? "Sign in to follow" : "Follow strategy"}
    </button>
    {message ? <p className="mt-2 text-xs leading-5 text-rose-200">{message}</p> : null}
    {upgradeOpen ? <div className="mt-3"><UpgradePrompt title="Follow this strategy in real time" body="Get new-position, exit, and rebalance alerts, plus the complete current model portfolio, with Premium." compact /></div> : null}
  </div>;
}
