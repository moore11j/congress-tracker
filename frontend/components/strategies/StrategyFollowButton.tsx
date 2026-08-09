"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  ApiError,
  deleteStrategySubscription,
  getStrategySubscription,
  updateStrategySubscription,
} from "@/lib/api";

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

  useEffect(() => {
    let active = true;
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
      if (error instanceof ApiError && error.status === 401) setSignedOut(true);
      else setMessage(error instanceof Error ? error.message : "Unable to update strategy alerts.");
    } finally {
      setLoading(false);
    }
  }

  if (signedOut) {
    return <Link href={`/login?next=${encodeURIComponent(`/strategies/${slug}`)}`} className="inline-flex items-center justify-center rounded-md border border-emerald-300/35 px-3 py-2 text-sm font-semibold text-emerald-100 hover:border-emerald-200 hover:bg-emerald-300/10">Sign in to follow</Link>;
  }

  return <div className={compact ? "" : "w-full"}>
    <button type="button" onClick={toggleFollowing} disabled={loading} className={`${compact ? "" : "w-full"} rounded-md px-3 py-2 text-sm font-semibold disabled:cursor-wait disabled:opacity-60 ${following ? "border border-white/15 text-slate-200 hover:border-rose-300/50 hover:text-rose-100" : "bg-emerald-400/20 text-emerald-50 ring-1 ring-emerald-300/35 hover:bg-emerald-400/30"}`}>
      {loading ? "Loading..." : following ? "Following strategy" : "Follow strategy"}
    </button>
    {message ? <p className="mt-2 text-xs leading-5 text-rose-200">{message}</p> : null}
  </div>;
}
