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
  panel?: boolean;
};

const defaultSubscription = {
  email_enabled: true,
  delivery_mode: "daily" as const,
  event_types: ["trade_added", "trade_exited", "rebalance_completed"],
};

const eventOptions = [
  ["trade_added", "New position added"],
  ["trade_exited", "Position exited"],
  ["position_rebalanced", "Position rebalanced"],
  ["rebalance_completed", "Daily summary"],
] as const;

export function StrategyFollowButton({ slug, compact = false, panel = false }: Props) {
  const [loading, setLoading] = useState(true);
  const [following, setFollowing] = useState(false);
  const [emailEnabled, setEmailEnabled] = useState(defaultSubscription.email_enabled);
  const [eventTypes, setEventTypes] = useState<string[]>(defaultSubscription.event_types);
  const [message, setMessage] = useState<string | null>(null);
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [entitlementsLoaded, setEntitlementsLoaded] = useState(false);
  const [upgradeOpen, setUpgradeOpen] = useState(false);

  useEffect(() => {
    let active = true;
    getEntitlements()
      .then((result) => active && setEntitlements(result))
      .catch(() => active && setEntitlements(defaultEntitlements))
      .finally(() => active && setEntitlementsLoaded(true));
    getStrategySubscription(slug)
      .then((result) => {
        if (!active || !result.subscription) return;
        setFollowing(Boolean(result.subscription.isActive));
        setEmailEnabled(Boolean(result.subscription.emailEnabled));
        setEventTypes(result.subscription.eventTypes);
      })
      .catch((error) => {
        if (!active || (error instanceof ApiError && error.status === 401)) return;
        setMessage(error instanceof Error ? error.message : "Unable to load follow status.");
      })
      .finally(() => active && setLoading(false));
    return () => { active = false; };
  }, [slug]);

  async function saveSubscription() {
    if (!hasEntitlement(entitlements, "notification_digests")) {
      setUpgradeOpen(true);
      return;
    }
    setLoading(true);
    setMessage(null);
    try {
      const result = await updateStrategySubscription(slug, {
        email_enabled: emailEnabled,
        delivery_mode: "daily",
        event_types: eventTypes,
      });
      setFollowing(Boolean(result.subscription.isActive));
      setEmailEnabled(Boolean(result.subscription.emailEnabled));
      setEventTypes(result.subscription.eventTypes);
      setMessage("Daily alert settings saved.");
    } catch (error) {
      if (error instanceof ApiError && (error.status === 401 || error.status === 402)) setUpgradeOpen(true);
      else setMessage(error instanceof Error ? error.message : "Unable to update strategy alerts.");
    } finally {
      setLoading(false);
    }
  }

  async function toggleFollowing() {
    if (following) {
      setLoading(true);
      setMessage(null);
      try {
        await deleteStrategySubscription(slug);
        setFollowing(false);
        setEmailEnabled(false);
      } catch (error) {
        setMessage(error instanceof Error ? error.message : "Unable to unfollow this strategy.");
      } finally {
        setLoading(false);
      }
      return;
    }
    await saveSubscription();
  }

  function toggleEventType(eventType: string) {
    setEventTypes((current) => current.includes(eventType) ? current.filter((value) => value !== eventType) : [...current, eventType]);
  }

  if (panel) {
    const canManage = entitlementsLoaded && hasEntitlement(entitlements, "notification_digests");
    if (!entitlementsLoaded) return <p className="py-4 text-sm text-slate-500">Loading alert preferences...</p>;
    if (!canManage) return <UpgradePrompt title="Follow this strategy" body="Premium includes the complete model portfolio and daily strategy-update emails." compact />;
    return <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-medium text-slate-200">Email alerts</div>
          <div className="mt-1 text-xs text-slate-500">{entitlements.user?.email ?? "Account email"}</div>
        </div>
        <label className="relative inline-flex cursor-pointer items-center">
          <input type="checkbox" checked={emailEnabled} onChange={(event) => setEmailEnabled(event.target.checked)} className="peer sr-only" />
          <span className="h-6 w-11 rounded-full bg-slate-700 transition peer-checked:bg-emerald-400/70" />
          <span className="absolute left-1 h-4 w-4 rounded-full bg-white transition peer-checked:translate-x-5" />
        </label>
      </div>
      <div className="border-t border-white/10 pt-4">
        <div className="text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">Alert types</div>
        <div className="mt-3 grid gap-2">
          {eventOptions.map(([eventType, label]) => <label key={eventType} className="flex cursor-pointer items-center gap-2 text-sm text-slate-300">
            <input type="checkbox" checked={eventTypes.includes(eventType)} onChange={() => toggleEventType(eventType)} className="h-4 w-4 rounded border-white/20 bg-slate-950 text-emerald-400 focus:ring-emerald-300/30" />
            {label}
          </label>)}
        </div>
      </div>
      <p className="border-t border-white/10 pt-4 text-sm leading-6 text-slate-400">Updates are delivered once daily after Walnut ingests newly available filings and evaluates the strategy.</p>
      <button type="button" onClick={saveSubscription} disabled={loading || eventTypes.length === 0} className="w-full rounded-md bg-emerald-400/20 px-3 py-2.5 text-sm font-semibold text-emerald-50 ring-1 ring-emerald-300/35 hover:bg-emerald-400/30 disabled:cursor-wait disabled:opacity-60">{loading ? "Saving..." : following ? "Save alert settings" : "Follow strategy"}</button>
      {following ? <button type="button" onClick={toggleFollowing} disabled={loading} className="w-full text-sm font-semibold text-slate-400 hover:text-rose-200 disabled:opacity-60">Unfollow strategy</button> : null}
      {message ? <p className={`text-xs leading-5 ${message === "Daily alert settings saved." ? "text-emerald-200" : "text-rose-200"}`}>{message}</p> : null}
      {upgradeOpen ? <UpgradePrompt title="Follow this strategy" body="Premium includes the complete model portfolio and daily strategy-update emails." compact /> : null}
    </div>;
  }

  return <div className={compact ? "" : "w-full"}>
    <button type="button" onClick={toggleFollowing} disabled={loading} className={`${compact ? "" : "w-full"} rounded-md px-3 py-2 text-sm font-semibold disabled:cursor-wait disabled:opacity-60 ${following ? "border border-white/15 text-slate-200 hover:border-rose-300/50 hover:text-rose-100" : "bg-emerald-400/20 text-emerald-50 ring-1 ring-emerald-300/35 hover:bg-emerald-400/30"}`}>{loading ? "Loading..." : following ? "Following strategy" : "Follow strategy"}</button>
    {message ? <p className="mt-2 text-xs leading-5 text-rose-200">{message}</p> : null}
    {upgradeOpen ? <div className="mt-3"><UpgradePrompt title="Follow this strategy" body="Get daily position, exit, and rebalance updates with Premium." compact /></div> : null}
  </div>;
}
