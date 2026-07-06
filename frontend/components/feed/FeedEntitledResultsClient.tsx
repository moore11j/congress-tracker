"use client";

import { useEffect, useState } from "react";
import { FeedResultsClient } from "@/components/feed/FeedResultsClient";
import { SkeletonBlock } from "@/components/ui/LoadingSkeleton";
import { getEntitlements } from "@/lib/api";
import { defaultEntitlements, hasEntitlement, type Entitlements } from "@/lib/entitlements";
import { isInstitutionalFeedMode, type FeedMode } from "@/lib/feedModes";

type FeedParamKey =
  | "symbol"
  | "member"
  | "chamber"
  | "party"
  | "trade_type"
  | "role"
  | "ownership"
  | "recent_days"
  | "department"
  | "sort_by"
  | "sort_dir";

type FeedEntitledResultsClientProps = {
  feedMode: FeedMode;
  queryDebug: boolean;
  debugLifecycle: boolean;
  page: number;
  pageSize: 25 | 50 | 100;
  activeParams: Record<FeedParamKey, string>;
};

function InstitutionalFeedLockedPanel() {
  return (
    <section className="space-y-4">
      <div className="rounded-3xl border border-emerald-400/20 bg-emerald-500/[0.06] p-5 shadow-card">
        <div className="max-w-2xl">
          <div className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-200">Institutional Activity</div>
          <h2 className="mt-2 text-xl font-semibold text-white">Pro required</h2>
          <p className="mt-2 text-sm leading-6 text-slate-300">
            Institutional Activity shows material 13F filing updates using filing dates and reported quarterly holdings.
          </p>
          <a
            href="/pricing"
            className="mt-4 inline-flex h-10 items-center justify-center rounded-xl border border-emerald-300/40 bg-emerald-400/10 px-4 text-sm font-semibold text-emerald-100 transition hover:bg-emerald-400/20"
          >
            Upgrade to Pro
          </a>
        </div>
      </div>
    </section>
  );
}

function InstitutionalEntitlementSkeleton() {
  return (
    <section className="space-y-4" aria-live="polite" aria-busy="true">
      <div className="rounded-3xl border border-white/10 bg-white/[0.03] p-5">
        <SkeletonBlock className="h-4 w-44" />
        <SkeletonBlock className="mt-3 h-7 w-64" />
        <SkeletonBlock className="mt-3 h-4 w-full max-w-xl" />
      </div>
    </section>
  );
}

export function FeedEntitledResultsClient({
  feedMode,
  queryDebug,
  debugLifecycle,
  page,
  pageSize,
  activeParams,
}: FeedEntitledResultsClientProps) {
  const [entitlements, setEntitlements] = useState<Entitlements>(defaultEntitlements);
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let cancelled = false;
    getEntitlements(undefined, { source: "FeedEntitledResultsClient" })
      .then((nextEntitlements) => {
        if (!cancelled) setEntitlements(nextEntitlements);
      })
      .catch(() => {
        if (!cancelled) setEntitlements(defaultEntitlements);
      })
      .finally(() => {
        if (!cancelled) setLoaded(true);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const institutionalMode = isInstitutionalFeedMode(feedMode);
  const canViewInstitutionalFeed = hasEntitlement(entitlements, "institutional_feed");
  const canViewPremiumMetrics = hasEntitlement(entitlements, "premium_feed_metrics");

  if (institutionalMode && !loaded) return <InstitutionalEntitlementSkeleton />;
  if (institutionalMode && !canViewInstitutionalFeed) return <InstitutionalFeedLockedPanel />;

  return (
    <FeedResultsClient
      feedMode={feedMode}
      queryDebug={queryDebug}
      debugLifecycle={debugLifecycle}
      page={page}
      pageSize={pageSize}
      activeParams={activeParams}
      canViewPremiumMetrics={loaded ? canViewPremiumMetrics : false}
    />
  );
}
