"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState, type ReactNode, type MouseEventHandler } from "react";
import { VisibleEvent } from "@/components/analytics/VisibleEvent";
import { trackEvent } from "@/lib/productAnalytics";
import { upgradePricingHref, upgradeReturnPath } from "@/lib/upgradeDestination";

type Gate = { feature: string; tier?: "Premium" | "Pro"; children: ReactNode; className?: string };

export function UpgradeImpression({ feature, tier = "Premium", children, className }: Gate) {
  return <VisibleEvent name="upgrade_prompt_viewed" properties={{ gated_feature: feature, target_plan: tier.toLowerCase() }} className={className}>{children}</VisibleEvent>;
}

export function UpgradeLink({ feature, tier = "Premium", children, className, compare = false, returnTo, onClick }: Gate & { compare?: boolean; returnTo?: string; onClick?: MouseEventHandler<HTMLAnchorElement> }) {
  const pathname = usePathname() || "/";
  const [context, setContext] = useState({ pathname, path: pathname });
  // Read search state after hydration; this component also works outside Suspense.
  useEffect(() => {
    const path = upgradeReturnPath(pathname, window.location.search, window.location.hash);
    setContext(current => current.pathname === pathname && current.path === path ? current : { pathname, path });
  });
  const destination = upgradePricingHref(tier, returnTo || (context.pathname === pathname ? context.path : pathname), compare);
  return <Link href={destination} prefetch={false} className={className} onClick={event => {
    trackEvent("upgrade_prompt_clicked", { gated_feature: feature, target_plan: tier.toLowerCase(), destination_page: "/pricing" });
    onClick?.(event);
  }}>{children}</Link>;
}
