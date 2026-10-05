"use client";

import Link from "next/link";
import { VisibleEvent } from "@/components/analytics/VisibleEvent";
import { trackEvent } from "@/lib/productAnalytics";
import type { ReactNode } from "react";

export function TickerDiscoveryLink({ ticker, href, destinationType, destinationId, children, compact = false }: { ticker: string; href: string; destinationType: string; destinationId?: string; children: ReactNode; compact?: boolean }) {
  const properties = { ticker, destination_type: destinationType, destination_id: destinationId ?? ticker };
  return <VisibleEvent name="ticker_related_content_viewed" properties={properties} className={compact ? "inline-flex" : "mt-3"}>
    <Link href={href} prefetch={false} onClick={(event) => {
      trackEvent("ticker_related_content_clicked", { ...properties, destination_page: href });
      if (href.startsWith("#") && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) {
        event.preventDefault();
        window.location.hash = href;
        window.dispatchEvent(new HashChangeEvent("hashchange"));
      }
    }} className={compact ? "inline-flex items-center whitespace-nowrap text-[11px] font-semibold text-emerald-200 underline-offset-4 hover:underline focus-visible:outline focus-visible:outline-emerald-300" : "inline-flex min-h-10 items-center text-sm font-semibold text-emerald-200 underline-offset-4 hover:underline focus-visible:outline focus-visible:outline-emerald-300"}>{children} <span aria-hidden="true" className="ml-1">→</span></Link>
  </VisibleEvent>;
}
